package topology_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"sre-platform/backend/internal/topology"
)

type seqScraper struct {
	mu    sync.Mutex
	steps []func() (topology.PodCounters, error)
	n     int
}

func (s *seqScraper) ScrapeEdgeCounters(ctx context.Context) (topology.PodCounters, time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.n
	s.n++
	if i >= len(s.steps) {
		i = len(s.steps) - 1
	}
	c, err := s.steps[i]()
	return c, time.Now(), err
}

type recordingPublisher struct {
	mu    sync.Mutex
	calls []map[topology.EdgeKey]topology.EdgeRate
}

func (r *recordingPublisher) Publish(rates map[topology.EdgeKey]topology.EdgeRate) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, rates)
}

func (r *recordingPublisher) snapshot() []map[topology.EdgeKey]topology.EdgeRate {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]map[topology.EdgeKey]topology.EdgeRate(nil), r.calls...)
}

func waitCalls(t *testing.T, p *recordingPublisher, n int) []map[topology.EdgeKey]topology.EdgeRate {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c := p.snapshot(); len(c) >= n {
			return c
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("got %d publishes, want >= %d", len(p.snapshot()), n)
	return nil
}

func TestRunTopologyPoller_ScrapesComputesRatesAndPublishes(t *testing.T) {
	k := topology.EdgeKey{ClientNS: "shop", Client: "web", ServerNS: "shop", Server: "api"}
	total := 0.0
	var mu sync.Mutex
	sc := &seqScraper{steps: []func() (topology.PodCounters, error){
		func() (topology.PodCounters, error) {
			mu.Lock()
			defer mu.Unlock()
			total += 100
			return topology.PodCounters{"pod": {k: {Total: total}}}, nil
		},
	}}
	pub := &recordingPublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		topology.RunTopologyPoller(ctx, 20*time.Millisecond, sc, &topology.RateCalculator{}, pub)
		close(done)
	}()
	calls := waitCalls(t, pub, 2)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("poller did not stop on ctx cancel")
	}
	if len(calls[0]) != 0 {
		t.Errorf("first tick has no baseline, want empty rates, got %v", calls[0])
	}
	if r, ok := calls[1][k]; !ok || r.RequestsPerSecond <= 0 {
		t.Errorf("second tick rates = %v, want positive rps for %v", calls[1], k)
	}
}

func TestRunTopologyPoller_ScrapeErrorSkipsTick(t *testing.T) {
	ok := func() (topology.PodCounters, error) {
		return topology.PodCounters{}, nil
	}
	sc := &seqScraper{steps: []func() (topology.PodCounters, error){
		func() (topology.PodCounters, error) { return nil, errors.New("boom") },
		ok,
	}}
	pub := &recordingPublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go topology.RunTopologyPoller(ctx, 10*time.Millisecond, sc, &topology.RateCalculator{}, pub)
	waitCalls(t, pub, 1)
	sc.mu.Lock()
	scrapes := sc.n
	sc.mu.Unlock()
	if got := len(pub.snapshot()); got >= scrapes {
		t.Fatalf("publishes (%d) should be fewer than scrapes (%d): error tick must be skipped", got, scrapes)
	}
}

func TestNoopScraper_NodesOnlyPublishesEmptyRates(t *testing.T) {
	pub := &recordingPublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go topology.RunTopologyPoller(ctx, 10*time.Millisecond, topology.NoopScraper{}, &topology.RateCalculator{}, pub)
	calls := waitCalls(t, pub, 2)
	for _, c := range calls {
		if c == nil || len(c) != 0 {
			t.Fatalf("want non-nil empty rates, got %v", c)
		}
	}
}

// timedScraper returns scripted counters with scripted sample (fetch) times.
type timedScraper struct {
	mu  sync.Mutex
	n   int
	k   topology.EdgeKey
	at0 time.Time
}

func (s *timedScraper) ScrapeEdgeCounters(context.Context) (topology.PodCounters, time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.n
	s.n++
	return topology.PodCounters{"pod": {s.k: {Total: float64(i * 100)}}}, s.at0.Add(time.Duration(i) * 10 * time.Second), nil
}

func TestRunTopologyPoller_UsesScraperSampleTimeForRates(t *testing.T) {
	k := topology.EdgeKey{ClientNS: "shop", Client: "web", ServerNS: "shop", Server: "api"}
	sc := &timedScraper{k: k, at0: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	pub := &recordingPublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go topology.RunTopologyPoller(ctx, 20*time.Millisecond, sc, &topology.RateCalculator{}, pub)
	calls := waitCalls(t, pub, 2)
	// 100 requests over the 10s between the reported fetch times, not over
	// the ~20ms wall-clock gap between ticks.
	if r := calls[1][k]; r.RequestsPerSecond != 10 {
		t.Fatalf("rps = %v, want 10 (computed over scraper-reported sample times)", r.RequestsPerSecond)
	}
}
