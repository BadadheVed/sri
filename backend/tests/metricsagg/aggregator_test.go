// backend/tests/metricsagg/aggregator_test.go
package metricsagg_test

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"sre-platform/backend/internal/histogramquantile"
	"sre-platform/backend/internal/metricsagg"
)

type fakePoll struct {
	samples map[metricsagg.SeriesKey]histogramquantile.Sample
	err     error
}

// fakeSource returns a canned poll for each successive call, in order;
// once the queue is exhausted it keeps repeating the last entry, so
// Run-based tests (which may tick more times than expected) don't panic.
type fakeSource struct {
	mu    sync.Mutex
	polls []fakePoll
	calls int
}

func newFakeSource(polls ...fakePoll) *fakeSource { return &fakeSource{polls: polls} }

func (f *fakeSource) Poll(ctx context.Context) (map[metricsagg.SeriesKey]histogramquantile.Sample, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	idx := f.calls
	if idx >= len(f.polls) {
		idx = len(f.polls) - 1
	}
	f.calls++
	return f.polls[idx].samples, f.polls[idx].err
}

func (f *fakeSource) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type publishCall struct {
	key    metricsagg.SeriesKey
	result histogramquantile.Result
}

// recordingSink records every Publish call in order; mutex-guarded so it's
// safe to read from the test goroutine while Aggregator.Run publishes
// from its own.
type recordingSink struct {
	mu    sync.Mutex
	calls []publishCall
}

func (s *recordingSink) Publish(key metricsagg.SeriesKey, result histogramquantile.Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, publishCall{key, result})
}

func (s *recordingSink) snapshot() []publishCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]publishCall(nil), s.calls...)
}

func TestAggregator_Tick_FirstPollPublishesZeroRateResult(t *testing.T) {
	key := metricsagg.SeriesKey{Namespace: "prod", Service: "checkout", Route: "/pay", Method: "POST"}
	curr := histogramquantile.Sample{
		Buckets: []histogramquantile.Bucket{
			{UpperBound: 10, CumulativeCount: 5},
			{UpperBound: math.Inf(1), CumulativeCount: 12},
		},
		Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	source := newFakeSource(fakePoll{samples: map[metricsagg.SeriesKey]histogramquantile.Sample{key: curr}})
	sink := &recordingSink{}
	agg := metricsagg.New(source, sink, []float64{0.5})

	agg.Tick(context.Background())

	calls := sink.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 Publish call, got %d", len(calls))
	}
	if calls[0].key != key {
		t.Errorf("Publish key = %+v, want %+v", calls[0].key, key)
	}
	if calls[0].result.RequestsPerSecond != 0 {
		t.Errorf("RequestsPerSecond = %v, want 0 on first sample", calls[0].result.RequestsPerSecond)
	}
	if calls[0].result.Reset {
		t.Error("Reset = true, want false on first sample")
	}
}

func TestAggregator_Tick_SecondPollComputesDeltaAgainstFirst(t *testing.T) {
	key := metricsagg.SeriesKey{Namespace: "prod", Service: "checkout", Route: "/pay", Method: "POST"}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	first := histogramquantile.Sample{
		Buckets: []histogramquantile.Bucket{
			{UpperBound: 10, CumulativeCount: 0}, {UpperBound: 25, CumulativeCount: 20},
			{UpperBound: 50, CumulativeCount: 60}, {UpperBound: math.Inf(1), CumulativeCount: 60},
		},
		Time: base,
	}
	second := histogramquantile.Sample{
		Buckets: []histogramquantile.Bucket{
			{UpperBound: 10, CumulativeCount: 0}, {UpperBound: 25, CumulativeCount: 30},
			{UpperBound: 50, CumulativeCount: 90}, {UpperBound: math.Inf(1), CumulativeCount: 90},
		},
		Time: base.Add(10 * time.Second),
	}
	source := newFakeSource(
		fakePoll{samples: map[metricsagg.SeriesKey]histogramquantile.Sample{key: first}},
		fakePoll{samples: map[metricsagg.SeriesKey]histogramquantile.Sample{key: second}},
	)
	sink := &recordingSink{}
	agg := metricsagg.New(source, sink, []float64{0.5, 0.9})

	agg.Tick(context.Background())
	agg.Tick(context.Background())

	calls := sink.snapshot()
	if len(calls) != 2 {
		t.Fatalf("expected 2 Publish calls, got %d", len(calls))
	}
	got := calls[1].result
	if got.RequestsPerSecond != 3 {
		t.Errorf("RequestsPerSecond = %v, want 3 (30 delta requests / 10s)", got.RequestsPerSecond)
	}
	wantQ := map[float64]float64{0.5: 31.25, 0.9: 46.25}
	for q, want := range wantQ {
		if math.Abs(got.Quantiles[q]-want) > 1e-9 {
			t.Errorf("Quantiles[%v] = %v, want %v", q, got.Quantiles[q], want)
		}
	}
}

func TestAggregator_Tick_PerSeriesErrorSkipsOnlyThatSeries(t *testing.T) {
	good := metricsagg.SeriesKey{Namespace: "prod", Service: "checkout", Route: "/pay", Method: "POST"}
	bad := metricsagg.SeriesKey{Namespace: "prod", Service: "checkout", Route: "/refund", Method: "POST"}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	mkSample := func(upper float64, count uint64, t time.Time) histogramquantile.Sample {
		return histogramquantile.Sample{
			Buckets: []histogramquantile.Bucket{{UpperBound: upper, CumulativeCount: count}, {UpperBound: math.Inf(1), CumulativeCount: count}},
			Time:    t,
		}
	}
	goodFirst, badFirst := mkSample(10, 5, base), mkSample(10, 5, base)
	goodSecond := mkSample(10, 8, base.Add(time.Second))
	// badSecond uses a different finite boundary (50 vs 10) than badFirst —
	// triggers Delta's boundary-mismatch error inside Aggregate.
	badSecond := mkSample(50, 8, base.Add(time.Second))

	source := newFakeSource(
		fakePoll{samples: map[metricsagg.SeriesKey]histogramquantile.Sample{good: goodFirst, bad: badFirst}},
		fakePoll{samples: map[metricsagg.SeriesKey]histogramquantile.Sample{good: goodSecond, bad: badSecond}},
	)
	sink := &recordingSink{}
	agg := metricsagg.New(source, sink, []float64{0.5})

	agg.Tick(context.Background())
	agg.Tick(context.Background())

	var goodCalls, badCalls int
	for _, c := range sink.snapshot() {
		switch c.key {
		case good:
			goodCalls++
		case bad:
			badCalls++
		}
	}
	if goodCalls != 2 {
		t.Errorf("good series published %d times, want 2", goodCalls)
	}
	if badCalls != 1 {
		t.Errorf("bad series published %d times, want 1 (tick 1 only; tick 2's boundary mismatch must be skipped, not crash the poll)", badCalls)
	}
}

func TestAggregator_Tick_EvictsPreviousSampleForSeriesMissingFromPoll(t *testing.T) {
	a := metricsagg.SeriesKey{Namespace: "prod", Service: "api", Route: "/a", Method: "GET"}
	b := metricsagg.SeriesKey{Namespace: "prod", Service: "api", Route: "/b", Method: "GET"}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	mk := func(count uint64, t time.Time) histogramquantile.Sample {
		return histogramquantile.Sample{
			Buckets: []histogramquantile.Bucket{{UpperBound: 10, CumulativeCount: count}, {UpperBound: math.Inf(1), CumulativeCount: count}},
			Time:    t,
		}
	}
	aSample1 := mk(1, base)
	// bSample1 has a HIGH count (100). If the Aggregator wrongly kept this
	// as b's tracked previous sample after b disappeared from poll 2,
	// poll 3's much lower count (5) would look like a counter reset.
	bSample1 := mk(100, base)
	aSample2 := mk(2, base.Add(time.Second))
	aSample3 := mk(3, base.Add(3*time.Second))
	// b reappears in poll 3 with a fresh, low counter (its pod was
	// recreated) — must be treated as a brand new series, not a reset.
	bSample3 := mk(5, base.Add(3*time.Second))

	source := newFakeSource(
		fakePoll{samples: map[metricsagg.SeriesKey]histogramquantile.Sample{a: aSample1, b: bSample1}},
		fakePoll{samples: map[metricsagg.SeriesKey]histogramquantile.Sample{a: aSample2}}, // b absent
		fakePoll{samples: map[metricsagg.SeriesKey]histogramquantile.Sample{a: aSample3, b: bSample3}},
	)
	sink := &recordingSink{}
	agg := metricsagg.New(source, sink, []float64{0.5})

	agg.Tick(context.Background())
	agg.Tick(context.Background())
	agg.Tick(context.Background())

	var bLast histogramquantile.Result
	var found bool
	for _, c := range sink.snapshot() {
		if c.key == b {
			bLast, found = c.result, true
		}
	}
	if !found {
		t.Fatal("expected b to be published again after reappearing in poll 3")
	}
	if bLast.Reset {
		t.Error("Reset = true, want false — b's stale high-count sample should have been evicted when b was absent from poll 2")
	}
	if bLast.RequestsPerSecond != 0 {
		t.Errorf("RequestsPerSecond = %v, want 0 — b should be treated as brand-new after eviction, not a continuation", bLast.RequestsPerSecond)
	}
}

func TestAggregator_Tick_PollErrorDoesNotPublishAndPreservesPriorState(t *testing.T) {
	key := metricsagg.SeriesKey{Namespace: "prod", Service: "api", Route: "/a", Method: "GET"}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mk := func(count uint64, t time.Time) histogramquantile.Sample {
		return histogramquantile.Sample{
			Buckets: []histogramquantile.Bucket{{UpperBound: 10, CumulativeCount: count}, {UpperBound: math.Inf(1), CumulativeCount: count}},
			Time:    t,
		}
	}
	first := mk(5, base)
	third := mk(9, base.Add(2*time.Second))

	source := newFakeSource(
		fakePoll{samples: map[metricsagg.SeriesKey]histogramquantile.Sample{key: first}},
		fakePoll{err: errors.New("beyla unreachable")},
		fakePoll{samples: map[metricsagg.SeriesKey]histogramquantile.Sample{key: third}},
	)
	sink := &recordingSink{}
	agg := metricsagg.New(source, sink, []float64{0.5})

	agg.Tick(context.Background()) // establishes prev = first
	agg.Tick(context.Background()) // Poll errors — must not publish, must not evict tracked state
	agg.Tick(context.Background()) // prev should still be `first`

	calls := sink.snapshot()
	if len(calls) != 2 {
		t.Fatalf("expected 2 Publish calls (tick 1 and tick 3; tick 2's Poll error must not publish), got %d", len(calls))
	}
	got := calls[1].result
	if got.RequestsPerSecond != 2 {
		t.Errorf("RequestsPerSecond = %v, want 2 (4 delta requests / 2s against tick 1's sample — proves tick 2's Poll error did not wipe tracked state)", got.RequestsPerSecond)
	}
}

func TestAggregator_Run_StopsWhenContextCancelledAndPollsOnEachTick(t *testing.T) {
	source := newFakeSource(fakePoll{samples: map[metricsagg.SeriesKey]histogramquantile.Sample{}})
	sink := &recordingSink{}
	agg := metricsagg.New(source, sink, []float64{0.5})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- agg.Run(ctx, 5*time.Millisecond) }()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned error %v, want nil on context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return within 1s of context cancellation")
	}
	if source.callCount() == 0 {
		t.Error("expected at least one Poll call before cancellation")
	}
}
