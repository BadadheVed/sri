// backend/internal/topology/poller.go
package topology

import (
	"context"
	"log/slog"
	"time"
)

// CounterScraper returns the current cumulative edge counters per Beyla pod
// (e.g. beylascrape.Source). Pods that failed to scrape are omitted. The
// returned time is when the counters were actually fetched (a cached scrape
// may be older than the call), used as the rate sample time.
type CounterScraper interface {
	ScrapeEdgeCounters(ctx context.Context) (PodCounters, time.Time, error)
}

// RatePublisher receives each tick's rates (e.g. *Hub).
type RatePublisher interface {
	Publish(rates map[EdgeKey]EdgeRate)
}

// NoopScraper reports no edges; used when Beyla is disabled so the poller
// still publishes each tick and node-only snapshots keep streaming.
type NoopScraper struct{}

// ScrapeEdgeCounters implements CounterScraper.
func (NoopScraper) ScrapeEdgeCounters(context.Context) (PodCounters, time.Time, error) {
	return PodCounters{}, time.Now(), nil
}

// RunTopologyPoller ticks immediately and then every interval until ctx
// ends: scrape counters -> calc.Update -> pub.Publish. A scrape error is
// logged and that tick skipped. calc is used only from this goroutine.
func RunTopologyPoller(ctx context.Context, interval time.Duration, scraper CounterScraper, calc *RateCalculator, pub RatePublisher) {
	tick := func() {
		counters, at, err := scraper.ScrapeEdgeCounters(ctx)
		if err != nil {
			slog.Warn("topology poller: scrape failed, skipping tick", "error", err)
			return
		}
		pub.Publish(calc.Update(at, counters))
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	tick()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick()
		}
	}
}
