// backend/internal/beylascrape/edges.go
package beylascrape

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"sre-platform/backend/internal/topology"
)

// edgeScrapeWorkers bounds concurrent pod scrapes in ScrapeEdgeCounters.
const edgeScrapeWorkers = 8

// ScrapeEdgeCounters implements topology.CounterScraper: discover the Beyla
// pods, scrape each in parallel (at most edgeScrapeWorkers at once) and
// parse its client-side HTTP edge counters. Counters are returned per pod (keyed
// "namespace/name"), NOT summed: topology.RateCalculator differences each
// pod against its own previous sample, so a pod that misses a scrape cannot
// masquerade as a counter reset. A pod that fails to scrape or parse is
// logged and omitted. The returned time is the sample time for rate
// calculation: the oldest actual fetch time among the pods (a cached scrape
// can be up to CacheTTL old), or now if no pod contributed; only a discovery failure fails the call.
func (s *Source) ScrapeEdgeCounters(ctx context.Context) (topology.PodCounters, time.Time, error) {
	pods, err := s.discoverTargets(ctx)
	if err != nil {
		return nil, time.Time{}, err
	}

	var (
		mu     sync.Mutex
		oldest time.Time
		out    = topology.PodCounters{}
		wg     sync.WaitGroup
		sem    = make(chan struct{}, edgeScrapeWorkers)
	)
	for _, pod := range pods {
		wg.Add(1)
		sem <- struct{}{}
		go func(pod podTarget) {
			defer wg.Done()
			defer func() { <-sem }()
			text, at, err := s.fetch(ctx, pod)
			if err != nil {
				slog.Warn("beylascrape: scraping pod for edges failed, skipping", "pod", pod.key, "pod_ip", pod.ip, "error", err)
				return
			}
			counters, err := topology.ParseEdgeCounters(text)
			if err != nil {
				slog.Warn("beylascrape: parsing pod edge counters failed, skipping", "pod", pod.key, "pod_ip", pod.ip, "error", err)
				return
			}
			mu.Lock()
			out[pod.key] = counters
			if oldest.IsZero() || at.Before(oldest) {
				oldest = at
			}
			mu.Unlock()
		}(pod)
	}
	wg.Wait()
	if oldest.IsZero() {
		oldest = time.Now()
	}
	return out, oldest, nil
}
