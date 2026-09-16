// backend/internal/metricsagg/aggregator.go
package metricsagg

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"sre-platform/backend/internal/histogramquantile"
)

// Aggregator polls a Source on a fixed interval, turns each series'
// successive samples into a histogramquantile.Result via Aggregate, and
// publishes each result to a Sink.
type Aggregator struct {
	source    Source
	sink      Sink
	quantiles []float64

	mu   sync.Mutex
	prev map[SeriesKey]histogramquantile.Sample
}

// New builds an Aggregator. quantiles is copied so the caller can't mutate
// it after construction.
func New(source Source, sink Sink, quantiles []float64) *Aggregator {
	return &Aggregator{
		source:    source,
		sink:      sink,
		quantiles: append([]float64(nil), quantiles...),
		prev:      make(map[SeriesKey]histogramquantile.Sample),
	}
}

// Run polls on interval until ctx is cancelled — same blocking-until-
// cancelled shape as k8swatch.Watcher.Run, driven by a time.Ticker instead
// of an informer. Unlike Watcher.Run, a failed poll (see Tick) is logged
// and does not stop the loop or return an error: a live metrics feed
// hiccuping is not worth taking the whole backend down for.
func (a *Aggregator) Run(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			a.Tick(ctx)
		}
	}
}

// Tick runs one poll: fetch this interval's samples, aggregate each series
// against its tracked previous sample, publish successes, and log-and-skip
// per-series Aggregate failures. The tracked previous-sample map is
// rebuilt from scratch each tick, containing only the series that
// successfully aggregated this time — a series absent from curr (its
// pod/route disappeared) or one that errored is simply not carried
// forward, so its state doesn't linger and can't cause a false
// reset-detection if that exact series reappears later with a fresh
// counter.
func (a *Aggregator) Tick(ctx context.Context) {
	curr, err := a.source.Poll(ctx)
	if err != nil {
		slog.Error("metricsagg: Source.Poll failed, skipping this interval", "error", err)
		return
	}

	a.mu.Lock()
	prev := a.prev
	a.mu.Unlock()

	next := make(map[SeriesKey]histogramquantile.Sample, len(curr))
	for key, sample := range curr {
		result, err := histogramquantile.Aggregate(prev[key], sample, a.quantiles)
		if err != nil {
			slog.Error("metricsagg: Aggregate failed for series, skipping",
				"namespace", key.Namespace, "service", key.Service, "route", key.Route, "method", key.Method, "error", err)
			continue
		}
		next[key] = sample
		a.sink.Publish(key, result)
	}

	a.mu.Lock()
	a.prev = next
	a.mu.Unlock()
}
