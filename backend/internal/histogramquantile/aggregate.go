// backend/internal/histogramquantile/aggregate.go
package histogramquantile

import (
	"fmt"
	"time"
)

// Sample is one cumulative histogram observation for a labeled series,
// taken at a point in our own scrape wall-clock. Buckets must be given in
// ascending UpperBound order with a final +Inf bucket, the same contract
// Delta and Quantile already assume.
type Sample struct {
	Buckets []Bucket
	Time    time.Time
}

// Result is the aggregated output for one poll window.
type Result struct {
	// Quantiles maps each requested quantile (e.g. 0.5, 0.95, 0.99) to its
	// estimated value over this window's incremental observations.
	Quantiles map[float64]float64
	// RequestsPerSecond is the +Inf bucket's delta divided by the elapsed
	// time between prev and curr. It is 0 on the first-ever sample for a
	// series, where there is no elapsed time to divide by yet.
	RequestsPerSecond float64
	// Reset reports whether Delta detected the underlying counter
	// restarting mid-window (see Delta's doc comment).
	Reset bool
}

// Aggregate turns two successive polls of the same cumulative histogram
// series into one window's worth of latency quantiles and request rate —
// the single entry point tying Delta, Rate and Quantile together for a
// scrape loop.
//
// prev with a nil Buckets field (the Sample zero value) signals there is
// no prior observation for this series yet — the ordinary state for a
// newly discovered series, not a fault. curr is then treated as the whole
// window (matching Delta's own nil-prev contract) and RequestsPerSecond is
// reported as 0. On every later call, curr.Time must be strictly after
// prev.Time; unlike a counter reset — a real phenomenon Delta already
// models — an out-of-order or duplicate timestamp signals a bug in the
// caller's polling loop, so Aggregate rejects it with an error naming the
// offending field, rather than letting it silently produce a zero or
// negative rate.
func Aggregate(prev, curr Sample, quantiles []float64) (Result, error) {
	delta, reset, err := Delta(prev.Buckets, curr.Buckets)
	if err != nil {
		return Result{}, fmt.Errorf("aggregate: %w", err)
	}
	if len(delta) == 0 {
		return Result{}, fmt.Errorf("aggregate: curr sample has no buckets")
	}

	var rate float64
	if prev.Buckets != nil {
		if !curr.Time.After(prev.Time) {
			return Result{}, fmt.Errorf("aggregate: curr.Time (%v) must be strictly after prev.Time (%v)", curr.Time, prev.Time)
		}
		elapsed := curr.Time.Sub(prev.Time)
		rate, err = Rate(delta[len(delta)-1].CumulativeCount, elapsed)
		if err != nil {
			return Result{}, fmt.Errorf("aggregate: %w", err)
		}
	}

	qs := make(map[float64]float64, len(quantiles))
	for _, q := range quantiles {
		v, err := Quantile(delta, q)
		if err != nil {
			return Result{}, fmt.Errorf("aggregate: %w", err)
		}
		qs[q] = v
	}

	return Result{Quantiles: qs, RequestsPerSecond: rate, Reset: reset}, nil
}
