// backend/internal/histogramquantile/quantile.go
package histogramquantile

import (
	"fmt"
	"math"
	"sort"
)

// Bucket is one cumulative histogram bucket in the same shape Prometheus's
// text exposition format (and therefore Beyla's own /metrics endpoint)
// produces: CumulativeCount is the total number of observations with a
// value <= UpperBound, not the count that fell in this bucket alone.
type Bucket struct {
	UpperBound      float64
	CumulativeCount uint64
}

// Quantile estimates the value at quantile q (0 <= q <= 1) from a set of
// cumulative histogram buckets, via the same linear-interpolation-within-
// a-bucket algorithm PromQL's histogram_quantile() uses. buckets must
// include a final +Inf bucket whose CumulativeCount is the total
// observation count — Quantile returns an error if one isn't present.
// buckets need not be pre-sorted. A histogram with zero observations
// yields NaN, matching histogram_quantile()'s own convention for an empty
// input rather than a fabricated 0.
func Quantile(buckets []Bucket, q float64) (float64, error) {
	if q < 0 || q > 1 {
		return 0, fmt.Errorf("quantile %v out of range: must be between 0 and 1", q)
	}
	if len(buckets) == 0 {
		return 0, fmt.Errorf("no buckets given")
	}

	sorted := make([]Bucket, len(buckets))
	copy(sorted, buckets)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].UpperBound < sorted[j].UpperBound })

	if !math.IsInf(sorted[len(sorted)-1].UpperBound, 1) {
		return 0, fmt.Errorf("buckets must include a final +Inf bucket")
	}

	ensureMonotonic(sorted)

	total := sorted[len(sorted)-1].CumulativeCount
	if total == 0 {
		return math.NaN(), nil
	}

	rank := q * float64(total)

	var prevBound float64 // lower bound of the current bucket; 0 for the first (non-negative observations, e.g. latency)
	var prevCount uint64
	for i, b := range sorted {
		if float64(b.CumulativeCount) >= rank {
			if math.IsInf(b.UpperBound, 1) {
				// The target rank only falls beyond the last finite bucket —
				// there's nothing to interpolate against past +Inf, so report
				// the highest finite bound we have as the closest estimate.
				if i == 0 {
					return 0, nil
				}
				return sorted[i-1].UpperBound, nil
			}
			if b.CumulativeCount == prevCount {
				// No observations landed in this bucket; nothing to interpolate.
				return b.UpperBound, nil
			}
			fraction := (rank - float64(prevCount)) / float64(b.CumulativeCount-prevCount)
			return prevBound + fraction*(b.UpperBound-prevBound), nil
		}
		prevBound = b.UpperBound
		prevCount = b.CumulativeCount
	}

	// Unreachable: the +Inf bucket's count equals total, and rank <= total.
	return sorted[len(sorted)-1].UpperBound, nil
}

// ensureMonotonic clamps any cumulative count that dips below an earlier
// bucket's count up to that earlier value, in place. Real cumulative
// histograms are non-decreasing by definition, but scrape timing and eBPF
// counter races can produce a momentary dip — coercing it forward (rather
// than erroring or trusting the glitch) keeps Quantile stable without
// discarding the sample.
func ensureMonotonic(buckets []Bucket) {
	var max uint64
	for i := range buckets {
		if buckets[i].CumulativeCount < max {
			buckets[i].CumulativeCount = max
		} else {
			max = buckets[i].CumulativeCount
		}
	}
}
