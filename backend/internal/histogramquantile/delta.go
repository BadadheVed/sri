// backend/internal/histogramquantile/delta.go
package histogramquantile

import (
	"fmt"
	"time"
)

// Delta computes the incremental bucket counts observed between two polls
// of the same cumulative histogram series. prev may be nil, meaning there
// is no prior sample yet (e.g. the first poll of a newly seen series) — in
// that case the entire curr is treated as this window's delta and reset is
// false, since nothing has actually reset.
//
// If curr's total (its +Inf bucket) is lower than prev's, the underlying
// counter must have restarted (e.g. the pod behind it was replaced).
// There's no valid baseline to diff against in that case, so Delta reports
// reset=true and returns curr itself as the delta, on the same assumption
// Prometheus's rate() makes: the counter went from 0 to its current value
// sometime during this window.
//
// prev and curr must share the same bucket boundaries (same UpperBound in
// the same order) — Delta returns an error otherwise, since diffing
// mismatched histograms isn't meaningful.
func Delta(prev, curr []Bucket) (delta []Bucket, reset bool, err error) {
	if prev == nil {
		return append([]Bucket(nil), curr...), false, nil
	}
	if len(prev) != len(curr) {
		return nil, false, fmt.Errorf("bucket count mismatch: prev has %d, curr has %d", len(prev), len(curr))
	}
	for i := range prev {
		if prev[i].UpperBound != curr[i].UpperBound {
			return nil, false, fmt.Errorf("bucket boundary mismatch at index %d: prev=%v, curr=%v", i, prev[i].UpperBound, curr[i].UpperBound)
		}
	}

	if curr[len(curr)-1].CumulativeCount < prev[len(prev)-1].CumulativeCount {
		return append([]Bucket(nil), curr...), true, nil
	}

	delta = make([]Bucket, len(curr))
	for i := range curr {
		delta[i] = Bucket{
			UpperBound:      curr[i].UpperBound,
			CumulativeCount: curr[i].CumulativeCount - prev[i].CumulativeCount,
		}
	}
	return delta, false, nil
}

// Rate converts an observation count into a requests-per-second figure
// over the given elapsed time. elapsed must be positive.
func Rate(count uint64, elapsed time.Duration) (float64, error) {
	if elapsed <= 0 {
		return 0, fmt.Errorf("elapsed must be positive, got %v", elapsed)
	}
	return float64(count) / elapsed.Seconds(), nil
}
