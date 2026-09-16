// backend/tests/histogramquantile/quantile_test.go
package histogramquantile_test

import (
	"math"
	"testing"

	"sre-platform/backend/internal/histogramquantile"
)

func TestQuantile_LinearInterpolationWithinBucket(t *testing.T) {
	b := []histogramquantile.Bucket{
		{UpperBound: 10, CumulativeCount: 0},
		{UpperBound: 25, CumulativeCount: 0},
		{UpperBound: 50, CumulativeCount: 80},
		{UpperBound: 100, CumulativeCount: 90},
		{UpperBound: math.Inf(1), CumulativeCount: 100},
	}
	got, err := histogramquantile.Quantile(b, 0.5)
	if err != nil {
		t.Fatalf("Quantile: %v", err)
	}
	want := 40.625
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("p50 = %v, want %v", got, want)
	}
}

func TestQuantile_ExactBucketBoundary(t *testing.T) {
	b := []histogramquantile.Bucket{
		{UpperBound: 10, CumulativeCount: 0},
		{UpperBound: 25, CumulativeCount: 0},
		{UpperBound: 50, CumulativeCount: 80},
		{UpperBound: 100, CumulativeCount: 90},
		{UpperBound: math.Inf(1), CumulativeCount: 100},
	}
	got, err := histogramquantile.Quantile(b, 0.9) // rank = 90, exactly the 100ms bucket's cumulative count
	if err != nil {
		t.Fatalf("Quantile: %v", err)
	}
	if math.Abs(got-100) > 1e-9 {
		t.Errorf("p90 = %v, want 100 (exact upper bound of the bucket that reaches rank)", got)
	}
}

func TestQuantile_RankFallsInInfBucket_ReturnsLastFiniteBound(t *testing.T) {
	b := []histogramquantile.Bucket{
		{UpperBound: 10, CumulativeCount: 0},
		{UpperBound: 25, CumulativeCount: 0},
		{UpperBound: 50, CumulativeCount: 80},
		{UpperBound: 100, CumulativeCount: 90},
		{UpperBound: math.Inf(1), CumulativeCount: 100},
	}
	got, err := histogramquantile.Quantile(b, 0.99) // rank = 99, only the +Inf bucket reaches it
	if err != nil {
		t.Fatalf("Quantile: %v", err)
	}
	if got != 100 {
		t.Errorf("p99 = %v, want 100 (last finite bucket bound, can't interpolate past +Inf)", got)
	}
}

func TestQuantile_EmptyHistogramReturnsNaN(t *testing.T) {
	b := []histogramquantile.Bucket{
		{UpperBound: 10, CumulativeCount: 0},
		{UpperBound: 25, CumulativeCount: 0},
		{UpperBound: math.Inf(1), CumulativeCount: 0},
	}
	got, err := histogramquantile.Quantile(b, 0.5)
	if err != nil {
		t.Fatalf("Quantile: %v", err)
	}
	if !math.IsNaN(got) {
		t.Errorf("p50 of an empty histogram = %v, want NaN", got)
	}
}

func TestQuantile_RejectsOutOfRangeQuantile(t *testing.T) {
	b := []histogramquantile.Bucket{
		{UpperBound: 10, CumulativeCount: 5},
		{UpperBound: math.Inf(1), CumulativeCount: 5},
	}
	for _, q := range []float64{-0.1, 1.1} {
		if _, err := histogramquantile.Quantile(b, q); err == nil {
			t.Errorf("Quantile(q=%v) = nil error, want an error (q must be in [0,1])", q)
		}
	}
}

func TestQuantile_RequiresInfBucket(t *testing.T) {
	b := []histogramquantile.Bucket{
		{UpperBound: 10, CumulativeCount: 5},
		{UpperBound: 25, CumulativeCount: 9},
	}
	if _, err := histogramquantile.Quantile(b, 0.5); err == nil {
		t.Error("Quantile with no +Inf bucket = nil error, want an error")
	}
}

func TestQuantile_CoercesNonMonotonicBuckets(t *testing.T) {
	// A data-quality glitch: the 50ms bucket's cumulative count (40) is
	// less than the 25ms bucket's (50) — impossible for a real cumulative
	// histogram, but eBPF/scrape races can produce it. Quantile must clamp
	// it up to 50 (the max seen so far) rather than trust the raw dip.
	b := []histogramquantile.Bucket{
		{UpperBound: 10, CumulativeCount: 0},
		{UpperBound: 25, CumulativeCount: 50},
		{UpperBound: 50, CumulativeCount: 40}, // glitch: should read >= 50
		{UpperBound: 100, CumulativeCount: 90},
		{UpperBound: math.Inf(1), CumulativeCount: 90},
	}
	got, err := histogramquantile.Quantile(b, 0.5) // rank = 45
	if err != nil {
		t.Fatalf("Quantile: %v", err)
	}
	want := 23.5 // computed against the coerced sequence [0,50,50,90,90], not the raw [0,50,40,90,90]
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("p50 = %v, want %v (expected non-monotonic dip to be coerced before interpolating)", got, want)
	}
}

func TestQuantile_SortsUnsortedBuckets(t *testing.T) {
	sorted := []histogramquantile.Bucket{
		{UpperBound: 10, CumulativeCount: 0},
		{UpperBound: 25, CumulativeCount: 0},
		{UpperBound: 50, CumulativeCount: 80},
		{UpperBound: 100, CumulativeCount: 90},
		{UpperBound: math.Inf(1), CumulativeCount: 100},
	}
	unsorted := []histogramquantile.Bucket{
		{UpperBound: 100, CumulativeCount: 90},
		{UpperBound: math.Inf(1), CumulativeCount: 100},
		{UpperBound: 10, CumulativeCount: 0},
		{UpperBound: 50, CumulativeCount: 80},
		{UpperBound: 25, CumulativeCount: 0},
	}
	want, err := histogramquantile.Quantile(sorted, 0.5)
	if err != nil {
		t.Fatalf("Quantile(sorted): %v", err)
	}
	got, err := histogramquantile.Quantile(unsorted, 0.5)
	if err != nil {
		t.Fatalf("Quantile(unsorted): %v", err)
	}
	if got != want {
		t.Errorf("Quantile(unsorted) = %v, want %v (same result as pre-sorted input)", got, want)
	}
}
