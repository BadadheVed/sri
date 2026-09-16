// backend/tests/histogramquantile/aggregate_test.go
package histogramquantile_test

import (
	"math"
	"strings"
	"testing"
	"time"

	"sre-platform/backend/internal/histogramquantile"
)

func TestAggregate_HappyPathComputesQuantilesAndRate(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	prev := histogramquantile.Sample{
		Buckets: []histogramquantile.Bucket{
			{UpperBound: 10, CumulativeCount: 0},
			{UpperBound: 25, CumulativeCount: 20},
			{UpperBound: 50, CumulativeCount: 60},
			{UpperBound: math.Inf(1), CumulativeCount: 60},
		},
		Time: base,
	}
	curr := histogramquantile.Sample{
		Buckets: []histogramquantile.Bucket{
			{UpperBound: 10, CumulativeCount: 0},
			{UpperBound: 25, CumulativeCount: 30},
			{UpperBound: 50, CumulativeCount: 90},
			{UpperBound: math.Inf(1), CumulativeCount: 90},
		},
		Time: base.Add(10 * time.Second),
	}

	result, err := histogramquantile.Aggregate(prev, curr, []float64{0.5, 0.9})
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	if result.Reset {
		t.Error("Reset = true, want false")
	}
	if result.RequestsPerSecond != 3 {
		t.Errorf("RequestsPerSecond = %v, want 3 (30 delta requests / 10s)", result.RequestsPerSecond)
	}
	wantQ := map[float64]float64{0.5: 31.25, 0.9: 46.25}
	for q, want := range wantQ {
		got, ok := result.Quantiles[q]
		if !ok {
			t.Fatalf("missing quantile %v in result", q)
		}
		if math.Abs(got-want) > 1e-9 {
			t.Errorf("Quantiles[%v] = %v, want %v", q, got, want)
		}
	}
}

func TestAggregate_ResetPropagatesFromDelta(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	prev := histogramquantile.Sample{
		Buckets: []histogramquantile.Bucket{
			{UpperBound: 10, CumulativeCount: 40},
			{UpperBound: math.Inf(1), CumulativeCount: 100},
		},
		Time: base,
	}
	curr := histogramquantile.Sample{
		Buckets: []histogramquantile.Bucket{
			{UpperBound: 10, CumulativeCount: 15},
			{UpperBound: math.Inf(1), CumulativeCount: 40},
		},
		Time: base.Add(10 * time.Second),
	}

	result, err := histogramquantile.Aggregate(prev, curr, []float64{0.5})
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	if !result.Reset {
		t.Error("Reset = false, want true (curr total < prev total)")
	}
	if result.RequestsPerSecond != 4 {
		t.Errorf("RequestsPerSecond = %v, want 4 (40 / 10s)", result.RequestsPerSecond)
	}
	want := 10.0 // rank=20 falls in the +Inf bucket; last finite bound is 10
	if result.Quantiles[0.5] != want {
		t.Errorf("Quantiles[0.5] = %v, want %v", result.Quantiles[0.5], want)
	}
}

func TestAggregate_FirstSampleHasZeroRate(t *testing.T) {
	curr := histogramquantile.Sample{
		Buckets: []histogramquantile.Bucket{
			{UpperBound: 10, CumulativeCount: 5},
			{UpperBound: math.Inf(1), CumulativeCount: 12},
		},
		Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	result, err := histogramquantile.Aggregate(histogramquantile.Sample{}, curr, []float64{0.5})
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	if result.Reset {
		t.Error("Reset = true, want false (no prior sample isn't a reset)")
	}
	if result.RequestsPerSecond != 0 {
		t.Errorf("RequestsPerSecond = %v, want 0 (no elapsed time on the first sample)", result.RequestsPerSecond)
	}
}

func TestAggregate_NonMonotonicTimingReturnsError(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	buckets := []histogramquantile.Bucket{
		{UpperBound: 10, CumulativeCount: 5},
		{UpperBound: math.Inf(1), CumulativeCount: 12},
	}
	prev := histogramquantile.Sample{Buckets: buckets, Time: base}
	for _, currTime := range []time.Time{base, base.Add(-1 * time.Second)} {
		curr := histogramquantile.Sample{Buckets: buckets, Time: currTime}
		_, err := histogramquantile.Aggregate(prev, curr, []float64{0.5})
		if err == nil {
			t.Fatalf("Aggregate(currTime=%v) = nil error, want an error (curr.Time must be strictly after prev.Time)", currTime)
		}
		if !strings.Contains(err.Error(), "curr.Time") {
			t.Errorf("Aggregate error = %q, want it to name curr.Time explicitly (not just Rate's generic elapsed message)", err.Error())
		}
	}
}

func TestAggregate_PropagatesQuantileError(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	buckets := []histogramquantile.Bucket{
		{UpperBound: 10, CumulativeCount: 5},
		{UpperBound: math.Inf(1), CumulativeCount: 12},
	}
	prev := histogramquantile.Sample{Buckets: buckets, Time: base}
	curr := histogramquantile.Sample{Buckets: buckets, Time: base.Add(time.Second)}
	if _, err := histogramquantile.Aggregate(prev, curr, []float64{0.5, 1.5}); err == nil {
		t.Error("Aggregate with an out-of-range quantile (1.5) = nil error, want an error")
	}
}

func TestAggregate_PropagatesDeltaError(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	prev := histogramquantile.Sample{
		Buckets: []histogramquantile.Bucket{
			{UpperBound: 10, CumulativeCount: 5},
			{UpperBound: math.Inf(1), CumulativeCount: 5},
		},
		Time: base,
	}
	curr := histogramquantile.Sample{
		Buckets: []histogramquantile.Bucket{
			{UpperBound: 50, CumulativeCount: 5}, // different boundary than prev
			{UpperBound: math.Inf(1), CumulativeCount: 5},
		},
		Time: base.Add(time.Second),
	}
	if _, err := histogramquantile.Aggregate(prev, curr, []float64{0.5}); err == nil {
		t.Error("Aggregate with mismatched bucket boundaries = nil error, want an error")
	}
}
