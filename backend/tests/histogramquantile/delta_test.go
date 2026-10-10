// backend/tests/histogramquantile/delta_test.go
package histogramquantile_test

import (
	"math"
	"testing"
	"time"

	"sre-platform/backend/internal/histogramquantile"
)

func TestDelta_NormalCase(t *testing.T) {
	prev := []histogramquantile.Bucket{
		{UpperBound: 10, CumulativeCount: 0},
		{UpperBound: 25, CumulativeCount: 20},
		{UpperBound: 50, CumulativeCount: 60},
		{UpperBound: math.Inf(1), CumulativeCount: 60},
	}
	curr := []histogramquantile.Bucket{
		{UpperBound: 10, CumulativeCount: 0},
		{UpperBound: 25, CumulativeCount: 30},
		{UpperBound: 50, CumulativeCount: 90},
		{UpperBound: math.Inf(1), CumulativeCount: 90},
	}

	delta, reset, err := histogramquantile.Delta(prev, curr)
	if err != nil {
		t.Fatalf("Delta: %v", err)
	}
	if reset {
		t.Error("reset = true, want false (counts only increased)")
	}
	want := []uint64{0, 10, 30, 30}
	for i, b := range delta {
		if b.CumulativeCount != want[i] {
			t.Errorf("delta[%d].CumulativeCount = %d, want %d", i, b.CumulativeCount, want[i])
		}
		if b.UpperBound != curr[i].UpperBound {
			t.Errorf("delta[%d].UpperBound = %v, want %v", i, b.UpperBound, curr[i].UpperBound)
		}
	}
}

func TestDelta_CounterResetDetected(t *testing.T) {
	// Simulates a pod restart: curr's cumulative total is lower than prev's,
	// which is impossible for a counter that hasn't reset.
	prev := []histogramquantile.Bucket{
		{UpperBound: 10, CumulativeCount: 40},
		{UpperBound: math.Inf(1), CumulativeCount: 100},
	}
	curr := []histogramquantile.Bucket{
		{UpperBound: 10, CumulativeCount: 15},
		{UpperBound: math.Inf(1), CumulativeCount: 40},
	}

	delta, reset, err := histogramquantile.Delta(prev, curr)
	if err != nil {
		t.Fatalf("Delta: %v", err)
	}
	if !reset {
		t.Error("reset = false, want true (curr total < prev total)")
	}
	// On a reset there's no valid baseline, so the whole current cumulative
	// value is treated as this window's delta.
	for i, b := range delta {
		if b.CumulativeCount != curr[i].CumulativeCount {
			t.Errorf("delta[%d].CumulativeCount = %d, want %d (= curr, on reset)", i, b.CumulativeCount, curr[i].CumulativeCount)
		}
	}
}

func TestDelta_NilPrevTreatsCurrAsWholeWindow(t *testing.T) {
	curr := []histogramquantile.Bucket{
		{UpperBound: 10, CumulativeCount: 5},
		{UpperBound: math.Inf(1), CumulativeCount: 12},
	}
	delta, reset, err := histogramquantile.Delta(nil, curr)
	if err != nil {
		t.Fatalf("Delta: %v", err)
	}
	if reset {
		t.Error("reset = true, want false (no prior sample isn't a counter reset)")
	}
	for i, b := range delta {
		if b.CumulativeCount != curr[i].CumulativeCount {
			t.Errorf("delta[%d].CumulativeCount = %d, want %d", i, b.CumulativeCount, curr[i].CumulativeCount)
		}
	}
}

func TestDelta_MismatchedBucketBoundariesReturnsError(t *testing.T) {
	prev := []histogramquantile.Bucket{
		{UpperBound: 10, CumulativeCount: 5},
		{UpperBound: math.Inf(1), CumulativeCount: 5},
	}
	curr := []histogramquantile.Bucket{
		{UpperBound: 50, CumulativeCount: 5}, // different boundary set than prev
		{UpperBound: math.Inf(1), CumulativeCount: 5},
	}
	if _, _, err := histogramquantile.Delta(prev, curr); err == nil {
		t.Error("Delta with mismatched bucket boundaries = nil error, want an error")
	}
}

func TestRate_NormalCase(t *testing.T) {
	got, err := histogramquantile.Rate(150, 15*time.Second)
	if err != nil {
		t.Fatalf("Rate: %v", err)
	}
	if got != 10 {
		t.Errorf("Rate = %v, want 10", got)
	}
}

func TestRate_RejectsNonPositiveElapsed(t *testing.T) {
	for _, elapsed := range []time.Duration{0, -1 * time.Second} {
		if _, err := histogramquantile.Rate(10, elapsed); err == nil {
			t.Errorf("Rate(elapsed=%v) = nil error, want an error", elapsed)
		}
	}
}
