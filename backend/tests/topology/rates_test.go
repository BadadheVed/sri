// backend/tests/topology/rates_test.go
package topology_test

import (
	"math"
	"testing"
	"time"

	"sre-platform/backend/internal/topology"
)

var rk = topology.EdgeKey{ClientNS: "a", Client: "x", ServerNS: "a", Server: "y"}

// cur is a single-pod sample (pod "p") for edge rk.
func cur(total, failed float64) topology.PodCounters {
	return topology.PodCounters{"p": {rk: {Total: total, Failed: failed}}}
}

func approx(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestRate_FirstSampleNoRate(t *testing.T) {
	var r topology.RateCalculator
	if got := r.Update(time.Unix(100, 0), cur(10, 1)); len(got) != 0 {
		t.Fatalf("got %v", got)
	}
}

func TestRate_Steady(t *testing.T) {
	var r topology.RateCalculator
	t0 := time.Unix(100, 0)
	r.Update(t0, cur(10, 1))
	got := r.Update(t0.Add(10*time.Second), cur(60, 6))
	g, ok := got[rk]
	if !ok || !approx(g.RequestsPerSecond, 5) || !approx(g.ErrorsPerSecond, 0.5) {
		t.Fatalf("got %+v ok=%v", g, ok)
	}
}

func TestRate_Reset(t *testing.T) {
	var r topology.RateCalculator
	t0 := time.Unix(100, 0)
	r.Update(t0, cur(100, 10))
	if got := r.Update(t0.Add(10*time.Second), cur(5, 0)); len(got) != 0 {
		t.Fatalf("reset should yield no rate, got %v", got)
	}
	got := r.Update(t0.Add(20*time.Second), cur(25, 0))
	if g := got[rk]; !approx(g.RequestsPerSecond, 2) {
		t.Fatalf("after reset got %+v", g)
	}
}

func TestRate_VanishedKeyForgotten(t *testing.T) {
	var r topology.RateCalculator
	t0 := time.Unix(100, 0)
	r.Update(t0, cur(10, 0))
	r.Update(t0.Add(10*time.Second), topology.PodCounters{"p": {}})
	if got := r.Update(t0.Add(20*time.Second), cur(50, 0)); len(got) != 0 {
		t.Fatalf("forgotten key should have no rate, got %v", got)
	}
}

func TestRate_NonPositiveDt(t *testing.T) {
	var r topology.RateCalculator
	t0 := time.Unix(100, 0)
	r.Update(t0, cur(10, 0))
	if got := r.Update(t0, cur(20, 0)); len(got) != 0 {
		t.Fatalf("got %v", got)
	}
}

func TestRate_SumsRatesAcrossPods(t *testing.T) {
	var r topology.RateCalculator
	t0 := time.Unix(100, 0)
	r.Update(t0, topology.PodCounters{"p1": {rk: {Total: 10}}, "p2": {rk: {Total: 1000, Failed: 10}}})
	got := r.Update(t0.Add(10*time.Second), topology.PodCounters{"p1": {rk: {Total: 30}}, "p2": {rk: {Total: 1100, Failed: 20}}})
	if g := got[rk]; !approx(g.RequestsPerSecond, 12) || !approx(g.ErrorsPerSecond, 1) {
		t.Fatalf("got %+v, want rps=2+10, eps=0+1", g)
	}
}

// The scenario that motivated per-pod deltas: summing counters across pods
// before differencing made a pod that missed a scrape look like a counter
// reset, and its return look like a huge spike.
func TestRate_PodDropsForOneTickThenReturns_NoSpikeNoVanishedEdge(t *testing.T) {
	interval := 10 * time.Second
	r := topology.NewRateCalculator(interval)
	other := topology.EdgeKey{ClientNS: "a", Client: "q", ServerNS: "a", Server: "z"}
	t0 := time.Unix(100, 0)
	// Each pod serves rk at 1 rps; p1 also serves `other` at 1 rps. p3 has
	// a large cumulative count, so summing would spike badly.
	sample := func(n float64, pods ...string) topology.PodCounters {
		pc := topology.PodCounters{}
		for _, p := range pods {
			base := 0.0
			if p == "p3" {
				base = 1e6
			}
			pc[p] = map[topology.EdgeKey]topology.EdgeCounter{rk: {Total: base + n*10}}
			if p == "p1" {
				pc[p][other] = topology.EdgeCounter{Total: n * 10}
			}
		}
		return pc
	}
	r.Update(t0, sample(0, "p1", "p2", "p3"))
	got := r.Update(t0.Add(interval), sample(1, "p1", "p2", "p3"))
	if !approx(got[rk].RequestsPerSecond, 3) || !approx(got[other].RequestsPerSecond, 1) {
		t.Fatalf("tick1 = %v", got)
	}
	// p3 fails its scrape this tick.
	got = r.Update(t0.Add(2*interval), sample(2, "p1", "p2"))
	g, ok := got[rk]
	if !ok || !approx(g.RequestsPerSecond, 2) {
		t.Fatalf("tick2 (p3 missing): rk = %+v ok=%v, want present at 2 rps", g, ok)
	}
	if !approx(got[other].RequestsPerSecond, 1) {
		t.Fatalf("tick2: other edge = %v, want 1 rps", got[other])
	}
	// p3 returns: its rate is computed from its own retained baseline over
	// 2 intervals, so the total is back to 3 rps, not a spike.
	got = r.Update(t0.Add(3*interval), sample(3, "p1", "p2", "p3"))
	if g := got[rk]; !approx(g.RequestsPerSecond, 3) {
		t.Fatalf("tick3 (p3 back): rk = %+v, want 3 rps (no spike)", g)
	}
}

func TestRate_PodReturnsAfterMaxGap_TreatedAsFirstSample(t *testing.T) {
	interval := 10 * time.Second
	r := topology.NewRateCalculator(interval)
	t0 := time.Unix(100, 0)
	r.Update(t0, topology.PodCounters{"p1": {rk: {Total: 0}}, "p2": {rk: {Total: 0}}})
	for i := 1; i <= 3; i++ { // p2 absent for 3 ticks -> baseline 4 intervals old on return
		r.Update(t0.Add(time.Duration(i)*interval), topology.PodCounters{"p1": {rk: {Total: float64(i * 10)}}})
	}
	got := r.Update(t0.Add(4*interval), topology.PodCounters{"p1": {rk: {Total: 40}}, "p2": {rk: {Total: 999}}})
	if g := got[rk]; !approx(g.RequestsPerSecond, 1) {
		t.Fatalf("got %+v, want only p1's 1 rps (p2 baseline too old)", g)
	}
	got = r.Update(t0.Add(5*interval), topology.PodCounters{"p1": {rk: {Total: 50}}, "p2": {rk: {Total: 1009}}})
	if g := got[rk]; !approx(g.RequestsPerSecond, 2) {
		t.Fatalf("got %+v, want 2 rps once p2 has a fresh baseline", g)
	}
}

func TestRate_PodReturnsWithinMaxGap_UsesRetainedBaseline(t *testing.T) {
	interval := 10 * time.Second
	r := topology.NewRateCalculator(interval)
	t0 := time.Unix(100, 0)
	r.Update(t0, topology.PodCounters{"p": {rk: {Total: 0}}})
	r.Update(t0.Add(interval), topology.PodCounters{})
	r.Update(t0.Add(2*interval), topology.PodCounters{})
	// exactly 3 intervals since the baseline: still allowed.
	got := r.Update(t0.Add(3*interval), topology.PodCounters{"p": {rk: {Total: 60}}})
	if g := got[rk]; !approx(g.RequestsPerSecond, 2) {
		t.Fatalf("got %+v, want 60/30s = 2 rps", g)
	}
}

func TestRate_PerPodResetOnlyAffectsThatPod(t *testing.T) {
	var r topology.RateCalculator
	t0 := time.Unix(100, 0)
	r.Update(t0, topology.PodCounters{"p1": {rk: {Total: 100}}, "p2": {rk: {Total: 500}}})
	got := r.Update(t0.Add(10*time.Second), topology.PodCounters{"p1": {rk: {Total: 120}}, "p2": {rk: {Total: 3}}})
	if g := got[rk]; !approx(g.RequestsPerSecond, 2) {
		t.Fatalf("got %+v, want p1's 2 rps only (p2 reset)", g)
	}
}
