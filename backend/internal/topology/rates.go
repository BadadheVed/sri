package topology

import "time"

// EdgeRate is a per-second rate for an edge.
//
// Both rates come from Beyla's client-side
// http_client_request_duration_seconds counts: RequestsPerSecond is all
// calls, ErrorsPerSecond the calls answered with a 5xx status. Each call is
// counted once (on the client), so ErrorsPerSecond <= RequestsPerSecond.
type EdgeRate struct {
	RequestsPerSecond, ErrorsPerSecond float64
}

// PodCounters holds cumulative edge counters per Beyla pod (keyed by a
// stable pod identity, e.g. "namespace/name"). A pod that failed to scrape
// is simply absent.
type PodCounters map[string]map[EdgeKey]EdgeCounter

type podSample struct {
	counters map[EdgeKey]EdgeCounter
	ts       time.Time
}

// RateCalculator turns successive per-pod cumulative counter samples into
// per-edge rates. Deltas are computed per (pod, edge) against that pod's own
// previous sample and only then summed per edge, so a pod that misses a
// scrape contributes nothing for that tick instead of looking like a
// counter reset (and later a spike).
//
// A pod absent from a sample keeps its baseline. When it returns, its rate
// is computed from that retained baseline (over the real elapsed time) if
// the baseline is at most MaxGap old; otherwise the returning sample is
// treated as a first sample (no rate). Baselines older than MaxGap of pods
// that are still absent are dropped. MaxGap <= 0 means no limit.
//
// The zero value is ready to use; it is not safe for concurrent use.
type RateCalculator struct {
	MaxGap time.Duration
	pods   map[string]podSample
}

// NewRateCalculator returns a calculator whose MaxGap is 3 poll intervals.
func NewRateCalculator(interval time.Duration) *RateCalculator {
	return &RateCalculator{MaxGap: 3 * interval}
}

// Update records cur at time now and returns the summed per-edge rates.
// Within one pod, edges with no previous sample or whose counter went
// backwards (reset) contribute nothing, and edges absent from the pod's new
// sample are forgotten. If now is not after a pod's previous sample, that
// pod contributes nothing and keeps its baseline.
func (r *RateCalculator) Update(now time.Time, cur PodCounters) map[EdgeKey]EdgeRate {
	if r.pods == nil {
		r.pods = map[string]podSample{}
	}
	out := map[EdgeKey]EdgeRate{}
	for pod, counters := range cur {
		prev, had := r.pods[pod]
		if had {
			elapsed := now.Sub(prev.ts)
			if elapsed <= 0 {
				continue
			}
			if r.MaxGap <= 0 || elapsed <= r.MaxGap {
				dt := elapsed.Seconds()
				for k, c := range counters {
					p, ok := prev.counters[k]
					if !ok || c.Total < p.Total || c.Failed < p.Failed {
						continue
					}
					e := out[k]
					e.RequestsPerSecond += (c.Total - p.Total) / dt
					e.ErrorsPerSecond += (c.Failed - p.Failed) / dt
					out[k] = e
				}
			}
		}
		next := make(map[EdgeKey]EdgeCounter, len(counters))
		for k, c := range counters {
			next[k] = c
		}
		r.pods[pod] = podSample{counters: next, ts: now}
	}
	if r.MaxGap > 0 {
		for pod, s := range r.pods {
			if _, seen := cur[pod]; !seen && now.Sub(s.ts) > r.MaxGap {
				delete(r.pods, pod)
			}
		}
	}
	return out
}
