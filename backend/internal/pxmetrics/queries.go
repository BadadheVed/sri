// backend/internal/pxmetrics/queries.go
package pxmetrics

import (
	"fmt"
	"regexp"
	"time"
)

// k8sNameRE matches Kubernetes' DNS-1123 label format, which every valid
// namespace/pod name already satisfies — rejecting anything else before
// it's interpolated into a PxL script string is this package's
// injection-safety boundary, since these strings ultimately originate from
// an LLM tool call (see backend/internal/introspect's equivalent trust
// boundary for the existing 3 read-only tools).
var k8sNameRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

func validateK8sName(field, value string) error {
	if len(value) == 0 || len(value) > 253 || !k8sNameRE.MatchString(value) {
		return fmt.Errorf("invalid %s %q: must be a valid Kubernetes DNS-1123 label", field, value)
	}
	return nil
}

// BuildPodCPUScript returns a PxL script computing this pod's CPU usage
// (fraction of one core, can exceed 1.0 across multiple processes) in
// windowSeconds buckets over the last lookback. Adapted from Pixie's own
// official demo (pixie-io/pixie-demos/custom-k8s-metrics-demo/pxl/pods.pxl),
// filtered to exactly one pod (df.ctx['namespace'] + df.ctx['pod']) instead
// of the demo's Service-wide filter, since nothing in SAGE resolves a
// Service name from a pod name — and per-pod isolation is arguably more
// useful for single-replica incident diagnosis anyway.
func BuildPodCPUScript(namespace, name string, lookback time.Duration, windowSeconds int) (string, error) {
	if err := validateK8sName("namespace", namespace); err != nil {
		return "", err
	}
	if err := validateK8sName("name", name); err != nil {
		return "", err
	}
	return fmt.Sprintf(`import px

window_s = %d
window_ns = px.DurationNanos(window_s * 1000 * 1000 * 1000)

df = px.DataFrame(table='process_stats', start_time='-%ds')
df = df[df.ctx['namespace'] == '%s']
df = df[df.ctx['pod'] == '%s']
df.timestamp = px.bin(df.time_, window_ns)

df = df.groupby(['upid', 'timestamp']).agg(
    cpu_utime_ns_max=('cpu_utime_ns', px.max),
    cpu_utime_ns_min=('cpu_utime_ns', px.min),
    cpu_ktime_ns_max=('cpu_ktime_ns', px.max),
    cpu_ktime_ns_min=('cpu_ktime_ns', px.min),
)
df.cpu_utime_ns = df.cpu_utime_ns_max - df.cpu_utime_ns_min
df.cpu_ktime_ns = df.cpu_ktime_ns_max - df.cpu_ktime_ns_min

df = df.groupby('timestamp').agg(
    cpu_ktime_ns=('cpu_ktime_ns', px.sum),
    cpu_utime_ns=('cpu_utime_ns', px.sum),
)
df.cpu_cores = (df.cpu_ktime_ns + df.cpu_utime_ns) / window_ns
df.time_ = df.timestamp

px.display(df[['time_', 'cpu_cores']], 'cpu')
`, windowSeconds, int(lookback.Seconds()), namespace, name), nil
}

// BuildPodTrafficScript returns a PxL script computing HTTP traffic
// involving this pod (request rate, error rate, p50/p90/p99 latency) in
// windowSeconds buckets over the last lookback. Same demo-script lineage and
// per-pod filtering rationale as BuildPodCPUScript.
//
// KNOWN LIMITATION, deliberate: http_events contains both inbound (this pod
// as server) and outbound (this pod as client) traffic, distinguished by a
// trace_role column. This script does NOT filter on trace_role, because the
// correct enum value for "inbound only" was not confirmed during design
// (docs/superpowers/specs/2026-08-12-pixie-observability-design.md §7, risk
// 3) — baking in an unverified guess risks silently producing wrong data,
// which is worse than an honestly-unfiltered mix. Confirm the real value
// against a live Vizier or docs.px.dev's http_events table reference, then
// add `df = df[df.trace_role == <confirmed value>]` right after the two
// ctx filters below, as a follow-up once Task 2's manual verification step
// (running this script against a real Vizier) happens.
func BuildPodTrafficScript(namespace, name string, lookback time.Duration, windowSeconds int) (string, error) {
	if err := validateK8sName("namespace", namespace); err != nil {
		return "", err
	}
	if err := validateK8sName("name", name); err != nil {
		return "", err
	}
	return fmt.Sprintf(`import px

window_s = %d
window_ns = px.DurationNanos(window_s * 1000 * 1000 * 1000)

df = px.DataFrame(table='http_events', start_time='-%ds')
df = df[df.ctx['namespace'] == '%s']
df = df[df.ctx['pod'] == '%s']
df.timestamp = px.bin(df.time_, window_ns)
df.failure = df.resp_status >= 400

df = df.groupby('timestamp').agg(
    errors=('failure', px.sum),
    requests=('timestamp', px.count),
    quantiles=('latency', px.quantiles),
)
df.requests_per_s = df.requests / window_s
df.errors_per_s = df.errors / window_s
df.latency_p50 = px.DurationNanos(px.floor(px.pluck_float64(df.quantiles, 'p50')))
df.latency_p90 = px.DurationNanos(px.floor(px.pluck_float64(df.quantiles, 'p90')))
df.latency_p99 = px.DurationNanos(px.floor(px.pluck_float64(df.quantiles, 'p99')))
df.time_ = df.timestamp

px.display(df[['time_', 'requests_per_s', 'errors_per_s', 'latency_p50', 'latency_p90', 'latency_p99']], 'traffic')
`, windowSeconds, int(lookback.Seconds()), namespace, name), nil
}
