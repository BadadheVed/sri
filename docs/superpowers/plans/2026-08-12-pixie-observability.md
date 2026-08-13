# Pixie Observability Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `ai/`'s LLM investigation loop two new read-only tools — CPU usage and inbound HTTP traffic stats (RPS/errors/latency) for the pod under diagnosis — sourced from a self-hosted Pixie (px.dev) deployment, added to the existing `mcp-readonly-server`.

**Architecture:** A new Go package `backend/internal/pxmetrics` wraps Pixie's official `px.dev/pxapi` Go client, running two PxL (Pixie Query Language) scripts adapted from Pixie's own official demo scripts. Two new `mcp.AddTool` calls expose it on the existing `mcp-readonly-server`, gated behind a `PixieEnabled` settings flag. `ai/` needs no new code to discover the tools (MCP discovery is already generic) but gets one independent fix: its system prompt's hardcoded 3-tool list becomes dynamic.

**Tech Stack:** Go (`px.dev/pxapi`), the same MCP/Helm/settings patterns already established in this codebase.

## Global Constraints

- Implementers stage changes but never commit (`git add`, no `git commit`) — standing project rule.
- `cmd/*/main.go` stays thin — wiring only, no business logic (existing convention, verified in `cmd/mcp-readonly-server/main.go`).
- Env vars centralize in `backend/internal/settings/settings.go` — no other backend package calls `os.Getenv` directly.
- New Go interfaces are defined locally at the point of use, not by the implementing package (existing convention, e.g. `reconcile.PodRestarter`).
- Go tests live in `backend/tests/<package>/`, mirroring `backend/internal/<package>/`; Python tests in `ai/tests/`.
- No test hits a real external service — Go tests use `fake.NewSimpleClientset`/hand-built types; Python tests mock `httpx`/LLM clients. Live-Pixie verification is a documented manual step, not a CI gate (matching `introspect_test.go`'s existing precedent for `GetPodLogs`).
- **This plan's Task 1 (Pixie deployment prerequisite) is out-of-band infrastructure work — not a code task, and not something the executing agent can do (no cluster access).** It is documented here as a manual prerequisite the user completes separately; the coding tasks (2–8) can be written and unit-tested without it, per the user's explicit choice (`docs/superpowers/specs/2026-08-12-pixie-observability-design.md` §3, §7).
- PxL scripts in this plan are adapted from Pixie's own official demo script (`github.com/pixie-io/pixie-demos/blob/main/custom-k8s-metrics-demo/pxl/pods.pxl`, fetched and verified directly during design) — not invented.

---

## Prerequisite (out-of-band, not a plan task): self-hosted Pixie Cloud + Vizier

Before Task 3's live-verification step and Task 6's Helm `enabled=true` deploy can be exercised for real, a self-hosted Pixie Cloud + Vizier must exist and be reachable. This is genuine infrastructure work, not a `helm install` — see `docs/superpowers/specs/2026-08-12-pixie-observability-design.md` §3 for the full reasoning. Outline (re-verify each step against the live `pixie-io/pixie` repo at execution time — docs and release tags drift):

1. `git clone https://github.com/pixie-io/pixie.git`, check out the latest `release/cloud/vX.Y.Z` tag.
2. Deploy Pixie Cloud's dependencies (an Elastic Operator + Elasticsearch, via `kustomize build k8s/cloud_deps/... | kubectl apply -f -`) into a `plc` namespace.
3. Deploy Pixie Cloud itself (`kustomize build k8s/cloud/public/ | kubectl apply -f -`) — Kratos/Hydra auth, API services, UI backend.
4. TLS via `mkcert`, DNS via the repo's `dev_dns_updater` (or your own DNS pointed at the cloud-proxy Service's external IP).
5. Generate a deploy key via the Cloud UI, then `px deploy --dev_cloud_namespace plc` to install Vizier into the target cluster.
6. Output needed for Task 6: the reachable cloud/Vizier address and an API key.

---

## Task 1: `pxmetrics/queries.go` — PxL script builders

**Files:**
- Create: `backend/internal/pxmetrics/queries.go`
- Test: `backend/tests/pxmetrics/queries_test.go`

**Interfaces:**
- Produces: `BuildPodCPUScript(namespace, name string, lookback time.Duration, windowSeconds int) (string, error)`, `BuildPodTrafficScript(namespace, name string, lookback time.Duration, windowSeconds int) (string, error)` — exported so Task 2's `client_test.go` and Task 2's `client.go` can both call them; Task 2's `client.go` uses them to build the scripts it hands to `pxapi.VizierClient.ExecuteScript`.

This task is pure string-building — no `pxapi` dependency, no live connection, fully unit-testable now.

- [ ] **Step 1: Write the failing tests**

```go
// backend/tests/pxmetrics/queries_test.go
package pxmetrics_test

import (
	"strings"
	"testing"
	"time"

	"sre-platform/backend/internal/pxmetrics"
)

func TestBuildPodCPUScript_EmbedsNamespaceAndPodFilters(t *testing.T) {
	script, err := pxmetrics.BuildPodCPUScript("default", "web-1", 5*time.Minute, 60)
	if err != nil {
		t.Fatalf("BuildPodCPUScript: %v", err)
	}
	if !strings.Contains(script, "table='process_stats'") {
		t.Errorf("expected script to read process_stats, got:\n%s", script)
	}
	if !strings.Contains(script, `df.ctx['namespace'] == 'default'`) {
		t.Errorf("expected a namespace filter on 'default', got:\n%s", script)
	}
	if !strings.Contains(script, `df.ctx['pod'] == 'web-1'`) {
		t.Errorf("expected a pod filter on 'web-1', got:\n%s", script)
	}
	if !strings.Contains(script, "start_time='-300s'") {
		t.Errorf("expected the 5m lookback rendered as -300s, got:\n%s", script)
	}
}

func TestBuildPodTrafficScript_EmbedsNamespaceAndPodFilters(t *testing.T) {
	script, err := pxmetrics.BuildPodTrafficScript("default", "web-1", 5*time.Minute, 60)
	if err != nil {
		t.Fatalf("BuildPodTrafficScript: %v", err)
	}
	if !strings.Contains(script, "table='http_events'") {
		t.Errorf("expected script to read http_events, got:\n%s", script)
	}
	if !strings.Contains(script, `df.ctx['namespace'] == 'default'`) {
		t.Errorf("expected a namespace filter on 'default', got:\n%s", script)
	}
	if !strings.Contains(script, `df.ctx['pod'] == 'web-1'`) {
		t.Errorf("expected a pod filter on 'web-1', got:\n%s", script)
	}
}

func TestBuildPodCPUScript_RejectsInvalidNamespace(t *testing.T) {
	cases := []string{"", "Default", "default;import os", "default namespace", strings.Repeat("a", 254)}
	for _, ns := range cases {
		if _, err := pxmetrics.BuildPodCPUScript(ns, "web-1", time.Minute, 60); err == nil {
			t.Errorf("expected an error for invalid namespace %q, got none", ns)
		}
	}
}

func TestBuildPodCPUScript_RejectsInvalidName(t *testing.T) {
	cases := []string{"", "Web-1", "web_1", "web-1'; DROP TABLE"}
	for _, name := range cases {
		if _, err := pxmetrics.BuildPodCPUScript("default", name, time.Minute, 60); err == nil {
			t.Errorf("expected an error for invalid name %q, got none", name)
		}
	}
}

func TestBuildPodTrafficScript_RejectsInvalidNamespaceAndName(t *testing.T) {
	if _, err := pxmetrics.BuildPodTrafficScript("bad ns", "web-1", time.Minute, 60); err == nil {
		t.Error("expected an error for invalid namespace")
	}
	if _, err := pxmetrics.BuildPodTrafficScript("default", "bad name", time.Minute, 60); err == nil {
		t.Error("expected an error for invalid name")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd backend && go test ./tests/pxmetrics/... -v`
Expected: FAIL — `package pxmetrics` doesn't exist yet.

- [ ] **Step 3: Implement `queries.go`**

```go
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd backend && go test ./tests/pxmetrics/... -v`
Expected: PASS

- [ ] **Step 5: Stage**

```bash
git add backend/internal/pxmetrics/queries.go backend/tests/pxmetrics/queries_test.go
```

---

## Task 2: `pxmetrics/client.go` — connection + public API

**Files:**
- Create: `backend/internal/pxmetrics/client.go`
- Test: `backend/tests/pxmetrics/client_test.go`

**Interfaces:**
- Consumes: `BuildPodCPUScript`/`BuildPodTrafficScript` (Task 1).
- Produces: `pxmetrics.Config{ConnMode, VizierAddr, APIKey, ClusterID}`, `pxmetrics.NewClient(ctx, cfg) (*Client, error)`, `(*Client) GetPodCPUUsage(ctx, namespace, name string, lookback time.Duration) ([]CPUSample, error)`, `(*Client) GetPodTrafficStats(ctx, namespace, name string, lookback time.Duration) ([]TrafficSample, error)` — Task 4's `cmd/mcp-readonly-server/main.go` calls these.

**Important — this task has one step that cannot be fully pre-written.** `px.dev/pxapi`'s row-result type (`types.Record`/`types.Datum`) has a confirmed *interface* shape (`TableMuxer`/`TableRecordHandler`, verified against `pkg.go.dev/px.dev/pxapi`) but its exact concrete `Datum` field-access pattern was not visible in the rendered docs during design — Step 1 has you discover it for real via `go doc` before writing `HandleRecord`, rather than trusting a guess. This mirrors how Task 20 of the prior `ai-diagnosis-service` plan resolved a real chart version live instead of hardcoding one.

- [ ] **Step 1: Add the dependency and discover the real row-result API**

```bash
cd backend
go get px.dev/pxapi@v0.5.0
go build ./...   # confirms it resolves cleanly against this module's existing k8s.io/client-go, github.com/modelcontextprotocol/go-sdk versions — if this fails with a dependency conflict, STOP and report it: the plan's fallback is calling Vizier's gRPC API directly without this SDK, a bigger scope change worth re-planning around (see design spec §7, risk 1)
go doc px.dev/pxapi
go doc px.dev/pxapi/types
go doc px.dev/pxapi/types Record
go doc px.dev/pxapi/types Datum
```
Read the output of the last four commands carefully — you need: (a) the exact `pxapi.NewClient`/`WithCloudAddr`/`WithAPIKey` option function signatures (confirmed to exist, per design research, but confirm exact param types), (b) `types.Record`'s method for getting a named column's value, (c) `types.Datum`'s concrete type(s) and how to extract a `float64`/timestamp from one. Write down what you find — Step 3 depends on it.

- [ ] **Step 2: Write the failing tests**

```go
// backend/tests/pxmetrics/client_test.go
package pxmetrics_test

import (
	"context"
	"testing"

	"sre-platform/backend/internal/pxmetrics"
)

func TestNewClient_RejectsUnknownConnMode(t *testing.T) {
	_, err := pxmetrics.NewClient(context.Background(), pxmetrics.Config{ConnMode: "bogus", VizierAddr: "vizier:443"})
	if err == nil {
		t.Fatal("expected an error for an unknown ConnMode")
	}
}

func TestNewClient_CloudModeRequiresAPIKeyAndClusterID(t *testing.T) {
	_, err := pxmetrics.NewClient(context.Background(), pxmetrics.Config{ConnMode: "cloud", VizierAddr: "cloud.example.com:443"})
	if err == nil {
		t.Fatal("expected an error when cloud mode is missing APIKey/ClusterID")
	}
}
```
(Row-parsing tests belong in this same file once Step 1's real `types.Record`/`Datum` shape is known — add a test per parsed field, hand-constructing a real `types.Record` value the way `introspect_test.go` hand-constructs real `corev1.Event`/`corev1.Pod` values, asserting `CPUSample`/`TrafficSample` output. Write these using the REAL types discovered in Step 1, not invented field names.)

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd backend && go test ./tests/pxmetrics/... -v -run TestNewClient`
Expected: FAIL — `NewClient`/`Config` don't exist yet.

- [ ] **Step 4: Implement `client.go`**

```go
// backend/internal/pxmetrics/client.go
package pxmetrics

import (
	"context"
	"fmt"
	"time"

	"px.dev/pxapi"
)

// Config selects how this package connects to Pixie's Vizier. "cloud" is
// used for both hosted Community Cloud and self-hosted Pixie Cloud — from
// the client's perspective they're the same wire protocol, just a different
// VizierAddr (see docs/superpowers/specs/2026-08-12-pixie-observability-design.md §3).
type Config struct {
	ConnMode   string // "direct" | "cloud"
	VizierAddr string
	APIKey     string // required when ConnMode == "cloud"
	ClusterID  string // required when ConnMode == "cloud"
}

type Client struct {
	vizier *pxapi.VizierClient
}

// NewClient dials Vizier per cfg. queryTimeout bounds every subsequent
// ExecuteScript call — new relative to introspect.go's client-go calls,
// because a hung Pixie call would otherwise stall the whole ReAct loop.
const queryTimeout = 20 * time.Second

func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	var opts []pxapi.ClientOption
	switch cfg.ConnMode {
	case "direct":
		opts = []pxapi.ClientOption{pxapi.WithDirectAddr(cfg.VizierAddr), pxapi.WithDirectCredsInsecure()}
	case "cloud":
		if cfg.APIKey == "" || cfg.ClusterID == "" {
			return nil, fmt.Errorf("pxmetrics: cloud connMode requires both APIKey and ClusterID")
		}
		opts = []pxapi.ClientOption{pxapi.WithCloudAddr(cfg.VizierAddr), pxapi.WithAPIKey(cfg.APIKey)}
	default:
		return nil, fmt.Errorf("pxmetrics: unknown ConnMode %q: must be \"direct\" or \"cloud\"", cfg.ConnMode)
	}

	client, err := pxapi.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("pxmetrics: connecting to Pixie: %w", err)
	}
	vizier, err := client.NewVizierClient(ctx, cfg.ClusterID)
	if err != nil {
		return nil, fmt.Errorf("pxmetrics: creating Vizier client: %w", err)
	}
	return &Client{vizier: vizier}, nil
}

type CPUSample struct {
	WindowStart time.Time `json:"window_start"`
	CPUCores    float64   `json:"cpu_cores"`
}

type TrafficSample struct {
	WindowStart    time.Time `json:"window_start"`
	RequestsPerSec float64   `json:"requests_per_sec"`
	ErrorsPerSec   float64   `json:"errors_per_sec"`
	LatencyP50Ms   float64   `json:"latency_p50_ms"`
	LatencyP90Ms   float64   `json:"latency_p90_ms"`
	LatencyP99Ms   float64   `json:"latency_p99_ms"`
}

// maxSamples bounds how many window buckets a query can return — a large
// lookback shouldn't be able to blow up the LLM's context with an
// unbounded time series.
const maxSamples = 20

// columnRecordHandler adapts pxapi's per-record callback style into a
// simple "read named columns as strings" interface, using types.Datum's
// confirmed String() method rather than its concrete per-type struct
// fields (whose exact names weren't visible via pkg.go.dev during design —
// see Task 2 Step 1). This sidesteps that uncertainty entirely: whatever
// concrete Datum type backs a given column, String() is guaranteed to
// exist on the Datum interface.
type columnRecordHandler struct {
	onRecord func(col func(name string) string) error
	err      error
}

func (h *columnRecordHandler) HandleInit(ctx context.Context, metadata types.TableMetadata) error {
	return nil
}
func (h *columnRecordHandler) HandleRecord(ctx context.Context, record *types.Record) error {
	err := h.onRecord(func(name string) string {
		d := record.GetDatum(name)
		if d == nil {
			return ""
		}
		return d.String()
	})
	if err != nil {
		h.err = err
	}
	return err
}
func (h *columnRecordHandler) HandleDone(ctx context.Context) error { return nil }

type singleHandlerMuxer struct {
	handler pxapi.TableRecordHandler
}

func (m *singleHandlerMuxer) AcceptTable(ctx context.Context, metadata types.TableMetadata) (pxapi.TableRecordHandler, error) {
	return m.handler, nil
}

// parseTimeDatum handles both string encodings observed for Pixie TIME64NS
// columns across client versions/docs: a raw integer count of nanoseconds
// since the Unix epoch (the more common convention), or an RFC3339-ish
// timestamp string. Tries integer first. VERIFY against a real Vizier
// response (Task 2 Step 4, item 3 below) and simplify to whichever this
// deployment's pxapi version actually produces.
func parseTimeDatum(s string) (time.Time, error) {
	if nanos, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(0, nanos).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("pxmetrics: unrecognized time datum format: %q", s)
}

// runCollectingScript executes script against Vizier, calling onRecord once
// per result row (bounded by maxSamples, checked by the caller) until the
// query completes.
func (c *Client) runCollectingScript(ctx context.Context, script string, onRecord func(col func(name string) string) error) error {
	handler := &columnRecordHandler{onRecord: onRecord}
	muxer := &singleHandlerMuxer{handler: handler}
	results, err := c.vizier.ExecuteScript(ctx, script, muxer)
	if err != nil {
		return fmt.Errorf("pxmetrics: ExecuteScript: %w", err)
	}
	if err := results.Stream(); err != nil {
		results.Close()
		return fmt.Errorf("pxmetrics: streaming results: %w", err)
	}
	results.Close()
	return handler.err
}

// GetPodCPUUsage's live behavior (does ExecuteScript actually return the
// time_/cpu_cores columns BuildPodCPUScript's px.display names, does
// parseTimeDatum's format guess hold) is not verifiable without a real
// Vizier — see the Prerequisite section. Treat running this against a real
// Vizier as a required manual step before trusting it in production, same
// precedent introspect_test.go already sets for GetPodLogs.
func (c *Client) GetPodCPUUsage(ctx context.Context, namespace, name string, lookback time.Duration) ([]CPUSample, error) {
	script, err := BuildPodCPUScript(namespace, name, lookback, 60)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	var samples []CPUSample
	err = c.runCollectingScript(ctx, script, func(col func(string) string) error {
		if len(samples) >= maxSamples {
			return nil
		}
		ts, err := parseTimeDatum(col("time_"))
		if err != nil {
			return err
		}
		cpu, err := strconv.ParseFloat(col("cpu_cores"), 64)
		if err != nil {
			return fmt.Errorf("pxmetrics: parsing cpu_cores: %w", err)
		}
		samples = append(samples, CPUSample{WindowStart: ts, CPUCores: cpu})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return samples, nil
}

// GetPodTrafficStats — same live-verification caveat as GetPodCPUUsage.
func (c *Client) GetPodTrafficStats(ctx context.Context, namespace, name string, lookback time.Duration) ([]TrafficSample, error) {
	script, err := BuildPodTrafficScript(namespace, name, lookback, 60)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	parseMs := func(col func(string) string, name string) (float64, error) {
		// latency_p50/p90/p99 are px.DurationNanos(...) values — stringify
		// as an integer nanosecond count per the same convention as
		// parseTimeDatum; convert to milliseconds for the output struct.
		ns, err := strconv.ParseInt(col(name), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("pxmetrics: parsing %s: %w", name, err)
		}
		return float64(ns) / 1e6, nil
	}

	var samples []TrafficSample
	err = c.runCollectingScript(ctx, script, func(col func(string) string) error {
		if len(samples) >= maxSamples {
			return nil
		}
		ts, err := parseTimeDatum(col("time_"))
		if err != nil {
			return err
		}
		rps, err := strconv.ParseFloat(col("requests_per_s"), 64)
		if err != nil {
			return fmt.Errorf("pxmetrics: parsing requests_per_s: %w", err)
		}
		eps, err := strconv.ParseFloat(col("errors_per_s"), 64)
		if err != nil {
			return fmt.Errorf("pxmetrics: parsing errors_per_s: %w", err)
		}
		p50, err := parseMs(col, "latency_p50")
		if err != nil {
			return err
		}
		p90, err := parseMs(col, "latency_p90")
		if err != nil {
			return err
		}
		p99, err := parseMs(col, "latency_p99")
		if err != nil {
			return err
		}
		samples = append(samples, TrafficSample{
			WindowStart: ts, RequestsPerSec: rps, ErrorsPerSec: eps,
			LatencyP50Ms: p50, LatencyP90Ms: p90, LatencyP99Ms: p99,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return samples, nil
}
```

Add `"strconv"` and `"px.dev/pxapi/types"` to the import block.

**Step 4 continues** — the implementer must now:
1. Confirm `types.Record.GetDatum(name string) types.Datum` and `types.Datum.String() string` are real, exported, and match this signature (per Step 1's `go doc` output) — this is the one piece of the code above resting on a design-time claim rather than a live check. If the real signature differs (e.g. `GetDatum` returns `(types.Datum, bool)`, or the method is named differently), adjust `columnRecordHandler.HandleRecord` to match — the rest of this task's code (parsing, script building, `Config`/`NewClient`) is independent of this detail and shouldn't need to change.
2. `pxapi.TableMuxer`/`pxapi.TableRecordHandler`/`pxapi.VizierClient.ExecuteScript`/`ScriptResults.Stream`/`.Close` — confirm these match Step 1's `go doc px.dev/pxapi` output; this shape was confirmed during design against `pkg.go.dev/px.dev/pxapi`'s rendered docs, so it's the most reliable part of this task, but a live `go doc` check costs one command and catches any drift since that page was last rendered.
3. This task's live behavior (does `ExecuteScript` actually return the columns these scripts `px.display`, does `parseTimeDatum`'s format guess hold, is `latency_p50` etc. really an integer-nanosecond string) is **not verifiable without a real Vizier** — the code comments above already flag this; treat "run both scripts against the real Vizier from the Prerequisite section once it exists, and fix `parseTimeDatum`/`parseMs` if the real format differs" as a required manual step before this task is considered fully done — not blocking for staging the code.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd backend && go test ./tests/pxmetrics/... -v`
Expected: PASS (the `TestNewClient_*` tests plus whatever row-parsing tests were added in Step 2 using the real discovered types).

- [ ] **Step 6: Stage**

```bash
git add backend/internal/pxmetrics/client.go backend/tests/pxmetrics/client_test.go backend/go.mod backend/go.sum
```

---

## Task 3: `settings.go` — Pixie fields + conditional validation

**Files:**
- Modify: `backend/internal/settings/settings.go`
- Modify: `backend/tests/settings/settings_test.go`

**Interfaces:**
- Produces: `Settings.PixieEnabled bool`, `Settings.PixieConnMode string`, `Settings.PixieVizierAddr string`, `Settings.PixieAPIKey string`, `Settings.PixieClusterID string` — Task 4's `cmd/mcp-readonly-server/main.go` reads these to construct `pxmetrics.Config`.

This is the first **conditionally**-required field group in this file — every existing field is unconditionally required. Justified because Pixie is optional/off-by-default, unlike Slack/MCP-execute which are core to every install.

- [ ] **Step 1: Write the failing tests**

```go
// add to backend/tests/settings/settings_test.go

func TestSettings_Validate_PixieDisabledRequiresNothingExtra(t *testing.T) {
	s := validSettings()
	s.PixieEnabled = false
	// deliberately leave every Pixie field empty
	if err := s.Validate(); err != nil {
		t.Fatalf("expected no error when Pixie is disabled, got: %v", err)
	}
}

func TestSettings_Validate_PixieEnabledRequiresConnModeAndVizierAddr(t *testing.T) {
	s := validSettings()
	s.PixieEnabled = true
	s.PixieConnMode = ""
	s.PixieVizierAddr = ""

	err := s.Validate()
	if err == nil {
		t.Fatal("expected an error")
	}
	if !contains(err.Error(), "PIXIE_CONN_MODE") || !contains(err.Error(), "PIXIE_VIZIER_ADDR") {
		t.Errorf("expected error to name both missing fields, got: %v", err)
	}
}

func TestSettings_Validate_PixieCloudModeRequiresAPIKeyAndClusterID(t *testing.T) {
	s := validSettings()
	s.PixieEnabled = true
	s.PixieConnMode = "cloud"
	s.PixieVizierAddr = "cloud.example.com:443"
	s.PixieAPIKey = ""
	s.PixieClusterID = ""

	err := s.Validate()
	if err == nil {
		t.Fatal("expected an error")
	}
	if !contains(err.Error(), "PIXIE_API_KEY") || !contains(err.Error(), "PIXIE_CLUSTER_ID") {
		t.Errorf("expected error to name both missing fields, got: %v", err)
	}
}

func TestSettings_Validate_PixieDirectModeDoesNotRequireAPIKeyOrClusterID(t *testing.T) {
	s := validSettings()
	s.PixieEnabled = true
	s.PixieConnMode = "direct"
	s.PixieVizierAddr = "vizier.pl.svc:59300"
	s.PixieAPIKey = ""
	s.PixieClusterID = ""

	if err := s.Validate(); err != nil {
		t.Fatalf("expected no error for direct mode without APIKey/ClusterID, got: %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd backend && go test ./tests/settings/... -v -run TestSettings_Validate_Pixie`
Expected: FAIL — `Settings` has no `Pixie*` fields yet.

- [ ] **Step 3: Implement**

In `backend/internal/settings/settings.go`, add fields to the `Settings` struct (after `DiagnosisCallbackToken`):

```go
	PixieEnabled    bool
	PixieConnMode   string
	PixieVizierAddr string
	PixieAPIKey     string
	PixieClusterID  string
```

In `Load()`, add:

```go
		PixieEnabled:    getenv("PIXIE_ENABLED", "false") == "true",
		PixieConnMode:   getenv("PIXIE_CONN_MODE", ""),
		PixieVizierAddr: getenv("PIXIE_VIZIER_ADDR", ""),
		PixieAPIKey:     getenv("PIXIE_API_KEY", ""),
		PixieClusterID:  getenv("PIXIE_CLUSTER_ID", ""),
```

In `Validate()`, add before the final `if len(missing) > 0` check:

```go
	if s.PixieEnabled {
		if s.PixieConnMode != "direct" && s.PixieConnMode != "cloud" {
			missing = append(missing, "PIXIE_CONN_MODE")
		}
		if s.PixieVizierAddr == "" {
			missing = append(missing, "PIXIE_VIZIER_ADDR")
		}
		if s.PixieConnMode == "cloud" {
			if s.PixieAPIKey == "" {
				missing = append(missing, "PIXIE_API_KEY")
			}
			if s.PixieClusterID == "" {
				missing = append(missing, "PIXIE_CLUSTER_ID")
			}
		}
	}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd backend && go test ./tests/settings/... -v`
Expected: PASS (all existing settings tests plus the 4 new ones).

- [ ] **Step 5: Stage**

```bash
git add backend/internal/settings/settings.go backend/tests/settings/settings_test.go
```

---

## Task 4: Wire `pxmetrics` into `cmd/mcp-readonly-server/main.go`

**Files:**
- Modify: `backend/cmd/mcp-readonly-server/main.go`

**Interfaces:**
- Consumes: `pxmetrics.NewClient`/`Config` (Task 2), `Settings.Pixie*` fields (Task 3).
- Produces: two new MCP tools, `get_pod_resource_usage` and `get_pod_traffic_stats`, discoverable by `ai/ai/mcp_client.py`'s existing generic tool discovery — no `ai/` code change needed for this task alone.

No dedicated test file — matches this file's existing precedent (the 3 existing tools' logic is unit-tested in `introspect`, not `main.go`; same here, logic is unit-tested in `pxmetrics`, Tasks 1–2).

- [ ] **Step 1: Add the input/output types and tool functions**

In `backend/cmd/mcp-readonly-server/main.go`, add after the existing `DescribePodOutput` type:

```go
type GetPodResourceUsageInput struct {
	Namespace       string `json:"namespace" jsonschema:"the pod's namespace"`
	Name            string `json:"name" jsonschema:"the pod's name"`
	LookbackSeconds int64  `json:"lookback_seconds" jsonschema:"how far back to look, in seconds (e.g. 300)"`
}
type GetPodResourceUsageOutput struct {
	Samples []pxmetrics.CPUSample `json:"samples"`
}

type GetPodTrafficStatsInput struct {
	Namespace       string `json:"namespace" jsonschema:"the pod's namespace"`
	Name            string `json:"name" jsonschema:"the pod's name"`
	LookbackSeconds int64  `json:"lookback_seconds" jsonschema:"how far back to look, in seconds (e.g. 300)"`
}
type GetPodTrafficStatsOutput struct {
	Samples []pxmetrics.TrafficSample `json:"samples"`
}
```

Add `"sre-platform/backend/internal/pxmetrics"` and `"time"` to the import block.

- [ ] **Step 2: Construct the Pixie client (when enabled) and add the tools**

In `main()`, after the existing `clientset` construction and before the `getPodLogs`/`getPodEvents`/`describePod` closures, add:

```go
	var pxClient *pxmetrics.Client
	if s.PixieEnabled {
		pxClient, err = pxmetrics.NewClient(context.Background(), pxmetrics.Config{
			ConnMode: s.PixieConnMode, VizierAddr: s.PixieVizierAddr,
			APIKey: s.PixieAPIKey, ClusterID: s.PixieClusterID,
		})
		if err != nil {
			slog.Error("pxmetrics.NewClient failed", "error", err)
			os.Exit(1)
		}
	}
```

After the existing `describePod` closure, add:

```go
	getPodResourceUsage := func(ctx context.Context, req *mcp.CallToolRequest, input GetPodResourceUsageInput) (*mcp.CallToolResult, GetPodResourceUsageOutput, error) {
		samples, err := pxClient.GetPodCPUUsage(ctx, input.Namespace, input.Name, time.Duration(input.LookbackSeconds)*time.Second)
		if err != nil {
			slog.Error("get_pod_resource_usage failed", "namespace", input.Namespace, "name", input.Name, "error", err)
			return nil, GetPodResourceUsageOutput{}, err
		}
		return nil, GetPodResourceUsageOutput{Samples: samples}, nil
	}
	getPodTrafficStats := func(ctx context.Context, req *mcp.CallToolRequest, input GetPodTrafficStatsInput) (*mcp.CallToolResult, GetPodTrafficStatsOutput, error) {
		samples, err := pxClient.GetPodTrafficStats(ctx, input.Namespace, input.Name, time.Duration(input.LookbackSeconds)*time.Second)
		if err != nil {
			slog.Error("get_pod_traffic_stats failed", "namespace", input.Namespace, "name", input.Name, "error", err)
			return nil, GetPodTrafficStatsOutput{}, err
		}
		return nil, GetPodTrafficStatsOutput{Samples: samples}, nil
	}
```

After the existing three `mcp.AddTool` calls, add:

```go
	if s.PixieEnabled {
		mcp.AddTool(server, &mcp.Tool{Name: "get_pod_resource_usage", Description: "Returns recent windowed CPU usage (fraction of one core) for a pod, via eBPF-collected metrics (Pixie). Read-only."}, getPodResourceUsage)
		mcp.AddTool(server, &mcp.Tool{Name: "get_pod_traffic_stats", Description: "Returns recent windowed inbound HTTP request rate, error rate, and latency percentiles for a pod, via eBPF (Pixie). Read-only."}, getPodTrafficStats)
	}
```

(`getPodResourceUsage`/`getPodTrafficStats` reference `pxClient`, which is `nil` when `!s.PixieEnabled` — but they're never registered as tools in that case, so they're never called; this matches the existing conditional pattern already used for the two `mcp.AddTool` calls themselves.)

- [ ] **Step 3: Verify it builds**

Run: `cd backend && go build ./... && go vet ./...`
Expected: clean build.

- [ ] **Step 4: Stage**

```bash
git add backend/cmd/mcp-readonly-server/main.go
```

---

## Task 5: Helm plumbing

**Files:**
- Modify: `helm/values.yaml`, `helm/templates/configmap.yaml`, `helm/templates/secret.yaml`
- Create: `helm/README.md`

**Interfaces:** none new — consumes `Settings.Pixie*`'s env var names from Task 3 (`PIXIE_ENABLED`, `PIXIE_CONN_MODE`, `PIXIE_VIZIER_ADDR`, `PIXIE_API_KEY`, `PIXIE_CLUSTER_ID`).

No new Deployment/Service/RBAC template — `deployment-mcp-readonly.yaml` already does `envFrom: configMapRef + secretRef`, so new keys flow through automatically, and `pxmetrics` makes zero K8s API calls (no new RBAC needed).

- [ ] **Step 1: Add the `pxMetrics` values block**

In `helm/values.yaml`, add after the existing `llm:` block:

```yaml
pxMetrics:
  # Off by default — Pixie is a real out-of-band infrastructure prerequisite
  # (self-hosted Pixie Cloud + Vizier), not something this chart installs.
  # See helm/README.md before setting enabled: true.
  enabled: false
  connMode: cloud   # "cloud" | "direct" — "cloud" covers both hosted Community Cloud and self-hosted Pixie Cloud, same wire protocol
  vizierAddr: ""     # required when enabled — the cloud/Vizier address (host:port)
  clusterId: ""       # required when enabled && connMode == cloud

pixieApiKey: ""        # required when pxMetrics.enabled && pxMetrics.connMode == cloud
```

- [ ] **Step 2: Render the new ConfigMap keys, conditionally**

In `helm/templates/configmap.yaml`, add after the existing `LLM_MODEL` line:

```yaml
  PIXIE_ENABLED: {{ .Values.pxMetrics.enabled | quote }}
  {{- if .Values.pxMetrics.enabled }}
  PIXIE_CONN_MODE: {{ .Values.pxMetrics.connMode | quote }}
  PIXIE_VIZIER_ADDR: {{ required "pxMetrics.vizierAddr is required when pxMetrics.enabled is true" .Values.pxMetrics.vizierAddr | quote }}
  PIXIE_CLUSTER_ID: {{ .Values.pxMetrics.clusterId | quote }}
  {{- end }}
```

- [ ] **Step 3: Render the new Secret key, conditionally**

In `helm/templates/secret.yaml`, add after the existing `LLM_API_KEY` line, still inside the `{{- if not .Values.secrets.existingSecret }}` block:

```yaml
  {{- if and .Values.pxMetrics.enabled (eq .Values.pxMetrics.connMode "cloud") }}
  PIXIE_API_KEY: {{ required "pixieApiKey is required when pxMetrics.enabled and pxMetrics.connMode == \"cloud\"" .Values.pixieApiKey | quote }}
  {{- end }}
```

- [ ] **Step 4: Write `helm/README.md`** (none exists today)

```markdown
# SAGE Helm Chart

## Pixie (eBPF observability) prerequisite

`pxMetrics.enabled` (default `false`) turns on two extra read-only MCP
tools — `get_pod_resource_usage`, `get_pod_traffic_stats` — backed by
[Pixie](https://px.dev). Pixie is **not installed by this chart** (see
`docs/superpowers/specs/2026-08-12-pixie-observability-design.md` §4 for
why) — it's a separate, real infrastructure prerequisite you stand up
first:

1. Deploy self-hosted Pixie Cloud + Vizier (see
   `docs/superpowers/plans/2026-08-12-pixie-observability.md`'s
   "Prerequisite" section for the current steps — re-verify against
   [pixie-io/pixie](https://github.com/pixie-io/pixie) directly, docs drift).
2. Once Vizier is reachable, deploy SAGE with Pixie enabled:
   ```bash
   helm upgrade sage ./helm -n sage -f helm/values.secret.yaml \
     --set pxMetrics.enabled=true \
     --set pxMetrics.connMode=cloud \
     --set pxMetrics.vizierAddr=<your-cloud-addr>:443 \
     --set pxMetrics.clusterId=<your-cluster-id> \
     --set pixieApiKey=<your-pixie-api-key>
   ```
```

- [ ] **Step 5: Verify the chart renders, both with and without Pixie enabled**

```bash
cd helm
helm template sage . -f values.secret.yaml --set nats.enabled=true \
  --set mcpReadonly.image.repository=test --set mcpReadonly.image.tag=test \
  --set ai.image.repository=test --set ai.image.tag=test \
  --set llm.provider=anthropic --set llm.model=test --set llm.apiKey=test \
  > /tmp/rendered-pixie-disabled.yaml
grep 'PIXIE_ENABLED: "false"' /tmp/rendered-pixie-disabled.yaml   # present
grep 'PIXIE_VIZIER_ADDR' /tmp/rendered-pixie-disabled.yaml         # absent — conditional block skipped

helm template sage . -f values.secret.yaml --set nats.enabled=true \
  --set mcpReadonly.image.repository=test --set mcpReadonly.image.tag=test \
  --set ai.image.repository=test --set ai.image.tag=test \
  --set llm.provider=anthropic --set llm.model=test --set llm.apiKey=test \
  --set pxMetrics.enabled=true --set pxMetrics.connMode=cloud \
  --set pxMetrics.vizierAddr=test:443 --set pxMetrics.clusterId=test-cluster \
  --set pixieApiKey=test-key \
  > /tmp/rendered-pixie-enabled.yaml
grep 'PIXIE_ENABLED: "true"' /tmp/rendered-pixie-enabled.yaml
grep 'PIXIE_VIZIER_ADDR: "test:443"' /tmp/rendered-pixie-enabled.yaml
grep 'PIXIE_API_KEY' /tmp/rendered-pixie-enabled.yaml
```
Expected: both renders exit 0; the disabled render has no `PIXIE_VIZIER_ADDR`/`PIXIE_CLUSTER_ID`/`PIXIE_API_KEY` keys at all (proving the conditionals work, not just that they're blank); the enabled render has all of them with the `--set` values.

- [ ] **Step 6: Stage**

```bash
git add helm/values.yaml helm/templates/configmap.yaml helm/templates/secret.yaml helm/README.md
```

---

## Task 6: `.env.example`

**Files:**
- Modify: `.env.example`

**Interfaces:** none — documentation only.

- [ ] **Step 1: Add the Pixie block**

Add after the existing `MCP_READONLY_TOKEN=placeholder-readonly-token` line (in the backend section):

```bash
# Pixie (eBPF observability, optional — see helm/README.md before enabling)
PIXIE_ENABLED=false
PIXIE_CONN_MODE=cloud
PIXIE_VIZIER_ADDR=
PIXIE_API_KEY=
PIXIE_CLUSTER_ID=
```

- [ ] **Step 2: Cross-check against `settings.go`**

Run: `grep -oE '"[A-Z_]+"' backend/internal/settings/settings.go | sort -u` from the repo root and confirm every name appears in `.env.example`, including the 5 new `PIXIE_*` ones.

- [ ] **Step 3: Stage**

```bash
git add .env.example
```

---

## Task 7: `ai/ai/investigate.py` — dynamic tool-list system prompt

**Files:**
- Modify: `ai/ai/investigate.py`
- Modify: `ai/tests/test_investigate.py`

**Interfaces:** `investigate()`'s public signature is unchanged — this is an internal fix. Independent of the rest of this plan (parallelizable), but surfaced by it: `SYSTEM_PROMPT` currently hardcodes `"get_pod_logs, get_pod_events, describe_pod"`, so it would never mention the two new Pixie tools even though the LLM agent can still call them — this task makes the list generate from whatever `tools` are actually passed in.

- [ ] **Step 1: Write the failing test**

```python
# add to ai/tests/test_investigate.py

def test_build_system_prompt_lists_given_tool_names():
    from ai.investigate import _build_system_prompt

    prompt = _build_system_prompt(["get_pod_logs", "get_pod_resource_usage"])
    assert "get_pod_logs" in prompt
    assert "get_pod_resource_usage" in prompt


def test_build_system_prompt_handles_no_tools():
    from ai.investigate import _build_system_prompt

    prompt = _build_system_prompt([])
    assert "none" in prompt
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ai && pytest tests/test_investigate.py -k build_system_prompt -v`
Expected: FAIL — `_build_system_prompt` doesn't exist yet.

- [ ] **Step 3: Implement**

In `ai/ai/investigate.py`, replace the module-level `SYSTEM_PROMPT` constant with a template + builder function:

```python
SYSTEM_PROMPT_TEMPLATE = """You are SAGE's incident diagnosis agent. You investigate a
Kubernetes pod failure using read-only tools ({tool_names}) and must end your
investigation with exactly one JSON object on its own line, matching this
shape:

{{"failure_mode": "<short name>", "recommended_action": "restart_pod" | "none", "confidence": <0.0-1.0>}}

Rules:
- recommended_action MUST be exactly "restart_pod" or "none" — no other
  value is ever wired to an executor, so anything else is silently
  equivalent to guessing wrong.
- Use "none" whenever restarting the pod would not plausibly fix the
  underlying problem (a bad image reference, a missing secret/config, an
  unschedulable resource request) — do not default to "restart_pod" just
  because you're unsure; lower confidence instead.
- Investigate before concluding: call at least one tool unless the incident
  summary alone is unambiguous.
- If resource exhaustion, latency degradation, or an error-rate spike could
  explain the failure and a resource/traffic tool is available, use it
  before concluding.
"""


def _build_system_prompt(tool_names: list[str]) -> str:
    return SYSTEM_PROMPT_TEMPLATE.format(tool_names=", ".join(tool_names) or "none")
```

In `investigate()`, replace the `SYSTEM_PROMPT` reference with a call to the builder:

```python
async def investigate(incident: PendingIncident, model: BaseChatModel, tools: list[BaseTool]) -> Diagnosis:
    """LLM-driven fallback for incidents no rule-based analyzer matched.
    Uses LangGraph's prebuilt ReAct agent loop rather than a hand-built
    StateGraph — this fallback only needs "call tools, reason, repeat until
    done," which is exactly what create_react_agent already implements."""
    agent = create_react_agent(model, tools)
    system_prompt = _build_system_prompt([t.name for t in tools])
    incident_summary = (
        f"Incident {incident.incident_id}: {incident.kind} {incident.namespace}/{incident.name}\n"
        f"Signals: {[s.type for s in incident.signals]}\n"
        f"First seen: {incident.first_seen}, last seen: {incident.last_seen}"
    )
    result = await agent.ainvoke({"messages": [("system", system_prompt), ("user", incident_summary)]})
    final_message = result["messages"][-1].content
    return _parse_diagnosis(final_message)
```

(`_parse_diagnosis` is unchanged — this task only touches the prompt-building path.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ai && pytest tests/test_investigate.py -v`
Expected: PASS (7 tests — the existing 5 plus these 2). Then the full suite: `pytest -v` — expect 27 passed (was 26 before this task).

- [ ] **Step 5: Stage**

```bash
git add ai/ai/investigate.py ai/tests/test_investigate.py
```

---

## Testing (repo-wide)

- Go: `backend/tests/pxmetrics` covers PxL script construction (pure, no network) and row-parsing (real `types.Record` values, no network) — see Task 2's discussion of what genuinely isn't unit-testable (live `ExecuteScript` semantics, PxL syntax correctness itself).
- Python: `ai/tests/test_investigate.py` gains 2 tests for the dynamic prompt.
- No existing test file changes behavior — this plan is purely additive to both languages' test suites.

## Verification

- `cd backend && go build ./... && go vet ./... && go test ./...`
- `cd ai && pytest`
- `helm template ./helm ...` for both `pxMetrics.enabled=false` (default) and `=true` (Task 5, Step 5) — both must render cleanly.
- Manual, deferred until the Prerequisite section's infrastructure exists: run both PxL scripts against the real Vizier (e.g. via `px run` or the Pixie UI) and confirm `get_pod_resource_usage`/`get_pod_traffic_stats` return sane data for a real pod — this is the one thing this plan's automated tests structurally cannot verify.
