# Pixie (eBPF Observability) Integration — Design

Status: Approved by user, 2026-08-12
Author: design session with Claude Code

## 1. Context and relationship to earlier designs

SAGE's `ai/` diagnosis service (`docs/superpowers/specs/2026-08-08-ai-diagnosis-service-design.md`,
built and complete) has a LangGraph LLM investigation loop that falls back to
read-only MCP tools — today just `get_pod_logs`, `get_pod_events`,
`describe_pod` — when no rule-based analyzer matches a failure. The user
asked whether this loop could also see CPU usage and per-pod HTTP request
rate (RPS), to give the LLM signal a log/event/status snapshot alone can't
explain: resource exhaustion, latency degradation, traffic-driven errors.

Neither exists today. CPU is available for free via Kubernetes'
`metrics-server` API, but RPS is not something the Kubernetes API exposes at
all — it requires either app-level instrumentation or traffic-observing
infrastructure (a service mesh, or eBPF). A repo-wide grep confirmed zero
existing metrics/observability integration anywhere (no Prometheus, Grafana,
OTel, or eBPF code). The user chose eBPF, specifically **Pixie** (px.dev) —
notably, this was already named in the project's own original vision doc
(`docs/overview.md`: "Grafana Beyla/Pixie + OTel (dependency graph,
mesh-agnostic)") and in the earlier, fuller v1 design
(`docs/superpowers/specs/2026-07-23-sre-platform-v1-design.md` §4, §8) that
got scoped down before the leaner v1 was actually built. This spec picks up
that deferred piece, narrowly:

| Original v1 design said | This phase does |
|---|---|
| eBPF DaemonSet (Beyla/Pixie) → OTel Collector Service Graph Connector → dependency-graph nodes/edges in Postgres | Just the two point-query MCP tools below — no OTel Collector, no Postgres dependency-graph subsystem, no standing pipeline |
| K8s read tools adopted from `containers/kubernetes-mcp-server` | (Unrelated to this spec — already superseded: the actual build hand-rolled `mcp-readonly-server` instead, see the Aug 8 spec) |
| Frontend service-connection graph (discussed alongside this feature, not in either prior spec) | Explicitly out of scope — separate, larger effort; `frontend/` is currently a blank Next.js scaffold with zero backend integration |

## 2. Goal

Give `ai/`'s LLM investigation loop two new read-only tools —
`get_pod_resource_usage` (CPU, windowed) and `get_pod_traffic_stats`
(inbound HTTP RPS/errors/latency percentiles, windowed) — sourced from a
self-hosted Pixie deployment, added to the existing `mcp-readonly-server`
rather than a new server or an adopted third-party tool (no official Pixie
MCP server exists — confirmed via web search; hits were for an unrelated
same-named accounting SaaS product).

**Non-goals this phase**: a persistent dependency graph, OTel Collector
integration, frontend visualization of any kind, Prometheus/Loki (also named
in the original vision but out of scope here — Pixie alone covers what this
phase needs).

## 3. Deployment mode — self-hosted Pixie Cloud (explicit user choice)

Pixie has three deployment modes, researched directly against `docs.px.dev`
and `pkg.go.dev/px.dev/pxapi` (Aug 2026, current):

1. **Hosted Community Cloud** (New Relic's free managed service) — lowest
   operational burden, but relies on a third party for auth/UI-routing.
2. **Self-hosted Pixie Cloud** — **chosen**. Full control, zero reliance on
   a third party. This is genuinely heavy: the official guide is a
   `git clone` of `pixie-io/pixie`, checking out a `release/cloud/vX.Y.Z`
   tag, `kustomize build | kubectl apply` (not `helm install`) for an
   Elastic Operator + Elasticsearch (a cloud dependency) and Pixie Cloud
   itself (Kratos/Hydra auth, API services, UI backend), `mkcert`-generated
   TLS, and a DNS-updater script whose name (`dev_dns_updater`) and default
   domain (`dev.withpixie.dev`) suggest a dev/reference pattern more than a
   hardened production one. This is closer to standing up a second internal
   platform than adding a dependency, and is **out-of-band infrastructure
   work** — not automated by SAGE's own Helm chart, not something this
   session can perform (no cluster access) — documented precisely as a
   prerequisite instead.
3. **Standalone/direct-PEM** — zero external dependency, but not confirmed
   as a documented production deployment path (only a client-library option
   exists, `pxapi.WithDirectAddr`). Considered and explicitly not chosen —
   see the risk list in §7.

In all modes, telemetry data itself is computed and stored **locally in the
cluster** — only auth/routing traffic (mode 1) or nothing (modes 2, 3)
leaves. This means SAGE's own code (`pxmetrics`, §5) is agnostic to which
cloud mode is running underneath it — it just needs a `cloudAddr` and API
key, pointed at whichever Pixie Cloud is reachable.

## 4. Why Pixie isn't a `Chart.yaml` dependency

Unlike `postgresql`/`nats` (fully namespace-scoped, safe to have N
independent copies across a cluster), Pixie's Vizier install uses OLM
(Operator Lifecycle Manager) — a cluster-singleton concern — plus a
privileged, host-level, eBPF-loading DaemonSet. Tying its install/upgrade/
removal to `helm install sage`/`helm uninstall sage` risks colliding with
another OLM consumer on the cluster, or tearing down cluster-shared
infrastructure when someone just wants to remove SAGE — the same reasoning
that keeps cert-manager/Prometheus Operator-shaped tools as pre-existing
cluster prerequisites in most charts, not bundled subchart dependencies.
`postgresql` already has the right precedent in this very chart:
`postgresql.enabled=false` + `externalDatabase.url` (bring-your-own).
**Pixie only ever gets the bring-your-own treatment.**

## 5. Architecture

```
ai/ (LangGraph agent) --MCP--> mcp-readonly-server (Go, existing)
                                  |  get_pod_logs / get_pod_events / describe_pod  (existing, client-go)
                                  |  get_pod_resource_usage / get_pod_traffic_stats  (new)
                                  v
                          backend/internal/pxmetrics (new Go package)
                                  |  wraps px.dev/pxapi (Pixie's official Go client)
                                  v
                          Vizier (self-hosted Pixie Cloud, out-of-band prerequisite)
```

**`backend/internal/pxmetrics`** — two files:

- `queries.go` — pure, unit-testable PxL script builders
  (`buildPodCPUScript`, `buildPodTrafficScript`) plus `pxapi.TableRecordHandler`
  row-parsing types. PxL adapted from Pixie's own official demo script
  (`github.com/pixie-io/pixie-demos/blob/main/custom-k8s-metrics-demo/pxl/pods.pxl`,
  fetched and verified directly, not invented):
  - CPU: reads the `process_stats` table, windows via `px.bin(df.time_, window_ns)`,
    computes counter deltas (`cpu_utime_ns`/`cpu_ktime_ns` max−min per
    window), `cpu_usage = (ktime+utime)/window_ns`.
  - Traffic: reads the `http_events` table, `requests_per_s = count/window_s`,
    `errors_per_s` from `resp_status >= 400`, `px.quantiles` for
    p50/p90/p99 latency.
  - **Design decision**: filter by `df.ctx['pod']` (matching this pod, same
    primary-key shape as the existing 3 tools) rather than `df.ctx['service']`
    as the demo script does — nothing in SAGE resolves a Service name from a
    pod name today, and per-pod filtering is arguably more useful for
    single-replica incident diagnosis anyway (isolates this replica's
    behavior, not the Service's aggregate).
  - Namespace/name are validated against Kubernetes' DNS-1123 label format
    before interpolating into the script string — the injection-safety
    boundary, since these strings ultimately originate from an LLM tool call.
- `client.go` — `Config{ConnMode, VizierAddr, APIKey, ClusterID}` +
  `NewClient`/`GetPodCPUUsage`/`GetPodTrafficStats`, wrapping `px.dev/pxapi`
  (`pxapi.NewClient` with `WithCloudAddr`+`WithAPIKey` — what self-hosted
  Pixie Cloud uses too, just pointed at your own domain). Each query wrapped
  in a ~20s `context.WithTimeout` — new relative to `introspect.go`, since a
  hung Pixie call would otherwise stall the whole ReAct loop.

Output types are small time-series structs (`[]CPUSample`, `[]TrafficSample`,
one per 60s window, capped at ~20 buckets) — a short trend, not a single
scalar, since "was fine, spiked 3 minutes ago" is more diagnostically useful
to a ReAct loop than one number.

## 6. MCP tools, settings, ai/, Helm

- **`mcp-readonly-server`**: two more `mcp.AddTool` calls
  (`get_pod_resource_usage`, `get_pod_traffic_stats`), gated behind
  `s.PixieEnabled`. Stays thin — construct `pxmetrics.Client` once at
  startup if enabled.
- **`ai/ai/mcp_client.py`**: no code change needed — `get_readonly_tools()`
  already does live MCP tool discovery against whatever the server exposes.
- **`ai/ai/investigate.py`**: one fix, surfaced by this change but
  independent of Pixie specifically — `SYSTEM_PROMPT` hardcodes the 3-tool
  list as a string constant, so it never mentions new tools even though
  they're callable. Make the tool-list clause dynamic
  (`", ".join(t.name for t in tools)`), generated at call time, plus one
  sentence of guidance on when to reach for resource/traffic tools.
- **`backend/internal/settings/settings.go`**: new fields `PixieEnabled`,
  `PixieConnMode`, `PixieVizierAddr`, `PixieAPIKey`, `PixieClusterID` —
  **conditionally** validated (new pattern in this file, justified because
  Pixie is optional/off-by-default, unlike Slack/MCP-execute which are
  required for every install).
- **Helm**: `values.yaml` gets a `pxMetrics:` block (`enabled: false` by
  default) and `pixieApiKey` alongside existing secret-token fields;
  `configmap.yaml`/`secret.yaml` render the new keys only
  `{{- if .Values.pxMetrics.enabled }}`. No new Deployment/Service/RBAC
  template — `deployment-mcp-readonly.yaml` already does
  `envFrom: configMapRef + secretRef`, and `pxmetrics` makes zero K8s API
  calls (PxL's own K8s-context columns resolve namespace/pod — no extra
  RBAC needed).

## 7. Real open risks (confirm during implementation, don't assume)

1. **`px.dev/pxapi`'s latest tag is v0.5.0** (dated ~2023 per pkg.go.dev)
   even though `pixie-io/pixie` itself shows current activity — confirm it
   still builds and round-trips against whatever Vizier version actually
   gets deployed before committing to the SDK path; the fallback is calling
   Vizier's gRPC API directly without it (bigger scope, replan if needed).
2. **Exact `Datum` concrete type names** for row parsing — not visible via
   `pkg.go.dev`'s rendered docs; get them from `go doc px.dev/pxapi/types`
   once the dependency is actually added, not from memory.
3. **`trace_role` enum value for "inbound to this pod"** in `http_events` —
   reasonably but not fully confident it's `2`; confirm against
   `docs.px.dev`'s table reference or a live script run before trusting it.
4. **The self-hosted install steps drift** — the guide's `git tag`/
   `release/cloud/vX.Y.Z` pattern assumes a live clone; re-verify against
   the actual current `pixie-io/pixie` repo at setup time, not this spec's
   snapshot.
5. **Task 1 (standing up self-hosted Pixie Cloud) requires real cluster
   access** this session doesn't have — it's the user's own out-of-band
   step. The rest of this plan's code can be written and unit-tested without
   it; live PxL verification against a real Vizier is a manual step,
   deferred until that infrastructure exists.

## 8. Testing

PxL *script construction* and *row parsing* are fully unit-testable — real
`pxapi` types, no network, same spirit as `fake.NewSimpleClientset`. PxL
*semantic/syntax correctness* is not unit-testable — Go tests can only prove
"we built the string we intended," never "this is valid PxL," since that's
compiled server-side by Vizier. Confirmed only by actually running both
scripts against a real Vizier at least once — a manual step, not a CI gate,
same precedent `introspect_test.go` already sets for `GetPodLogs`.

## 9. Verification

- `cd backend && go build ./... && go vet ./... && go test ./...`
- `cd ai && pytest`
- `helm template ./helm --set pxMetrics.enabled=false` (default) and
  `--set pxMetrics.enabled=true --set pxMetrics.connMode=cloud --set ...`
  (all required values) both render cleanly.
- Manual (deferred until the user completes the Pixie Cloud prerequisite):
  run both PxL scripts against a real Vizier; confirm
  `get_pod_resource_usage`/`get_pod_traffic_stats` return sane data for a
  real pod.
