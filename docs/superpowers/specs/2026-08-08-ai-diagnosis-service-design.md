# ai/ Diagnosis Service — Design (SAGE Next Phase)

Status: Approved by user, 2026-08-08
Author: design session with Claude Code

## 1. Context and relationship to the original v1 design

The original v1 design (`docs/superpowers/specs/2026-07-23-sre-platform-v1-design.md`)
envisioned a much larger architecture: Kafka as an event backbone, a
dependency graph with blast-radius scoring, Qdrant history matching, an
Isolation Forest anomaly detector, six execute actions, and read-only K8s
tools adopted from `containers/kubernetes-mcp-server`. What actually got
built and deployed to the real EKS cluster (`docs/superpowers/plans/2026-07-23-sre-platform-v1-core-loop.md`)
is a deliberately leaner subset: no Kafka, no dependency graph, no blast
radius, one execute action (`restart_pod`), one detected failure mode
(`CrashLoopBackOff`), diagnosis done by a single hardcoded Go rule
(`backend/internal/analyze.CrashLoopAnalyzer`), and no `ai/` service at
all — it's an empty scaffold.

This spec covers the next real increment: building `ai/` for real. It
intentionally does **not** try to catch the codebase up to the full
original vision in one pass. Scope was narrowed through brainstorming with
the user on 2026-08-08:

| Original v1 design said | This phase does |
|---|---|
| Kafka event backbone, every stage publishes | **NATS JetStream**, used only for backend→ai/ dispatch (narrower, not a general backbone) |
| Qdrant similarity check as diagnosis step 1 | Deferred — left a clean seam, not built |
| Read tools adopted from `containers/kubernetes-mcp-server` | Hand-rolled `mcp-readonly-server` (Go), matching the existing hand-rolled `mcp-execute-server` already in the codebase |
| Six execute actions | Still just `restart_pod` — `execute`/`mcpexecute` haven't grown a new action, so ai/'s output vocabulary can't either |
| LLM unspecified | User-selectable at deploy time: Anthropic / OpenAI / OpenRouter (self-hosted deployment, provider choice shouldn't be hardcoded) |

Everything else this spec touches (Slack approval flow, the hard safety
floor in `gate.go`, idempotent execute actions, structured audit logging)
is unchanged and reused as-is.

## 2. Goal

Replace the hardcoded Go analyzer with a real diagnosis service: rule-based
analyzers for known failure modes, falling back to an LLM-driven
investigation loop (read-only cluster tools via MCP) for anything a rule
doesn't recognize. The Go backend stops doing diagnosis entirely and
becomes purely detect → dispatch → (later) resume.

**Non-goals this phase**: Qdrant/history matching, expanding the execute
action set beyond `restart_pod`, a staleness alert for incidents stuck
awaiting diagnosis, multi-cluster support.

## 3. Architecture

```
watcher → correlate → [Reconciler] → NATS JetStream ("sage.incidents.pending") → [ai/ service]
                            ↑                                                          │
                            │                                                          ▼
                    HTTP callback  ◄────────────────────────  rule-based analyzer → (no match) → LLM investigation loop
              POST /internal/incidents/{id}/diagnosis                                              │ (via MCP tool calls)
                            │                                                                       ▼
                            ▼                                                          mcp-readonly-server (Go, new)
                  gate → execute → verify                                          get_pod_logs / get_pod_events / describe_pod
```

**backend/ (Go)** — watch → correlate → persist a `pending_diagnosis`
incident → publish to NATS → (on callback) gate → execute → verify.
`backend/internal/analyze`'s `Analyzer` interface and `CrashLoopAnalyzer`
are deleted; only the `Diagnosis` struct shape survives as the callback's
wire contract.

**ai/ (Python)** — a NATS JetStream durable consumer pulls pending
incidents, tries rule-based analyzers first (one function per known
failure mode, ported from the deleted Go logic), and falls back to a
LangGraph agent with MCP tool access when no rule matches. The LLM is
pluggable via `LLM_PROVIDER=anthropic|openai|openrouter` (LangChain chat-model
abstraction; OpenRouter reuses the OpenAI-compatible client with a
different `base_url`). Once diagnosed, POSTs the result to backend's
callback endpoint.

**mcp-readonly-server (Go, new)** — built exactly like the existing
`mcp-execute-server`: bearer-token auth via the existing
`mcpauth.RequireBearerToken`, its own ServiceAccount + ClusterRole scoped
to `get`/`list`/`watch` only (pods, pods/log, events — no delete, no
write). ai/'s LangGraph agent calls it as an MCP client; ai/ never gets
direct cluster credentials — same defense-in-depth reasoning already
applied to the execute side (see original design §8).

## 4. Data flow / state machine

1. `correlate` produces an incident → `Reconciler` calls a new
   `store.CreatePendingIncident(...)` (replaces `CreateIncident`'s
   `failureMode` param — not known yet). Row gets `status='pending_diagnosis'`;
   `incidents.failure_mode` becomes nullable (migration `0002_pending_diagnosis.sql`).
2. Reconciler keeps the incident's context (signals, GroupKey) in an
   in-memory map keyed by incident ID and publishes
   `{incidentID, namespace, kind, name, signals, groupKey}` to NATS. Same
   accepted tradeoff already documented for `restartAttempts` in
   `reconcile.go` — in-memory, lost on backend restart (§5).
3. ai/ consumes, diagnoses (rule match or LLM fallback), POSTs
   `{failure_mode, recommended_action, confidence, source}` to
   `POST /internal/incidents/{id}/diagnosis`.
4. Backend's handler is today's `handleIncident` logic minus the
   `analyzer.Analyze` call: look up the in-memory incident context by ID,
   `store.RecordDiagnosis(...)`, the existing restart-limit check (moves
   here, was pre-callback before), `gate.Evaluate`, `CreateRemediationAction`,
   then Slack approval or `executeAndVerify` — unchanged past this point.

## 5. Error handling / safety

- **Orphaned callback** (incident ID not in the in-memory map — backend
  restarted mid-flight): record the diagnosis via `WriteAudit` for the
  trail, but don't auto-execute — the Signals needed for
  `verify.CheckPodHealthy` are gone. Post a Slack alert asking for manual
  review instead of silently dropping it. Same fail-closed pattern
  `gate.go` already documents (§12 of the original design: "MCP server-side
  token validation failing is treated as a hard stop, not a
  fallback-to-unrestricted-access condition" — same instinct applied here).
- **Idempotent callback**: if an incident's status is already past
  `pending_diagnosis` when a callback arrives (NATS at-least-once
  redelivery, or an ai/-side retry), log and no-op rather than re-running
  gate→execute→verify twice.
- **ai/ or NATS down**: incidents queue durably in JetStream until ai/
  recovers — no fallback rule exists anymore since the Go analyzer is
  deleted. A staleness alert is an explicit non-goal this phase (§2).
- **Action vocabulary stays narrow**: `execute`/`mcpexecute` still only
  implement `restart_pod`. ai/'s rule set and LLM system prompt must
  constrain output to `{"restart_pod", "none"}` — `executeAndVerify`
  unconditionally calls `RestartPod` regardless of the action label, so
  anything else would be mislabeled-but-executed. This is a pre-existing
  latent mismatch in `reconcile.go`, not fixed by this phase, just a hard
  boundary on ai/'s output until the execute side grows more actions.

## 6. Detection surface

`k8swatch.knownReasons` currently maps exactly one K8s event reason
(`"BackOff"` → `"CrashLoopBackOff"`). This phase adds a small set of
additional common reasons (image-pull failures, failed probes, failed
scheduling) so more than one failure mode reaches ai/ — otherwise every
incident matches the single rule and the LLM fallback path is never
exercised by real traffic. Exact `Reason` strings need confirming against
real cluster events during implementation (image-pull failures in
particular don't map to one clean reason string).

## 7. Deployment (Helm)

- New `nats` dependency in `helm/Chart.yaml` (mirrors the existing
  `postgresql` subchart pattern via `condition:`), JetStream enabled, small
  PVC for stream persistence.
- New `mcp-readonly-server` Deployment/Service/ServiceAccount/ClusterRole/Secret,
  mirroring `helm/templates/deployment-mcp-execute.yaml` /
  `rbac-mcp-execute.yaml` exactly.
- New `ai` Deployment — no public Service needed, only outbound calls to
  NATS, `mcp-readonly-server`, backend's callback endpoint, and the LLM
  provider — with its own Secret for `LLM_PROVIDER`/`LLM_API_KEY`/`LLM_MODEL`.
- New backend settings in `backend/internal/settings/settings.go`:
  `NATSURL`, a new bearer token for the diagnosis-callback route.

## 8. Testing

- Go: `backend/tests/reconcile` gains tests for `OnDiagnosis` (including
  orphaned-callback and duplicate-callback cases) using an in-process NATS
  test server; `backend/tests/store` and `backend/tests/httpserver` get
  corresponding coverage. `backend/tests/analyze` (if present) is removed
  along with the deleted package.
- Python: `ai/tests/` gets fast, no-network unit tests per rule-based
  analyzer, plus a mocked-LLM test for the LangGraph fallback graph
  (LangChain fake chat models — no real API calls in CI).

## 9. Verification

- `cd backend && go build ./... && go vet ./... && go test ./...`
- `cd ai && pytest`
- `helm lint helm/` after the new templates/dependency are added.
- End-to-end: deploy to the real cluster (same pattern as v1 —
  `helm upgrade`), trigger a `CrashLoopBackOff` test pod, confirm the
  incident flows through NATS to ai/ and back, and confirm a genuinely
  unmatched failure mode (e.g. a bad image) reaches the LLM fallback path
  and produces a sane diagnosis without ai/ ever mutating the cluster.
