# Autonomous SRE Platform for Kubernetes — v1 Design (Self-Healing Core)

Status: Approved by user, 2026-07-23
Author: design session with Claude Code

## 1. Vision & Phasing

Build an autonomous SRE platform for generic Kubernetes clusters (no cloud-specific
lock-in), delivered as a real production system, built solo over a few months.

The full vision is large enough to require decomposition into phases. Each phase
reuses the same backend/ai/frontend foundation, approval model, and Slack
integration — nothing built in v1 is thrown away later.

| Phase | Scope |
|---|---|
| **v1 (this doc)** | Self-healing core: detect, diagnose, gate, remediate, verify, learn. |
| v1.5 | Multi-Agent DevOps Orchestrator: GitHub PR triggers a Deploy → Test → Security → Promote → Rollback agent pipeline; sandbox-first deploy, promote only if checks pass, auto-revert on post-deploy issues. Also: a **frontend UI for configuring MCP execute-tool permissions/tokens** (which action types are permitted, per environment/cluster) — v1 uses a static, config-based permission set instead of a UI for this. |
| v2+ | Deeper LLM-assisted RCA, predictive/anomaly detection maturation, full incident-management workflow (paging, postmortem generation). |

**v1 goal**: detect common and novel K8s failure modes, diagnose them, gate the
proposed fix through a configurable approval model with a hard safety floor,
execute the fix, verify it worked, and record it so repeat failures are recognized
instead of endlessly re-remediated.

**v1 non-goals**: the multi-agent deploy pipeline (v1.5), the frontend MCP
tool/permission configuration UI (v1.5 — v1 hardcodes the permission set in
config), predictive/forecast-driven incident prevention (v2+), paging/on-call
workflow beyond Slack approval (v2+), service-mesh-dependent features (design
stays mesh-agnostic).

## 2. Architecture Overview

```
                    +------------------+
   view only  <-->  |  frontend/       |   Next.js: dashboards, incident
                    |  (Next.js)       |   history, audit log (view-only in v1 —
                    +--------+---------+   MCP tool/permission config UI is
                             | REST         v1.5; no approval actions here)
                             |
   K8s Events   -->  +-------v----------+        +----------------------+
   Alertmanager -->  |   backend/        | <----> |   ai/ (Python)        |
   Loki alerts  -->  |   (Go)            |  MCP   |   LangGraph agent     |
                    |                    | (read  |                       |
                    |  Monitor           |  tools)|  1. Qdrant similarity |
                    |  Correlate         |        |     check             |
                    |  Dependency graph  |        |  2. rule-based        |
                    |  Blast radius      |        |     analyzers         |
                    |  Plan + Gate       |        |  3. Isolation Forest/ |
                    |  Execute (sole     |        |     seasonal anomaly  |
                    |   write path)      |        |     scoring (batch)   |
                    |  Verify            |        |  4. LLM fallback via  |
                    +----+----------+----+        |     read-only MCP     |
                         |          |              |     tools             |
              K8s API <--+          +--> Postgres  +----------+-----------+
              (mutate,                   (incidents,          |
               MCP execute               actions,             |
               tools)                    approvals,           |
                         |                audit log,          |
                         |                dep-graph            |
                         |                nodes/edges) <-------+
                         |                     |
                    +----v-----+          Qdrant (failure-signature +
                    |  Slack   |           postmortem-narrative embeddings)
                    |  bot     |
                    +----------+     eBPF DaemonSet (Beyla/Pixie) + OTel
                    manual-approval    Collector (Service Graph Connector)
                    mode: interactive  --> feeds backend/'s dependency graph
                    approve/deny
```

**Non-negotiable invariants** (carried through every design decision below):

1. **ai/ never mutates the cluster.** It only reads (via a read-only MCP tool set)
   and recommends. backend/ is the only component holding cluster-mutation
   credentials and the only caller of MCP *execute* tools.
2. **A hard safety floor cannot be configured away.** A static denylist of
   irreversible/high-blast-radius action types (delete PVC, scale-to-zero,
   delete node, ...) always requires Slack approval, regardless of the
   auto/manual toggle or permission-token scope.
3. **A dynamic blast-radius threshold is a second, independent gate.** Even a
   normally "safe" action requires approval if its computed blast radius
   exceeds a configured threshold at the time it's proposed.
4. **Every remediation action is idempotent and safe to retry.**
5. **Every decision (auto or manual) is logged to Postgres for audit**, whether
   or not it required a human.
6. **Guardrails are layered, not singular**: Gate logic in backend/, MCP-enforced
   token scope at the MCP server, and the hard safety floor are three
   independent checks — no single one is trusted alone.
7. **Every stage transition is an event, not just a function call.** backend/
   publishes to Kafka at each transition; consumers (Slack notifier, frontend/
   dashboard, audit logger, future integrations) subscribe to topics instead
   of backend/ calling each one directly — new notification targets don't
   require a backend/ code change.

### Event Backbone (Kafka)

The system is event-driven end to end, not just request/response. Each stage
of the loop publishes to a Kafka topic on completion; consumers react
independently:

| Topic | Published by | Consumed by |
|---|---|---|
| `incidents.detected` | backend/ (after Correlate + ai/ diagnosis) | frontend/ (dashboard feed), audit logger |
| `remediation.approval_requested` | backend/ Gate | Slack-notifier consumer (posts the approval message) |
| `remediation.approval_decided` | Slack interaction webhook handler | backend/ Execute (resumes the paused incident) |
| `remediation.executed` | backend/ Execute | audit logger, frontend/ |
| `remediation.done` | backend/ Verify (outcome = resolved) | frontend/, Slack-notifier (posts resolution), knowledge-store writer (Postgres + Qdrant) |
| `remediation.failed` | backend/ Verify (outcome = unresolved / escalated) | Slack-notifier (escalation message), frontend/ |

This decouples "the loop decided something" from "who needs to know" — e.g.
the v1.5 multi-agent orchestrator or a future PagerDuty integration can
subscribe to `remediation.failed` without backend/'s core loop knowing they
exist. Kafka is the event backbone from day one (not introduced later at a
scaling trigger — see §10).

## 3. Detection (Monitor)

Two complementary paths feed the same normalized `Signal` schema
(`{source, type, severity, involvedObject{kind,namespace,name}, timestamp, raw}`):

- **Fast path — backend/ (Go)**: K8s Events (via informer watch, mapped from
  K8s's own `Reason` taxonomy — `OOMKilling`, `CrashLoopBackOff`,
  `FailedScheduling`, `NodeNotReady`, `Evicted`, ...), Alertmanager webhook
  receipts, Loki Ruler alerts (log-pattern-based, e.g. panics/kernel OOM-killer
  lines), and inline EWMA/threshold checks on ingested metrics for near-instant
  detection of sudden shifts.
- **Slow path — ai/ (Python), batch, every few minutes**: Isolation Forest over
  metric windows (catches multivariate/novel deviations no single threshold
  would flag) and seasonal-baseline forecasting (catches gradual drift — memory
  leaks, latency creep — against a learned weekly/daily pattern; requires ~1
  week of history before it's trusted). Emits `Signal{source: anomaly_detector}`
  back into the same pipeline.

**Failure taxonomy v1 targets**: crash loops, OOMKilled, bad rollouts, node
resource pressure, latency/tail degradation, resource exhaustion trends
(leaks, throttling), cascading dependency timeouts, config-drift-induced
misbehavior, traffic/seasonality anomalies. (Beyond simple HTTP 4xx/5xx counts —
the anomaly-detection slow path exists specifically to catch the categories a
static rule can't.)

## 4. Correlation & Dependency Graph

**Correlation is mechanical, not LLM-driven, and lives in backend/ (Go)** — it
needs the live K8s ownership graph client-go's informers already maintain.
Signals are grouped into one `Incident` when they share an involved object *or*
a topology/dependency-graph relationship (same node, same owning Deployment,
adjacent in the dependency graph) within a correlation time window (e.g. 60s).
This prevents one real problem from surfacing as three uncorrelated alerts.

**Dependency graph** (new first-class subsystem, mesh-agnostic):

1. **K8s-native topology** (Service → EndpointSlice → Pod selector matching,
   OwnerReferences) — free, already-watched data, gives the ownership skeleton
   and a fallback when no traffic data exists.
2. **eBPF enrichment**: a DaemonSet running **Grafana Beyla** (or Pixie) —
   zero code changes, auto-instruments RED metrics + trace spans, no service
   mesh required.
3. Spans flow through an **OTel Collector's Service Graph Connector** →
   traffic-weighted edges (request rate, error rate, latency per edge).
4. Stored as nodes/edges tables in **Postgres** (no dedicated graph DB needed
   at this scale).
5. If the cluster already runs Cilium or a service mesh, prefer its native
   telemetry (Hubble / Istio/Linkerd) over the eBPF DaemonSet — same graph
   schema, higher-fidelity source.

**Blast radius score** = `f(reachable_node_count, traffic_share_of_edges,
criticality_tier, count_of_threatened_SLOs)`, computed by backend/ over this
graph. `criticality_tier` is a small manually-curated tier-0/1/2 registry
(not auto-inferred). Computed both reactively (scoping an ongoing incident)
and prospectively (gating a proposed remediation action, before the approval
decision) — same function, two call sites. Surfaced explicitly in the Slack
approval message.

## 5. Diagnosis (Analyze) — ai/ LangGraph agent

Framework: **LangGraph (Python)**. Chosen over Google ADK (Go SDK exists but
~9 months old, HITL primitives still experimental), CrewAI, AutoGen/AG2, and
OpenAI's Agents SDK because its `interrupt()`/checkpointer pattern is the most
production-proven pause/resume primitive of the group — though notably, v1
doesn't use that primitive for the approval gate (see §6). LangGraph's
checkpointer is backed by the shared Postgres instance, not per-worker memory,
so diagnosis workers stay horizontally scalable and stateless from the infra's
perspective.

**Investigation loop**:
1. Qdrant similarity check — "have we seen this failure signature before?"
2. Rule-based analyzers for known failure signatures (fast, free, deterministic —
   handles the common-case path with zero LLM cost).
3. If unrecognized: LLM-driven investigation using a **read-only** MCP tool set
   — `get_pod_logs`, `get_events`, `query_prometheus`, `query_loki`,
   `get_dependency_graph`, `query_qdrant_similar_incidents`,
   `query_postgres_incident_history`. The agent iterates these tools, forms
   and tests hypotheses, until it reaches a diagnosis. The K8s-native tools
   in this set (`get_pod_logs`, `get_events`, and general resource
   listing/describe) are **not hand-written** — they're provided by adopting
   [`containers/kubernetes-mcp-server`](https://github.com/containers/kubernetes-mcp-server)
   run with `--read-only` (see §8). The remaining tools
   (`query_prometheus`, `query_loki`, `get_dependency_graph`,
   `query_qdrant_similar_incidents`, `query_postgres_incident_history`) are
   custom, since they integrate systems that server doesn't cover.
4. Output: `{diagnosis, recommended_action, confidence, evidence}` returned to
   backend/. ai/ never holds execute-tool access — those tools are not even
   present in its MCP context, both to keep token overhead down and to bound
   the blast radius of a compromised/poisoned tool description.

## 6. Plan, Gate, Execute, Verify — backend/ (Go)

The approval pause is a **deterministic Go/Postgres state machine, not a
LangGraph interrupt** — it doesn't depend on the LLM framework's checkpointer
behaving correctly; it's a Postgres row (`Incident.status = pending_approval`)
flipped by a Slack webhook.

```
backend/ Plan+Gate, in order:
  1. action type in hard safety floor denylist?      -> Slack approval, always
  2. computed blast radius over configured threshold? -> Slack approval, always
  3. platform mode == manual-approve?                 -> Slack approval
  4. platform mode == auto-approve:
       permission token grants this action type?
         yes -> proceed to Execute
         no  -> Slack approval (fallback)

Slack approval: interactive approve/deny buttons -> human response
  -> Postgres row updated -> backend/ resumes

backend/ Execute (sole path that calls MCP execute tools):
  -> authenticates to MCP server with its scoped token
  -> MCP server independently re-validates token scope before running the
     tool (second, independent enforcement layer)
  -> calls execute tool: restart_pod / rollback_deployment / scale_deployment
     / cordon_node / drain_node / evict_pod — each idempotent, safe to retry

backend/ Verify:
  -> re-checks the health signal that triggered diagnosis, over a window
  -> healthy -> mark resolved, write structured record to Postgres, embed
     narrative + upsert failure-signature to Qdrant, publish remediation.done
  -> still unhealthy -> auto-rollback if the action supports it, else
     escalate to Slack as "remediation failed", publish remediation.failed
```

Every arrow above that crosses a component boundary (Gate → Slack, Slack →
Execute, Verify → notifications/knowledge-store) is a Kafka publish/consume,
not a direct call — see the Event Backbone table in §2.

**Permission/token system**: which execute-tool types are permitted at all for
a given environment/cluster, and the scoped token backend/ uses to
authenticate to the MCP execute server, are defined here — this is the
mechanism that fills in step 4 above. **In v1 this permission set is static
config** (loaded by backend/ at startup, not editable at runtime); a
**frontend UI for an admin to create/edit these permissions and generate
tokens is v1.5 scope**, not v1 (see §1 phasing) — the frontend in v1 stays
strictly view-only. The auto/manual mode toggle is also static config in v1
for the same reason. Individual incident approvals remain Slack-only in both
v1 and v1.5.

## 7. Knowledge Store

**Postgres (structured, source of truth)**: `incidents`, `remediation_actions`,
`approvals`, `audit_log`, `dependency_graph_nodes`, `dependency_graph_edges`.
Relationships between entities (`service → failure-mode → root-cause →
remediation → outcome`) are modeled as foreign keys — no dedicated graph
database needed at v1 scale; only revisit this if multi-hop traversal queries
become genuinely awkward in SQL.

**Qdrant (embeddings)**: free-text narrative fields (symptom description,
root-cause explanation) embedded for semantic similarity search — "has
something like this happened before, in different words." Referenced back to
Postgres rows by ID.

**Retrieval pattern (hybrid)**: vector search in Qdrant finds candidate similar
incidents → join back to Postgres for structured context (same service? same
failure-mode? what remediation was tried, did it work?) → both feed the
LangGraph agent's diagnosis step. This is what prevents the "stateless loop"
failure mode (endlessly re-remediating a recurring issue as if it were novel).

**Postmortem record fields** (per resolved incident): summary, timeline
(detection → escalation → mitigation → resolution), impact (services
affected, duration, blast radius at time of incident), root cause +
contributing factors, detection method, remediation taken, outcome,
classification metadata (for future trend analysis).

## 8. MCP — role and boundaries

MCP is kept as an **interface/legibility layer, not a security boundary**. It
reduces tool-code duplication (one tool definition usable by any
MCP-compatible framework) and makes the tool surface auditable. It is
explicitly *not* trusted as the authorization control — Slack approval, the
Gate logic in backend/, and token-scope enforcement at the MCP server are the
real controls, layered independently.

Two separately scoped tool sets, never merged, **sourced differently**:

- **Read-only** (ai/'s context only): logs, events, metrics, dependency graph,
  history/similarity lookups.
  - The general K8s tools (pod logs, events, resource listing/describe) are
    **adopted, not hand-written**: run
    [`containers/kubernetes-mcp-server`](https://github.com/containers/kubernetes-mcp-server)
    (Go-native, talks to the K8s API directly, not a kubectl wrapper) in its
    built-in `--read-only` mode. This is a real, actively-maintained project —
    no need to reimplement general K8s introspection.
  - Prometheus/Loki/dependency-graph/Qdrant/Postgres-history tools are custom,
    since no existing K8s-focused MCP server covers them.
- **Execute** (backend/'s context only, token-gated): the mutating actions
  listed in §6 — `restart_pod`, `rollback_deployment`, `scale_deployment`,
  `cordon_node`, `drain_node`, `evict_pod`. **Deliberately hand-written, not
  adopted from `kubernetes-mcp-server`'s write mode.** That project's write
  surface is intentionally general (any create/update/delete on any
  resource) — appropriate for an interactive admin tool, but it's the
  opposite of what this system's safety design depends on: a small,
  individually safety-reviewed, idempotent action set gated by the hard
  safety floor and blast-radius threshold. Adopting a generic "delete/update
  anything" tool would reopen exactly the risk that narrow action set exists
  to close. These six actions are implemented as plain Go functions over
  client-go (already what the core-loop implementation plan builds), then
  exposed as a small custom MCP server so backend/'s call path stays
  consistent with the read side and with the token-scope re-validation
  described in §6 — never present in ai/'s MCP context at all.

This split also bounds token overhead (real-world measurements show tool
schemas alone can consume ~34% of a 200K context window with just 7 servers)
and limits the blast radius of a poisoned tool description, since the agent
that can be manipulated by untrusted text (logs, LLM-facing data) never has
execute tools in reach to begin with.

## 9. Tech Stack

| Component | Choice | Why |
|---|---|---|
| backend/ | Go, client-go/informers | Native, idiomatic K8s watch/execute; matches the Operator ecosystem's own idioms. |
| ai/ | Python, LangGraph | Most production-proven HITL/checkpointer pattern; ML ecosystem (scikit-learn Isolation Forest) native. |
| frontend/ | Next.js | Dashboards, history, audit log, permission/token config — view/config only, no approval actions. |
| Structured store | Postgres | Transactional guarantees, relational modeling of incident/remediation/audit data. |
| Vector store | Qdrant | Failure-signature and postmortem-narrative similarity search. |
| Approval channel | Slack | Interactive approve/deny for the manual-approval path and safety-floor/blast-radius escalations. |
| Tool interface | MCP | Standardized, auditable tool surface; not the security boundary. |
| K8s read tools (ai/) | [`containers/kubernetes-mcp-server`](https://github.com/containers/kubernetes-mcp-server), `--read-only` | Adopted, not hand-written — Go-native, actively maintained, covers logs/events/resource listing. |
| K8s execute tools (backend/) | Custom, thin MCP server over the hand-written client-go functions in §6 | Deliberately narrow — see §8 for why the above project's generic write mode isn't adopted for this side. |
| Dependency graph enrichment | Grafana Beyla or Pixie (eBPF) + OTel Service Graph Connector | Mesh-agnostic, zero app instrumentation. |
| Event backbone | **Kafka** | Event-driven from day one — every stage transition publishes to a topic (see §2 Event Backbone); decouples backend/'s core loop from notification/integration consumers. |

## 10. Scaling (staged, not front-loaded)

Kafka itself is in place from day one (per the design decision to be
event-driven throughout, not introduced later as a scaling trigger). What
still scales in stages is everything *around* it:

- **Now (single cluster)**: single-broker Kafka (or a small cluster), topics
  from the Event Backbone table (§2) with modest partition counts; single
  Postgres instance; single Qdrant node.
- **Next trigger (a few clusters / moderate incident volume)**: increase
  Kafka partition counts per topic so backend/'s Monitor and ai/'s diagnosis
  workers scale horizontally as independent consumer groups; add Postgres
  time-based partitioning on `incidents`/`audit_log` (cheap, non-disruptive,
  pays off immediately).
- **Later trigger (many clusters, high volume, multi-tenant)**: tag every
  signal/incident/event with `cluster_id` from day one (free now, required
  later) and key Kafka partitions by it for consumer-group locality; Qdrant
  multi-shard/multi-replica; Postgres read replicas or Citus if write volume
  genuinely demands it; scale the Kafka cluster itself (more brokers, higher
  replication factor) as throughput grows.

## 11. Testing

- **Unit tests**: rule-based analyzers, blast-radius scoring function, Gate
  logic (safety floor + threshold + mode + token combinations).
- **Integration tests**: against a local **kind** cluster.
- **Chaos validation**: **Litmus** or **Chaos Mesh** inject controlled faults
  (pod-kill, network delay, CPU/memory stress, node drain) to verify the
  self-healing loop actually detects, diagnoses, gates, and remediates
  correctly — this is the outer validation loop around the whole system, not
  optional hardening.

## 12. Error Handling

- Every execute action is idempotent — safe to retry on backend/ restart
  mid-execution.
- Verify failures trigger auto-rollback (if the action supports it) or
  escalate to Slack rather than retrying blindly, avoiding the
  "stateless loop" failure mode of re-attempting the same ineffective fix.
- If ai/'s diagnosis is inconclusive (low confidence, no rule match, LLM
  fallback also inconclusive), the incident is marked `unrecognized` and
  routed directly to Slack — no remediation is attempted blind.
- MCP server-side token validation failing is treated as a hard stop, not a
  fallback-to-unrestricted-access condition.
