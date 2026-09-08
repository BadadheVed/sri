# Langfuse Prompt Management Integration — Design

Status: Approved by user, 2026-09-04
Author: design session with Claude Code

## 1. Context

SAGE's `ai/` diagnosis service has exactly one hardcoded LLM prompt today —
`ai/ai/investigate.py`'s `SYSTEM_PROMPT_TEMPLATE` (the investigation
agent's system message) plus an inline `incident_summary` f-string (the
user message sent alongside it). Changing either currently requires
editing Python and redeploying `ai/`. The goal is to move both into
Langfuse Prompt Management so prompt changes can be made centrally, with a
safe rollback path (labels/versions) and no code change or redeploy
required for the common case.

**Verified exhaustively** (full-repo search, not assumed): this is the
*only* hardcoded prompt anywhere in the repo. `backend/` (Go) makes zero
LLM calls and has zero prompt text — only incidental comments mention
"LLM". `frontend/` is a blank Next.js scaffold with zero AI logic. Nothing
else in `ai/` builds prompt/message content — `diagnose.py`, `analyzers.py`,
`llm.py`, `mcp_client.py`, `consumer.py`, `callback.py`, `models.py`,
`settings.py` all confirmed clean.

## 2. Decisions (both explicit user choices)

1. **Self-hosted Langfuse**, not Langfuse Cloud — same posture as the
   earlier Pixie decision in this codebase: full control, no third-party
   dependency, even though it's real infrastructure to run.
2. **Both messages migrate** — the system prompt *and* the user-role
   incident summary both become Langfuse-managed content, as one
   "chat"-type prompt with two messages — not just the system message.
   This is the wider-blast-radius-reduction option. It requires
   re-deriving 7 variables as Mustache placeholders and relies on
   LangGraph accepting Langfuse's dict-shaped compiled messages the same
   way it accepts today's tuples — verified with moderate-high confidence
   via LangChain's documented message-coercion utilities (`{"role":...,
   "content":...}` dicts are an explicitly supported input shape); the
   implementation's existing fake-model end-to-end test becomes the
   concrete verification, since LangGraph's `add_messages` state reducer
   coerces the `messages` list into `BaseMessage` objects upstream of
   whichever model is used — this is not something that needs a real LLM
   to test.

## 3. What's migrating (exact current text, for byte-for-byte behavioral
   preservation)

`ai/ai/investigate.py:12-36`:
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
and, inline (not currently a constant), `investigate.py:46-51`:
```python
incident_summary = (
    f"Incident {incident.incident_id}: {incident.kind} {incident.namespace}/{incident.name}\n"
    f"Signals: {[s.type for s in incident.signals]}\n"
    f"First seen: {incident.first_seen}, last seen: {incident.last_seen}"
)
...
result = await agent.ainvoke({"messages": [("system", system_prompt), ("user", incident_summary)]})
```

**Placeholder inventory** (all become `{{double-curly}}` Mustache variables
in Langfuse — different syntax from today's Python `.format()` single-curly):
`tool_names` (system message), `incident_id`, `kind`, `namespace`, `name`,
`signals`, `first_seen`, `last_seen` (user message). The literal
`{"failure_mode": ...}` JSON example in the system text is doubled to
`{{"failure_mode"...}}` today *only* to escape Python's `.format()` — under
Mustache it needs no escaping, collapses back to single braces.

**Exact-preservation detail**: today's f-string renders `signals` as
Python's list-repr (`"['CrashLoopBackOff']"`) and `first_seen`/`last_seen`
via `datetime.__str__()`. To keep byte-for-byte behavioral equivalence,
pass these to `.compile()` as pre-stringified values computed the same way
the old f-string did (`str([s.type for s in incident.signals])`,
`str(incident.first_seen)`), not as raw objects.

## 4. Design

**New module `ai/ai/prompts.py`** — mirrors `ai/ai/llm.py`'s shape (one
small module, settings-driven client construction, no wrapper class):
- `get_prompt_client(settings) -> Langfuse | None` — `None` when
  `settings.langfuse_enabled` is `False` (the default). Same explicit
  opt-in gate this repo already uses for Pixie (`PIXIE_ENABLED`).
- `get_investigation_messages(client, incident, tool_names, *, cache_ttl_seconds) -> list[dict]`
  — returns `[{"role":..., "content":...}, ...]` ready to pass directly as
  `agent.ainvoke({"messages": messages})`. `client=None` → local fallback,
  formatted identically to pre-migration behavior. `client` set → fetches
  the `"production"`-labeled chat prompt via `client.get_prompt(name,
  type="chat", label="production", cache_ttl_seconds=..., fallback=...)`,
  always passing `fallback=` so a Langfuse outage degrades to the same
  safe messages rather than raising. A genuine bug (e.g. wrong `type`
  argument) is allowed to propagate — matches `ai/consumer.py`'s
  deliberate crash-and-let-NATS-redeliver philosophy; no blanket
  try/except in this module.

**Langfuse SDK contract this design leans on** (verified against current
docs, not assumed): fresh cache hit → returned instantly, zero network
call. Stale cache (TTL expired) → served immediately while a background
refresh happens; if that refresh fails, the stale version keeps being
served, no exception. Empty cache + fetch fails + `fallback` given →
fallback returned. Empty cache + fetch fails + no fallback → raises.
**Always passing `fallback=` is what makes requirement 6 (sensible
error handling) hold** — this leans on a real SDK feature rather than
hand-rolling retry/circuit-breaker logic.

**`investigate.py` changes**: delete `SYSTEM_PROMPT_TEMPLATE`,
`_build_system_prompt`, and the inline `incident_summary` construction.
`investigate(incident, model, tools, prompt_client)` gains a 4th
parameter; the two-message construction becomes one call to
`prompts.get_investigation_messages(...)`, then
`agent.ainvoke({"messages": messages})`.

**Threading**: `prompt_client` follows the exact same path `model`/`tools`
already take — constructed once in `main()`, passed down through
`diagnose()` → `handle_message()` → `consumer.run()`.

**Settings** (`ai/ai/settings.py`) — all defaulted + gated behind an
explicit `_enabled` flag, reusing the pattern Pixie already established
for optional integrations (avoids breaking every existing test file's
inline `Settings(...)` construction):
```python
langfuse_enabled: bool = False
langfuse_host: str = ""
langfuse_public_key: str = ""
langfuse_secret_key: str = ""
langfuse_prompt_cache_ttl_seconds: int = 60
```

## 5. Self-hosted Langfuse — same treatment as Pixie

Langfuse has an official Helm chart (`langfuse-k8s` repo, chart name
`langfuse`) — better than Pixie's kustomize-only setup, but it bundles its
own Postgres + ClickHouse + Redis, and chart v2.0.0+ requires a
pre-installed ClickHouse Kubernetes Operator and cert-manager —
cluster-scoped operators, the same category of cluster-singleton risk that
kept Pixie out of `Chart.yaml`'s `dependencies:`. The chart's own docs
describe standalone installation, not subchart use.

**Same decision as Pixie: out-of-band prerequisite, not a `Chart.yaml`
dependency.** SAGE's chart only adds config plumbing to consume an
already-running Langfuse. Since `ai`'s Deployment already has a
`checksum/config` pod annotation (added during the Pixie plan's final
review fix), enabling Langfuse via `helm upgrade` correctly rolls the `ai`
pod with zero extra Helm template work.

## 6. Non-goals

- Migrating any prompt other than the one identified (there is only one).
- Building a general-purpose prompt-templating abstraction beyond what
  this one prompt needs.
- Langfuse tracing/observability features (this integration is
  Prompt Management only, not the broader LLM-observability product).

## 7. Testing

New `ai/tests/test_prompts.py` with a small local fake Langfuse client
(mirrors this repo's existing test-double idiom — `httpx.MockTransport` for
`callback.py`, `GenericFakeChatModel` for LLM calls). Covers: disabled →
no client constructed; local-fallback path renders correctly; happy-path
fetch; **the fallback-path contract test** (simulates what the real SDK
returns when unreachable-and-uncached, asserts the result is still
complete and usable — this is what requirement 6 hinges on); a brace-escaping
regression test (single, not doubled, braces in the JSON example).

`test_investigate.py`'s existing fake-model end-to-end test is where the
dict-vs-tuple message-format question gets real verification, once updated
to pass Langfuse's dict-shaped output.

## 8. Verification

- `cd ai && pytest -v` — full suite green.
- `cd backend && go build ./... && go vet ./... && go test ./...` — unaffected, confirm still clean.
- `cd helm && helm template` for both `langfuse.enabled=false`/`=true` — both render cleanly, keys genuinely absent (not blank) when disabled.
- Manual, deferred until self-hosted Langfuse exists (out-of-band): run the seed script against the real instance, confirm the prompt appears labeled `production`, trigger a real incident, confirm a successful fetch (not fallback) in `ai/`'s logs.
