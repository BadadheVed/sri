# Langfuse Prompt Management Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move SAGE's one hardcoded LLM prompt (`ai/ai/investigate.py`'s system + user messages) into Langfuse Prompt Management, fetched at runtime, so prompt changes ship without a code change or redeploy.

**Architecture:** A new `ai/ai/prompts.py` module wraps the Langfuse Python SDK behind two functions (`get_prompt_client`, `get_investigation_messages`), gated by an explicit `LANGFUSE_ENABLED` settings flag (defaults off). `investigate()` gains a `prompt_client` parameter threaded top-to-bottom the same way `model`/`tools` already are. The SDK's own client-side caching + `fallback=` mechanism (verified live against the installed package) provides the outage-safety story — no hand-rolled retry logic.

**Tech Stack:** Python, `langfuse` SDK 4.x, pydantic-settings (existing pattern), self-hosted Langfuse (official `langfuse-k8s` Helm chart, out-of-band prerequisite — same treatment as this repo's earlier Pixie integration).

**Spec:** `docs/superpowers/specs/2026-09-04-langfuse-prompt-management-design.md`

## Global Constraints

- This is the *only* hardcoded prompt in the repo — verified by full-repo search (`backend/` has zero LLM calls, `frontend/` is a blank scaffold). Do not go looking for others.
- Preserve existing behavior byte-for-byte when Langfuse is disabled (`LANGFUSE_ENABLED=false`, the default) — this is requirement 7 ("keep functionality unchanged").
- Always pass `fallback=` to `client.get_prompt(...)` — this is what makes the outage-handling requirement (6) hold; verified live: an unreachable host with no cache returns the compiled fallback, not an exception.
- Langfuse uses `{{double-curly}}` Mustache placeholders — different from this codebase's existing `.format()` single-curly. Get this right in both the seeded prompt and the local fallback text.
- New Settings fields are defaulted + gated behind `langfuse_enabled`, reusing the pattern this repo already established for Pixie (an earlier optional integration) — this is what keeps `test_llm.py`/`test_consumer.py`/`test_callback.py` unmodified.
- No blanket try/except around Langfuse calls in `prompts.py` — `ai/consumer.py`'s `run()` loop deliberately has no try/except around `handle_message`, relying on NATS redelivery + the backend's idempotent callback; a genuine bug should crash the same way, not be masked.
- Implementers stage changes but never commit (`git add`, no `git commit`) — standing project rule. Note: `docs/superpowers/` is currently excluded by `.gitignore`'s bare `*superpowers*` glob (a known, deliberately-left-as-is situation from an earlier plan) — this plan's own spec/plan docs were staged with `git add -f`; don't "fix" the gitignore as part of this plan, that's a separate decision already made.

---

## Task 1: Settings fields

**Files:**
- Modify: `ai/ai/settings.py`
- Modify: `ai/tests/test_settings.py`
- Modify: `ai/pyproject.toml`
- Modify: `.env.example`

**Interfaces:**
- Produces: `Settings.langfuse_enabled: bool = False`, `Settings.langfuse_host: str = ""`, `Settings.langfuse_public_key: str = ""`, `Settings.langfuse_secret_key: str = ""`, `Settings.langfuse_prompt_cache_ttl_seconds: int = 60` — Task 2's `prompts.py` reads these.

- [ ] **Step 1: Write the failing tests**

Add to `ai/tests/test_settings.py`:
```python
def test_settings_langfuse_defaults_to_disabled():
    s = Settings(
        nats_url="n", backend_callback_url="b", diagnosis_callback_token="d",
        mcp_readonly_url="m", mcp_readonly_token="t",
        llm_provider="anthropic", llm_api_key="k", llm_model="claude-sonnet-4-5",
    )
    assert s.langfuse_enabled is False
    assert s.langfuse_host == ""
    assert s.langfuse_prompt_cache_ttl_seconds == 60


def test_settings_langfuse_overrides_from_environment(monkeypatch):
    for key, val in {
        "NATS_URL": "n", "BACKEND_CALLBACK_URL": "b", "DIAGNOSIS_CALLBACK_TOKEN": "d",
        "MCP_READONLY_URL": "m", "MCP_READONLY_TOKEN": "t",
        "LLM_PROVIDER": "anthropic", "LLM_API_KEY": "k", "LLM_MODEL": "claude-sonnet-4-5",
        "LANGFUSE_ENABLED": "true", "LANGFUSE_HOST": "http://langfuse:3000",
        "LANGFUSE_PUBLIC_KEY": "pk-1", "LANGFUSE_SECRET_KEY": "sk-1",
        "LANGFUSE_PROMPT_CACHE_TTL_SECONDS": "120",
    }.items():
        monkeypatch.setenv(key, val)

    s = Settings()

    assert s.langfuse_enabled is True
    assert s.langfuse_host == "http://langfuse:3000"
    assert s.langfuse_public_key == "pk-1"
    assert s.langfuse_secret_key == "sk-1"
    assert s.langfuse_prompt_cache_ttl_seconds == 120
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ai && .venv/bin/python3 -m pytest tests/test_settings.py -v`
Expected: FAIL — `Settings` has no `langfuse_*` fields yet.

- [ ] **Step 3: Implement**

In `ai/ai/settings.py`, add after `llm_model: str`:
```python
    langfuse_enabled: bool = False
    langfuse_host: str = ""
    langfuse_public_key: str = ""
    langfuse_secret_key: str = ""
    langfuse_prompt_cache_ttl_seconds: int = 60
```

In `ai/pyproject.toml`, add to `dependencies` (after `"pydantic-settings>=2.5",`):
```
    "langfuse>=4.0",
```
(Resolve and confirm the real current version is compatible with `requires-python = ">=3.11"` — at design time this was 4.15.1, `requires-python: >=3.10,<4.0`, compatible; re-check live rather than trusting this note.)

In `.env.example`, add after the existing `LLM_MODEL=claude-sonnet-4-5` line (end of the "ai/ (Python diagnosis service)" section):
```bash

# Langfuse Prompt Management (optional — the investigation prompt has a
# safe local fallback when this is disabled/unreachable; see
# ai/ai/prompts.py). Self-hosted — see helm/README.md before enabling.
LANGFUSE_ENABLED=false
LANGFUSE_HOST=
LANGFUSE_PUBLIC_KEY=
LANGFUSE_SECRET_KEY=
LANGFUSE_PROMPT_CACHE_TTL_SECONDS=60
```

- [ ] **Step 4: Install the new dependency and run tests to verify they pass**

Run: `cd ai && .venv/bin/pip install -e . && .venv/bin/python3 -m pytest tests/test_settings.py -v`
Expected: PASS (4 tests — 2 existing + 2 new).

Then confirm nothing else broke:
Run: `.venv/bin/python3 -m pytest -v`
Expected: all existing tests still pass (`test_llm.py`/`test_consumer.py`/`test_callback.py` untouched by this task and must show zero change in behavior).

- [ ] **Step 5: Cross-check `.env.example` against `settings.py`**

Run: `grep -oE '"[A-Z_]+"' ai/ai/settings.py` — this won't directly work since Python fields aren't quoted strings like Go's `getenv` calls; instead, manually confirm each of the 5 new `langfuse_*` fields has a matching `LANGFUSE_*` line in `.env.example` (pydantic-settings maps `langfuse_prompt_cache_ttl_seconds` → `LANGFUSE_PROMPT_CACHE_TTL_SECONDS` etc. — uppercase, same name).

- [ ] **Step 6: Stage**

```bash
git add ai/ai/settings.py ai/tests/test_settings.py ai/pyproject.toml .env.example
```

---

## Task 2: `ai/ai/prompts.py`

**Files:**
- Create: `ai/ai/prompts.py`
- Create: `ai/tests/test_prompts.py`

**Interfaces:**
- Consumes: `Settings.langfuse_*` fields (Task 1).
- Produces: `PromptClient` (dataclass, fields `langfuse`, `cache_ttl_seconds`), `get_prompt_client(settings: Settings) -> PromptClient | None`, `get_investigation_messages(client: PromptClient | None, incident: PendingIncident, tool_names: list[str]) -> list[dict[str, str]]`, `PROMPT_NAME: str`, `_FALLBACK_MESSAGES: list[dict[str, str]]` — Task 3's `investigate.py` calls `get_investigation_messages`; Task 4's `__main__.py` calls `get_prompt_client`; Task 5's `seed_prompt.py` reuses `PROMPT_NAME`/`_FALLBACK_MESSAGES`.

**Verified live against the installed `langfuse==4.15.1`, don't re-derive**:
`Langfuse(public_key=..., secret_key=..., host=...)` is a real constructor.
`Langfuse.get_prompt(name, version=None, label=None, type="text", cache_ttl_seconds=None, fallback=None, max_retries=None, fetch_timeout_seconds=None)`.
`ChatPromptClient.compile(**kwargs) -> list[dict]` where each dict is `{"role": str, "content": str}`.
Pointing `get_prompt()` at an unreachable host with no cache returns a `ChatPromptClient` built from the `fallback=` argument (confirmed by direct execution, not docs) — `.compile()` on it works exactly like a real fetch.

- [ ] **Step 1: Write the failing tests**

```python
# ai/tests/test_prompts.py
from datetime import datetime, timezone

from ai.models import PendingIncident, SignalPayload
from ai.prompts import PromptClient, _FALLBACK_MESSAGES, get_investigation_messages, get_prompt_client
from ai.settings import Settings


def _incident() -> PendingIncident:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return PendingIncident(
        incident_id="incident-1", namespace="default", kind="Pod", name="web-1",
        group_key="default/Pod/web-1",
        signals=[SignalPayload(type="CrashLoopBackOff", severity="warning", timestamp=now, raw="")],
        first_seen=now, last_seen=now,
    )


def _settings(**overrides) -> Settings:
    base = dict(
        nats_url="n", backend_callback_url="b", diagnosis_callback_token="d",
        mcp_readonly_url="m", mcp_readonly_token="t",
        llm_provider="anthropic", llm_api_key="k", llm_model="claude-sonnet-4-5",
    )
    base.update(overrides)
    return Settings(**base)


def test_get_prompt_client_returns_none_when_disabled():
    assert get_prompt_client(_settings(langfuse_enabled=False)) is None


def test_get_investigation_messages_local_fallback_when_disabled():
    messages = get_investigation_messages(None, _incident(), ["get_pod_logs"])
    assert messages[0]["role"] == "system"
    assert "get_pod_logs" in messages[0]["content"]
    assert messages[1]["role"] == "user"
    assert "incident-1" in messages[1]["content"]
    assert "CrashLoopBackOff" in messages[1]["content"]


def test_get_investigation_messages_local_fallback_handles_no_tools():
    messages = get_investigation_messages(None, _incident(), [])
    assert "none" in messages[0]["content"]


class _FakeCompiledPrompt:
    """Mimics langfuse.model.ChatPromptClient's real, verified .compile()
    contract: substitutes {{var}} across every message's content, returns
    a plain list of {"role", "content"} dicts."""

    def __init__(self, messages):
        self._messages = messages

    def compile(self, **kwargs):
        compiled = []
        for m in self._messages:
            content = m["content"]
            for key, value in kwargs.items():
                content = content.replace("{{" + key + "}}", value)
            compiled.append({"role": m["role"], "content": content})
        return compiled


class _FakeLangfuseClient:
    """Mimics the two real Langfuse.get_prompt() behaviors this module
    depends on: a normal fetch when should_fail is False, and echoing back
    the fallback= argument when should_fail is True — verified live
    against the real SDK to be exactly what it does when unreachable with
    an empty cache."""

    def __init__(self, template_messages, *, should_fail=False):
        self._template_messages = template_messages
        self.should_fail = should_fail
        self.last_call_kwargs = None

    def get_prompt(self, name, **kwargs):
        self.last_call_kwargs = {"name": name, **kwargs}
        if self.should_fail:
            return _FakeCompiledPrompt(kwargs["fallback"])
        return _FakeCompiledPrompt(self._template_messages)


def test_get_investigation_messages_fetches_from_client():
    fake = _FakeLangfuseClient([
        {"role": "system", "content": "CUSTOM SYSTEM {{tool_names}}"},
        {"role": "user", "content": "CUSTOM USER {{incident_id}}"},
    ])
    client = PromptClient(langfuse=fake, cache_ttl_seconds=60)

    messages = get_investigation_messages(client, _incident(), ["get_pod_logs"])

    assert messages == [
        {"role": "system", "content": "CUSTOM SYSTEM get_pod_logs"},
        {"role": "user", "content": "CUSTOM USER incident-1"},
    ]
    assert fake.last_call_kwargs["type"] == "chat"
    assert fake.last_call_kwargs["label"] == "production"
    assert fake.last_call_kwargs["fallback"] is _FALLBACK_MESSAGES
    assert fake.last_call_kwargs["cache_ttl_seconds"] == 60


def test_get_investigation_messages_uses_fallback_when_client_unreachable():
    # Simulates exactly what the real Langfuse SDK does when the host is
    # unreachable and no cache exists: get_prompt() returns a compiled
    # prompt built from the fallback= argument, not an exception.
    fake = _FakeLangfuseClient([], should_fail=True)
    client = PromptClient(langfuse=fake, cache_ttl_seconds=60)

    messages = get_investigation_messages(client, _incident(), ["get_pod_logs"])

    assert messages[0]["role"] == "system"
    assert "get_pod_logs" in messages[0]["content"]
    assert messages[1]["role"] == "user"
    assert "incident-1" in messages[1]["content"]


def test_fallback_messages_have_single_braces_not_doubled():
    # Guards the Python .format()-escaping-vs-Mustache mistake: the old
    # SYSTEM_PROMPT_TEMPLATE doubled its literal JSON example's braces only
    # to escape .format(). This is now a plain string literal (no .format()
    # involved) and must NOT have doubled braces.
    system_content = _FALLBACK_MESSAGES[0]["content"]
    assert '{"failure_mode"' in system_content
    assert '{{"failure_mode"' not in system_content
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ai && .venv/bin/python3 -m pytest tests/test_prompts.py -v`
Expected: FAIL — `ai.prompts` doesn't exist yet.

- [ ] **Step 3: Implement**

```python
# ai/ai/prompts.py
from __future__ import annotations

from dataclasses import dataclass

from langfuse import Langfuse

from ai.models import PendingIncident
from ai.settings import Settings

PROMPT_NAME = "sage-investigation"

# Verbatim migration of the pre-Langfuse hardcoded prompt (Mustache syntax
# instead of Python .format()'s single-curly) — single source of truth for
# both the Langfuse seed content (see seed_prompt.py) and the local
# fallback used when Langfuse is disabled or unreachable.
_FALLBACK_MESSAGES: list[dict[str, str]] = [
    {
        "role": "system",
        "content": (
            "You are SAGE's incident diagnosis agent. You investigate a\n"
            "Kubernetes pod failure using read-only tools ({{tool_names}}) and must end your\n"
            "investigation with exactly one JSON object on its own line, matching this\n"
            "shape:\n"
            "\n"
            '{"failure_mode": "<short name>", "recommended_action": "restart_pod" | "none", "confidence": <0.0-1.0>}\n'
            "\n"
            "Rules:\n"
            "- recommended_action MUST be exactly \"restart_pod\" or \"none\" — no other\n"
            "  value is ever wired to an executor, so anything else is silently\n"
            "  equivalent to guessing wrong.\n"
            "- Use \"none\" whenever restarting the pod would not plausibly fix the\n"
            "  underlying problem (a bad image reference, a missing secret/config, an\n"
            "  unschedulable resource request) — do not default to \"restart_pod\" just\n"
            "  because you're unsure; lower confidence instead.\n"
            "- Investigate before concluding: call at least one tool unless the incident\n"
            "  summary alone is unambiguous.\n"
            "- If resource exhaustion, latency degradation, or an error-rate spike could\n"
            "  explain the failure and a resource/traffic tool is available, use it\n"
            "  before concluding."
        ),
    },
    {
        "role": "user",
        "content": (
            "Incident {{incident_id}}: {{kind}} {{namespace}}/{{name}}\n"
            "Signals: {{signals}}\n"
            "First seen: {{first_seen}}, last seen: {{last_seen}}"
        ),
    },
]


def _compile_literal(text: str, variables: dict[str, str]) -> str:
    """Local {{var}} substitution for the Langfuse-disabled path — kept
    independent of any langfuse.model internals (constructing a real
    ChatPromptClient requires a full Prompt_Chat object, more machinery
    than this path needs) so it never touches the SDK at all."""
    for key, value in variables.items():
        text = text.replace("{{" + key + "}}", value)
    return text


@dataclass
class PromptClient:
    """Bundles the Langfuse SDK client with the configured cache TTL, so
    callers (investigate.py) don't need a Settings object just to read one
    int — keeps investigate()/diagnose() free of Settings, matching this
    codebase's existing layering where settings stop at handle_message."""

    langfuse: Langfuse
    cache_ttl_seconds: int


def get_prompt_client(settings: Settings) -> PromptClient | None:
    """None when Langfuse is disabled (LANGFUSE_ENABLED=false, the
    default) — same explicit opt-in gate this repo already uses for Pixie
    (PIXIE_ENABLED). No client is constructed in that case, so there's no
    risk of the SDK doing anything (network, validation) against blank
    credentials."""
    if not settings.langfuse_enabled:
        return None
    return PromptClient(
        langfuse=Langfuse(
            public_key=settings.langfuse_public_key,
            secret_key=settings.langfuse_secret_key,
            host=settings.langfuse_host,
        ),
        cache_ttl_seconds=settings.langfuse_prompt_cache_ttl_seconds,
    )


def get_investigation_messages(
    client: PromptClient | None, incident: PendingIncident, tool_names: list[str],
) -> list[dict[str, str]]:
    """Returns [{"role": ..., "content": ...}, ...] ready to pass directly
    as agent.ainvoke({"messages": messages}).

    client=None (Langfuse disabled) -> local fallback text, substituted
    locally, never touching the network.

    client set -> fetches the "production"-labeled chat prompt from
    Langfuse, always passing fallback=_FALLBACK_MESSAGES. The Langfuse SDK
    itself handles unreachability: a fresh cache hit returns instantly; a
    stale cache hit is served immediately while a background refresh
    happens (and keeps being served if that refresh fails); an empty cache
    plus a failed fetch returns the fallback we passed — verified live
    against the installed SDK, not just documentation. A bug in our own
    call (e.g. a wrong `type` argument) is allowed to propagate — matches
    ai/consumer.py's deliberate crash-and-let-NATS-redeliver philosophy;
    no blanket try/except here.
    """
    variables = {
        "tool_names": ", ".join(tool_names) or "none",
        "incident_id": incident.incident_id,
        "kind": incident.kind,
        "namespace": incident.namespace,
        "name": incident.name,
        "signals": str([s.type for s in incident.signals]),
        "first_seen": str(incident.first_seen),
        "last_seen": str(incident.last_seen),
    }
    if client is None:
        return [
            {"role": m["role"], "content": _compile_literal(m["content"], variables)}
            for m in _FALLBACK_MESSAGES
        ]
    prompt = client.langfuse.get_prompt(
        PROMPT_NAME,
        type="chat",
        label="production",
        cache_ttl_seconds=client.cache_ttl_seconds,
        fallback=_FALLBACK_MESSAGES,
    )
    return list(prompt.compile(**variables))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ai && .venv/bin/python3 -m pytest tests/test_prompts.py -v`
Expected: PASS (7 tests).

- [ ] **Step 5: Stage**

```bash
git add ai/ai/prompts.py ai/tests/test_prompts.py
```

---

## Task 3: `ai/ai/investigate.py`

**Files:**
- Modify: `ai/ai/investigate.py`
- Modify: `ai/tests/test_investigate.py`

**Interfaces:**
- Consumes: `PromptClient`, `get_investigation_messages` (Task 2).
- Produces: `investigate(incident: PendingIncident, model: BaseChatModel, tools: list[BaseTool], prompt_client: PromptClient | None) -> Diagnosis` — signature change (new 4th parameter) that Task 4's `diagnose.py` must match.

This is where the plan's one real technical uncertainty (does LangGraph accept Langfuse's dict-shaped `{"role":..., "content":...}` messages the same way it accepts today's `(role, content)` tuples?) gets a concrete, real answer — via the existing fake-model end-to-end test, once it's exercising the new dict-shaped path. LangGraph's `add_messages` state reducer coerces the `messages` list into `BaseMessage` objects upstream of the model itself, so a fake model is sufficient to test this — it doesn't require a real LLM.

- [ ] **Step 1: Update the failing test**

In `ai/tests/test_investigate.py`:

1. Delete `test_build_system_prompt_lists_given_tool_names` and `test_build_system_prompt_handles_no_tools` (lines 58-75) — this behavior now lives in `test_prompts.py`, already covered by Task 2.

2. Update `test_investigate_returns_diagnosis_from_fake_model_final_answer` to pass the new parameter:
```python
async def test_investigate_returns_diagnosis_from_fake_model_final_answer():
    now = datetime.now(timezone.utc)
    incident = PendingIncident(
        incident_id="incident-1", namespace="default", kind="Pod", name="web-1",
        group_key="default/Pod/web-1",
        signals=[SignalPayload(type="ImagePullError", severity="warning", timestamp=now, raw="")],
        first_seen=now, last_seen=now,
    )
    fake_model = GenericFakeChatModel(
        messages=iter([AIMessage(content='{"failure_mode": "ImagePullError", "recommended_action": "none", "confidence": 0.8}')])
    )

    diagnosis = await investigate(incident, fake_model, tools=[], prompt_client=None)

    assert diagnosis.failure_mode == "ImagePullError"
    assert diagnosis.recommended_action == "none"
    assert diagnosis.confidence == 0.8
```
(`prompt_client=None` means `get_investigation_messages` returns the local-fallback, dict-shaped messages — this is exactly the path that needs verifying against real LangGraph message coercion.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ai && .venv/bin/python3 -m pytest tests/test_investigate.py -v`
Expected: FAIL — `investigate()` doesn't accept a `prompt_client` argument yet.

- [ ] **Step 3: Implement**

Replace `ai/ai/investigate.py`'s top section (everything from the imports through the end of `investigate()`) with:
```python
from __future__ import annotations

import json
import re

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.tools import BaseTool
from langgraph.prebuilt import create_react_agent

from ai.models import Diagnosis, PendingIncident
from ai.prompts import PromptClient, get_investigation_messages


async def investigate(
    incident: PendingIncident, model: BaseChatModel, tools: list[BaseTool], prompt_client: PromptClient | None,
) -> Diagnosis:
    """LLM-driven fallback for incidents no rule-based analyzer matched.
    Uses LangGraph's prebuilt ReAct agent loop rather than a hand-built
    StateGraph — this fallback only needs "call tools, reason, repeat until
    done," which is exactly what create_react_agent already implements.
    The system/user messages come from Langfuse Prompt Management (or a
    local fallback when Langfuse is disabled/unreachable) — see
    ai/ai/prompts.py."""
    agent = create_react_agent(model, tools)
    messages = get_investigation_messages(prompt_client, incident, [t.name for t in tools])
    result = await agent.ainvoke({"messages": messages})
    final_message = result["messages"][-1].content
    return _parse_diagnosis(final_message)
```
(`SYSTEM_PROMPT_TEMPLATE` and `_build_system_prompt` are deleted entirely.
`_parse_diagnosis` — everything from `def _parse_diagnosis(text: str) -> Diagnosis:` to end of file — is UNCHANGED, leave it exactly as-is.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ai && .venv/bin/python3 -m pytest tests/test_investigate.py -v`
Expected: PASS (5 tests — the 4 `_parse_diagnosis` tests unchanged, plus the updated end-to-end test). If the end-to-end test fails specifically at the `agent.ainvoke` call with a message-format error, this is the plan's flagged uncertainty surfacing for real — read the actual error, it will indicate whether dict-shaped messages need converting to tuples or `BaseMessage` instances before being passed in; adjust `get_investigation_messages`'s return shape or `investigate()`'s call site accordingly and note the deviation in your report.

- [ ] **Step 5: Stage**

```bash
git add ai/ai/investigate.py ai/tests/test_investigate.py
```

---

## Task 4: Thread `prompt_client` through `diagnose`/`consumer`/`__main__`

**Files:**
- Modify: `ai/ai/diagnose.py`
- Modify: `ai/ai/consumer.py`
- Modify: `ai/ai/__main__.py`
- Modify: `ai/tests/test_diagnose.py`
- Modify: `ai/tests/test_consumer.py`

**Interfaces:**
- Consumes: `investigate()`'s new signature (Task 3), `get_prompt_client` (Task 2).
- Produces: `diagnose(incident, model, tools, prompt_client)`, `handle_message(data, settings, model, tools, prompt_client)`, `consumer.run(settings, model, tools, prompt_client)` — all gain the same 4th parameter, threaded from `main()`.

- [ ] **Step 1: Update the failing tests**

In `ai/tests/test_diagnose.py`, add `prompt_client=None` to both call sites:
```python
async def test_diagnose_uses_rule_when_available_without_touching_the_model():
    fake_model = GenericFakeChatModel(messages=iter([]))
    diagnosis = await diagnose(_incident("CrashLoopBackOff"), fake_model, tools=[], prompt_client=None)
    assert diagnosis.failure_mode == "CrashLoopBackOff"


async def test_diagnose_falls_back_to_llm_when_no_rule_matches():
    fake_model = GenericFakeChatModel(
        messages=iter([AIMessage(content='{"failure_mode": "SchedulingFailed", "recommended_action": "none", "confidence": 0.5}')])
    )
    diagnosis = await diagnose(_incident("SchedulingFailed"), fake_model, tools=[], prompt_client=None)
    assert diagnosis.failure_mode == "SchedulingFailed"
    assert diagnosis.recommended_action == "none"
```

In `ai/tests/test_consumer.py`, update the one call site:
```python
    with patch("ai.consumer.post_diagnosis", new=AsyncMock()) as mock_post:
        # model=None, tools=[], prompt_client=None is safe here: CrashLoopBackOff
        # matches a rule in ai.analyzers, so diagnose() never touches the
        # model or Langfuse.
        await handle_message(data, _settings(), model=None, tools=[], prompt_client=None)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ai && .venv/bin/python3 -m pytest tests/test_diagnose.py tests/test_consumer.py -v`
Expected: FAIL — `diagnose()`/`handle_message()` don't accept `prompt_client` yet.

- [ ] **Step 3: Implement**

In `ai/ai/diagnose.py`, replace the whole file:
```python
from __future__ import annotations

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.tools import BaseTool

from ai.analyzers import run_rule_based_analyzers
from ai.investigate import investigate
from ai.models import Diagnosis, PendingIncident
from ai.prompts import PromptClient


async def diagnose(
    incident: PendingIncident, model: BaseChatModel, tools: list[BaseTool], prompt_client: PromptClient | None,
) -> Diagnosis:
    """Rules first (fast, free, deterministic), LLM investigation only when
    no rule matches — the diagnosis engine docs/overview.md originally
    described for ai/."""
    diagnosis = run_rule_based_analyzers(incident)
    if diagnosis is not None:
        return diagnosis
    return await investigate(incident, model, tools, prompt_client)
```

In `ai/ai/consumer.py`, change `run()` and `handle_message()`'s signatures and bodies (everything else in the file — imports, `STREAM_NAME`/`PENDING_SUBJECT`/`DURABLE_NAME`, the `nats.connect`/`pull_subscribe` setup — is UNCHANGED):
```python
from ai.prompts import PromptClient
```
(add this import alongside the existing ones)
```python
async def run(settings: Settings, model, tools, prompt_client: PromptClient | None) -> None:
    """Durable JetStream consumer loop: pulls pending incidents published by
    backend/, diagnoses each, POSTs the result back, then acks. Runs
    forever — called once from __main__.py at process startup."""
    nc = await nats.connect(settings.nats_url)
    js = nc.jetstream()
    sub = await js.pull_subscribe(
        PENDING_SUBJECT,
        durable=DURABLE_NAME,
        config=ConsumerConfig(ack_policy=AckPolicy.EXPLICIT, ack_wait=180),
    )

    logger.info("ai/ consumer started, subscribed to %s", PENDING_SUBJECT)
    while True:
        try:
            msgs = await sub.fetch(1, timeout=5)
        except TimeoutError:
            continue
        for msg in msgs:
            await handle_message(msg.data, settings, model, tools, prompt_client)
            await msg.ack()


async def handle_message(data: bytes, settings: Settings, model, tools, prompt_client: PromptClient | None) -> None:
    incident = PendingIncident.model_validate_json(data)
    logger.info("diagnosing incident %s (%s/%s)", incident.incident_id, incident.namespace, incident.name)
    diagnosis = await diagnose(incident, model, tools, prompt_client)
    logger.info(
        "diagnosis for %s: failure_mode=%s recommended_action=%s confidence=%s",
        incident.incident_id, diagnosis.failure_mode, diagnosis.recommended_action, diagnosis.confidence,
    )
    await post_diagnosis(settings, incident.incident_id, diagnosis)
```

In `ai/ai/__main__.py`, replace the whole file:
```python
from __future__ import annotations

import asyncio
import logging

from ai import consumer
from ai.llm import get_chat_model
from ai.mcp_client import get_readonly_tools
from ai.prompts import get_prompt_client
from ai.settings import load_settings


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = load_settings()
    model = get_chat_model(settings)
    tools = await get_readonly_tools(settings)
    prompt_client = get_prompt_client(settings)
    await consumer.run(settings, model, tools, prompt_client)


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ai && .venv/bin/python3 -m pytest -v`
Expected: PASS — full suite, all files (this is the point where every call site across the codebase is exercised together).

- [ ] **Step 5: Stage**

```bash
git add ai/ai/diagnose.py ai/ai/consumer.py ai/ai/__main__.py ai/tests/test_diagnose.py ai/tests/test_consumer.py
```

---

## Task 5: Seed script

**Files:**
- Create: `ai/ai/seed_prompt.py`
- Create: `ai/tests/test_seed_prompt.py`

**Interfaces:**
- Consumes: `PROMPT_NAME`, `_FALLBACK_MESSAGES` (Task 2), `load_settings` (existing).
- Produces: `seed() -> None`, runnable via `python -m ai.seed_prompt`.

- [ ] **Step 1: Write the failing tests**

```python
# ai/tests/test_seed_prompt.py
from unittest.mock import MagicMock, patch

import pytest

from ai.seed_prompt import seed
from ai.settings import Settings


def _settings(**overrides) -> Settings:
    base = dict(
        nats_url="n", backend_callback_url="b", diagnosis_callback_token="d",
        mcp_readonly_url="m", mcp_readonly_token="t",
        llm_provider="anthropic", llm_api_key="k", llm_model="claude-sonnet-4-5",
        langfuse_enabled=True, langfuse_host="http://localhost:3000",
        langfuse_public_key="pk-test", langfuse_secret_key="sk-lf-test",
    )
    base.update(overrides)
    return Settings(**base)


def test_seed_raises_when_langfuse_disabled():
    with patch("ai.seed_prompt.load_settings", return_value=_settings(langfuse_enabled=False)):
        with pytest.raises(SystemExit):
            seed()


def test_seed_creates_chat_prompt_labeled_production():
    fake_client = MagicMock()
    with patch("ai.seed_prompt.load_settings", return_value=_settings()), \
         patch("ai.seed_prompt.Langfuse", return_value=fake_client):
        seed()
    fake_client.create_prompt.assert_called_once()
    _, kwargs = fake_client.create_prompt.call_args
    assert kwargs["name"] == "sage-investigation"
    assert kwargs["type"] == "chat"
    assert kwargs["labels"] == ["production"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ai && .venv/bin/python3 -m pytest tests/test_seed_prompt.py -v`
Expected: FAIL — `ai.seed_prompt` doesn't exist yet.

- [ ] **Step 3: Implement**

```python
# ai/ai/seed_prompt.py
"""One-time (or safe-to-rerun) bootstrap: creates/updates the
"sage-investigation" chat prompt in Langfuse from this module's own
fallback text, so the seeded content and the local fallback can never
drift apart.

Run via: python -m ai.seed_prompt

Note: Langfuse prompts are versioned/append-only — re-running this creates
a new version and re-labels it "production" rather than being a strict
no-op. Safe to re-run, not idempotent in the strict sense.
"""
from __future__ import annotations

from langfuse import Langfuse

from ai.prompts import PROMPT_NAME, _FALLBACK_MESSAGES
from ai.settings import load_settings


def seed() -> None:
    settings = load_settings()
    if not settings.langfuse_enabled:
        raise SystemExit(
            "LANGFUSE_ENABLED is false — nothing to seed. "
            "Set it and the other LANGFUSE_* vars first."
        )
    client = Langfuse(
        public_key=settings.langfuse_public_key,
        secret_key=settings.langfuse_secret_key,
        host=settings.langfuse_host,
    )
    client.create_prompt(name=PROMPT_NAME, type="chat", prompt=_FALLBACK_MESSAGES, labels=["production"])
    print(f"Seeded/updated prompt {PROMPT_NAME!r} in Langfuse, labeled 'production'.")


if __name__ == "__main__":
    seed()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ai && .venv/bin/python3 -m pytest tests/test_seed_prompt.py -v`
Expected: PASS (2 tests). Then full suite: `.venv/bin/python3 -m pytest -v` — expect all prior tests plus this task's, all green.

- [ ] **Step 5: Stage**

```bash
git add ai/ai/seed_prompt.py ai/tests/test_seed_prompt.py
```

---

## Task 6: Helm plumbing

**Files:**
- Modify: `helm/values.yaml`
- Modify: `helm/templates/configmap.yaml`
- Modify: `helm/templates/secret.yaml`
- Modify: `helm/README.md`

**Interfaces:** none new — consumes the env var names Task 1 defined (`LANGFUSE_ENABLED`, `LANGFUSE_HOST`, `LANGFUSE_PUBLIC_KEY`, `LANGFUSE_SECRET_KEY`, `LANGFUSE_PROMPT_CACHE_TTL_SECONDS`).

No new Deployment/Service/RBAC — `ai`'s existing Deployment already renders `envFrom: configMapRef + secretRef`, and (from the earlier Pixie plan's final-review fix) already has a `checksum/config` pod annotation, so enabling Langfuse via `helm upgrade` correctly rolls the `ai` pod with zero additional Helm template work.

- [ ] **Step 1: Add the `langfuse` values block**

In `helm/values.yaml`, add after the existing `llm:` block (before `pxMetrics:`):
```yaml
langfuse:
  # Off by default — self-hosted Langfuse is a real out-of-band
  # infrastructure prerequisite (its own Postgres+ClickHouse+Redis+web
  # stack), not something this chart installs. See helm/README.md before
  # setting enabled: true.
  enabled: false
  host: ""   # e.g. http://langfuse-web.langfuse.svc.cluster.local:3000
  promptCacheTtlSeconds: 60

langfusePublicKey: ""
langfuseSecretKey: ""   # required when langfuse.enabled is true
```

- [ ] **Step 2: Render the new ConfigMap keys, conditionally**

In `helm/templates/configmap.yaml`, add after the existing `LLM_MODEL` line:
```yaml
  LANGFUSE_ENABLED: {{ .Values.langfuse.enabled | quote }}
  {{- if .Values.langfuse.enabled }}
  LANGFUSE_HOST: {{ required "langfuse.host is required when langfuse.enabled is true" .Values.langfuse.host | quote }}
  LANGFUSE_PUBLIC_KEY: {{ required "langfusePublicKey is required when langfuse.enabled is true" .Values.langfusePublicKey | quote }}
  LANGFUSE_PROMPT_CACHE_TTL_SECONDS: {{ .Values.langfuse.promptCacheTtlSeconds | quote }}
  {{- end }}
```

- [ ] **Step 3: Render the new Secret key, conditionally**

In `helm/templates/secret.yaml`, add after the existing `LLM_API_KEY` line, still inside the `{{- if not .Values.secrets.existingSecret }}` block:
```yaml
  {{- if .Values.langfuse.enabled }}
  LANGFUSE_SECRET_KEY: {{ required "langfuseSecretKey is required when langfuse.enabled is true" .Values.langfuseSecretKey | quote }}
  {{- end }}
```

- [ ] **Step 4: Add a Langfuse prerequisite section to `helm/README.md`**

Read the file first to find the exact end of the existing "## Pixie (eBPF observability) prerequisite" section, then add a new section after it:
```markdown

## Langfuse (prompt management) prerequisite

`langfuse.enabled` (default `false`) makes `ai/` fetch its investigation
prompt from [Langfuse](https://langfuse.com) instead of using a local
fallback — see `ai/ai/prompts.py`. Not installed by this chart (real
infrastructure — Langfuse's self-hosted stack bundles its own
Postgres+ClickHouse+Redis, and its Helm chart v2.0.0+ requires a
pre-installed ClickHouse Kubernetes Operator and cert-manager, same
category of cluster-scoped prerequisite as Pixie's OLM requirement):

1. Deploy self-hosted Langfuse (re-verify against
   [langfuse/langfuse-k8s](https://github.com/langfuse/langfuse-k8s) at
   execution time — chart major versions change prerequisites):
   ```bash
   helm repo add langfuse https://langfuse.github.io/langfuse-k8s
   helm repo update
   helm install langfuse langfuse/langfuse -n langfuse --create-namespace
   ```
2. Once Langfuse is reachable, seed the investigation prompt:
   ```bash
   LANGFUSE_ENABLED=true LANGFUSE_HOST=<url> \
   LANGFUSE_PUBLIC_KEY=<key> LANGFUSE_SECRET_KEY=<secret> \
   python -m ai.seed_prompt
   ```
3. Deploy SAGE with Langfuse enabled:
   ```bash
   helm upgrade sage ./helm -n sage -f helm/values.secret.yaml \
     --set langfuse.enabled=true \
     --set langfuse.host=<your-langfuse-url> \
     --set langfusePublicKey=<your-public-key> \
     --set langfuseSecretKey=<your-secret-key>
   ```
```

- [ ] **Step 5: Verify the chart renders, both with and without Langfuse enabled**

```bash
cd helm
helm template sage . -f values.secret.yaml --set nats.enabled=true \
  --set mcpReadonly.image.repository=test --set mcpReadonly.image.tag=test \
  --set ai.image.repository=test --set ai.image.tag=test \
  --set llm.provider=anthropic --set llm.model=test --set llm.apiKey=test \
  > /tmp/rendered-langfuse-disabled.yaml
grep 'LANGFUSE_ENABLED: "false"' /tmp/rendered-langfuse-disabled.yaml   # present
grep 'LANGFUSE_HOST' /tmp/rendered-langfuse-disabled.yaml                # absent — conditional block skipped

helm template sage . -f values.secret.yaml --set nats.enabled=true \
  --set mcpReadonly.image.repository=test --set mcpReadonly.image.tag=test \
  --set ai.image.repository=test --set ai.image.tag=test \
  --set llm.provider=anthropic --set llm.model=test --set llm.apiKey=test \
  --set langfuse.enabled=true --set langfuse.host=http://langfuse:3000 \
  --set langfusePublicKey=pk-test --set langfuseSecretKey=sk-test \
  > /tmp/rendered-langfuse-enabled.yaml
grep 'LANGFUSE_ENABLED: "true"' /tmp/rendered-langfuse-enabled.yaml
grep 'LANGFUSE_HOST: "http://langfuse:3000"' /tmp/rendered-langfuse-enabled.yaml
grep 'LANGFUSE_SECRET_KEY' /tmp/rendered-langfuse-enabled.yaml

# confirm the checksum/config annotation on ai's Deployment genuinely
# differs between the two renders (proving a real helm upgrade would roll
# the ai pod when Langfuse gets enabled)
grep -A1 'checksum/config' /tmp/rendered-langfuse-disabled.yaml
grep -A1 'checksum/config' /tmp/rendered-langfuse-enabled.yaml
```
Expected: both renders exit 0; disabled render has no `LANGFUSE_HOST`/`LANGFUSE_PUBLIC_KEY`/`LANGFUSE_SECRET_KEY`/`LANGFUSE_PROMPT_CACHE_TTL_SECONDS` keys at all; enabled render has all of them; the `checksum/config` values differ between the two renders.

- [ ] **Step 6: Stage**

```bash
git add helm/values.yaml helm/templates/configmap.yaml helm/templates/secret.yaml helm/README.md
```

---

## Task 7: Full regression

**Files:** none — verification only.

- [ ] **Step 1: Full `ai/` test suite**

Run: `cd ai && .venv/bin/python3 -m pytest -v`
Expected: all tests pass (Tasks 1-5's new/updated tests plus every pre-existing test file untouched in behavior).

- [ ] **Step 2: Confirm `backend/` is unaffected**

Run: `cd backend && go build ./... && go vet ./... && go test ./...`
Expected: clean — this plan touches only `ai/` and `helm/`, backend/ Go code should show zero diff.

- [ ] **Step 3: Both Helm render states** (repeat Task 6 Step 5's commands if not already fresh)

- [ ] **Step 4: Final `git status` sanity check**

Run: `git status --short` from the repo root — confirm every file this plan touched is staged, nothing committed.

---

## Testing summary

- `test_settings.py`: 2 new tests (Langfuse defaults, Langfuse overrides).
- `test_prompts.py` (new): 7 tests — disabled-client, local-fallback (2 cases), happy-path fetch (asserts exact call kwargs: `type="chat"`, `label="production"`, `fallback=` identity, `cache_ttl_seconds`), the fallback-on-unreachable contract test (what requirement 6 hinges on), and the brace-escaping regression guard.
- `test_investigate.py`: 2 tests removed (moved to `test_prompts.py`), 1 updated (now the concrete verification of LangGraph accepting dict-shaped messages).
- `test_diagnose.py`/`test_consumer.py`: call sites updated for the new parameter, no new test cases needed (existing assertions already prove the LLM/Langfuse aren't touched on the rule-based path).
- `test_seed_prompt.py` (new): 2 tests — refuses when disabled, calls `create_prompt` with the right `name`/`type`/`labels`.

## Verification

- `cd ai && .venv/bin/python3 -m pytest -v` — full suite green.
- `cd backend && go build ./... && go vet ./... && go test ./...` — unaffected, confirm still clean.
- `cd helm && helm template ...` for both `langfuse.enabled=false`/`=true` — both render cleanly, keys genuinely absent (not blank) when disabled, `checksum/config` differs between the two states.
- Manual, deferred until self-hosted Langfuse actually exists (out-of-band, same as the Pixie plan's Vizier prerequisite): run `python -m ai.seed_prompt` against the real instance, confirm the prompt appears in the Langfuse UI labeled `production`, then trigger a real incident and confirm `ai/`'s logs show a successful fetch (not a fallback) and the LLM receives the expected two messages, and that editing the prompt in the Langfuse UI (no redeploy) changes the next incident's investigation without any code change.

## Execution mechanism

Same as the two prior plans in this repo: `superpowers:subagent-driven-development`, staged only, never committed, per this session's standing rule.
