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
