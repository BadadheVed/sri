import pytest
from pydantic import ValidationError

from ai.settings import Settings


def test_settings_requires_all_fields(monkeypatch):
    for key in [
        "NATS_URL", "BACKEND_CALLBACK_URL", "DIAGNOSIS_CALLBACK_TOKEN",
        "MCP_READONLY_URL", "MCP_READONLY_TOKEN", "LLM_PROVIDER", "LLM_API_KEY", "LLM_MODEL",
    ]:
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(ValidationError):
        Settings()


def test_settings_loads_from_environment(monkeypatch):
    monkeypatch.setenv("NATS_URL", "nats://localhost:4222")
    monkeypatch.setenv("BACKEND_CALLBACK_URL", "http://backend:8080")
    monkeypatch.setenv("DIAGNOSIS_CALLBACK_TOKEN", "token-a")
    monkeypatch.setenv("MCP_READONLY_URL", "http://mcp-readonly:8091")
    monkeypatch.setenv("MCP_READONLY_TOKEN", "token-b")
    monkeypatch.setenv("LLM_PROVIDER", "anthropic")
    monkeypatch.setenv("LLM_API_KEY", "sk-test")
    monkeypatch.setenv("LLM_MODEL", "claude-sonnet-4-5")

    settings = Settings()

    assert settings.nats_url == "nats://localhost:4222"
    assert settings.llm_provider == "anthropic"
    assert settings.llm_model == "claude-sonnet-4-5"


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
