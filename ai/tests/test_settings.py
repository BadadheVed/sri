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
