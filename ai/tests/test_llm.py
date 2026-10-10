import pytest
from langchain_anthropic import ChatAnthropic
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_openai import ChatOpenAI

from ai.llm import get_chat_model
from ai.settings import Settings


def _settings(**overrides) -> Settings:
    base = dict(
        nats_url="nats://localhost:4222", backend_callback_url="http://backend:8080",
        diagnosis_callback_token="t", mcp_readonly_url="http://mcp:8091", mcp_readonly_token="t",
        llm_provider="anthropic", llm_api_key="sk-test", llm_model="claude-sonnet-4-5",
    )
    base.update(overrides)
    return Settings(**base)


def test_anthropic_provider_returns_chat_anthropic():
    model = get_chat_model(_settings(llm_provider="anthropic"))
    assert isinstance(model, ChatAnthropic)


def test_openai_provider_returns_chat_openai():
    model = get_chat_model(_settings(llm_provider="openai", llm_model="gpt-4o"))
    assert isinstance(model, ChatOpenAI)


def test_openrouter_provider_returns_chat_openai_pointed_at_openrouter():
    model = get_chat_model(_settings(llm_provider="openrouter", llm_model="deepseek/deepseek-chat"))
    assert isinstance(model, ChatOpenAI)
    # ChatOpenAI's base_url field name has moved before across langchain-openai
    # versions (openai_api_base vs base_url) — see this phase's API-drift note.
    assert "openrouter.ai" in str(getattr(model, "openai_api_base", None) or getattr(model, "base_url", ""))


def test_google_provider_returns_chat_google_generative_ai():
    model = get_chat_model(_settings(llm_provider="google", llm_model="gemini-2.5-flash", llm_api_key="sk-google-test"))
    assert isinstance(model, ChatGoogleGenerativeAI)
    assert model.model == "gemini-2.5-flash"
    # api_key is an alias for the real field, google_api_key — assert it
    # actually resolved rather than trusting the alias silently worked.
    assert model.google_api_key.get_secret_value() == "sk-google-test"


def test_unknown_provider_raises():
    with pytest.raises(ValueError, match="unknown LLM_PROVIDER"):
        get_chat_model(_settings(llm_provider="bogus"))
