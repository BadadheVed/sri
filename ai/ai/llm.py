from __future__ import annotations

from langchain_core.language_models.chat_models import BaseChatModel

from ai.settings import Settings


def get_chat_model(settings: Settings) -> BaseChatModel:
    """Returns a LangChain chat model for whichever provider the deployer
    configured via LLM_PROVIDER — the seam that makes ai/ usable against a
    self-hosted OpenRouter endpoint instead of one hardcoded provider (see
    docs/superpowers/specs/2026-08-08-ai-diagnosis-service-design.md)."""
    if settings.llm_provider == "anthropic":
        from langchain_anthropic import ChatAnthropic

        return ChatAnthropic(model=settings.llm_model, api_key=settings.llm_api_key)
    if settings.llm_provider == "openai":
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(model=settings.llm_model, api_key=settings.llm_api_key)
    if settings.llm_provider == "openrouter":
        # OpenRouter speaks the OpenAI-compatible API — same client, just a
        # different base_url, so no separate SDK is needed for this branch.
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(
            model=settings.llm_model,
            api_key=settings.llm_api_key,
            base_url="https://openrouter.ai/api/v1",
        )
    if settings.llm_provider == "google":
        from langchain_google_genai import ChatGoogleGenerativeAI

        return ChatGoogleGenerativeAI(model=settings.llm_model, api_key=settings.llm_api_key)
    raise ValueError(f"unknown LLM_PROVIDER {settings.llm_provider!r}: must be \"anthropic\", \"openai\", \"openrouter\", or \"google\"")
