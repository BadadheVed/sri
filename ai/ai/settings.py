from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Single source of truth for every environment-derived value used
    anywhere in ai/ — mirrors backend/internal/settings/settings.go's rule
    that no other module reads the environment directly."""

    model_config = SettingsConfigDict(extra="ignore")

    nats_url: str
    backend_callback_url: str
    diagnosis_callback_token: str
    mcp_readonly_url: str
    mcp_readonly_token: str

    llm_provider: str  # "anthropic" | "openai" | "openrouter" | "google" — see ai/llm.py
    llm_api_key: str
    llm_model: str


def load_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]  # fields are populated from the environment by pydantic-settings
