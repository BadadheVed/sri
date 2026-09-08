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
        tracing_enabled=False,
    )
    client.create_prompt(name=PROMPT_NAME, type="chat", prompt=_FALLBACK_MESSAGES, labels=["production"])
    print(f"Seeded/updated prompt {PROMPT_NAME!r} in Langfuse, labeled 'production'.")


if __name__ == "__main__":
    seed()
