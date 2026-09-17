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
