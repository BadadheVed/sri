from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from ai.consumer import handle_message
from ai.models import PendingIncident, SignalPayload
from ai.settings import Settings


def _settings() -> Settings:
    return Settings(
        nats_url="nats://localhost:4222", backend_callback_url="http://backend:8080",
        diagnosis_callback_token="t", mcp_readonly_url="http://mcp:8091",
        mcp_readonly_token="t", llm_provider="anthropic", llm_api_key="sk-test", llm_model="claude-sonnet-4-5",
    )


async def test_handle_message_diagnoses_and_posts_back():
    now = datetime.now(timezone.utc)
    incident = PendingIncident(
        incident_id="incident-1", namespace="default", kind="Pod", name="web-1",
        group_key="default/Pod/web-1",
        signals=[SignalPayload(type="CrashLoopBackOff", severity="warning", timestamp=now, raw="")],
        first_seen=now, last_seen=now,
    )
    data = incident.model_dump_json().encode()

    with patch("ai.consumer.post_diagnosis", new=AsyncMock()) as mock_post:
        # model=None, tools=[] is safe here: CrashLoopBackOff matches a
        # rule in ai.analyzers, so diagnose() never touches the model.
        await handle_message(data, _settings(), model=None, tools=[])

    mock_post.assert_awaited_once()
    call_args = mock_post.call_args.args
    assert call_args[1] == "incident-1"
    assert call_args[2].failure_mode == "CrashLoopBackOff"
