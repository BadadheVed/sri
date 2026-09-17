import httpx
import pytest

from ai.callback import post_diagnosis
from ai.models import Diagnosis
from ai.settings import Settings


def _settings() -> Settings:
    return Settings(
        nats_url="nats://localhost:4222", backend_callback_url="http://backend:8080",
        diagnosis_callback_token="cb-token", mcp_readonly_url="http://mcp:8091",
        mcp_readonly_token="t", llm_provider="anthropic", llm_api_key="sk-test", llm_model="claude-sonnet-4-5",
    )


async def test_post_diagnosis_sends_expected_request():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["auth"] = request.headers.get("Authorization")
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await post_diagnosis(
            _settings(), "incident-1",
            Diagnosis(failure_mode="CrashLoopBackOff", recommended_action="restart_pod", confidence=0.9),
            client=client,
        )

    assert captured["url"] == "http://backend:8080/internal/incidents/incident-1/diagnosis"
    assert captured["auth"] == "Bearer cb-token"


async def test_post_diagnosis_raises_on_error_response():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await post_diagnosis(
                _settings(), "incident-1",
                Diagnosis(failure_mode="X", recommended_action="none", confidence=0.1),
                client=client,
            )
