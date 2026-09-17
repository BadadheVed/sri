from unittest.mock import AsyncMock, patch

from ai.mcp_client import get_readonly_tools
from ai.settings import Settings


def _settings() -> Settings:
    return Settings(
        nats_url="nats://localhost:4222", backend_callback_url="http://backend:8080",
        diagnosis_callback_token="t", mcp_readonly_url="http://mcp-readonly:8091",
        mcp_readonly_token="readonly-token", llm_provider="anthropic", llm_api_key="sk-test", llm_model="claude-sonnet-4-5",
    )


async def test_get_readonly_tools_configures_streamable_http_with_bearer_token():
    fake_tools = ["get_pod_logs", "get_pod_events", "describe_pod"]
    with patch("ai.mcp_client.MultiServerMCPClient") as mock_client_cls:
        mock_client_cls.return_value.get_tools = AsyncMock(return_value=fake_tools)

        tools = await get_readonly_tools(_settings())

        assert tools == fake_tools
        config = mock_client_cls.call_args[0][0]
        assert config["sre-readonly"]["url"] == "http://mcp-readonly:8091"
        assert config["sre-readonly"]["headers"]["Authorization"] == "Bearer readonly-token"
        assert config["sre-readonly"]["transport"] == "streamable_http"
