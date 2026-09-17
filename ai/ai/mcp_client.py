from __future__ import annotations

from langchain_core.tools import BaseTool
from langchain_mcp_adapters.client import MultiServerMCPClient

from ai.settings import Settings


async def get_readonly_tools(settings: Settings) -> list[BaseTool]:
    """Fetches the read-only tool set mcp-readonly-server exposes
    (get_pod_logs, get_pod_events, describe_pod — see
    backend/cmd/mcp-readonly-server/main.go) as LangChain tools the
    investigation graph (investigate.py) can call directly. ai/ never gets
    direct cluster credentials — this is its only path to cluster data."""
    client = MultiServerMCPClient(
        {
            "sre-readonly": {
                "transport": "streamable_http",
                "url": settings.mcp_readonly_url,
                "headers": {"Authorization": f"Bearer {settings.mcp_readonly_token}"},
            }
        }
    )
    return await client.get_tools()
