from __future__ import annotations

import asyncio
import logging

from ai import consumer
from ai.llm import get_chat_model
from ai.mcp_client import get_readonly_tools
from ai.prompts import get_prompt_client
from ai.settings import load_settings


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = load_settings()
    model = get_chat_model(settings)
    tools = await get_readonly_tools(settings)
    prompt_client = get_prompt_client(settings)
    await consumer.run(settings, model, tools, prompt_client)


if __name__ == "__main__":
    asyncio.run(main())
