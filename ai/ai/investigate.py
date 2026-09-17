from __future__ import annotations

import json
import re

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.tools import BaseTool
from langgraph.prebuilt import create_react_agent

from ai.models import Diagnosis, PendingIncident
from ai.prompts import PromptClient, get_investigation_messages


async def investigate(
    incident: PendingIncident, model: BaseChatModel, tools: list[BaseTool], prompt_client: PromptClient | None,
) -> Diagnosis:
    """LLM-driven fallback for incidents no rule-based analyzer matched.
    Uses LangGraph's prebuilt ReAct agent loop rather than a hand-built
    StateGraph — this fallback only needs "call tools, reason, repeat until
    done," which is exactly what create_react_agent already implements.
    The system/user messages come from Langfuse Prompt Management (or a
    local fallback when Langfuse is disabled/unreachable) — see
    ai/ai/prompts.py."""
    agent = create_react_agent(model, tools)
    messages = get_investigation_messages(prompt_client, incident, [t.name for t in tools])
    result = await agent.ainvoke({"messages": messages})
    final_message = result["messages"][-1].content
    return _parse_diagnosis(final_message)


def _parse_diagnosis(text: str) -> Diagnosis:
    """Extracts the trailing JSON object the system prompt requires. Falls
    back to a safe "none" diagnosis with confidence 0 if the model didn't
    produce parseable JSON — a formatting slip must never be treated as
    license to restart a pod nobody actually recommended restarting. The
    regex search lives inside the try block because `text` isn't always a
    str in practice: langchain-anthropic sets AIMessage.content to a list
    of content blocks (not a plain string) when a response has multiple
    blocks (e.g. extended thinking + text) or citations, and that must fall
    back safely too, not raise past this function."""
    try:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            return Diagnosis(failure_mode="unrecognized", recommended_action="none", confidence=0.0)
        data = json.loads(match.group(0))
        action = data.get("recommended_action")
        return Diagnosis(
            failure_mode=str(data.get("failure_mode", "unrecognized")),
            recommended_action=action if action in ("restart_pod", "none") else "none",
            confidence=float(data.get("confidence", 0.0)),
        )
    except (json.JSONDecodeError, TypeError, ValueError):
        return Diagnosis(failure_mode="unrecognized", recommended_action="none", confidence=0.0)
