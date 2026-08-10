from __future__ import annotations

import json
import re

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.tools import BaseTool
from langgraph.prebuilt import create_react_agent

from ai.models import Diagnosis, PendingIncident

SYSTEM_PROMPT = """You are SAGE's incident diagnosis agent. You investigate a
Kubernetes pod failure using read-only tools (get_pod_logs, get_pod_events,
describe_pod) and must end your investigation with exactly one JSON object
on its own line, matching this shape:

{"failure_mode": "<short name>", "recommended_action": "restart_pod" | "none", "confidence": <0.0-1.0>}

Rules:
- recommended_action MUST be exactly "restart_pod" or "none" — no other
  value is ever wired to an executor, so anything else is silently
  equivalent to guessing wrong.
- Use "none" whenever restarting the pod would not plausibly fix the
  underlying problem (a bad image reference, a missing secret/config, an
  unschedulable resource request) — do not default to "restart_pod" just
  because you're unsure; lower confidence instead.
- Investigate before concluding: call at least one tool unless the incident
  summary alone is unambiguous.
"""


async def investigate(incident: PendingIncident, model: BaseChatModel, tools: list[BaseTool]) -> Diagnosis:
    """LLM-driven fallback for incidents no rule-based analyzer matched.
    Uses LangGraph's prebuilt ReAct agent loop rather than a hand-built
    StateGraph — this fallback only needs "call tools, reason, repeat until
    done," which is exactly what create_react_agent already implements."""
    agent = create_react_agent(model, tools)
    incident_summary = (
        f"Incident {incident.incident_id}: {incident.kind} {incident.namespace}/{incident.name}\n"
        f"Signals: {[s.type for s in incident.signals]}\n"
        f"First seen: {incident.first_seen}, last seen: {incident.last_seen}"
    )
    result = await agent.ainvoke({"messages": [("system", SYSTEM_PROMPT), ("user", incident_summary)]})
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
