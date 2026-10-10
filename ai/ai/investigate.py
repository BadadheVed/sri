from __future__ import annotations

import json
import re
from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.tools import BaseTool
from langgraph.prebuilt import create_react_agent

from ai.models import Diagnosis, PendingIncident
from ai.prompts import PromptClient, get_investigation_messages

_VALID_ACTIONS = ("restart_pod", "scale_deployment", "patch_resources", "rollback_deployment", "none")
_NO_PARAM_ACTIONS = ("restart_pod", "rollback_deployment", "none")


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


def _validate_action_params(action: str, params: Any) -> dict[str, Any] | None:
    """Returns the validated params dict for action, or None if params is
    malformed — the caller must then clamp the WHOLE diagnosis to "none",
    never wire a partially-valid action through. This is the
    safety-critical check: ai/ is the only thing standing between an LLM's
    raw text output and a real Kubernetes mutation, so a
    replicas="a lot" or a missing memory_limit must never reach backend/
    (backend/'s own reconcile.go does the same check again as
    defense-in-depth, but must never be the only thing catching this).

    Note: this validates *shape and type* only, not Kubernetes quantity
    syntax (e.g. that "512Mi" actually parses) — that's validated a second
    time, for real, by resource.ParseQuantity in
    backend/internal/execute/execute.go's PatchResources. Duplicating a
    real k8s quantity parser here isn't worth it for a string that gets
    re-validated one hop later regardless.
    """
    if action in _NO_PARAM_ACTIONS:
        return {}
    if not isinstance(params, dict):
        return None
    if action == "scale_deployment":
        replicas = params.get("replicas")
        # bool is a subclass of int in Python, so isinstance(True, int) is
        # True — excluded explicitly so {"replicas": true} doesn't silently
        # pass as replicas=1.
        if not isinstance(replicas, int) or isinstance(replicas, bool) or replicas < 0:
            return None
        return {"replicas": replicas}
    if action == "patch_resources":
        memory_limit = params.get("memory_limit")
        cpu_limit = params.get("cpu_limit")
        if memory_limit is not None and not isinstance(memory_limit, str):
            return None
        if cpu_limit is not None and not isinstance(cpu_limit, str):
            return None
        if not memory_limit and not cpu_limit:
            return None  # at least one of the two is required
        validated: dict[str, Any] = {}
        if memory_limit:
            validated["memory_limit"] = memory_limit
        if cpu_limit:
            validated["cpu_limit"] = cpu_limit
        return validated
    return None  # unreachable given _VALID_ACTIONS, kept as defense-in-depth


def _parse_diagnosis(text: str) -> Diagnosis:
    """Extracts the trailing JSON object the system prompt requires. Falls
    back to a safe "none" diagnosis with confidence 0 if the model didn't
    produce parseable JSON — a formatting slip must never be treated as
    license to restart a pod nobody actually recommended restarting. The
    regex search lives inside the try block because `text` isn't always a
    str in practice: langchain-anthropic sets AIMessage.content to a list
    of content blocks (not a plain string) when a response has multiple
    blocks (e.g. extended thinking + text) or citations, and that must fall
    back safely too, not raise past this function.

    Validation happens in two layers: first the recommended_action is
    clamped to the whitelist in _VALID_ACTIONS, then _validate_action_params
    checks the shape of action_params for whichever action survived that
    clamp. Either layer failing clamps the WHOLE diagnosis to ("none", {})
    — a valid action name with garbage params must never reach the Go
    backend as anything but "none"."""
    try:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            return Diagnosis(failure_mode="unrecognized", recommended_action="none", confidence=0.0)
        data = json.loads(match.group(0))
        action = data.get("recommended_action")
        if action not in _VALID_ACTIONS:
            action = "none"
        params = _validate_action_params(action, data.get("action_params", {}))
        if params is None:
            action, params = "none", {}
        return Diagnosis(
            failure_mode=str(data.get("failure_mode", "unrecognized")),
            recommended_action=action,
            confidence=float(data.get("confidence", 0.0)),
            action_params=params,
        )
    except (json.JSONDecodeError, TypeError, ValueError):
        return Diagnosis(failure_mode="unrecognized", recommended_action="none", confidence=0.0)
