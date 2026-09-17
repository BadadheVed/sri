from __future__ import annotations

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.tools import BaseTool

from ai.analyzers import run_rule_based_analyzers
from ai.investigate import investigate
from ai.models import Diagnosis, PendingIncident
from ai.prompts import PromptClient


async def diagnose(
    incident: PendingIncident, model: BaseChatModel, tools: list[BaseTool], prompt_client: PromptClient | None,
) -> Diagnosis:
    """Rules first (fast, free, deterministic), LLM investigation only when
    no rule matches — the diagnosis engine docs/overview.md originally
    described for ai/."""
    diagnosis = run_rule_based_analyzers(incident)
    if diagnosis is not None:
        return diagnosis
    return await investigate(incident, model, tools, prompt_client)
