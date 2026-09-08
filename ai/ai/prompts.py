from __future__ import annotations

from dataclasses import dataclass

from langfuse import Langfuse

from ai.models import PendingIncident
from ai.settings import Settings

PROMPT_NAME = "sage-investigation"

# Verbatim migration of the pre-Langfuse hardcoded prompt (Mustache syntax
# instead of Python .format()'s single-curly) — single source of truth for
# both the Langfuse seed content (see seed_prompt.py) and the local
# fallback used when Langfuse is disabled or unreachable.
_FALLBACK_MESSAGES: list[dict[str, str]] = [
    {
        "role": "system",
        "content": (
            "You are SAGE's incident diagnosis agent. You investigate a\n"
            "Kubernetes pod failure using read-only tools ({{tool_names}}) and must end your\n"
            "investigation with exactly one JSON object on its own line, matching this\n"
            "shape:\n"
            "\n"
            '{"failure_mode": "<short name>", "recommended_action": "restart_pod" | "none", "confidence": <0.0-1.0>}\n'
            "\n"
            "Rules:\n"
            "- recommended_action MUST be exactly \"restart_pod\" or \"none\" — no other\n"
            "  value is ever wired to an executor, so anything else is silently\n"
            "  equivalent to guessing wrong.\n"
            "- Use \"none\" whenever restarting the pod would not plausibly fix the\n"
            "  underlying problem (a bad image reference, a missing secret/config, an\n"
            "  unschedulable resource request) — do not default to \"restart_pod\" just\n"
            "  because you're unsure; lower confidence instead.\n"
            "- Investigate before concluding: call at least one tool unless the incident\n"
            "  summary alone is unambiguous.\n"
            "- If resource exhaustion, latency degradation, or an error-rate spike could\n"
            "  explain the failure and a resource/traffic tool is available, use it\n"
            "  before concluding.\n"
        ),
    },
    {
        "role": "user",
        "content": (
            "Incident {{incident_id}}: {{kind}} {{namespace}}/{{name}}\n"
            "Signals: {{signals}}\n"
            "First seen: {{first_seen}}, last seen: {{last_seen}}"
        ),
    },
]


def _compile_literal(text: str, variables: dict[str, str]) -> str:
    """Local {{var}} substitution for the Langfuse-disabled path — kept
    independent of any langfuse.model internals (constructing a real
    ChatPromptClient requires a full Prompt_Chat object, more machinery
    than this path needs) so it never touches the SDK at all."""
    for key, value in variables.items():
        text = text.replace("{{" + key + "}}", value)
    return text


@dataclass
class PromptClient:
    """Bundles the Langfuse SDK client with the configured cache TTL, so
    callers (investigate.py) don't need a Settings object just to read one
    int — keeps investigate()/diagnose() free of Settings, matching this
    codebase's existing layering where settings stop at handle_message."""

    langfuse: Langfuse
    cache_ttl_seconds: int


def get_prompt_client(settings: Settings) -> PromptClient | None:
    """None when Langfuse is disabled (LANGFUSE_ENABLED=false, the
    default) — same explicit opt-in gate this repo already uses for Pixie
    (PIXIE_ENABLED). No client is constructed in that case, so there's no
    risk of the SDK doing anything (network, validation) against blank
    credentials."""
    if not settings.langfuse_enabled:
        return None
    return PromptClient(
        langfuse=Langfuse(
            public_key=settings.langfuse_public_key,
            secret_key=settings.langfuse_secret_key,
            host=settings.langfuse_host,
            # This integration is Prompt Management only, not the broader
            # LLM-observability product — tracing defaults to True in the
            # SDK and would otherwise install a global OTEL tracer provider
            # and background export threads in this long-lived consumer.
            tracing_enabled=False,
        ),
        cache_ttl_seconds=settings.langfuse_prompt_cache_ttl_seconds,
    )


def get_investigation_messages(
    client: PromptClient | None, incident: PendingIncident, tool_names: list[str],
) -> list[dict[str, str]]:
    """Returns [{"role": ..., "content": ...}, ...] ready to pass directly
    as agent.ainvoke({"messages": messages}).

    client=None (Langfuse disabled) -> local fallback text, substituted
    locally, never touching the network.

    client set -> fetches the "production"-labeled chat prompt from
    Langfuse, always passing fallback=_FALLBACK_MESSAGES. The Langfuse SDK
    itself handles unreachability: a fresh cache hit returns instantly; a
    stale cache hit is served immediately while a background refresh
    happens (and keeps being served if that refresh fails); an empty cache
    plus a failed fetch returns the fallback we passed — verified live
    against the installed SDK, not just documentation. A bug in our own
    call (e.g. a wrong `type` argument) is allowed to propagate — matches
    ai/consumer.py's deliberate crash-and-let-NATS-redeliver philosophy;
    no blanket try/except here.
    """
    variables = {
        "tool_names": ", ".join(tool_names) or "none",
        "incident_id": incident.incident_id,
        "kind": incident.kind,
        "namespace": incident.namespace,
        "name": incident.name,
        "signals": str([s.type for s in incident.signals]),
        "first_seen": str(incident.first_seen),
        "last_seen": str(incident.last_seen),
    }
    if client is None:
        return [
            {"role": m["role"], "content": _compile_literal(m["content"], variables)}
            for m in _FALLBACK_MESSAGES
        ]
    prompt = client.langfuse.get_prompt(
        PROMPT_NAME,
        type="chat",
        label="production",
        cache_ttl_seconds=client.cache_ttl_seconds,
        fallback=_FALLBACK_MESSAGES,
    )
    return list(prompt.compile(**variables))
