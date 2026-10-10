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
            '{"failure_mode": "<short name>", "recommended_action": "restart_pod" | "scale_deployment" | "patch_resources" | "rollback_deployment" | "none", "action_params": {...}, "confidence": <0.0-1.0>}\n'
            "\n"
            "action_params depends on recommended_action:\n"
            '- "restart_pod", "rollback_deployment", "none": {} (omit or leave empty)\n'
            '- "scale_deployment": {"replicas": <int>} — desired replica count\n'
            '- "patch_resources": {"memory_limit": "<k8s quantity, e.g. 512Mi>", "cpu_limit": "<k8s quantity, e.g. 500m>"} — at least one of the two is required\n'
            "\n"
            "Rules:\n"
            '- recommended_action MUST be exactly one of "restart_pod", "scale_deployment",\n'
            '  "patch_resources", "rollback_deployment", or "none" — no other value is ever\n'
            "  wired to an executor, so anything else is silently equivalent to guessing\n"
            "  wrong, and a malformed action_params for the action you chose is treated the\n"
            '  same as choosing "none".\n'
            '- Use "restart_pod" for a transient crash where the same container image and\n'
            "  resource limits should simply run again.\n"
            '- Use "scale_deployment" when the workload is under-provisioned for current\n'
            "  load (e.g. repeated resource pressure with otherwise-healthy individual\n"
            "  replicas, or a capacity signal that more replicas would help).\n"
            '- Use "patch_resources" when a replica is failing from hitting its own memory\n'
            "  or CPU limit (e.g. OOMKilled) and a higher limit would plausibly prevent\n"
            "  recurrence.\n"
            '- Use "rollback_deployment" whenever describe_deployment reports a\n'
            "  previous_revision — that field is direct evidence a prior, different\n"
            "  revision exists for this Deployment, regardless of whether you can\n"
            "  independently confirm that revision was healthy. This includes an\n"
            "  image-pull failure: a bad/missing image tag on a Deployment that has a\n"
            "  previous_revision is almost always a bad rollout, not a permanently\n"
            "  broken workload, and rollback_deployment is safe to try even when\n"
            "  you're not fully certain — it fails cleanly with no side effect if there\n"
            "  truly was nothing usable to roll back to.\n"
            '- Use "none" only when no action is plausible at all — e.g. a missing\n'
            "  secret/config, an unschedulable resource request, or describe_deployment\n"
            "  shows no previous_revision (so there is nothing rollback_deployment could\n"
            "  do either). Do not default to \"none\" just because you're unsure whether\n"
            "  an action will help — if a plausible action exists, prefer attempting it\n"
            "  and lower confidence instead; \"none\" should mean \"no automated action\n"
            "  applies,\" not \"I'm not fully sure.\"\n"
            "- Always call at least one tool before concluding — never skip\n"
            "  investigation just because the incident's signal names look familiar\n"
            "  or unambiguous. Signal names come from a coarse, mechanical event\n"
            "  classifier upstream and are hints, not verified facts: for example,\n"
            "  Kubernetes reuses the same \"BackOff\" event reason both for a\n"
            "  crash-looping container AND for a stalled image pull, so a signal\n"
            "  literally named \"CrashLoopBackOff\" can still actually be an image\n"
            "  pull problem. Use describe_pod and get_pod_events to confirm what is\n"
            "  really happening, and use describe_deployment whenever the pod belongs\n"
            "  to a Deployment to check its rollout/revision history before choosing\n"
            "  between rollback_deployment and none.\n"
            "- If resource exhaustion, latency degradation, or an error-rate spike could\n"
            "  explain the failure and a resource/traffic tool is available, use it before\n"
            "  concluding.\n"
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
    default) — same explicit opt-in gate this repo already uses for Beyla
    (BEYLA_ENABLED). No client is constructed in that case, so there's no
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
