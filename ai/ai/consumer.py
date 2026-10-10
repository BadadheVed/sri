from __future__ import annotations

import logging

import nats
from langchain_core.exceptions import ModelError
from nats.js.api import AckPolicy, ConsumerConfig

from ai.callback import post_diagnosis
from ai.diagnose import diagnose
from ai.models import PendingIncident
from ai.prompts import PromptClient
from ai.settings import Settings

logger = logging.getLogger("ai.consumer")

# Must match backend/internal/incidentqueue.StreamName / PendingSubject
# exactly — this and that Go file are the two ends of the same contract.
STREAM_NAME = "SAGE_INCIDENTS"
PENDING_SUBJECT = "sage.incidents.pending"
DURABLE_NAME = "ai-diagnosis-worker"


async def run(settings: Settings, model, tools, prompt_client: PromptClient | None) -> None:
    """Durable JetStream consumer loop: pulls pending incidents published by
    backend/, diagnoses each, POSTs the result back, then acks. Runs
    forever — called once from __main__.py at process startup."""
    nc = await nats.connect(settings.nats_url)
    js = nc.jetstream()
    sub = await js.pull_subscribe(
        PENDING_SUBJECT,
        durable=DURABLE_NAME,
        # ack_wait (seconds — nats-py's ConsumerConfig.ack_wait is a float,
        # not a timedelta) defaults to the NATS server's 30s if unset, but
        # handle_message only acks after a full diagnose->investigate
        # (up to several tool calls at up to queryTimeout=20s each) ->
        # post_diagnosis round trip. 180s gives real headroom above the
        # worst-case investigation latency so the same incident doesn't get
        # redelivered and diagnosed/POSTed twice mid-investigation.
        config=ConsumerConfig(ack_policy=AckPolicy.EXPLICIT, ack_wait=180),
    )

    logger.info("ai/ consumer started, subscribed to %s", PENDING_SUBJECT)
    while True:
        try:
            msgs = await sub.fetch(1, timeout=5)
        except TimeoutError:
            continue
        for msg in msgs:
            await _process_message(msg, settings, model, tools, prompt_client)


async def _process_message(msg, settings: Settings, model, tools, prompt_client: PromptClient | None) -> None:
    """Handles one message, then acks on success. A retryable ModelError —
    the LLM provider itself failing transiently (rate limit, 5xx/overload,
    timeout, connection) — is logged as one concise line and the message is
    left unacked, so JetStream redelivers it after ack_wait without this
    process ever crashing or printing a full traceback for an expected,
    recoverable provider hiccup. Anything else (a non-retryable ModelError,
    or a genuine bug) still propagates and crashes the process — this
    module's existing crash-and-redeliver philosophy, preserved for
    failures that aren't just "the provider was briefly unavailable"."""
    try:
        await handle_message(msg.data, settings, model, tools, prompt_client)
    except ModelError as e:
        logger.error("model call failed (retryable=%s): %s", e.is_retryable, e)
        if not e.is_retryable:
            raise
        return
    await msg.ack()


async def handle_message(data: bytes, settings: Settings, model, tools, prompt_client: PromptClient | None) -> None:
    incident = PendingIncident.model_validate_json(data)
    logger.info("diagnosing incident %s (%s/%s)", incident.incident_id, incident.namespace, incident.name)
    diagnosis = await diagnose(incident, model, tools, prompt_client)
    logger.info(
        "diagnosis for %s: failure_mode=%s recommended_action=%s confidence=%s",
        incident.incident_id, diagnosis.failure_mode, diagnosis.recommended_action, diagnosis.confidence,
    )
    await post_diagnosis(settings, incident.incident_id, diagnosis)
