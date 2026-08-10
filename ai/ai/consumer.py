from __future__ import annotations

import logging

import nats
from nats.js.api import AckPolicy, ConsumerConfig

from ai.callback import post_diagnosis
from ai.diagnose import diagnose
from ai.models import PendingIncident
from ai.settings import Settings

logger = logging.getLogger("ai.consumer")

# Must match backend/internal/incidentqueue.StreamName / PendingSubject
# exactly — this and that Go file are the two ends of the same contract.
STREAM_NAME = "SAGE_INCIDENTS"
PENDING_SUBJECT = "sage.incidents.pending"
DURABLE_NAME = "ai-diagnosis-worker"


async def run(settings: Settings, model, tools) -> None:
    """Durable JetStream consumer loop: pulls pending incidents published by
    backend/, diagnoses each, POSTs the result back, then acks. Runs
    forever — called once from __main__.py at process startup."""
    nc = await nats.connect(settings.nats_url)
    js = nc.jetstream()
    sub = await js.pull_subscribe(
        PENDING_SUBJECT, durable=DURABLE_NAME, config=ConsumerConfig(ack_policy=AckPolicy.EXPLICIT)
    )

    logger.info("ai/ consumer started, subscribed to %s", PENDING_SUBJECT)
    while True:
        try:
            msgs = await sub.fetch(1, timeout=5)
        except TimeoutError:
            continue
        for msg in msgs:
            await handle_message(msg.data, settings, model, tools)
            await msg.ack()


async def handle_message(data: bytes, settings: Settings, model, tools) -> None:
    incident = PendingIncident.model_validate_json(data)
    logger.info("diagnosing incident %s (%s/%s)", incident.incident_id, incident.namespace, incident.name)
    diagnosis = await diagnose(incident, model, tools)
    logger.info(
        "diagnosis for %s: failure_mode=%s recommended_action=%s confidence=%s",
        incident.incident_id, diagnosis.failure_mode, diagnosis.recommended_action, diagnosis.confidence,
    )
    await post_diagnosis(settings, incident.incident_id, diagnosis)
