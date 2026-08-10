from datetime import datetime, timezone

from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage

from ai.investigate import _parse_diagnosis, investigate
from ai.models import PendingIncident, SignalPayload


def test_parse_diagnosis_extracts_trailing_json():
    text = 'I looked at the logs. Conclusion:\n{"failure_mode": "BadConfig", "recommended_action": "none", "confidence": 0.6}'
    diagnosis = _parse_diagnosis(text)
    assert diagnosis.failure_mode == "BadConfig"
    assert diagnosis.recommended_action == "none"
    assert diagnosis.confidence == 0.6


def test_parse_diagnosis_falls_back_safely_on_unparseable_text():
    diagnosis = _parse_diagnosis("I'm not sure what happened here.")
    assert diagnosis.recommended_action == "none"
    assert diagnosis.confidence == 0.0


def test_parse_diagnosis_rejects_out_of_vocabulary_action():
    text = '{"failure_mode": "X", "recommended_action": "scale_up", "confidence": 0.9}'
    diagnosis = _parse_diagnosis(text)
    assert diagnosis.recommended_action == "none"


def test_parse_diagnosis_falls_back_safely_on_non_string_content():
    # langchain-anthropic sets AIMessage.content to a list of content
    # blocks (not a str) for multi-block responses (e.g. extended
    # thinking + text, or citations) — must fall back, never raise.
    diagnosis = _parse_diagnosis([{"type": "text", "text": "irrelevant"}])
    assert diagnosis.recommended_action == "none"
    assert diagnosis.confidence == 0.0


async def test_investigate_returns_diagnosis_from_fake_model_final_answer():
    now = datetime.now(timezone.utc)
    incident = PendingIncident(
        incident_id="incident-1", namespace="default", kind="Pod", name="web-1",
        group_key="default/Pod/web-1",
        signals=[SignalPayload(type="ImagePullError", severity="warning", timestamp=now, raw="")],
        first_seen=now, last_seen=now,
    )
    fake_model = GenericFakeChatModel(
        messages=iter([AIMessage(content='{"failure_mode": "ImagePullError", "recommended_action": "none", "confidence": 0.8}')])
    )

    diagnosis = await investigate(incident, fake_model, tools=[])

    assert diagnosis.failure_mode == "ImagePullError"
    assert diagnosis.recommended_action == "none"
    assert diagnosis.confidence == 0.8
