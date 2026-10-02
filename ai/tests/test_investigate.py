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
    # "scale_up" is deliberately not one of the valid action names (the
    # valid one is "scale_deployment") — this must still clamp to "none".
    text = '{"failure_mode": "X", "recommended_action": "scale_up", "confidence": 0.9}'
    diagnosis = _parse_diagnosis(text)
    assert diagnosis.recommended_action == "none"


def test_parse_diagnosis_accepts_scale_deployment_with_valid_replicas():
    text = '{"failure_mode": "X", "recommended_action": "scale_deployment", "action_params": {"replicas": 3}, "confidence": 0.8}'
    diagnosis = _parse_diagnosis(text)
    assert diagnosis.recommended_action == "scale_deployment"
    assert diagnosis.action_params == {"replicas": 3}


def test_parse_diagnosis_clamps_scale_deployment_with_missing_replicas_to_none():
    text = '{"failure_mode": "X", "recommended_action": "scale_deployment", "action_params": {}, "confidence": 0.8}'
    diagnosis = _parse_diagnosis(text)
    assert diagnosis.recommended_action == "none"
    assert diagnosis.action_params == {}


def test_parse_diagnosis_clamps_scale_deployment_with_non_int_replicas_to_none():
    text = '{"failure_mode": "X", "recommended_action": "scale_deployment", "action_params": {"replicas": "three"}, "confidence": 0.8}'
    assert _parse_diagnosis(text).recommended_action == "none"


def test_parse_diagnosis_clamps_scale_deployment_bool_replicas_to_none():
    text = '{"failure_mode": "X", "recommended_action": "scale_deployment", "action_params": {"replicas": true}, "confidence": 0.8}'
    assert _parse_diagnosis(text).recommended_action == "none"


def test_parse_diagnosis_accepts_patch_resources_with_memory_limit_only():
    text = '{"failure_mode": "OOMKilled", "recommended_action": "patch_resources", "action_params": {"memory_limit": "512Mi"}, "confidence": 0.85}'
    diagnosis = _parse_diagnosis(text)
    assert diagnosis.recommended_action == "patch_resources"
    assert diagnosis.action_params == {"memory_limit": "512Mi"}


def test_parse_diagnosis_clamps_patch_resources_with_no_limits_to_none():
    text = '{"failure_mode": "OOMKilled", "recommended_action": "patch_resources", "action_params": {}, "confidence": 0.85}'
    assert _parse_diagnosis(text).recommended_action == "none"


def test_parse_diagnosis_accepts_rollback_deployment_with_empty_params():
    text = '{"failure_mode": "BadRollout", "recommended_action": "rollback_deployment", "confidence": 0.75}'  # note: no action_params key at all
    diagnosis = _parse_diagnosis(text)
    assert diagnosis.recommended_action == "rollback_deployment"
    assert diagnosis.action_params == {}


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

    diagnosis = await investigate(incident, fake_model, tools=[], prompt_client=None)

    assert diagnosis.failure_mode == "ImagePullError"
    assert diagnosis.recommended_action == "none"
    assert diagnosis.confidence == 0.8
