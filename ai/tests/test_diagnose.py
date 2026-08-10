from datetime import datetime, timezone

from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage

from ai.diagnose import diagnose
from ai.models import PendingIncident, SignalPayload


def _incident(signal_type: str) -> PendingIncident:
    now = datetime.now(timezone.utc)
    return PendingIncident(
        incident_id="incident-1", namespace="default", kind="Pod", name="web-1",
        group_key="default/Pod/web-1",
        signals=[SignalPayload(type=signal_type, severity="warning", timestamp=now, raw="")],
        first_seen=now, last_seen=now,
    )


async def test_diagnose_uses_rule_when_available_without_touching_the_model():
    # An empty message iterator would raise if the model were ever invoked —
    # this proves the rule path really does short-circuit before the LLM.
    fake_model = GenericFakeChatModel(messages=iter([]))
    diagnosis = await diagnose(_incident("CrashLoopBackOff"), fake_model, tools=[])
    assert diagnosis.failure_mode == "CrashLoopBackOff"


async def test_diagnose_falls_back_to_llm_when_no_rule_matches():
    fake_model = GenericFakeChatModel(
        messages=iter([AIMessage(content='{"failure_mode": "SchedulingFailed", "recommended_action": "none", "confidence": 0.5}')])
    )
    diagnosis = await diagnose(_incident("SchedulingFailed"), fake_model, tools=[])
    assert diagnosis.failure_mode == "SchedulingFailed"
    assert diagnosis.recommended_action == "none"
