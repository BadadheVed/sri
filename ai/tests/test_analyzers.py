from datetime import datetime, timezone

from ai.analyzers import run_rule_based_analyzers
from ai.models import PendingIncident, SignalPayload


def _incident(signal_type: str) -> PendingIncident:
    now = datetime.now(timezone.utc)
    return PendingIncident(
        incident_id="incident-1", namespace="default", kind="Pod", name="web-1",
        group_key="default/Pod/web-1",
        signals=[SignalPayload(type=signal_type, severity="warning", timestamp=now, raw="")],
        first_seen=now, last_seen=now,
    )


def test_matches_crashloop():
    diagnosis = run_rule_based_analyzers(_incident("CrashLoopBackOff"))
    assert diagnosis is not None
    assert diagnosis.failure_mode == "CrashLoopBackOff"
    assert diagnosis.recommended_action == "restart_pod"


def test_matches_probe_failure():
    diagnosis = run_rule_based_analyzers(_incident("ProbeFailure"))
    assert diagnosis is not None
    assert diagnosis.recommended_action == "restart_pod"


def test_no_rule_matches_scheduling_failed():
    assert run_rule_based_analyzers(_incident("SchedulingFailed")) is None


def test_no_rule_matches_image_pull_error():
    assert run_rule_based_analyzers(_incident("ImagePullError")) is None
