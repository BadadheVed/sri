from ai.models import Diagnosis, PendingIncident


def test_pending_incident_parses_go_shaped_payload():
    raw = """
    {
      "incident_id": "incident-1",
      "namespace": "default",
      "kind": "Pod",
      "name": "web-1",
      "group_key": "default/Pod/web-1",
      "signals": [
        {"type": "CrashLoopBackOff", "severity": "warning", "labels": {"app": "web"}, "timestamp": "2026-08-08T00:00:00Z", "raw": "Back-off restarting"}
      ],
      "first_seen": "2026-08-08T00:00:00Z",
      "last_seen": "2026-08-08T00:00:01Z"
    }
    """
    incident = PendingIncident.model_validate_json(raw)
    assert incident.incident_id == "incident-1"
    assert incident.group_key == "default/Pod/web-1"
    assert incident.signals[0].type == "CrashLoopBackOff"
    assert incident.signals[0].labels["app"] == "web"


def test_diagnosis_serializes_to_expected_json_keys():
    diag = Diagnosis(failure_mode="CrashLoopBackOff", recommended_action="restart_pod", confidence=0.9)
    assert diag.model_dump() == {
        "failure_mode": "CrashLoopBackOff",
        "recommended_action": "restart_pod",
        "confidence": 0.9,
    }


def test_pending_incident_handles_null_signals():
    """Test that signals: null (from Go's nil slice) parses correctly as an empty list."""
    raw = """
    {
      "incident_id": "incident-2",
      "namespace": "default",
      "kind": "Pod",
      "name": "web-2",
      "group_key": "default/Pod/web-2",
      "signals": null,
      "first_seen": "2026-08-08T00:00:00Z",
      "last_seen": "2026-08-08T00:00:01Z"
    }
    """
    incident = PendingIncident.model_validate_json(raw)
    assert incident.incident_id == "incident-2"
    assert incident.signals == []


def test_pending_incident_handles_missing_signals_key():
    """Test that missing signals key (completely omitted from JSON) parses correctly as an empty list."""
    raw = """
    {
      "incident_id": "incident-3",
      "namespace": "default",
      "kind": "Pod",
      "name": "web-3",
      "group_key": "default/Pod/web-3",
      "first_seen": "2026-08-08T00:00:00Z",
      "last_seen": "2026-08-08T00:00:01Z"
    }
    """
    incident = PendingIncident.model_validate_json(raw)
    assert incident.incident_id == "incident-3"
    assert incident.signals == []
