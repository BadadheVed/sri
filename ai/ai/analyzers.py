from __future__ import annotations

from collections.abc import Callable

from ai.models import Diagnosis, PendingIncident

Analyzer = Callable[[PendingIncident], "Diagnosis | None"]


def analyze_crashloop(incident: PendingIncident) -> Diagnosis | None:
    """Ported from the deleted backend/internal/analyze.CrashLoopAnalyzer —
    same match condition, same confidence, same recommended action."""
    for s in incident.signals:
        if s.type == "CrashLoopBackOff":
            return Diagnosis(failure_mode="CrashLoopBackOff", recommended_action="restart_pod", confidence=0.9)
    return None


def analyze_probe_failure(incident: PendingIncident) -> Diagnosis | None:
    for s in incident.signals:
        if s.type == "ProbeFailure":
            return Diagnosis(failure_mode="ProbeFailure", recommended_action="restart_pod", confidence=0.7)
    return None


# Tried in order; the first analyzer to return a non-None Diagnosis wins.
# SchedulingFailed and ImagePullError are deliberately NOT covered by a
# rule here — restarting the pod doesn't fix either one (a scheduling
# constraint or a bad image reference survives a restart unchanged), so
# they're exactly the cases meant to reach the LLM investigation loop
# (investigate.py) instead of being force-matched to a wrong rule.
REGISTRY: list[Analyzer] = [analyze_crashloop, analyze_probe_failure]


def run_rule_based_analyzers(incident: PendingIncident) -> Diagnosis | None:
    for analyzer in REGISTRY:
        diagnosis = analyzer(incident)
        if diagnosis is not None:
            return diagnosis
    return None
