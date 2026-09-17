from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class SignalPayload(BaseModel):
    type: str
    severity: str
    labels: dict[str, str] = Field(default_factory=dict)
    timestamp: datetime
    raw: str = ""


class PendingIncident(BaseModel):
    """Mirrors backend/internal/incidentqueue.PendingIncident's JSON shape
    exactly. The `json:"..."` tags there and the field names here are the
    only contract between the two languages — keep them in sync by hand."""

    incident_id: str
    namespace: str
    kind: str
    name: str
    group_key: str
    signals: list[SignalPayload] = Field(default_factory=list)
    first_seen: datetime
    last_seen: datetime

    @field_validator("signals", mode="before")
    @classmethod
    def convert_null_signals_to_list(cls, v: any) -> list[SignalPayload]:
        """Convert JSON null to empty list for signals field.

        This handles the case where Go's json.Marshal serializes a nil slice
        as JSON null. The default_factory=list handles the case where the
        "signals" key is omitted entirely from the JSON."""
        if v is None:
            return []
        return v


class Diagnosis(BaseModel):
    """Mirrors the JSON body backend/internal/httpserver/diagnosis.go
    decodes on POST /internal/incidents/{id}/diagnosis."""

    failure_mode: str
    recommended_action: str  # constrained to "restart_pod" | "none" by ai/diagnose.py and investigate.py — see Global Constraints
    confidence: float
