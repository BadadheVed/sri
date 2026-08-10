from __future__ import annotations

import httpx

from ai.models import Diagnosis
from ai.settings import Settings


async def post_diagnosis(
    settings: Settings, incident_id: str, diagnosis: Diagnosis, *, client: httpx.AsyncClient | None = None
) -> None:
    """POSTs the diagnosis to backend's callback route
    (backend/internal/httpserver/diagnosis.go). Raises on a non-2xx
    response so the caller (consumer.py) knows the diagnosis was NOT
    delivered rather than silently losing it. `client` is injectable so
    tests can supply an httpx.MockTransport instead of hitting the network."""
    url = f"{settings.backend_callback_url}/internal/incidents/{incident_id}/diagnosis"
    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=10.0)
    try:
        response = await client.post(
            url,
            json=diagnosis.model_dump(),
            headers={"Authorization": f"Bearer {settings.diagnosis_callback_token}"},
        )
        response.raise_for_status()
    finally:
        if owns_client:
            await client.aclose()
