-- Incidents that were persisted (have an ID) but could never be published
-- to NATS for ai/ to diagnose, even after retrying — captured here for
-- manual reprocessing/inspection, alongside the existing Slack alert and
-- audit_log "dispatch_failed" entry (see reconcile.alertStrandedIncident).
CREATE TABLE IF NOT EXISTS dead_letter_dispatches (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id UUID NOT NULL REFERENCES incidents(id),
    namespace TEXT NOT NULL,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    reason TEXT NOT NULL,
    attempts INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
