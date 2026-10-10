-- Diagnosis now happens asynchronously in ai/ (see docs/superpowers/specs/2026-08-08-ai-diagnosis-service-design.md),
-- so an incident is persisted before its failure_mode is known.
ALTER TABLE incidents ALTER COLUMN failure_mode DROP NOT NULL;
