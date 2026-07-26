# Autonomous SRE Platform for Kubernetes — Project Overview

## What we're building

An autonomous SRE platform for generic Kubernetes clusters that **detects, diagnoses, and safely self-heals** production incidents — with a human approval gate that can be toggled between fully automatic and manual, and a hard safety floor that can never be bypassed for irreversible actions. Every decision, automatic or human, is logged for audit.

## Block Diagram

| Stage | Owner | What happens |
|---|---|---|
| **Observe** | Kubernetes API · Prometheus · Loki · eBPF/OTel | Continuous stream of events, metrics, logs, and live service topology. |
| **Detect & Diagnose** | backend/ (Go) + ai/ (Python) | backend/ correlates raw signals into one incident; ai/ diagnoses it (read-only) and recommends a fix. |
| **Approve** | backend/ Gate + Slack | Safety floor, blast radius, and the auto/manual toggle decide whether a human must approve first. |
| **Heal** | backend/ (Go) | Executes the fix (idempotent), rechecks the cluster recovered, rolls back if it didn't. |
| **Record & View** | Postgres + Qdrant + frontend/ | Every decision is logged; the dashboard shows incident history and status. |

## Component summary

| Component | Role |
|---|---|
| **backend/ (Go)** | Watches the cluster, correlates raw signals into incidents, computes blast radius, enforces the approval gate, and is the *only* component allowed to execute a remediation action. |
| **ai/ (Python, LangGraph)** | Diagnoses incidents — checks history for a similar past case, runs rule-based analyzers, falls back to an LLM investigation loop using read-only tools. Never touches the cluster. |
| **frontend/ (Next.js)** | View-only dashboard: incident history, audit log, and permission/token configuration. Individual incident approvals happen in Slack, not here. |
| **Postgres** | Source of truth — incidents, remediation actions, approvals, audit log, dependency graph. |
| **Qdrant** | Vector search over past-incident narratives, for "have we seen this before?" matching. |
| **Slack** | Human approval channel — used whenever the gate requires a decision. |
| **MCP** | Standardized tool interface between ai/ and its data sources — not the security boundary (that's the Gate + Slack + token scope, layered). |

## Non-negotiable guardrails

1. **ai/ never mutates the cluster** — it only reads and recommends.
2. **A hard safety floor cannot be configured away** — a small denylist of irreversible actions (delete PVC, scale-to-zero, delete node) always requires human approval, in *any* mode.
3. **A blast-radius threshold is a second, independent gate** — even a normally "safe" action needs approval if it would affect too much.
4. **Every remediation action is idempotent** — safe to retry.
5. **Every decision, automatic or human, is logged** for audit.

## Tech stack

Go (backend/execution) · Python + LangGraph (ai/diagnosis) · Next.js (dashboard) · Postgres + Qdrant (knowledge store) · Slack (approvals) · MCP (tool interface) · Grafana Beyla/Pixie + OTel (dependency graph, mesh-agnostic) · NATS JetStream (introduced when scaling beyond a single cluster).

