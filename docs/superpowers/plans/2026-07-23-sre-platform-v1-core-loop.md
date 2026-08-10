# SRE Platform v1 — Core Self-Healing Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the smallest working, end-to-end slice of the v1 self-healing core: detect a crash-looping pod via Kubernetes Events, correlate it into an incident, diagnose it with a rule-based analyzer, gate the remediation through the hard-safety-floor + auto/manual toggle, request Slack approval when required, execute an idempotent restart, verify the pod recovers, and audit every step to Postgres.

**Architecture:** Single Go backend service (`backend/`) wired from `cmd/backend/main.go` through five focused internal packages (`signal`, `correlate`, `analyze`, `gate`, `execute`/`verify`) plus a `store` package with a `Store` interface (Postgres-backed in production, in-memory for fast tests) and a `slackapproval` package for the human-in-the-loop gate. `ai/` and `frontend/` get scaffolding only — their real logic (LangGraph diagnosis, dashboard) is later plans per the design doc.

**Tech Stack:** Go 1.22+, `client-go` (K8s API), `pgx/v5` (Postgres driver), plain `net/http` (Slack webhook), Python 3.11+ / `pytest` (ai/ scaffolding), Next.js 14+ / TypeScript / Jest (frontend/ scaffolding), Docker Compose (local Postgres), `kind` (local test cluster).

## Global Constraints

- Target generic Kubernetes — no cloud-provider-specific APIs (from spec §1).
- `ai/` never gets cluster-mutation credentials; only `backend/` calls K8s mutating APIs (spec §2 invariant 1). Not exercised by this plan (no `ai/` logic yet) but the package boundary must not violate it later — `backend/` owns `execute/`.
- Hard safety floor cannot be bypassed by mode or config (spec §2 invariant 2, §6).
- Every remediation action must be idempotent / safe to retry (spec §2 invariant 4, §6).
- Every decision (auto or manual) is logged to Postgres for audit (spec §2 invariant 5).
- **Execute actions are called via MCP, not in-process** (spec §6, §8): `backend/`'s core loop is an MCP *client* that calls a separate `mcp-execute-server` process over the MCP protocol with a bearer token; the server independently re-validates that token before running the tool — a second, independent check beyond `backend/`'s own Gate decision. Use the official SDK, `github.com/modelcontextprotocol/go-sdk/mcp`.
- **`cmd/backend/main.go` stays a thin wiring layer only** — construct dependencies, then hand off to dedicated packages. All reconciliation logic (correlate → analyze → gate → execute → verify → audit) lives in `internal/reconcile`, not in `main.go`. All HTTP routes are registered in `internal/httpserver` via a router with prefix-grouped routes (`github.com/go-chi/chi/v5`), not inline in `main.go` with the stdlib `http.NewServeMux()`.
- Reference design doc: `docs/superpowers/specs/2026-07-23-sre-platform-v1-design.md`.

## Commit Policy

**Implementers never run `git commit` in this plan.** Every task below ends with a "Stage the changes" step: run the listed `git add` command(s) and stop — the line below it, "Commit message for the user to use: `...`", is documentation of the exact message for the *user* to run themselves, not an instruction for the implementer. Concretely:

1. Stage the listed files with `git add` (and, for Task 1 only, `git init` first — that's one-time repo setup, not a commit, and is fine to run).
2. Stop and report `DONE`, including the exact commit message text from the task in your report.
3. Do not run `git commit` or any further git operations. The user reviews the staged diff and commits it themselves.

This applies to every task (1–14) — when dispatching each task, the controller repeats this instruction explicitly regardless of the exact wording in that task's own step.

## Scope Note

The full v1 design (`docs/superpowers/specs/2026-07-23-sre-platform-v1-design.md`) covers multiple independent subsystems: this core loop, the dependency graph + blast radius, the Qdrant/LangGraph diagnosis layer, the permission/token system, and the dashboard. Per the writing-plans scope check, those are **separate follow-up plans**, not part of this one. This plan produces working, testable software on its own: a backend that heals crash-looping pods end-to-end, gated by Slack approval, with a full audit trail — without those later subsystems.

---

## File Structure

```
.
├── .gitignore
├── .env.example
├── docker-compose.yml
├── backend/
│   ├── go.mod
│   ├── go.sum
│   ├── cmd/
│   │   ├── backend/main.go
│   │   └── mcp-execute-server/main.go
│   ├── internal/
│   │   ├── settings/settings.go
│   │   ├── signal/signal.go
│   │   ├── correlate/correlate.go
│   │   ├── analyze/analyze.go
│   │   ├── gate/gate.go
│   │   ├── execute/execute.go
│   │   ├── verify/verify.go
│   │   ├── k8swatch/watcher.go
│   │   ├── slackapproval/slack.go
│   │   ├── mcpauth/bearer.go
│   │   ├── mcpexecute/client.go
│   │   ├── reconcile/reconcile.go
│   │   ├── httpserver/router.go
│   │   └── store/
│   │       ├── store.go
│   │       ├── postgres.go
│   │       └── memory.go
│   ├── tests/                       <- all backend Go tests live here,
│   │   ├── signal/signal_test.go       mirroring internal/'s package tree
│   │   ├── correlate/correlate_test.go (compatible with Go's internal/
│   │   ├── analyze/analyze_test.go     import rule because tests/ is
│   │   ├── gate/gate_test.go           nested inside backend/, not a
│   │   ├── execute/execute_test.go     project-root sibling)
│   │   ├── verify/verify_test.go
│   │   ├── k8swatch/watcher_test.go
│   │   ├── slackapproval/slack_test.go
│   │   ├── mcpauth/bearer_test.go
│   │   ├── mcpexecute/client_test.go
│   │   ├── reconcile/reconcile_test.go
│   │   ├── httpserver/router_test.go
│   │   └── store/
│   │       ├── postgres_test.go
│   │       └── memory_test.go
│   └── migrations/0001_init.sql
├── ai/
│   ├── pyproject.toml
│   ├── ai/__init__.py
│   └── tests/test_smoke.py
└── frontend/
    ├── package.json
    ├── app/page.tsx
    └── tests/smoke.test.tsx
```

---

### Task 1: Repo scaffolding — .gitignore, .env.example, docker-compose, module skeletons

**Files:**
- Create: `.gitignore`
- Create: `.env.example`
- Create: `docker-compose.yml`
- Create: `backend/go.mod`
- Create: `ai/pyproject.toml`, `ai/ai/__init__.py`, `ai/tests/test_smoke.py`
- Create: `frontend/package.json` (via `create-next-app`), `frontend/tests/smoke.test.tsx`
- Test: `ai/tests/test_smoke.py`, `frontend/tests/smoke.test.tsx`

**Interfaces:**
- Produces: `backend` Go module named `sre-platform/backend` (all later Go packages import as `sre-platform/backend/internal/...`).

- [ ] **Step 1: Write `.gitignore`**

```gitignore
# Environments
.env
.env.local
*.env

# Go
backend/bin/
backend/*.test
backend/coverage.out

# Python
ai/.venv/
ai/__pycache__/
ai/**/__pycache__/
ai/*.egg-info/
ai/.pytest_cache/

# Node / Next.js
frontend/node_modules/
frontend/.next/
frontend/out/
frontend/coverage/

# Editors / OS
.vscode/
.idea/
.DS_Store

# kind / kubeconfig
kubeconfig*.yaml
*.kubeconfig
```

- [ ] **Step 2: Write `.env.example`**

```dotenv
# Postgres (local docker-compose)
DATABASE_URL=postgres://sre:sre@localhost:5432/sre_platform?sslmode=disable

# Kubernetes
KUBECONFIG=./kubeconfig.yaml

# Remediation gate
REMEDIATION_MODE=manual        # "auto" or "manual"
VERIFY_TIMEOUT_SECONDS=60
CORRELATION_WINDOW_SECONDS=60

# Slack approval integration
SLACK_BOT_TOKEN=xoxb-placeholder
SLACK_SIGNING_SECRET=placeholder-signing-secret
SLACK_APPROVAL_CHANNEL=#sre-approvals

# backend HTTP server (Slack webhook receiver)
BACKEND_HTTP_ADDR=:8080
```

- [ ] **Step 3: Write `docker-compose.yml`**

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: sre
      POSTGRES_PASSWORD: sre
      POSTGRES_DB: sre_platform
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

volumes:
  pgdata:
```

- [ ] **Step 4: Initialize the Go module**

Run:
```bash
mkdir -p backend/cmd/backend backend/internal backend/migrations backend/tests
cd backend && go mod init sre-platform/backend
```
Expected: `backend/go.mod` created with `module sre-platform/backend`.

- [ ] **Step 5: Scaffold `ai/` with a smoke test**

```toml
# ai/pyproject.toml
[project]
name = "ai"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = []

[tool.pytest.ini_options]
testpaths = ["tests"]
```

```python
# ai/ai/__init__.py
```

```python
# ai/tests/test_smoke.py
def test_pytest_is_wired():
    """Verifies the ai/ test harness runs before real diagnosis logic lands in a later plan."""
    assert True
```

Run:
```bash
cd ai && python3 -m venv .venv && .venv/bin/pip install pytest && .venv/bin/pytest -v
```
Expected: `1 passed`.

- [ ] **Step 6: Scaffold `frontend/` with a smoke test**

Run:
```bash
npx --yes create-next-app@latest frontend --typescript --eslint --app --no-tailwind --no-src-dir --import-alias "@/*" --use-npm
cd frontend && npm install --save-dev jest @testing-library/react @testing-library/jest-dom @types/jest jest-environment-jsdom
```

```tsx
// frontend/tests/smoke.test.tsx
import { render, screen } from '@testing-library/react'
import '@testing-library/jest-dom'
import Home from '../app/page'

test('renders the SRE Platform landing page', () => {
  render(<Home />)
  expect(screen.getByText(/SRE Platform/i)).toBeInTheDocument()
})
```

```tsx
// frontend/app/page.tsx (replace the create-next-app default)
export default function Home() {
  return (
    <main>
      <h1>SRE Platform</h1>
      <p>Dashboard — incidents, history, and audit log land here in a later plan.</p>
    </main>
  )
}
```

Add to `frontend/package.json` `"scripts"`: `"test": "jest"`, and a `frontend/jest.config.js`:

```js
module.exports = {
  testEnvironment: 'jsdom',
  testMatch: ['**/tests/**/*.test.tsx'],
}
```

Run:
```bash
cd frontend && npm test
```
Expected: `1 passed`.

- [ ] **Step 7: Initialize the repo and stage the changes (do not commit — see Commit Policy)**

```bash
git init
git add .gitignore .env.example docker-compose.yml backend/go.mod ai/ frontend/
```
Commit message for the user to use: `chore: scaffold backend/ai/frontend modules, env template, gitignore`

(`git init` itself is fine to run — it's one-time repo setup, not a commit. Only the actual `git commit` is the user's to run.)

---

### Task 2: Postgres schema + Store interface + PostgresStore

**Files:**
- Create: `backend/migrations/0001_init.sql`
- Create: `backend/internal/store/store.go`
- Create: `backend/internal/store/postgres.go`
- Test: `backend/tests/store/postgres_test.go`

**Interfaces:**
- Produces:
  - `store.Incident{ID, Namespace, Kind, Name, FailureMode, Status string, FirstSeen, LastSeen time.Time}`
  - `store.Store` interface: `CreateIncident(ctx, namespace, kind, name, failureMode string, firstSeen, lastSeen time.Time) (incidentID string, err error)`, `CreateRemediationAction(ctx, incidentID, actionType string, requiresApproval bool, reason string) (actionID string, err error)`, `RecordApprovalDecision(ctx, actionID, decidedBy, decision string) error`, `MarkExecuted(ctx, actionID string) error`, `MarkVerified(ctx, actionID, outcome string) error`, `WriteAudit(ctx, incidentID, eventType string, detail map[string]any) error`
  - `store.NewPostgresStore(ctx, dsn string) (*store.PostgresStore, error)` implementing `Store`.

- [ ] **Step 1: Write the migration**

```sql
-- backend/migrations/0001_init.sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE incidents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace TEXT NOT NULL,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    failure_mode TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'detected',
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE remediation_actions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id UUID NOT NULL REFERENCES incidents(id),
    action_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    requires_approval BOOLEAN NOT NULL,
    approval_reason TEXT,
    executed_at TIMESTAMPTZ,
    verified_at TIMESTAMPTZ,
    outcome TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE approvals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    remediation_action_id UUID NOT NULL REFERENCES remediation_actions(id),
    slack_message_ts TEXT,
    decided_by TEXT,
    decision TEXT,
    decided_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id UUID REFERENCES incidents(id),
    event_type TEXT NOT NULL,
    detail JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

- [ ] **Step 2: Write the failing test**

```go
// backend/tests/store/postgres_test.go
package store_test

import (
	"context"
	"os"
	"testing"
	"time"

	"sre-platform/backend/internal/store"
)

func testDSN(t *testing.T) string {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set — run `docker compose up -d postgres` and set DATABASE_URL to run this test")
	}
	return dsn
}

func TestPostgresStore_IncidentLifecycle(t *testing.T) {
	ctx := context.Background()
	s, err := store.NewPostgresStore(ctx, testDSN(t))
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}

	now := time.Now().UTC()
	incidentID, err := s.CreateIncident(ctx, "default", "Pod", "web-1", "CrashLoopBackOff", now, now)
	if err != nil {
		t.Fatalf("CreateIncident: %v", err)
	}
	if incidentID == "" {
		t.Fatal("expected non-empty incident ID")
	}

	actionID, err := s.CreateRemediationAction(ctx, incidentID, "restart_pod", true, "manual_mode")
	if err != nil {
		t.Fatalf("CreateRemediationAction: %v", err)
	}

	if err := s.RecordApprovalDecision(ctx, actionID, "alice", "approved"); err != nil {
		t.Fatalf("RecordApprovalDecision: %v", err)
	}
	if err := s.MarkExecuted(ctx, actionID); err != nil {
		t.Fatalf("MarkExecuted: %v", err)
	}
	if err := s.MarkVerified(ctx, actionID, "resolved"); err != nil {
		t.Fatalf("MarkVerified: %v", err)
	}
	if err := s.WriteAudit(ctx, incidentID, "remediation_verified", map[string]any{"outcome": "resolved"}); err != nil {
		t.Fatalf("WriteAudit: %v", err)
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd backend && go test ./tests/store/... -run TestPostgresStore_IncidentLifecycle -v`
Expected: FAIL — `package store: no Go files` (package doesn't exist yet).

- [ ] **Step 4: Write `store.go` (interface + types)**

```go
// backend/internal/store/store.go
package store

import (
	"context"
	"time"
)

type Incident struct {
	ID          string
	Namespace   string
	Kind        string
	Name        string
	FailureMode string
	Status      string
	FirstSeen   time.Time
	LastSeen    time.Time
}

type Store interface {
	CreateIncident(ctx context.Context, namespace, kind, name, failureMode string, firstSeen, lastSeen time.Time) (string, error)
	CreateRemediationAction(ctx context.Context, incidentID, actionType string, requiresApproval bool, reason string) (string, error)
	RecordApprovalDecision(ctx context.Context, actionID, decidedBy, decision string) error
	MarkExecuted(ctx context.Context, actionID string) error
	MarkVerified(ctx context.Context, actionID, outcome string) error
	WriteAudit(ctx context.Context, incidentID, eventType string, detail map[string]any) error
}
```

- [ ] **Step 5: Add the pgx dependency and write `postgres.go`**

Run: `cd backend && go get github.com/jackc/pgx/v5/pgxpool`

```go
// backend/internal/store/postgres.go
package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) CreateIncident(ctx context.Context, namespace, kind, name, failureMode string, firstSeen, lastSeen time.Time) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx,
		`INSERT INTO incidents (namespace, kind, name, failure_mode, first_seen, last_seen)
		 VALUES ($1,$2,$3,$4,$5,$6) RETURNING id`,
		namespace, kind, name, failureMode, firstSeen, lastSeen,
	).Scan(&id)
	return id, err
}

func (s *PostgresStore) CreateRemediationAction(ctx context.Context, incidentID, actionType string, requiresApproval bool, reason string) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx,
		`INSERT INTO remediation_actions (incident_id, action_type, requires_approval, approval_reason)
		 VALUES ($1,$2,$3,$4) RETURNING id`,
		incidentID, actionType, requiresApproval, reason,
	).Scan(&id)
	return id, err
}

func (s *PostgresStore) RecordApprovalDecision(ctx context.Context, actionID, decidedBy, decision string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO approvals (remediation_action_id, decided_by, decision, decided_at)
		 VALUES ($1,$2,$3, now())`,
		actionID, decidedBy, decision,
	)
	return err
}

func (s *PostgresStore) MarkExecuted(ctx context.Context, actionID string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE remediation_actions SET status = 'executed', executed_at = now() WHERE id = $1`,
		actionID,
	)
	return err
}

func (s *PostgresStore) MarkVerified(ctx context.Context, actionID, outcome string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE remediation_actions SET status = 'verified', verified_at = now(), outcome = $2 WHERE id = $1`,
		actionID, outcome,
	)
	return err
}

func (s *PostgresStore) WriteAudit(ctx context.Context, incidentID, eventType string, detail map[string]any) error {
	raw, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO audit_log (incident_id, event_type, detail) VALUES ($1,$2,$3)`,
		incidentID, eventType, raw,
	)
	return err
}
```

- [ ] **Step 6: Run test to verify it passes**

Run:
```bash
docker compose up -d postgres
docker exec -i $(docker compose ps -q postgres) psql -U sre -d sre_platform < backend/migrations/0001_init.sql
export DATABASE_URL="postgres://sre:sre@localhost:5432/sre_platform?sslmode=disable"
cd backend && go test ./tests/store/... -run TestPostgresStore_IncidentLifecycle -v
```
Expected: `PASS`.

- [ ] **Step 7: Stage the changes (do not commit — see Commit Policy)**

```bash
git add backend/migrations backend/internal/store backend/tests/store backend/go.mod backend/go.sum
```
Commit message for the user to use: `feat: Postgres schema and Store interface for incidents/actions/approvals/audit`

---

### Task 3: In-memory Store test double

**Files:**
- Create: `backend/internal/store/memory.go`
- Test: `backend/tests/store/memory_test.go`

**Interfaces:**
- Consumes: `store.Store` interface from Task 2.
- Produces: `store.NewMemoryStore() *store.MemoryStore` implementing `Store` — used by Task 11's end-to-end test so it doesn't require a live Postgres.

- [ ] **Step 1: Write the failing test**

```go
// backend/tests/store/memory_test.go
package store_test

import (
	"context"
	"testing"
	"time"

	"sre-platform/backend/internal/store"
)

func TestMemoryStore_SatisfiesStoreInterface(t *testing.T) {
	var _ store.Store = store.NewMemoryStore()

	ctx := context.Background()
	s := store.NewMemoryStore()
	now := time.Now().UTC()

	incidentID, err := s.CreateIncident(ctx, "default", "Pod", "web-1", "CrashLoopBackOff", now, now)
	if err != nil || incidentID == "" {
		t.Fatalf("CreateIncident: id=%q err=%v", incidentID, err)
	}

	actionID, err := s.CreateRemediationAction(ctx, incidentID, "restart_pod", false, "auto_approved")
	if err != nil || actionID == "" {
		t.Fatalf("CreateRemediationAction: id=%q err=%v", actionID, err)
	}

	if err := s.MarkExecuted(ctx, actionID); err != nil {
		t.Fatalf("MarkExecuted: %v", err)
	}
	if err := s.MarkVerified(ctx, actionID, "resolved"); err != nil {
		t.Fatalf("MarkVerified: %v", err)
	}
	if err := s.WriteAudit(ctx, incidentID, "remediation_verified", map[string]any{"outcome": "resolved"}); err != nil {
		t.Fatalf("WriteAudit: %v", err)
	}

	got := s.Incidents[incidentID]
	if got.Status != "detected" {
		t.Errorf("expected incident status 'detected', got %q", got.Status)
	}
	if s.Actions[actionID].Status != "verified" {
		t.Errorf("expected action status 'verified', got %q", s.Actions[actionID].Status)
	}
	if len(s.AuditEntries) != 1 {
		t.Errorf("expected 1 audit entry, got %d", len(s.AuditEntries))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && go test ./tests/store/... -run TestMemoryStore_SatisfiesStoreInterface -v`
Expected: FAIL — `undefined: store.NewMemoryStore`.

- [ ] **Step 3: Write `memory.go`**

```go
// backend/internal/store/memory.go
package store

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type memAction struct {
	ID               string
	IncidentID       string
	ActionType       string
	Status           string
	RequiresApproval bool
	ApprovalReason   string
	Outcome          string
}

type memAuditEntry struct {
	IncidentID string
	EventType  string
	Detail     map[string]any
}

type MemoryStore struct {
	mu           sync.Mutex
	seq          int
	Incidents    map[string]Incident
	Actions      map[string]memAction
	AuditEntries []memAuditEntry
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		Incidents: make(map[string]Incident),
		Actions:   make(map[string]memAction),
	}
}

func (s *MemoryStore) nextID(prefix string) string {
	s.seq++
	return fmt.Sprintf("%s-%d", prefix, s.seq)
}

func (s *MemoryStore) CreateIncident(ctx context.Context, namespace, kind, name, failureMode string, firstSeen, lastSeen time.Time) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextID("incident")
	s.Incidents[id] = Incident{
		ID: id, Namespace: namespace, Kind: kind, Name: name,
		FailureMode: failureMode, Status: "detected",
		FirstSeen: firstSeen, LastSeen: lastSeen,
	}
	return id, nil
}

func (s *MemoryStore) CreateRemediationAction(ctx context.Context, incidentID, actionType string, requiresApproval bool, reason string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextID("action")
	s.Actions[id] = memAction{
		ID: id, IncidentID: incidentID, ActionType: actionType,
		Status: "pending", RequiresApproval: requiresApproval, ApprovalReason: reason,
	}
	return id, nil
}

func (s *MemoryStore) RecordApprovalDecision(ctx context.Context, actionID, decidedBy, decision string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.Actions[actionID]
	if !ok {
		return fmt.Errorf("unknown action id %q", actionID)
	}
	if decision == "approved" {
		a.Status = "approved"
	} else {
		a.Status = "denied"
	}
	s.Actions[actionID] = a
	return nil
}

func (s *MemoryStore) MarkExecuted(ctx context.Context, actionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.Actions[actionID]
	if !ok {
		return fmt.Errorf("unknown action id %q", actionID)
	}
	a.Status = "executed"
	s.Actions[actionID] = a
	return nil
}

func (s *MemoryStore) MarkVerified(ctx context.Context, actionID, outcome string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.Actions[actionID]
	if !ok {
		return fmt.Errorf("unknown action id %q", actionID)
	}
	a.Status = "verified"
	a.Outcome = outcome
	s.Actions[actionID] = a
	return nil
}

func (s *MemoryStore) WriteAudit(ctx context.Context, incidentID, eventType string, detail map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.AuditEntries = append(s.AuditEntries, memAuditEntry{IncidentID: incidentID, EventType: eventType, Detail: detail})
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && go test ./tests/store/... -run TestMemoryStore_SatisfiesStoreInterface -v`
Expected: `PASS`.

- [ ] **Step 5: Stage the changes (do not commit — see Commit Policy)**

```bash
git add backend/internal/store/memory.go backend/tests/store/memory_test.go
```
Commit message for the user to use: `feat: in-memory Store test double for fast, Postgres-free tests`

---

### Task 4: Signal type + normalization

**Files:**
- Create: `backend/internal/signal/signal.go`
- Test: `backend/tests/signal/signal_test.go`

**Interfaces:**
- Produces: `signal.Signal{Source, Type, Severity, Namespace, Kind, Name string, Labels map[string]string, Timestamp time.Time, Raw string}`, `signal.SourceK8sEvent` constant.

- [ ] **Step 1: Write the failing test**

```go
// backend/tests/signal/signal_test.go
package signal_test

import (
	"testing"
	"time"

	"sre-platform/backend/internal/signal"
)

func TestSignal_Fields(t *testing.T) {
	now := time.Now()
	s := signal.Signal{
		Source:    signal.SourceK8sEvent,
		Type:      "CrashLoopBackOff",
		Severity:  "warning",
		Namespace: "default",
		Kind:      "Pod",
		Name:      "web-1",
		Labels:    map[string]string{"app": "web"},
		Timestamp: now,
		Raw:       "Back-off restarting failed container",
	}

	if s.Source != signal.SourceK8sEvent {
		t.Errorf("expected source %q, got %q", signal.SourceK8sEvent, s.Source)
	}
	if s.Labels["app"] != "web" {
		t.Errorf("expected label app=web, got %v", s.Labels)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && go test ./tests/signal/... -v`
Expected: FAIL — `package signal: no Go files`.

- [ ] **Step 3: Write `signal.go`**

```go
// backend/internal/signal/signal.go
package signal

import "time"

type Source string

const (
	SourceK8sEvent Source = "k8s_event"
)

type Signal struct {
	Source    Source
	Type      string
	Severity  string
	Namespace string
	Kind      string
	Name      string
	Labels    map[string]string
	Timestamp time.Time
	Raw       string
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && go test ./tests/signal/... -v`
Expected: `PASS`.

- [ ] **Step 5: Stage the changes (do not commit — see Commit Policy)**

```bash
git add backend/internal/signal backend/tests/signal
```
Commit message for the user to use: `feat: normalized Signal type shared across detection sources`

---

### Task 5: Correlation — group signals into incidents

**Files:**
- Create: `backend/internal/correlate/correlate.go`
- Test: `backend/tests/correlate/correlate_test.go`

**Interfaces:**
- Consumes: `signal.Signal` from Task 4.
- Produces: `correlate.Incident{Namespace, Kind, Name string, Signals []signal.Signal, FirstSeen, LastSeen time.Time}`, `correlate.Correlate(signals []signal.Signal, window time.Duration) []correlate.Incident`.

- [ ] **Step 1: Write the failing test**

```go
// backend/tests/correlate/correlate_test.go
package correlate_test

import (
	"testing"
	"time"

	"sre-platform/backend/internal/correlate"
	"sre-platform/backend/internal/signal"
)

func TestCorrelate_GroupsSameObjectWithinWindow(t *testing.T) {
	base := time.Now()
	signals := []signal.Signal{
		{Namespace: "default", Kind: "Pod", Name: "web-1", Type: "BackOff", Timestamp: base},
		{Namespace: "default", Kind: "Pod", Name: "web-1", Type: "BackOff", Timestamp: base.Add(10 * time.Second)},
		{Namespace: "default", Kind: "Pod", Name: "db-1", Type: "BackOff", Timestamp: base.Add(5 * time.Second)},
	}

	incidents := correlate.Correlate(signals, 60*time.Second)

	if len(incidents) != 2 {
		t.Fatalf("expected 2 incidents, got %d", len(incidents))
	}

	var webIncident *correlate.Incident
	for i := range incidents {
		if incidents[i].Name == "web-1" {
			webIncident = &incidents[i]
		}
	}
	if webIncident == nil {
		t.Fatal("expected an incident for web-1")
	}
	if len(webIncident.Signals) != 2 {
		t.Errorf("expected 2 signals grouped for web-1, got %d", len(webIncident.Signals))
	}
}

func TestCorrelate_SplitsSignalsOutsideWindow(t *testing.T) {
	base := time.Now()
	signals := []signal.Signal{
		{Namespace: "default", Kind: "Pod", Name: "web-1", Type: "BackOff", Timestamp: base},
		{Namespace: "default", Kind: "Pod", Name: "web-1", Type: "BackOff", Timestamp: base.Add(5 * time.Minute)},
	}

	incidents := correlate.Correlate(signals, 60*time.Second)

	if len(incidents) != 2 {
		t.Fatalf("expected 2 separate incidents outside the correlation window, got %d", len(incidents))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && go test ./tests/correlate/... -v`
Expected: FAIL — `package correlate: no Go files`.

- [ ] **Step 3: Write `correlate.go`**

```go
// backend/internal/correlate/correlate.go
package correlate

import (
	"time"

	"sre-platform/backend/internal/signal"
)

type Incident struct {
	Namespace string
	Kind      string
	Name      string
	Signals   []signal.Signal
	FirstSeen time.Time
	LastSeen  time.Time
}

type objectKey struct {
	Namespace string
	Kind      string
	Name      string
}

// Correlate groups signals into incidents by involved object, splitting a new
// incident whenever the gap since the last signal for that object exceeds window.
func Correlate(signals []signal.Signal, window time.Duration) []Incident {
	byObject := make(map[objectKey][]signal.Signal)
	for _, s := range signals {
		key := objectKey{s.Namespace, s.Kind, s.Name}
		byObject[key] = append(byObject[key], s)
	}

	var incidents []Incident
	for key, sigs := range byObject {
		sortByTimestamp(sigs)

		var current []signal.Signal
		flush := func() {
			if len(current) == 0 {
				return
			}
			incidents = append(incidents, Incident{
				Namespace: key.Namespace,
				Kind:      key.Kind,
				Name:      key.Name,
				Signals:   append([]signal.Signal{}, current...),
				FirstSeen: current[0].Timestamp,
				LastSeen:  current[len(current)-1].Timestamp,
			})
		}

		for _, s := range sigs {
			if len(current) > 0 && s.Timestamp.Sub(current[len(current)-1].Timestamp) > window {
				flush()
				current = nil
			}
			current = append(current, s)
		}
		flush()
	}
	return incidents
}

func sortByTimestamp(sigs []signal.Signal) {
	for i := 1; i < len(sigs); i++ {
		for j := i; j > 0 && sigs[j].Timestamp.Before(sigs[j-1].Timestamp); j-- {
			sigs[j], sigs[j-1] = sigs[j-1], sigs[j]
		}
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && go test ./tests/correlate/... -v`
Expected: `PASS` (both tests).

- [ ] **Step 5: Stage the changes (do not commit — see Commit Policy)**

```bash
git add backend/internal/correlate backend/tests/correlate
```
Commit message for the user to use: `feat: correlate signals into incidents by object and time window`

---

### Task 6: Rule-based CrashLoopBackOff analyzer

**Files:**
- Create: `backend/internal/analyze/analyze.go`
- Test: `backend/tests/analyze/analyze_test.go`

**Interfaces:**
- Consumes: `correlate.Incident` from Task 5.
- Produces: `analyze.Diagnosis{FailureMode, RecommendedAction string, Confidence float64}`, `analyze.Analyzer` interface with `Analyze(inc correlate.Incident) (*analyze.Diagnosis, bool)`, `analyze.CrashLoopAnalyzer{}`.

- [ ] **Step 1: Write the failing test**

```go
// backend/tests/analyze/analyze_test.go
package analyze_test

import (
	"testing"

	"sre-platform/backend/internal/analyze"
	"sre-platform/backend/internal/correlate"
	"sre-platform/backend/internal/signal"
)

func TestCrashLoopAnalyzer_MatchesKnownSignature(t *testing.T) {
	inc := correlate.Incident{
		Namespace: "default", Kind: "Pod", Name: "web-1",
		Signals: []signal.Signal{{Type: "CrashLoopBackOff"}},
	}

	var a analyze.CrashLoopAnalyzer
	diag, matched := a.Analyze(inc)

	if !matched {
		t.Fatal("expected analyzer to match a CrashLoopBackOff signal")
	}
	if diag.FailureMode != "CrashLoopBackOff" {
		t.Errorf("expected failure mode CrashLoopBackOff, got %q", diag.FailureMode)
	}
	if diag.RecommendedAction != "restart_pod" {
		t.Errorf("expected recommended action restart_pod, got %q", diag.RecommendedAction)
	}
}

func TestCrashLoopAnalyzer_NoMatchForUnrelatedSignal(t *testing.T) {
	inc := correlate.Incident{
		Namespace: "default", Kind: "Pod", Name: "web-1",
		Signals: []signal.Signal{{Type: "FailedScheduling"}},
	}

	var a analyze.CrashLoopAnalyzer
	_, matched := a.Analyze(inc)

	if matched {
		t.Error("expected no match for an unrelated signal type")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && go test ./tests/analyze/... -v`
Expected: FAIL — `package analyze: no Go files`.

- [ ] **Step 3: Write `analyze.go`**

```go
// backend/internal/analyze/analyze.go
package analyze

import "sre-platform/backend/internal/correlate"

type Diagnosis struct {
	FailureMode       string
	RecommendedAction string
	Confidence        float64
}

type Analyzer interface {
	Analyze(inc correlate.Incident) (*Diagnosis, bool)
}

type CrashLoopAnalyzer struct{}

func (CrashLoopAnalyzer) Analyze(inc correlate.Incident) (*Diagnosis, bool) {
	for _, s := range inc.Signals {
		if s.Type == "CrashLoopBackOff" {
			return &Diagnosis{
				FailureMode:       "CrashLoopBackOff",
				RecommendedAction: "restart_pod",
				Confidence:        0.9,
			}, true
		}
	}
	return nil, false
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && go test ./tests/analyze/... -v`
Expected: `PASS` (both tests).

- [ ] **Step 5: Stage the changes (do not commit — see Commit Policy)**

```bash
git add backend/internal/analyze backend/tests/analyze
```
Commit message for the user to use: `feat: rule-based CrashLoopBackOff analyzer`

---

### Task 7: Gate — hard safety floor + auto/manual toggle

**Files:**
- Create: `backend/internal/gate/gate.go`
- Test: `backend/tests/gate/gate_test.go`

**Interfaces:**
- Produces: `gate.Mode` (`gate.ModeAuto`, `gate.ModeManual`), `gate.Decision{RequiresApproval bool, Reason string}`, `gate.Evaluate(action string, mode gate.Mode) gate.Decision`.

- [ ] **Step 1: Write the failing test**

```go
// backend/tests/gate/gate_test.go
package gate_test

import (
	"testing"

	"sre-platform/backend/internal/gate"
)

func TestEvaluate_SafetyFloorAlwaysRequiresApproval(t *testing.T) {
	d := gate.Evaluate("delete_pvc", gate.ModeAuto)
	if !d.RequiresApproval {
		t.Error("expected delete_pvc to require approval even in auto mode")
	}
	if d.Reason != "hard_safety_floor" {
		t.Errorf("expected reason hard_safety_floor, got %q", d.Reason)
	}
}

func TestEvaluate_ManualModeRequiresApproval(t *testing.T) {
	d := gate.Evaluate("restart_pod", gate.ModeManual)
	if !d.RequiresApproval {
		t.Error("expected restart_pod to require approval in manual mode")
	}
	if d.Reason != "manual_mode" {
		t.Errorf("expected reason manual_mode, got %q", d.Reason)
	}
}

func TestEvaluate_AutoModeAllowsNonFloorAction(t *testing.T) {
	d := gate.Evaluate("restart_pod", gate.ModeAuto)
	if d.RequiresApproval {
		t.Error("expected restart_pod in auto mode to not require approval")
	}
	if d.Reason != "auto_approved" {
		t.Errorf("expected reason auto_approved, got %q", d.Reason)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && go test ./tests/gate/... -v`
Expected: FAIL — `package gate: no Go files`.

- [ ] **Step 3: Write `gate.go`**

```go
// backend/internal/gate/gate.go
package gate

type Mode string

const (
	ModeAuto   Mode = "auto"
	ModeManual Mode = "manual"
)

// SafetyFloor lists action types that always require approval, regardless of
// mode. This cannot be overridden by configuration — see design doc §2, §6.
var SafetyFloor = map[string]bool{
	"delete_pvc":    true,
	"scale_to_zero": true,
	"delete_node":   true,
}

type Decision struct {
	RequiresApproval bool
	Reason           string
}

func Evaluate(action string, mode Mode) Decision {
	if SafetyFloor[action] {
		return Decision{RequiresApproval: true, Reason: "hard_safety_floor"}
	}
	if mode == ModeManual {
		return Decision{RequiresApproval: true, Reason: "manual_mode"}
	}
	return Decision{RequiresApproval: false, Reason: "auto_approved"}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && go test ./tests/gate/... -v`
Expected: `PASS` (all three tests).

- [ ] **Step 5: Stage the changes (do not commit — see Commit Policy)**

```bash
git add backend/internal/gate backend/tests/gate
```
Commit message for the user to use: `feat: gate logic with hard safety floor and auto/manual toggle`

---

### Task 8: Execute — idempotent restart_pod action

**Files:**
- Create: `backend/internal/execute/execute.go`
- Test: `backend/tests/execute/execute_test.go`

**Interfaces:**
- Produces: `execute.Executor{}`, `execute.NewExecutor(clientset kubernetes.Interface) *execute.Executor`, `(*execute.Executor).RestartPod(ctx context.Context, namespace, name string) error` — deleting an already-gone pod is treated as success (idempotent).

- [ ] **Step 1: Add client-go and write the failing test**

Run: `cd backend && go get k8s.io/client-go@latest k8s.io/api@latest k8s.io/apimachinery@latest`

```go
// backend/tests/execute/execute_test.go
package execute_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sre-platform/backend/internal/execute"
)

func TestRestartPod_DeletesExistingPod(t *testing.T) {
	ctx := context.Background()
	clientset := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default"},
	})

	e := execute.NewExecutor(clientset)
	if err := e.RestartPod(ctx, "default", "web-1"); err != nil {
		t.Fatalf("RestartPod: %v", err)
	}

	_, err := clientset.CoreV1().Pods("default").Get(ctx, "web-1", metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("expected pod to be deleted, got err=%v", err)
	}
}

func TestRestartPod_IdempotentWhenAlreadyGone(t *testing.T) {
	ctx := context.Background()
	clientset := fake.NewSimpleClientset()

	e := execute.NewExecutor(clientset)
	if err := e.RestartPod(ctx, "default", "already-gone"); err != nil {
		t.Fatalf("expected RestartPod to succeed idempotently on a missing pod, got: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && go test ./tests/execute/... -v`
Expected: FAIL — `package execute: no Go files`.

- [ ] **Step 3: Write `execute.go`**

```go
// backend/internal/execute/execute.go
package execute

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Executor struct {
	clientset kubernetes.Interface
}

func NewExecutor(clientset kubernetes.Interface) *Executor {
	return &Executor{clientset: clientset}
}

// RestartPod deletes the pod so its owning controller (ReplicaSet/Deployment)
// recreates it. Deleting an already-absent pod is treated as success, since a
// remediation action must be safe to retry.
func (e *Executor) RestartPod(ctx context.Context, namespace, name string) error {
	err := e.clientset.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && go test ./tests/execute/... -v`
Expected: `PASS` (both tests).

- [ ] **Step 5: Stage the changes (do not commit — see Commit Policy)**

```bash
git add backend/internal/execute backend/tests/execute backend/go.mod backend/go.sum
```
Commit message for the user to use: `feat: idempotent restart_pod execute action`

---

### Task 9: Verify — recheck pod health after remediation

**Files:**
- Create: `backend/internal/verify/verify.go`
- Test: `backend/tests/verify/verify_test.go`

**Interfaces:**
- Consumes: `kubernetes.Interface` (client-go, already a dependency from Task 8).
- Produces: `verify.CheckPodHealthy(ctx context.Context, clientset kubernetes.Interface, namespace string, labels map[string]string, timeout, pollInterval time.Duration) (bool, error)` — polls for at least one Ready pod matching the given label selector.

- [ ] **Step 1: Write the failing test**

```go
// backend/tests/verify/verify_test.go
package verify_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sre-platform/backend/internal/verify"
)

func readyPod(name string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: labels},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
}

func TestCheckPodHealthy_ReturnsTrueWhenReadyPodExists(t *testing.T) {
	ctx := context.Background()
	labels := map[string]string{"app": "web"}
	clientset := fake.NewSimpleClientset(readyPod("web-2", labels))

	healthy, err := verify.CheckPodHealthy(ctx, clientset, "default", labels, 2*time.Second, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("CheckPodHealthy: %v", err)
	}
	if !healthy {
		t.Error("expected healthy=true when a Ready pod matches the labels")
	}
}

func TestCheckPodHealthy_ReturnsFalseWhenNoPodBecomesReady(t *testing.T) {
	ctx := context.Background()
	labels := map[string]string{"app": "web"}
	clientset := fake.NewSimpleClientset()

	healthy, err := verify.CheckPodHealthy(ctx, clientset, "default", labels, 200*time.Millisecond, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("CheckPodHealthy: %v", err)
	}
	if healthy {
		t.Error("expected healthy=false when no matching pod exists within the timeout")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && go test ./tests/verify/... -v`
Expected: FAIL — `package verify: no Go files`.

- [ ] **Step 3: Write `verify.go`**

```go
// backend/internal/verify/verify.go
package verify

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

// CheckPodHealthy polls until at least one pod matching labels in namespace
// reports Ready, or timeout elapses.
func CheckPodHealthy(ctx context.Context, clientset kubernetes.Interface, namespace string, podLabels map[string]string, timeout, pollInterval time.Duration) (bool, error) {
	deadline := time.Now().Add(timeout)
	selector := labels.SelectorFromSet(podLabels).String()

	for {
		pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return false, err
		}
		for _, p := range pods.Items {
			if isReady(p) {
				return true, nil
			}
		}
		if time.Now().After(deadline) {
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

func isReady(p corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && go test ./tests/verify/... -v`
Expected: `PASS` (both tests).

- [ ] **Step 5: Stage the changes (do not commit — see Commit Policy)**

```bash
git add backend/internal/verify backend/tests/verify
```
Commit message for the user to use: `feat: post-remediation health verification via label-selector polling`

---

### Task 10: Slack approval — post request + signature-verified webhook

**Files:**
- Create: `backend/internal/slackapproval/slack.go`
- Test: `backend/tests/slackapproval/slack_test.go`

**Interfaces:**
- Consumes: `store.Store` from Task 2 (`RecordApprovalDecision`).
- Produces: `slackapproval.ApprovalRequest{IncidentID, ActionID, FailureMode, Action, Namespace, Name string}`, `slackapproval.NewClient(token, channel, signingSecret string, httpClient *http.Client) *slackapproval.Client`, `(*Client).PostApproval(ctx, req ApprovalRequest) (messageTS string, err error)`, `(*Client).VerifySignature(timestamp, signature string, body []byte) bool`, `(*Client).InteractionHandler(s store.Store) http.HandlerFunc`.

- [ ] **Step 1: Write the failing test**

```go
// backend/tests/slackapproval/slack_test.go
package slackapproval_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"sre-platform/backend/internal/slackapproval"
	"sre-platform/backend/internal/store"
)

func TestPostApproval_SendsMessageAndReturnsTimestamp(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat.postMessage" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true,"ts":"1700000000.000100"}`))
	}))
	defer server.Close()

	c := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", server.Client())
	c.APIBaseURL = server.URL

	ts, err := c.PostApproval(t.Context(), slackapproval.ApprovalRequest{
		IncidentID: "incident-1", ActionID: "action-1",
		FailureMode: "CrashLoopBackOff", Action: "restart_pod",
		Namespace: "default", Name: "web-1",
	})
	if err != nil {
		t.Fatalf("PostApproval: %v", err)
	}
	if ts != "1700000000.000100" {
		t.Errorf("expected ts from Slack response, got %q", ts)
	}
}

func signSlackRequest(secret string, timestamp string, body string) string {
	base := fmt.Sprintf("v0:%s:%s", timestamp, body)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(base))
	return "v0=" + hex.EncodeToString(mac.Sum(nil))
}

func TestInteractionHandler_ApprovesOnValidSignature(t *testing.T) {
	c := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	memStore := store.NewMemoryStore()

	incidentID, _ := memStore.CreateIncident(t.Context(), "default", "Pod", "web-1", "CrashLoopBackOff", time.Now(), time.Now())
	actionID, _ := memStore.CreateRemediationAction(t.Context(), incidentID, "restart_pod", true, "manual_mode")

	payload := url.Values{}
	payload.Set("payload", fmt.Sprintf(`{"actions":[{"action_id":"approve","value":%q}],"user":{"username":"alice"}}`, actionID))
	body := payload.Encode()

	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	sig := signSlackRequest("signing-secret", timestamp, body)

	req := httptest.NewRequest(http.MethodPost, "/slack/interactions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Slack-Request-Timestamp", timestamp)
	req.Header.Set("X-Slack-Signature", sig)
	rec := httptest.NewRecorder()

	c.InteractionHandler(memStore)(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if memStore.Actions[actionID].Status != "approved" {
		t.Errorf("expected action status approved, got %q", memStore.Actions[actionID].Status)
	}
}

func TestInteractionHandler_RejectsInvalidSignature(t *testing.T) {
	c := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	memStore := store.NewMemoryStore()

	req := httptest.NewRequest(http.MethodPost, "/slack/interactions", strings.NewReader("payload=invalid"))
	req.Header.Set("X-Slack-Request-Timestamp", "1700000000")
	req.Header.Set("X-Slack-Signature", "v0=deadbeef")
	rec := httptest.NewRecorder()

	c.InteractionHandler(memStore)(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for invalid signature, got %d", rec.Code)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && go test ./tests/slackapproval/... -v`
Expected: FAIL — `package slackapproval: no Go files`.

- [ ] **Step 3: Write `slack.go`**

```go
// backend/internal/slackapproval/slack.go
package slackapproval

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"sre-platform/backend/internal/store"
)

type ApprovalRequest struct {
	IncidentID  string
	ActionID    string
	FailureMode string
	Action      string
	Namespace   string
	Name        string
}

type Client struct {
	token         string
	channel       string
	signingSecret string
	httpClient    *http.Client
	APIBaseURL    string
}

func NewClient(token, channel, signingSecret string, httpClient *http.Client) *Client {
	return &Client{
		token: token, channel: channel, signingSecret: signingSecret,
		httpClient: httpClient, APIBaseURL: "https://slack.com/api",
	}
}

type postMessageResponse struct {
	OK bool   `json:"ok"`
	TS string `json:"ts"`
}

func (c *Client) PostApproval(ctx context.Context, req ApprovalRequest) (string, error) {
	text := fmt.Sprintf(
		"Remediation approval needed\nIncident: %s/%s in %s\nDiagnosis: %s\nProposed action: %s\nAction ID: %s",
		req.Namespace, req.Name, req.Namespace, req.FailureMode, req.Action, req.ActionID,
	)
	body, err := json.Marshal(map[string]string{"channel": c.channel, "text": text})
	if err != nil {
		return "", err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.APIBaseURL+"/chat.postMessage", strings.NewReader(string(body)))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var parsed postMessageResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", err
	}
	if !parsed.OK {
		return "", fmt.Errorf("slack chat.postMessage returned ok=false")
	}
	return parsed.TS, nil
}

// VerifySignature implements Slack's request signing verification:
// https://api.slack.com/authentication/verifying-requests-from-slack
func (c *Client) VerifySignature(timestamp, signature string, body []byte) bool {
	base := fmt.Sprintf("v0:%s:%s", timestamp, string(body))
	mac := hmac.New(sha256.New, []byte(c.signingSecret))
	mac.Write([]byte(base))
	expected := "v0=" + hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(signature))
}

type interactionPayload struct {
	Actions []struct {
		ActionID string `json:"action_id"`
		Value    string `json:"value"`
	} `json:"actions"`
	User struct {
		Username string `json:"username"`
	} `json:"user"`
}

// InteractionHandler returns an http.HandlerFunc for Slack's interactivity
// webhook. It verifies the request signature, then records the human's
// approve/deny decision against the given Store.
func (c *Client) InteractionHandler(s store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		timestamp := r.Header.Get("X-Slack-Request-Timestamp")
		signature := r.Header.Get("X-Slack-Signature")
		if !c.VerifySignature(timestamp, signature, bodyBytes) {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}

		form, err := url.ParseQuery(string(bodyBytes))
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		var payload interactionPayload
		if err := json.Unmarshal([]byte(form.Get("payload")), &payload); err != nil {
			http.Error(w, "bad payload", http.StatusBadRequest)
			return
		}
		if len(payload.Actions) == 0 {
			http.Error(w, "no action in payload", http.StatusBadRequest)
			return
		}

		actionID := payload.Actions[0].Value
		decision := "denied"
		if payload.Actions[0].ActionID == "approve" {
			decision = "approved"
		}

		if err := s.RecordApprovalDecision(r.Context(), actionID, payload.User.Username, decision); err != nil {
			http.Error(w, "failed to record decision", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}

var _ = time.Now // retained for future timestamp-freshness check (Slack replay protection)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && go test ./tests/slackapproval/... -v`
Expected: `PASS` (all three tests).

- [ ] **Step 5: Stage the changes (do not commit — see Commit Policy)**

```bash
git add backend/internal/slackapproval backend/tests/slackapproval
```
Commit message for the user to use: `feat: Slack approval request + signature-verified interaction webhook`

---

### Task 11: K8s event watcher, extracted Reconciler, chi router, and a thin main.go

**Files:**
- Create: `backend/internal/k8swatch/watcher.go`
- Create: `backend/internal/settings/settings.go`
- Create: `backend/internal/reconcile/reconcile.go`
- Create: `backend/internal/httpserver/router.go`
- Create: `backend/cmd/backend/main.go`
- Test: `backend/tests/k8swatch/watcher_test.go`
- Test: `backend/tests/settings/settings_test.go`
- Test: `backend/tests/reconcile/reconcile_test.go`
- Test: `backend/tests/httpserver/router_test.go`

**Interfaces:**
- Consumes: everything from Tasks 3–10.
- Produces: `k8swatch.Watcher{}`, `k8swatch.NewWatcher(clientset kubernetes.Interface, onSignal func(signal.Signal)) *k8swatch.Watcher`, `(*Watcher).HandleAddEvent(obj any)` (exposed for direct testing without running the informer loop); `settings.Settings` struct, `settings.Load() Settings` (env-reading, fails fast), `(Settings).Validate() error` (the pure, testable part); `reconcile.PodRestarter` interface, `reconcile.New(...) *reconcile.Reconciler`, `(*Reconciler).OnSignal(ctx, signal.Signal)`; `httpserver.NewRouter(slackClient *slackapproval.Client, s store.Store) http.Handler`.

**Why the split:** all business logic (correlate → analyze → gate → execute → verify → audit) lives in `internal/reconcile`, not in `main.go` — `main.go` only constructs dependencies and wires them together (Global Constraints). `reconcile.Reconciler` depends on a small `PodRestarter` interface rather than a concrete executor type, so Task 14 can swap in the MCP-backed implementation by changing one line in `main.go`, without touching `reconcile.go` at all.

- [ ] **Step 1: Write the failing watcher test**

```go
// backend/tests/k8swatch/watcher_test.go
package k8swatch_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sre-platform/backend/internal/k8swatch"
	"sre-platform/backend/internal/signal"
)

func TestWatcher_HandleAddEvent_EmitsCrashLoopSignal(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-1", Namespace: "default",
			Labels: map[string]string{"app": "web"},
		},
	}
	clientset := fake.NewSimpleClientset(pod)

	var got []signal.Signal
	w := k8swatch.NewWatcher(clientset, func(s signal.Signal) { got = append(got, s) })

	ev := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: "web-1.abc", Namespace: "default"},
		InvolvedObject: corev1.ObjectReference{
			Kind: "Pod", Namespace: "default", Name: "web-1",
		},
		Reason:        "BackOff",
		Message:       "Back-off restarting failed container",
		LastTimestamp: metav1.NewTime(time.Now()),
	}

	w.HandleAddEvent(ev)

	if len(got) != 1 {
		t.Fatalf("expected 1 signal emitted, got %d", len(got))
	}
	if got[0].Type != "CrashLoopBackOff" {
		t.Errorf("expected type CrashLoopBackOff, got %q", got[0].Type)
	}
	if got[0].Labels["app"] != "web" {
		t.Errorf("expected pod labels to be fetched, got %v", got[0].Labels)
	}
}

func TestWatcher_HandleAddEvent_IgnoresUnrelatedReasons(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	var got []signal.Signal
	w := k8swatch.NewWatcher(clientset, func(s signal.Signal) { got = append(got, s) })

	ev := &corev1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "web-1"},
		Reason:         "Scheduled",
		LastTimestamp:  metav1.NewTime(time.Now()),
	}
	w.HandleAddEvent(ev)

	if len(got) != 0 {
		t.Errorf("expected no signal for an unrelated event reason, got %d", len(got))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && go test ./tests/k8swatch/... -v`
Expected: FAIL — `package k8swatch: no Go files`.

- [ ] **Step 3: Write `watcher.go`**

```go
// backend/internal/k8swatch/watcher.go
package k8swatch

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"sre-platform/backend/internal/signal"
)

// knownReasons maps a K8s Event's Reason field to our internal failure-mode
// taxonomy. Kubernetes itself emits "BackOff" (not "CrashLoopBackOff") for a
// crash-looping container; the pod status condition uses the longer name.
var knownReasons = map[string]string{
	"BackOff": "CrashLoopBackOff",
}

type Watcher struct {
	clientset kubernetes.Interface
	onSignal  func(signal.Signal)
}

func NewWatcher(clientset kubernetes.Interface, onSignal func(signal.Signal)) *Watcher {
	return &Watcher{clientset: clientset, onSignal: onSignal}
}

func (w *Watcher) Run(ctx context.Context) error {
	factory := informers.NewSharedInformerFactory(w.clientset, 30*time.Second)
	eventInformer := factory.Core().V1().Events().Informer()
	_, err := eventInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: w.HandleAddEvent,
	})
	if err != nil {
		return err
	}

	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())
	<-ctx.Done()
	return nil
}

func (w *Watcher) HandleAddEvent(obj any) {
	ev, ok := obj.(*corev1.Event)
	if !ok {
		return
	}
	failureMode, known := knownReasons[ev.Reason]
	if !known || ev.InvolvedObject.Kind != "Pod" {
		return
	}

	labels := map[string]string{}
	pod, err := w.clientset.CoreV1().Pods(ev.InvolvedObject.Namespace).Get(context.Background(), ev.InvolvedObject.Name, metav1.GetOptions{})
	if err == nil {
		labels = pod.Labels
	}

	w.onSignal(signal.Signal{
		Source:    signal.SourceK8sEvent,
		Type:      failureMode,
		Severity:  "warning",
		Namespace: ev.InvolvedObject.Namespace,
		Kind:      ev.InvolvedObject.Kind,
		Name:      ev.InvolvedObject.Name,
		Labels:    labels,
		Timestamp: ev.LastTimestamp.Time,
		Raw:       ev.Message,
	})
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && go test ./tests/k8swatch/... -v`
Expected: `PASS` (both tests).

**Why settings is centralized:** every env var used anywhere in the backend module is declared exactly once, in this one file, as a field on `Settings` — no package ever calls `os.Getenv` directly except `settings.go` itself (Global Constraints). `Load()` fails fast (`log.Fatal`) if a required field is missing, so `main.go` — and `cmd/mcp-execute-server`'s `main.go` in Task 12 — cannot proceed to connect to Postgres, start the HTTP server, or start the watcher with incomplete configuration. This task defines every field the whole plan needs (including the MCP fields Tasks 12–14 consume) up front, so later tasks only ever *read* `Settings`, never add to it.

- [ ] **Step 5: Write the failing settings-validation test**

```go
// backend/tests/settings/settings_test.go
package settings_test

import (
	"testing"
	"time"

	"sre-platform/backend/internal/gate"
	"sre-platform/backend/internal/settings"
)

func validSettings() settings.Settings {
	return settings.Settings{
		DatabaseURL:          "postgres://sre:sre@localhost:5432/sre_platform?sslmode=disable",
		Mode:                 gate.ModeManual,
		VerifyTimeout:        60 * time.Second,
		CorrelationWindow:    60 * time.Second,
		SlackBotToken:        "xoxb-real-token",
		SlackSigningSecret:   "real-signing-secret",
		SlackApprovalChannel: "#sre-approvals",
		HTTPAddr:             ":8080",
		MCPExecuteAddr:       ":8090",
		MCPExecuteToken:      "real-shared-secret",
		MCPExecuteURL:        "http://localhost:8090",
	}
}

func TestSettings_Validate_PassesWhenRequiredFieldsSet(t *testing.T) {
	if err := validSettings().Validate(); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestSettings_Validate_FailsWhenRequiredFieldsMissing(t *testing.T) {
	s := settings.Settings{}
	err := s.Validate()
	if err == nil {
		t.Fatal("expected an error when required settings are missing")
	}
}

func TestSettings_Validate_ListsEachMissingRequiredField(t *testing.T) {
	s := validSettings()
	s.SlackBotToken = ""
	s.MCPExecuteToken = ""

	err := s.Validate()
	if err == nil {
		t.Fatal("expected an error")
	}
	if !contains(err.Error(), "SLACK_BOT_TOKEN") || !contains(err.Error(), "MCP_EXECUTE_TOKEN") {
		t.Errorf("expected error to name both missing fields, got: %v", err)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		func() bool {
			for i := 0; i+len(substr) <= len(s); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
			return false
		}())
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `cd backend && go test ./tests/settings/... -v`
Expected: FAIL — `package settings: no Go files`.

- [ ] **Step 7: Write `settings.go`**

```go
// backend/internal/settings/settings.go
package settings

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"sre-platform/backend/internal/gate"
)

// Settings is the single source of truth for every environment-derived
// value used anywhere in the backend module (cmd/backend and
// cmd/mcp-execute-server both load one of these) — no other package calls
// os.Getenv directly.
type Settings struct {
	DatabaseURL          string
	Kubeconfig           string
	Mode                 gate.Mode
	VerifyTimeout        time.Duration
	CorrelationWindow    time.Duration
	SlackBotToken        string
	SlackSigningSecret   string
	SlackApprovalChannel string
	HTTPAddr             string
	MCPExecuteAddr       string
	MCPExecuteToken      string
	MCPExecuteURL        string
}

// Load reads Settings from the process environment and fails fast — the
// caller is guaranteed either a fully-populated Settings or a process that
// has already exited via log.Fatal, so nothing downstream (Postgres
// connection, HTTP server, watcher) can start against incomplete config.
func Load() Settings {
	s := Settings{
		DatabaseURL:          getenv("DATABASE_URL", ""),
		Kubeconfig:           getenv("KUBECONFIG", ""),
		Mode:                 gate.Mode(getenv("REMEDIATION_MODE", "manual")),
		VerifyTimeout:        seconds(getenv("VERIFY_TIMEOUT_SECONDS", "60")),
		CorrelationWindow:    seconds(getenv("CORRELATION_WINDOW_SECONDS", "60")),
		SlackBotToken:        getenv("SLACK_BOT_TOKEN", ""),
		SlackSigningSecret:   getenv("SLACK_SIGNING_SECRET", ""),
		SlackApprovalChannel: getenv("SLACK_APPROVAL_CHANNEL", "#sre-approvals"),
		HTTPAddr:             getenv("BACKEND_HTTP_ADDR", ":8080"),
		MCPExecuteAddr:       getenv("MCP_EXECUTE_ADDR", ":8090"),
		MCPExecuteToken:      getenv("MCP_EXECUTE_TOKEN", ""),
		MCPExecuteURL:        getenv("MCP_EXECUTE_URL", "http://localhost:8090"),
	}
	if err := s.Validate(); err != nil {
		log.Fatalf("invalid settings: %v", err)
	}
	return s
}

// Validate reports every required-but-missing field at once (Kubeconfig is
// intentionally not required — an empty value is a valid "use in-cluster
// config" signal to client-go).
func (s Settings) Validate() error {
	var missing []string
	if s.DatabaseURL == "" {
		missing = append(missing, "DATABASE_URL")
	}
	if s.SlackBotToken == "" {
		missing = append(missing, "SLACK_BOT_TOKEN")
	}
	if s.SlackSigningSecret == "" {
		missing = append(missing, "SLACK_SIGNING_SECRET")
	}
	if s.MCPExecuteToken == "" {
		missing = append(missing, "MCP_EXECUTE_TOKEN")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}
	return nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func seconds(s string) time.Duration {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 60 * time.Second
	}
	return time.Duration(n) * time.Second
}
```

- [ ] **Step 8: Run test to verify it passes**

Run: `cd backend && go test ./tests/settings/... -v`
Expected: `PASS` (all three tests).

- [ ] **Step 9: Add the chi router dependency**

Run: `cd backend && go get github.com/go-chi/chi/v5`

- [ ] **Step 10: Write the failing Reconciler test**

```go
// backend/tests/reconcile/reconcile_test.go
package reconcile_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sre-platform/backend/internal/execute"
	"sre-platform/backend/internal/gate"
	"sre-platform/backend/internal/k8swatch"
	"sre-platform/backend/internal/reconcile"
	"sre-platform/backend/internal/signal"
	"sre-platform/backend/internal/slackapproval"
	"sre-platform/backend/internal/store"
)

// TestWatcherToReconciler_HealsCrashLoopInAutoMode drives the full detect ->
// correlate -> analyze -> gate -> execute -> verify -> audit loop for a
// crash-looping pod in auto mode, using the in-memory Store and a fake
// Kubernetes clientset so it needs no live cluster or Slack workspace. This
// is the plan's end-to-end test for the core loop.
func TestWatcherToReconciler_HealsCrashLoopInAutoMode(t *testing.T) {
	ctx := context.Background()
	crashingPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default", Labels: map[string]string{"app": "web"}},
	}
	healthyReplacement := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-2", Namespace: "default", Labels: map[string]string{"app": "web"}},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	clientset := fake.NewSimpleClientset(crashingPod, healthyReplacement)
	memStore := store.NewMemoryStore()
	executor := execute.NewExecutor(clientset)
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)

	r := reconcile.New(memStore, executor, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)
	watcher := k8swatch.NewWatcher(clientset, func(s signal.Signal) { r.OnSignal(ctx, s) })

	watcher.HandleAddEvent(&corev1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "web-1"},
		Reason:         "BackOff",
		Message:        "Back-off restarting failed container",
		LastTimestamp:  metav1.NewTime(time.Now()),
	})

	var resolved bool
	for _, action := range memStore.Actions {
		if action.Status == "verified" && action.Outcome == "resolved" {
			resolved = true
		}
	}
	if !resolved {
		t.Fatalf("expected a verified/resolved remediation action, got: %+v", memStore.Actions)
	}
	if len(memStore.AuditEntries) != 1 {
		t.Errorf("expected 1 audit entry recorded, got %d", len(memStore.AuditEntries))
	}
}

func TestReconciler_OnSignal_ManualModeRequestsApprovalAndDoesNotExecute(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default", Labels: map[string]string{"app": "web"}},
	}
	clientset := fake.NewSimpleClientset(pod)
	memStore := store.NewMemoryStore()
	executor := execute.NewExecutor(clientset)
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0" // unreachable on purpose — approval path must not need it to succeed for this assertion

	r := reconcile.New(memStore, executor, slackClient, clientset, gate.ModeManual, 60*time.Second, 2*time.Second)

	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "CrashLoopBackOff",
		Namespace: "default", Kind: "Pod", Name: "web-1",
		Labels: map[string]string{"app": "web"}, Timestamp: time.Now(),
	})

	for _, action := range memStore.Actions {
		if action.Status == "executed" || action.Status == "verified" {
			t.Fatalf("expected manual mode to stop before execution, got status %q", action.Status)
		}
		if !action.RequiresApproval {
			t.Errorf("expected RequiresApproval=true in manual mode")
		}
	}
}
```

- [ ] **Step 11: Run test to verify it fails**

Run: `cd backend && go test ./tests/reconcile/... -v`
Expected: FAIL — `package reconcile: no Go files`.

- [ ] **Step 12: Write `reconcile.go`**

```go
// backend/internal/reconcile/reconcile.go
package reconcile

import (
	"context"
	"log"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"

	"sre-platform/backend/internal/analyze"
	"sre-platform/backend/internal/correlate"
	"sre-platform/backend/internal/gate"
	"sre-platform/backend/internal/signal"
	"sre-platform/backend/internal/slackapproval"
	"sre-platform/backend/internal/store"
	"sre-platform/backend/internal/verify"
)

// PodRestarter is satisfied by both execute.Executor (direct client-go calls,
// used until Task 14) and mcpexecute.Client (calls restart_pod over MCP,
// wired in by Task 14) — the Reconciler doesn't care which is used, only
// that it can restart a pod. This is what lets Task 14 swap the call path
// without changing this file.
type PodRestarter interface {
	RestartPod(ctx context.Context, namespace, name string) error
}

type Reconciler struct {
	mu                sync.Mutex
	pending           []signal.Signal
	store             store.Store
	restarter         PodRestarter
	slack             *slackapproval.Client
	clientset         kubernetes.Interface
	analyzer          analyze.CrashLoopAnalyzer
	mode              gate.Mode
	correlationWindow time.Duration
	verifyTimeout     time.Duration
}

func New(s store.Store, restarter PodRestarter, slack *slackapproval.Client, clientset kubernetes.Interface, mode gate.Mode, correlationWindow, verifyTimeout time.Duration) *Reconciler {
	return &Reconciler{
		store: s, restarter: restarter, slack: slack, clientset: clientset,
		mode: mode, correlationWindow: correlationWindow, verifyTimeout: verifyTimeout,
	}
}

// OnSignal is the watcher's callback: append the new signal, re-correlate
// everything pending, and drive any newly-recognized incident through
// analyze -> gate -> execute -> verify -> audit.
func (r *Reconciler) OnSignal(ctx context.Context, s signal.Signal) {
	r.mu.Lock()
	r.pending = append(r.pending, s)
	pendingCopy := append([]signal.Signal{}, r.pending...)
	r.mu.Unlock()

	for _, incident := range correlate.Correlate(pendingCopy, r.correlationWindow) {
		r.handleIncident(ctx, incident)
	}
}

func (r *Reconciler) handleIncident(ctx context.Context, incident correlate.Incident) {
	diagnosis, matched := r.analyzer.Analyze(incident)
	if !matched {
		return
	}

	incidentID, err := r.store.CreateIncident(ctx, incident.Namespace, incident.Kind, incident.Name, diagnosis.FailureMode, incident.FirstSeen, incident.LastSeen)
	if err != nil {
		log.Printf("CreateIncident: %v", err)
		return
	}

	decision := gate.Evaluate(diagnosis.RecommendedAction, r.mode)
	actionID, err := r.store.CreateRemediationAction(ctx, incidentID, diagnosis.RecommendedAction, decision.RequiresApproval, decision.Reason)
	if err != nil {
		log.Printf("CreateRemediationAction: %v", err)
		return
	}

	if decision.RequiresApproval {
		if _, err := r.slack.PostApproval(ctx, slackapproval.ApprovalRequest{
			IncidentID: incidentID, ActionID: actionID,
			FailureMode: diagnosis.FailureMode, Action: diagnosis.RecommendedAction,
			Namespace: incident.Namespace, Name: incident.Name,
		}); err != nil {
			log.Printf("PostApproval: %v", err)
		}
		return // execution resumes from the Slack interaction handler in a later plan
	}

	r.executeAndVerify(ctx, incident, incidentID, actionID)
}

func (r *Reconciler) executeAndVerify(ctx context.Context, incident correlate.Incident, incidentID, actionID string) {
	if err := r.restarter.RestartPod(ctx, incident.Namespace, incident.Name); err != nil {
		log.Printf("RestartPod: %v", err)
		return
	}
	if err := r.store.MarkExecuted(ctx, actionID); err != nil {
		log.Printf("MarkExecuted: %v", err)
	}

	healthy, err := verify.CheckPodHealthy(ctx, r.clientset, incident.Namespace, incident.Signals[0].Labels, r.verifyTimeout, time.Second)
	if err != nil {
		log.Printf("CheckPodHealthy: %v", err)
		return
	}
	outcome := "resolved"
	if !healthy {
		outcome = "unresolved"
	}
	if err := r.store.MarkVerified(ctx, actionID, outcome); err != nil {
		log.Printf("MarkVerified: %v", err)
	}
	if err := r.store.WriteAudit(ctx, incidentID, "remediation_verified", map[string]any{"outcome": outcome}); err != nil {
		log.Printf("WriteAudit: %v", err)
	}
}
```

- [ ] **Step 13: Run test to verify it passes**

Run: `cd backend && go test ./tests/reconcile/... -v`
Expected: `PASS` (both tests).

- [ ] **Step 14: Write the failing router test**

```go
// backend/tests/httpserver/router_test.go
package httpserver_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"sre-platform/backend/internal/httpserver"
	"sre-platform/backend/internal/slackapproval"
	"sre-platform/backend/internal/store"
)

func TestNewRouter_HealthzReturnsOK(t *testing.T) {
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	router := httpserver.NewRouter(slackClient, store.NewMemoryStore())

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestNewRouter_RoutesSlackInteractionsUnderPrefix(t *testing.T) {
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	router := httpserver.NewRouter(slackClient, store.NewMemoryStore())

	// No signature header, so the handler itself should reject with 401 — a
	// 404 here would mean the /slack prefix group isn't actually wired,
	// which is what this test guards against.
	req := httptest.NewRequest(http.MethodPost, "/slack/interactions", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code == http.StatusNotFound {
		t.Fatal("expected /slack/interactions to be routed, got 404 — check the /slack prefix group")
	}
}
```

- [ ] **Step 15: Run test to verify it fails**

Run: `cd backend && go test ./tests/httpserver/... -v`
Expected: FAIL — `package httpserver: no Go files`.

- [ ] **Step 16: Write `router.go`**

```go
// backend/internal/httpserver/router.go
package httpserver

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"sre-platform/backend/internal/slackapproval"
	"sre-platform/backend/internal/store"
)

// NewRouter builds backend/'s entire HTTP surface, grouped by prefix, so
// main.go never defines a route directly. New route groups (a future REST
// API for the dashboard, additional webhooks) attach here.
func NewRouter(slackClient *slackapproval.Client, s store.Store) http.Handler {
	r := chi.NewRouter()

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	r.Route("/slack", func(r chi.Router) {
		r.Post("/interactions", slackClient.InteractionHandler(s))
	})

	return r
}
```

- [ ] **Step 17: Run test to verify it passes**

Run: `cd backend && go test ./tests/httpserver/... -v`
Expected: `PASS` (both tests).

- [ ] **Step 18: Write the thin `main.go`**

```go
// backend/cmd/backend/main.go
package main

import (
	"context"
	"log"
	"net/http"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"sre-platform/backend/internal/execute"
	"sre-platform/backend/internal/httpserver"
	"sre-platform/backend/internal/k8swatch"
	"sre-platform/backend/internal/reconcile"
	"sre-platform/backend/internal/settings"
	"sre-platform/backend/internal/signal"
	"sre-platform/backend/internal/slackapproval"
	"sre-platform/backend/internal/store"
)

func main() {
	cfg := settings.Load()
	ctx := context.Background()

	clientset := buildClientset(cfg.Kubeconfig)
	pgStore, err := store.NewPostgresStore(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("connecting to Postgres: %v", err)
	}
	slackClient := slackapproval.NewClient(cfg.SlackBotToken, cfg.SlackApprovalChannel, cfg.SlackSigningSecret, http.DefaultClient)
	restarter := execute.NewExecutor(clientset)

	reconciler := reconcile.New(pgStore, restarter, slackClient, clientset, cfg.Mode, cfg.CorrelationWindow, cfg.VerifyTimeout)
	watcher := k8swatch.NewWatcher(clientset, func(s signal.Signal) { reconciler.OnSignal(ctx, s) })

	router := httpserver.NewRouter(slackClient, pgStore)
	go func() {
		log.Printf("listening on %s", cfg.HTTPAddr)
		log.Fatal(http.ListenAndServe(cfg.HTTPAddr, router))
	}()

	if err := watcher.Run(ctx); err != nil {
		log.Fatalf("watcher.Run: %v", err)
	}
}

func buildClientset(kubeconfig string) kubernetes.Interface {
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatalf("building kubeconfig: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		log.Fatalf("building clientset: %v", err)
	}
	return clientset
}
```

- [ ] **Step 19: Build to catch wiring errors**

Run: `cd backend && go build ./...`
Expected: builds with no errors.

- [ ] **Step 20: Stage the changes (do not commit — see Commit Policy)**

```bash
git add backend/internal/k8swatch backend/internal/settings backend/internal/reconcile backend/internal/httpserver backend/tests/k8swatch backend/tests/settings backend/tests/reconcile backend/tests/httpserver backend/cmd backend/go.mod backend/go.sum
```
Commit message for the user to use: `feat: wire detect->correlate->analyze->gate->execute->verify loop end-to-end via a Reconciler and chi router`

---

### Task 12: MCP execute server — bearer-token-protected `restart_pod` tool

**Files:**
- Create: `backend/internal/mcpauth/bearer.go`
- Create: `backend/cmd/mcp-execute-server/main.go`
- Test: `backend/tests/mcpauth/bearer_test.go`

**Interfaces:**
- Consumes: `execute.Executor`/`execute.NewExecutor` from Task 8, `settings.Load()` from Task 11.
- Produces: `mcpauth.RequireBearerToken(expectedToken string, next http.Handler) http.Handler` — constant-time bearer-token check, used to independently re-validate the caller before any MCP tool call reaches `execute.Executor`. A running `mcp-execute-server` process exposing one MCP tool, `restart_pod`, over Streamable HTTP.

- [ ] **Step 1: Add the MCP SDK dependency**

Run: `cd backend && go get github.com/modelcontextprotocol/go-sdk/mcp`

- [ ] **Step 2: Write the failing bearer-auth test**

```go
// backend/tests/mcpauth/bearer_test.go
package mcpauth_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"sre-platform/backend/internal/mcpauth"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func TestRequireBearerToken_AllowsMatchingToken(t *testing.T) {
	handler := mcpauth.RequireBearerToken("secret-token", okHandler())

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestRequireBearerToken_RejectsWrongToken(t *testing.T) {
	handler := mcpauth.RequireBearerToken("secret-token", okHandler())

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestRequireBearerToken_RejectsMissingHeader(t *testing.T) {
	handler := mcpauth.RequireBearerToken("secret-token", okHandler())

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd backend && go test ./tests/mcpauth/... -v`
Expected: FAIL — `package mcpauth: no Go files`.

- [ ] **Step 4: Write `bearer.go`**

```go
// backend/internal/mcpauth/bearer.go
package mcpauth

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// RequireBearerToken wraps an http.Handler so every request must present
// "Authorization: Bearer <token>" matching expectedToken, checked in
// constant time. This is the MCP server's own independent check of the
// backend/'s permission token — it does not trust backend/'s Gate decision
// alone (design doc §6, §8).
func RequireBearerToken(expectedToken string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(auth, prefix) {
			http.Error(w, "missing bearer token", http.StatusUnauthorized)
			return
		}
		token := strings.TrimPrefix(auth, prefix)
		if subtle.ConstantTimeCompare([]byte(token), []byte(expectedToken)) != 1 {
			http.Error(w, "invalid bearer token", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd backend && go test ./tests/mcpauth/... -v`
Expected: `PASS` (all three tests).

- [ ] **Step 6: Write the MCP execute server entrypoint**

```go
// backend/cmd/mcp-execute-server/main.go
package main

import (
	"context"
	"log"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"sre-platform/backend/internal/execute"
	"sre-platform/backend/internal/mcpauth"
	"sre-platform/backend/internal/settings"
)

type RestartPodInput struct {
	Namespace string `json:"namespace" jsonschema:"the pod's namespace"`
	Name      string `json:"name" jsonschema:"the pod's name"`
}

type RestartPodOutput struct {
	Status string `json:"status" jsonschema:"result of the restart, e.g. 'deleted'"`
}

func main() {
	// settings.Load() fails fast (log.Fatal) if MCP_EXECUTE_TOKEN or any
	// other required var is missing — this process cannot reach
	// ListenAndServe below with incomplete config.
	s := settings.Load()

	restConfig, err := clientcmd.BuildConfigFromFlags("", s.Kubeconfig)
	if err != nil {
		log.Fatalf("building kubeconfig: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		log.Fatalf("building clientset: %v", err)
	}
	executor := execute.NewExecutor(clientset)

	restartPod := func(ctx context.Context, req *mcp.CallToolRequest, input RestartPodInput) (*mcp.CallToolResult, RestartPodOutput, error) {
		if err := executor.RestartPod(ctx, input.Namespace, input.Name); err != nil {
			return nil, RestartPodOutput{}, err
		}
		return nil, RestartPodOutput{Status: "deleted"}, nil
	}

	server := mcp.NewServer(&mcp.Implementation{Name: "sre-execute", Version: "v1.0.0"}, nil)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "restart_pod",
		Description: "Deletes a pod so its owning controller recreates it. Idempotent — safe to call on an already-gone pod.",
	}, restartPod)

	handler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		return server
	}, nil)

	log.Printf("mcp-execute-server listening on %s", s.MCPExecuteAddr)
	log.Fatal(http.ListenAndServe(s.MCPExecuteAddr, mcpauth.RequireBearerToken(s.MCPExecuteToken, handler)))
}
```

- [ ] **Step 7: Add its env vars to `.env.example`**

Append to `.env.example`:
```dotenv
# MCP execute server (backend/'s sole path to mutate the cluster)
MCP_EXECUTE_ADDR=:8090
MCP_EXECUTE_TOKEN=placeholder-shared-secret-token
```

- [ ] **Step 8: Build to catch wiring errors**

Run: `cd backend && go build ./...`
Expected: builds with no errors.

- [ ] **Step 9: Stage the changes (do not commit — see Commit Policy)**

```bash
git add backend/internal/mcpauth backend/tests/mcpauth backend/cmd/mcp-execute-server .env.example backend/go.mod backend/go.sum
```
Commit message for the user to use: `feat: MCP execute server exposing restart_pod behind bearer-token auth`

---

### Task 13: MCP execute client — `backend/`'s call path to the execute server

**Files:**
- Create: `backend/internal/mcpexecute/client.go`
- Test: `backend/tests/mcpexecute/client_test.go`

**Interfaces:**
- Consumes: `mcpauth.RequireBearerToken` (Task 12) and `execute.NewExecutor` (Task 8) — the test spins up its own in-process MCP server using both, so it exercises a real MCP round trip without a live Kubernetes cluster.
- Produces: `mcpexecute.NewClient(ctx context.Context, endpoint, token string) (*mcpexecute.Client, error)`, `(*mcpexecute.Client).RestartPod(ctx context.Context, namespace, name string) error`. This is what Task 14 wires into `main.go` in place of calling `execute.Executor` directly.

- [ ] **Step 1: Write the failing test**

```go
// backend/tests/mcpexecute/client_test.go
package mcpexecute_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sre-platform/backend/internal/execute"
	"sre-platform/backend/internal/mcpauth"
	"sre-platform/backend/internal/mcpexecute"
)

type restartPodInput struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type restartPodOutput struct {
	Status string `json:"status"`
}

// startTestServer spins up a real MCP server, over real HTTP, backed by a
// fake Kubernetes clientset — so the test below exercises the actual MCP
// wire protocol and bearer-token check, not a mock of them.
func startTestServer(t *testing.T, token string, clientset *fake.Clientset) *httptest.Server {
	t.Helper()
	executor := execute.NewExecutor(clientset)

	restartPod := func(ctx context.Context, req *mcp.CallToolRequest, input restartPodInput) (*mcp.CallToolResult, restartPodOutput, error) {
		if err := executor.RestartPod(ctx, input.Namespace, input.Name); err != nil {
			return nil, restartPodOutput{}, err
		}
		return nil, restartPodOutput{Status: "deleted"}, nil
	}

	server := mcp.NewServer(&mcp.Implementation{Name: "sre-execute-test", Version: "v1.0.0"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "restart_pod", Description: "test tool"}, restartPod)

	handler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server { return server }, nil)
	ts := httptest.NewServer(mcpauth.RequireBearerToken(token, handler))
	t.Cleanup(ts.Close)
	return ts
}

func TestClient_RestartPod_CallsToolOverMCP(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default"}}
	clientset := fake.NewSimpleClientset(pod)

	ts := startTestServer(t, "test-token", clientset)

	client, err := mcpexecute.NewClient(ctx, ts.URL, "test-token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	if err := client.RestartPod(ctx, "default", "web-1"); err != nil {
		t.Fatalf("RestartPod: %v", err)
	}

	_, err = clientset.CoreV1().Pods("default").Get(ctx, "web-1", metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("expected pod to be deleted via the MCP call, got err=%v", err)
	}
}

func TestClient_NewClient_FailsWithWrongToken(t *testing.T) {
	ctx := context.Background()
	clientset := fake.NewSimpleClientset()
	ts := startTestServer(t, "correct-token", clientset)

	_, err := mcpexecute.NewClient(ctx, ts.URL, "wrong-token")
	if err == nil {
		t.Fatal("expected NewClient to fail the MCP handshake when the bearer token is wrong")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && go test ./tests/mcpexecute/... -v`
Expected: FAIL — `package mcpexecute: no Go files`.

- [ ] **Step 3: Write `client.go`**

```go
// backend/internal/mcpexecute/client.go
package mcpexecute

import (
	"context"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type Client struct {
	session *mcp.ClientSession
}

type authRoundTripper struct {
	token string
	base  http.RoundTripper
}

func (rt authRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer "+rt.token)
	return rt.base.RoundTrip(req)
}

// NewClient connects to a running mcp-execute-server at endpoint,
// authenticating every request with token. The MCP handshake itself goes
// through the server's bearer-token check, so an invalid token fails here.
func NewClient(ctx context.Context, endpoint, token string) (*Client, error) {
	httpClient := &http.Client{Transport: authRoundTripper{token: token, base: http.DefaultTransport}}

	mcpClient := mcp.NewClient(&mcp.Implementation{Name: "sre-backend", Version: "v1.0.0"}, nil)
	session, err := mcpClient.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: endpoint, HTTPClient: httpClient}, nil)
	if err != nil {
		return nil, err
	}
	return &Client{session: session}, nil
}

func (c *Client) RestartPod(ctx context.Context, namespace, name string) error {
	_, err := c.session.CallTool(ctx, &mcp.CallToolParams{
		Name: "restart_pod",
		Arguments: map[string]any{
			"namespace": namespace,
			"name":      name,
		},
	})
	return err
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && go test ./tests/mcpexecute/... -v`
Expected: `PASS` (both tests). If the SDK's exported type/method names have shifted since this plan was written, run `go doc github.com/modelcontextprotocol/go-sdk/mcp` to find the current names and adjust — the behavior this task requires (connect with a bearer-token-injecting HTTP client, call the `restart_pod` tool, surface handshake failures as an error from `NewClient`) does not change.

- [ ] **Step 5: Stage the changes (do not commit — see Commit Policy)**

```bash
git add backend/internal/mcpexecute backend/tests/mcpexecute backend/go.mod backend/go.sum
```
Commit message for the user to use: `feat: MCP execute client used by backend/'s core loop`

---

### Task 14: Wire `backend/`'s core loop to execute via MCP instead of in-process

**Files:**
- Modify: `backend/internal/settings/settings.go`
- Modify: `backend/cmd/backend/main.go`
- Modify: `backend/tests/reconcile/reconcile_test.go`
- Modify: `.env.example`

**Interfaces:**
- Consumes: `mcpexecute.NewClient`/`(*Client).RestartPod` (Task 13), `reconcile.PodRestarter` (Task 11).
- Produces: nothing new — this task replaces `execute.NewExecutor(clientset)` as the concrete `reconcile.PodRestarter` used by the core loop with `mcpexecute.Client`. Because `Reconciler` already depends on the `PodRestarter` interface (Task 11), `reconcile.go` itself needs no changes — only `main.go` (which constructs the restarter) and the test (which should now prove the real call path) change.

- [ ] **Step 1: Add MCP client config to `config.go`**

In `backend/internal/settings/settings.go`, add two fields to `Settings` and load them in `Load()`:

```go
// Add to the Config struct:
	MCPExecuteURL   string
	MCPExecuteToken string
```

```go
// Add inside Load()'s returned Settings{...}:
		MCPExecuteURL:   getenv("MCP_EXECUTE_URL", "http://localhost:8090"),
		MCPExecuteToken: getenv("MCP_EXECUTE_TOKEN", ""),
```

- [ ] **Step 2: Add the client-side env var to `.env.example`**

Append:
```dotenv
# backend/'s MCP execute client (must match mcp-execute-server's MCP_EXECUTE_TOKEN)
MCP_EXECUTE_URL=http://localhost:8090
```

- [ ] **Step 3: Rewrite `TestWatcherToReconciler_HealsCrashLoopInAutoMode` to exercise a real MCP round trip**

In `backend/tests/reconcile/reconcile_test.go`, swap the direct `execute.NewExecutor(clientset)` restarter for a real MCP client talking to an in-process test server (same pattern as Task 13's `startTestServer`), so this test proves the actual production call path:

```go
// Replace this import block:
//   "sre-platform/backend/internal/execute"
// with:
	"net/http/httptest"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"sre-platform/backend/internal/execute"
	"sre-platform/backend/internal/mcpauth"
	"sre-platform/backend/internal/mcpexecute"
```

```go
// Replace:
//   executor := execute.NewExecutor(clientset)
// with:
	type restartPodInput struct {
		Namespace string `json:"namespace"`
		Name      string `json:"name"`
	}
	type restartPodOutput struct {
		Status string `json:"status"`
	}

	executor := execute.NewExecutor(clientset)
	mcpServer := mcp.NewServer(&mcp.Implementation{Name: "sre-execute-test", Version: "v1.0.0"}, nil)
	mcp.AddTool(mcpServer, &mcp.Tool{Name: "restart_pod", Description: "test tool"}, func(ctx context.Context, req *mcp.CallToolRequest, input restartPodInput) (*mcp.CallToolResult, restartPodOutput, error) {
		if err := executor.RestartPod(ctx, input.Namespace, input.Name); err != nil {
			return nil, restartPodOutput{}, err
		}
		return nil, restartPodOutput{Status: "deleted"}, nil
	})
	mcpHandler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server { return mcpServer }, nil)
	ts := httptest.NewServer(mcpauth.RequireBearerToken("test-token", mcpHandler))
	defer ts.Close()

	mcpClient, err := mcpexecute.NewClient(ctx, ts.URL, "test-token")
	if err != nil {
		t.Fatalf("mcpexecute.NewClient: %v", err)
	}
```

```go
// Replace the reconcile.New(...) call's second argument:
//   r := reconcile.New(memStore, executor, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)
// with:
	r := reconcile.New(memStore, mcpClient, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)
```

- [ ] **Step 4: Run the reconcile package tests to verify they still pass**

Run: `cd backend && go test ./tests/reconcile/... -v`
Expected: `PASS` — `TestWatcherToReconciler_HealsCrashLoopInAutoMode` now exercises a real MCP handshake, bearer-token check, and tool call instead of an in-process function call; `TestReconciler_OnSignal_ManualModeRequestsApprovalAndDoesNotExecute` is untouched and still passes.

- [ ] **Step 5: Wire `main.go` to use the MCP client**

In `backend/cmd/backend/main.go`:

```go
// Replace this import:
//   "sre-platform/backend/internal/execute"
// with:
	"sre-platform/backend/internal/mcpexecute"
```

```go
// Replace:
//   restarter := execute.NewExecutor(clientset)
// with:
	restarter, err := mcpexecute.NewClient(ctx, cfg.MCPExecuteURL, cfg.MCPExecuteToken)
	if err != nil {
		log.Fatalf("connecting to mcp-execute-server: %v", err)
	}
```

No other line in `main.go` changes — `reconcile.New(pgStore, restarter, ...)` already takes `restarter` as the `PodRestarter` interface, so passing an `*mcpexecute.Client` instead of an `*execute.Executor` there is the entire change.

- [ ] **Step 6: Build both binaries to catch wiring errors**

Run: `cd backend && go build ./...`
Expected: builds with no errors — this compiles both `cmd/backend` and `cmd/mcp-execute-server`.

- [ ] **Step 7: Run the full backend test suite**

Run: `cd backend && go test ./... -v`
Expected: `PASS` across all packages.

- [ ] **Step 8: Stage the changes (do not commit — see Commit Policy)**

```bash
git add backend/internal/settings backend/tests/reconcile backend/cmd/backend .env.example
```
Commit message for the user to use: `refactor: route backend/'s Execute step through the MCP execute server`

---

## Self-Review Notes

- **Spec coverage**: §3 Detection (fast path — K8s events via watcher; slow path/anomaly detection is a later plan, noted in Task list), §4 Correlation (Task 5; dependency-graph-based correlation deferred to its own plan per Scope Note), §6 Plan/Gate/Execute/Verify (Tasks 7–9, 11), the hard safety floor and audit-log invariants (Tasks 7, 2), Slack approval channel (Task 10), §8 MCP execute path — backend/ as an MCP client, bearer-token re-validated independently by the MCP server (Tasks 12–14). §5 (LangGraph diagnosis), §7 Qdrant knowledge store, the read-only MCP tool set for ai/, and the full frontend-configurable permission/token system (v1.5 per design doc §1/§6 — v1 uses one static shared-secret token, which is what Tasks 12–14 build) are explicitly out of scope for this plan (see Scope Note) and belong to follow-up plans.
- **Placeholder scan**: no TBD/TODO markers; the one deferred-behavior note in `main.go` (Slack-approved actions resuming execution) is called out explicitly as follow-up-plan scope, not left as an unimplemented stub silently.
- **Type consistency**: `signal.Signal`, `correlate.Incident`, `analyze.Diagnosis`, `gate.Decision`, `store.Store` method signatures are used identically across Tasks 5–11 and in the integration test in Task 11. Task 14 updates that same integration test and `main.go` to route through `mcpexecute.Client` (Task 13) instead of calling `execute.Executor` (Task 8) directly — `execute.Executor` itself is unchanged, just no longer called in-process from the core loop.

## Follow-up Plans (not in this document)

1. Dependency graph (K8s topology + eBPF/OTel enrichment) + blast-radius gate.
2. `ai/` LangGraph diagnosis agent, Qdrant similarity search, and its **read-only** MCP tool set. MCP tool sourcing decision (see design doc §8): adopt
   [`containers/kubernetes-mcp-server`](https://github.com/containers/kubernetes-mcp-server)
   run with `--read-only` for ai/'s general K8s read tools (logs, events,
   resource listing) rather than hand-writing them; write custom tools only
   for what that server doesn't cover (Prometheus, Loki, dependency graph,
   Qdrant, Postgres history). (The **execute** side — backend/'s MCP server
   and client — is no longer a follow-up; it's built in this plan's Tasks
   12–14, using the same narrow hand-written action approach described in
   design doc §8's reasoning against adopting that project's generic write
   mode.)
3. Resuming execution after a Slack-approved manual decision (currently the interaction handler records the decision; wiring it back to trigger Execute is part of this follow-up).
4. Frontend UI for admins to create/edit MCP execute-tool permissions and generate scoped tokens (design doc §1/§6, v1.5) — v1 (Tasks 12–14) uses one static shared-secret token from config instead.
5. Frontend dashboard (incident history, audit log).
6. Kafka event backbone wiring (topics per design doc §2 Event Backbone) + Postgres partitioning (scaling triggers, per design doc §10).
