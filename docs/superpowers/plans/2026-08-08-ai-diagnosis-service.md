# ai/ Diagnosis Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the hardcoded Go `CrashLoopAnalyzer` with a real `ai/` diagnosis service — rule-based analyzers falling back to an LLM investigation loop over read-only MCP tools — connected to the Go backend asynchronously via NATS JetStream.

**Architecture:** Go backend detects/correlates/persists a `pending_diagnosis` incident and publishes it to NATS; a new Python `ai/` service consumes it, diagnoses it (rules first, then a LangGraph LLM agent calling a new read-only `mcp-readonly-server`), and POSTs the result back to a new backend HTTP callback that resumes gate→execute→verify.

**Tech Stack:** Go 1.26 (existing backend), `github.com/nats-io/nats.go` (JetStream), Python 3.11+, LangGraph + LangChain (`langchain-anthropic`, `langchain-openai`), `nats-py`, `mcp` Python SDK / `langchain-mcp-adapters`, Helm (new `nats` subchart dependency).

## Global Constraints

- Spec: `docs/superpowers/specs/2026-08-08-ai-diagnosis-service-design.md`. Read it before starting — every task below implements a specific section of it.
- Standing repo rules (from `CLAUDE.md`/project convention, unchanged by this plan): implementers **stage but never commit** (`git add` only, end every task there); `cmd/*/main.go` stays thin (wiring only, no logic); every environment variable is read exactly once, in `backend/internal/settings/settings.go` (Go) or `ai/ai/settings.py` (Python) — no `os.Getenv`/`os.environ` anywhere else; Go tests live in `backend/tests/<pkg>/`, mirroring `backend/internal/<pkg>/` (never co-located `_test.go` files); Python tests live in `ai/tests/`.
- ai/'s diagnosis output is constrained to exactly two `recommended_action` values this phase: `"restart_pod"` and `"none"` — nothing else is wired to an executor yet (spec §5).
- Every new Go package follows the existing small-interface-at-the-call-site pattern already used for `PodRestarter` (`backend/internal/reconcile/reconcile.go`) — define the interface where it's consumed, not where it's implemented.
- Go module is `sre-platform/backend` (Go 1.26); MCP Go SDK is already a dependency at `github.com/modelcontextprotocol/go-sdk v1.6.1` — reuse it for the new server, don't add a second MCP library.
- Every new Helm template mirrors an existing one byte-for-byte in structure (auth pattern, security context, RBAC scoping) — see each task's "mirrors" pointer.

---

## Phase A — Go backend: async dispatch + resume

### Task 1: Store — `CreatePendingIncident` + `RecordDiagnosis`

Removing `CreateIncident` from the `Store` interface breaks `backend/internal/reconcile/reconcile.go`, which still calls it — that file isn't touched until Task 4. This is expected and intentional (same pattern as Tasks 2, 4, and 5 elsewhere in this plan): do not modify `reconcile.go` here, and don't expect a whole-module `go build ./...` to pass after this task — see Step 7.

`backend/tests/slackapproval/slack_test.go` also calls the old `CreateIncident(...)` signature (as a test-fixture setup call, unrelated to what that file actually tests) — unlike `reconcile.go`, nothing later in this plan ever revisits `slackapproval`'s tests, so this one genuinely must be fixed here, not deferred. Update its one call site to `CreatePendingIncident(...)` (drop the `"CrashLoopBackOff"` argument — the fixture doesn't need a real diagnosis, just a valid incident ID).

**Files:**
- Create: `backend/migrations/0002_pending_diagnosis.sql`
- Modify: `backend/internal/store/store.go`
- Modify: `backend/internal/store/postgres.go`
- Modify: `backend/internal/store/memory.go`
- Modify: `backend/tests/store/memory_test.go`
- Modify: `backend/tests/store/postgres_test.go`
- Modify: `backend/tests/slackapproval/slack_test.go` (one-line fixture call update, see above)

**Interfaces:**
- Produces: `store.Store` gains `CreatePendingIncident(ctx context.Context, namespace, kind, name string, firstSeen, lastSeen time.Time) (string, error)` and `RecordDiagnosis(ctx context.Context, incidentID, failureMode string) error`. `CreateIncident` (the old signature, taking `failureMode` up front) is **removed** — nothing may call it after this task.

- [ ] **Step 1: Write the migration**

```sql
-- backend/migrations/0002_pending_diagnosis.sql
-- Diagnosis now happens asynchronously in ai/ (see docs/superpowers/specs/2026-08-08-ai-diagnosis-service-design.md),
-- so an incident is persisted before its failure_mode is known.
ALTER TABLE incidents ALTER COLUMN failure_mode DROP NOT NULL;
```

- [ ] **Step 2: Write the failing memory-store test**

Replace `TestMemoryStore_SatisfiesStoreInterface` in `backend/tests/store/memory_test.go`:

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

	incidentID, err := s.CreatePendingIncident(ctx, "default", "Pod", "web-1", now, now)
	if err != nil || incidentID == "" {
		t.Fatalf("CreatePendingIncident: id=%q err=%v", incidentID, err)
	}
	if got := s.Incidents[incidentID].Status; got != "pending_diagnosis" {
		t.Fatalf("expected status 'pending_diagnosis' right after creation, got %q", got)
	}

	if err := s.RecordDiagnosis(ctx, incidentID, "CrashLoopBackOff"); err != nil {
		t.Fatalf("RecordDiagnosis: %v", err)
	}
	if got := s.Incidents[incidentID]; got.Status != "diagnosed" || got.FailureMode != "CrashLoopBackOff" {
		t.Fatalf("expected status 'diagnosed' and failure_mode 'CrashLoopBackOff' after RecordDiagnosis, got status=%q failure_mode=%q", got.Status, got.FailureMode)
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

	if s.Actions[actionID].Status != "verified" {
		t.Errorf("expected action status 'verified', got %q", s.Actions[actionID].Status)
	}
	if len(s.AuditEntries) != 1 {
		t.Errorf("expected 1 audit entry, got %d", len(s.AuditEntries))
	}
}

func TestMemoryStore_RecordDiagnosis_UnknownIncidentErrors(t *testing.T) {
	s := store.NewMemoryStore()
	if err := s.RecordDiagnosis(context.Background(), "does-not-exist", "CrashLoopBackOff"); err == nil {
		t.Fatal("expected an error recording a diagnosis for an unknown incident id")
	}
}
```

Also update `backend/tests/store/postgres_test.go`'s `TestPostgresStore_IncidentLifecycle`:

```go
// replace the CreateIncident call and add a RecordDiagnosis call right after it
	now := time.Now().UTC()
	incidentID, err := s.CreatePendingIncident(ctx, "default", "Pod", "web-1", now, now)
	if err != nil {
		t.Fatalf("CreatePendingIncident: %v", err)
	}
	if incidentID == "" {
		t.Fatal("expected non-empty incident ID")
	}
	if err := s.RecordDiagnosis(ctx, incidentID, "CrashLoopBackOff"); err != nil {
		t.Fatalf("RecordDiagnosis: %v", err)
	}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd backend && go test ./tests/store/... -run TestMemoryStore -v`
Expected: FAIL — `CreatePendingIncident`/`RecordDiagnosis` undefined on `*store.MemoryStore`.

- [ ] **Step 4: Update `store.go`'s interface**

In `backend/internal/store/store.go`, replace the `CreateIncident` line in the `Store` interface:

```go
type Store interface {
	CreatePendingIncident(ctx context.Context, namespace, kind, name string, firstSeen, lastSeen time.Time) (string, error)
	RecordDiagnosis(ctx context.Context, incidentID, failureMode string) error
	CreateRemediationAction(ctx context.Context, incidentID, actionType string, requiresApproval bool, reason string) (string, error)
	RecordApprovalDecision(ctx context.Context, actionID, decidedBy, decision string) error
	MarkExecuted(ctx context.Context, actionID string) error
	MarkVerified(ctx context.Context, actionID, outcome string) error
	WriteAudit(ctx context.Context, incidentID, eventType string, detail map[string]any) error
}
```

- [ ] **Step 5: Implement in `postgres.go`**

Replace `CreateIncident` in `backend/internal/store/postgres.go` with:

```go
func (s *PostgresStore) CreatePendingIncident(ctx context.Context, namespace, kind, name string, firstSeen, lastSeen time.Time) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx,
		`INSERT INTO incidents (namespace, kind, name, status, first_seen, last_seen)
		 VALUES ($1,$2,$3,'pending_diagnosis',$4,$5) RETURNING id`,
		namespace, kind, name, firstSeen, lastSeen,
	).Scan(&id)
	return id, err
}

func (s *PostgresStore) RecordDiagnosis(ctx context.Context, incidentID, failureMode string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE incidents SET failure_mode = $2, status = 'diagnosed' WHERE id = $1`,
		incidentID, failureMode,
	)
	return err
}
```

- [ ] **Step 6: Implement in `memory.go`**

Replace `CreateIncident` in `backend/internal/store/memory.go` with:

```go
func (s *MemoryStore) CreatePendingIncident(ctx context.Context, namespace, kind, name string, firstSeen, lastSeen time.Time) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextID("incident")
	s.Incidents[id] = Incident{
		ID: id, Namespace: namespace, Kind: kind, Name: name,
		Status:    "pending_diagnosis",
		FirstSeen: firstSeen, LastSeen: lastSeen,
	}
	return id, nil
}

func (s *MemoryStore) RecordDiagnosis(ctx context.Context, incidentID, failureMode string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	inc, ok := s.Incidents[incidentID]
	if !ok {
		return fmt.Errorf("unknown incident id %q", incidentID)
	}
	inc.FailureMode = failureMode
	inc.Status = "diagnosed"
	s.Incidents[incidentID] = inc
	return nil
}
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `cd backend && go test ./tests/store/... -v`
Expected: PASS (Postgres test still skips without `DATABASE_URL` set — that's expected and unchanged from today).

**Do not run a whole-module `go build ./...` and expect it to pass.** `backend/internal/reconcile/reconcile.go` still calls the old `r.store.CreateIncident(...)` (with the `failureMode` parameter this task just removed from the `Store` interface) — it isn't touched until Task 4. This is the same "one file left broken on purpose between tasks" situation Tasks 2, 4, and 5 call out explicitly elsewhere in this plan; Task 1 just didn't say so originally. Confirm the *only* build errors are inside `reconcile.go`:

Run: `cd backend && go build ./... 2>&1 | grep -v internal/reconcile`
Expected: no output (confirms nothing outside `reconcile` broke — do not modify `reconcile.go` in this task, that's Task 4's job).

- [ ] **Step 8: Stage**

```bash
git add backend/migrations/0002_pending_diagnosis.sql backend/internal/store/store.go backend/internal/store/postgres.go backend/internal/store/memory.go backend/tests/store/memory_test.go backend/tests/store/postgres_test.go
```

---

### Task 2: Trim `analyze` to just the `Diagnosis` wire type

Go no longer diagnoses anything itself — that logic moves to `ai/` in Phase C. `backend/internal/analyze` keeps only the shared struct the HTTP callback deserializes into.

**Files:**
- Modify: `backend/internal/analyze/analyze.go`
- Delete: `backend/tests/analyze/analyze_test.go`

**Interfaces:**
- Consumes: nothing (this task only deletes code).
- Produces: `analyze.Diagnosis{FailureMode, RecommendedAction string; Confidence float64}` — unchanged shape, still importable by `reconcile` and (Task 5) `httpserver`.

- [ ] **Step 1: Replace `analyze.go`**

```go
// backend/internal/analyze/analyze.go
//
// Diagnosis is produced by ai/ (see docs/superpowers/specs/2026-08-08-ai-diagnosis-service-design.md)
// and delivered to backend/ over the HTTP callback in httpserver.NewRouter's
// diagnosis route. This package now holds only that wire-format shape — the
// analyzers that used to live here (CrashLoopAnalyzer) were ported to
// ai/ai/analyzers/.
package analyze

type Diagnosis struct {
	FailureMode       string
	RecommendedAction string
	Confidence        float64
}
```

- [ ] **Step 2: Delete the old test file**

```bash
rm backend/tests/analyze/analyze_test.go
rmdir backend/tests/analyze
```

- [ ] **Step 3: Confirm the module still builds**

Run: `cd backend && go build ./...`
Expected: FAILS at this point — `reconcile.go` still references `analyze.CrashLoopAnalyzer`/`analyze.Analyzer`, which no longer exist. That's expected; Task 4 fixes `reconcile.go`. Do not attempt to fix it here — verify the *only* build errors are inside `backend/internal/reconcile/reconcile.go`:

Run: `cd backend && go build ./... 2>&1 | grep -v internal/reconcile`
Expected: no output (confirms nothing outside `reconcile` broke).

- [ ] **Step 4: Stage**

```bash
git add backend/internal/analyze/analyze.go backend/tests/analyze
```

---

### Task 3: `incidentqueue` — NATS JetStream publisher

**Note on external API drift:** this task uses `github.com/nats-io/nats.go`'s `jetstream` subpackage and `github.com/nats-io/nats-server/v2`'s test helpers. If a method signature below doesn't match what `go build` reports against the versions `go get` resolves, run `go doc github.com/nats-io/nats.go/jetstream <Symbol>` to check the installed version's actual signature before guessing — don't silently change behavior to work around a compile error.

**Files:**
- Create: `backend/internal/incidentqueue/incidentqueue.go`
- Create: `backend/tests/incidentqueue/incidentqueue_test.go`
- Modify: `backend/go.mod`, `backend/go.sum`

**Interfaces:**
- Consumes: `correlate.Incident` (already defined, unchanged).
- Produces: `incidentqueue.Client` with `PublishPendingIncident(ctx context.Context, incidentID string, incident correlate.Incident) error` and `Close()`. `incidentqueue.PendingIncident` is the JSON wire struct ai/ will deserialize (Task 11 mirrors these field names exactly on the Python side). `incidentqueue.StreamName = "SAGE_INCIDENTS"`, `incidentqueue.PendingSubject = "sage.incidents.pending"`.

- [ ] **Step 1: Add the NATS dependencies**

```bash
cd backend
go get github.com/nats-io/nats.go@latest
go get github.com/nats-io/nats-server/v2@latest
```

- [ ] **Step 2: Write the failing test**

```go
// backend/tests/incidentqueue/incidentqueue_test.go
package incidentqueue_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"

	"sre-platform/backend/internal/correlate"
	"sre-platform/backend/internal/incidentqueue"
	"sre-platform/backend/internal/signal"
)

func startTestNATS(t *testing.T) string {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	var srv *natsserver.Server
	srv = natstest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

func TestClient_PublishPendingIncident_DeliversToConsumer(t *testing.T) {
	ctx := context.Background()
	url := startTestNATS(t)

	client, err := incidentqueue.NewClient(ctx, url)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	inc := correlate.Incident{
		Namespace: "default", Kind: "Pod", Name: "web-1", GroupKey: "default/Pod/web-1",
		Signals:   []signal.Signal{{Type: "CrashLoopBackOff", Severity: "warning", Raw: "Back-off restarting"}},
		FirstSeen: time.Now(), LastSeen: time.Now(),
	}
	if err := client.PublishPendingIncident(ctx, "incident-1", inc); err != nil {
		t.Fatalf("PublishPendingIncident: %v", err)
	}

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, incidentqueue.StreamName, jetstream.ConsumerConfig{
		Durable: "test-consumer", AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer: %v", err)
	}
	msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	var got incidentqueue.PendingIncident
	count := 0
	for msg := range msgs.Messages() {
		count++
		if err := json.Unmarshal(msg.Data(), &got); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		msg.Ack()
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 message delivered, got %d", count)
	}
	if got.IncidentID != "incident-1" || got.Namespace != "default" || got.GroupKey != "default/Pod/web-1" {
		t.Fatalf("unexpected payload: %+v", got)
	}
	if len(got.Signals) != 1 || got.Signals[0].Type != "CrashLoopBackOff" {
		t.Fatalf("expected 1 CrashLoopBackOff signal in payload, got %+v", got.Signals)
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd backend && go test ./tests/incidentqueue/... -v`
Expected: FAIL — `package incidentqueue` doesn't exist yet.

- [ ] **Step 4: Implement `incidentqueue.go`**

```go
// backend/internal/incidentqueue/incidentqueue.go
package incidentqueue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"sre-platform/backend/internal/correlate"
)

const StreamName = "SAGE_INCIDENTS"
const PendingSubject = "sage.incidents.pending"

// PendingIncident is the JSON payload published to PendingSubject. ai/'s
// consumer (ai/ai/models.py) must keep its field names in sync with the
// `json` tags here — there is no shared schema, this struct and its Python
// mirror are the contract.
type PendingIncident struct {
	IncidentID string          `json:"incident_id"`
	Namespace  string          `json:"namespace"`
	Kind       string          `json:"kind"`
	Name       string          `json:"name"`
	GroupKey   string          `json:"group_key"`
	Signals    []SignalPayload `json:"signals"`
	FirstSeen  time.Time       `json:"first_seen"`
	LastSeen   time.Time       `json:"last_seen"`
}

type SignalPayload struct {
	Type      string            `json:"type"`
	Severity  string            `json:"severity"`
	Labels    map[string]string `json:"labels"`
	Timestamp time.Time         `json:"timestamp"`
	Raw       string            `json:"raw"`
}

type Client struct {
	nc *nats.Conn
	js jetstream.JetStream
}

// NewClient connects to NATS and ensures StreamName exists (idempotent —
// CreateOrUpdateStream is safe to call on every process start).
func NewClient(ctx context.Context, url string) (*Client, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamName,
		Subjects: []string{PendingSubject},
		Storage:  jetstream.FileStorage,
	}); err != nil {
		nc.Close()
		return nil, err
	}
	return &Client{nc: nc, js: js}, nil
}

func (c *Client) PublishPendingIncident(ctx context.Context, incidentID string, incident correlate.Incident) error {
	payload := PendingIncident{
		IncidentID: incidentID,
		Namespace:  incident.Namespace,
		Kind:       incident.Kind,
		Name:       incident.Name,
		GroupKey:   incident.GroupKey,
		FirstSeen:  incident.FirstSeen,
		LastSeen:   incident.LastSeen,
	}
	for _, s := range incident.Signals {
		payload.Signals = append(payload.Signals, SignalPayload{
			Type: s.Type, Severity: s.Severity, Labels: s.Labels, Timestamp: s.Timestamp, Raw: s.Raw,
		})
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = c.js.Publish(ctx, PendingSubject, data)
	return err
}

func (c *Client) Close() {
	c.nc.Close()
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd backend && go test ./tests/incidentqueue/... -v`
Expected: PASS

- [ ] **Step 6: Stage**

```bash
git add backend/internal/incidentqueue backend/tests/incidentqueue backend/go.mod backend/go.sum
```

---

### Task 4: Split `reconcile.Reconciler` into dispatch + `OnDiagnosis`

This is the core of the whole plan. Task 2 already left `backend/internal/reconcile/reconcile.go` non-compiling on purpose (it references the now-deleted `analyze.CrashLoopAnalyzer`/`analyze.Analyzer`) — so this task doesn't follow red/green in the usual order: the whole module can't build, let alone test, until `reconcile.go` itself is rewritten. Rewrite the production file first, then the test file, then verify both together.

This task's own change to `reconcile.New`'s signature (adding a `publisher` parameter) will, in turn, leave `backend/cmd/backend/main.go` broken — that file isn't touched until Task 6, which wires in the real `incidentqueue.Client`. Do not touch `main.go` here; see Step 3 for how to verify only `cmd/backend` is affected.

**Files:**
- Modify: `backend/internal/reconcile/reconcile.go`
- Modify: `backend/tests/reconcile/reconcile_test.go`

**Interfaces:**
- Consumes: `analyze.Diagnosis` (Task 2), `store.Store.CreatePendingIncident`/`RecordDiagnosis` (Task 1), `incidentqueue.Client` via the new local `IncidentPublisher` interface (Task 3, satisfies it structurally — no import needed in `reconcile.go` itself).
- Produces: `reconcile.New(s store.Store, restarter PodRestarter, publisher IncidentPublisher, slack *slackapproval.Client, clientset kubernetes.Interface, mode gate.Mode, correlationWindow, verifyTimeout time.Duration) *Reconciler` (note the new `publisher` parameter, inserted after `restarter`) and `(*Reconciler).OnDiagnosis(ctx context.Context, incidentID string, diag analyze.Diagnosis)` — Task 5's HTTP handler calls this directly.

- [ ] **Step 1: Rewrite `reconcile.go`**

```go
// backend/internal/reconcile/reconcile.go
package reconcile

import (
	"context"
	"log/slog"
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

// PodRestarter is satisfied by both execute.Executor (direct client-go calls)
// and mcpexecute.Client (calls restart_pod over MCP) — the Reconciler
// doesn't care which is used, only that it can restart a pod.
type PodRestarter interface {
	RestartPod(ctx context.Context, namespace, name string) error
}

// IncidentPublisher dispatches a newly-detected, not-yet-diagnosed incident
// to ai/ for diagnosis. Satisfied by *incidentqueue.Client in production.
type IncidentPublisher interface {
	PublishPendingIncident(ctx context.Context, incidentID string, incident correlate.Incident) error
}

// maxAutoRestarts caps how many times SAGE will auto-execute restart_pod for
// the same underlying object (see signal.Signal.GroupKey) before giving up
// and alerting a human instead. The count is in-memory and resets on
// backend restart; that's an accepted tradeoff for a v1 safety cap, not a
// persisted circuit breaker.
const maxAutoRestarts = 5

type Reconciler struct {
	mu              sync.Mutex
	pending         []signal.Signal
	restartAttempts map[string]int
	// awaiting holds the correlate.Incident context for every incident
	// currently dispatched to ai/ and not yet diagnosed — namespace/name/
	// GroupKey/Signals aren't retrievable from store.Store, so this is the
	// only place OnDiagnosis can find them again. In-memory only, same
	// accepted tradeoff as restartAttempts: lost on backend restart, which
	// OnDiagnosis's orphaned-callback path exists specifically to handle
	// safely rather than pretend it can't happen.
	awaiting map[string]correlate.Incident
	// diagnosed marks incident IDs that have already had a diagnosis
	// callback processed, so NATS at-least-once redelivery or an ai/-side
	// retry can't run gate->execute->verify twice. Grows for the process
	// lifetime — acceptable for v1, same tradeoff as the two maps above.
	diagnosed map[string]bool

	store             store.Store
	restarter         PodRestarter
	publisher         IncidentPublisher
	slack             *slackapproval.Client
	clientset         kubernetes.Interface
	mode              gate.Mode
	correlationWindow time.Duration
	verifyTimeout     time.Duration
}

func New(s store.Store, restarter PodRestarter, publisher IncidentPublisher, slack *slackapproval.Client, clientset kubernetes.Interface, mode gate.Mode, correlationWindow, verifyTimeout time.Duration) *Reconciler {
	return &Reconciler{
		store: s, restarter: restarter, publisher: publisher, slack: slack, clientset: clientset,
		mode: mode, correlationWindow: correlationWindow, verifyTimeout: verifyTimeout,
		restartAttempts: make(map[string]int),
		awaiting:        make(map[string]correlate.Incident),
		diagnosed:       make(map[string]bool),
	}
}

// recordRestartAttempt increments and returns the running restart-attempt
// count for groupKey.
func (r *Reconciler) recordRestartAttempt(groupKey string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.restartAttempts[groupKey]++
	return r.restartAttempts[groupKey]
}

// OnSignal is the watcher's callback: append the new signal, re-correlate
// everything pending, and dispatch any newly-recognized incident to ai/ for
// diagnosis.
//
// Once dispatchIncident has accepted an incident, every pending signal for
// that object is dropped (forgetObjectSignals) so it can never be
// re-correlated and re-dispatched by a later, unrelated signal arrival. A
// genuinely new failure for the same object produces a fresh signal, which
// starts a new incident from scratch — the correct behavior.
func (r *Reconciler) OnSignal(ctx context.Context, s signal.Signal) {
	r.mu.Lock()
	r.pending = append(r.pending, s)
	pendingCopy := append([]signal.Signal{}, r.pending...)
	r.mu.Unlock()

	for _, incident := range correlate.Correlate(pendingCopy, r.correlationWindow) {
		r.dispatchIncident(ctx, incident)
		r.forgetObjectSignals(incident.Namespace, incident.Kind, incident.Name)
	}
}

// dispatchIncident persists incident as pending_diagnosis, remembers its
// context in r.awaiting keyed by the new incident ID, and publishes it to
// ai/ over NATS. Diagnosis, gating, execution, and verification all resume
// later from OnDiagnosis, once ai/ calls back — unlike the old synchronous
// analyzer, Go never itself decides an incident is "unrecognized" anymore.
func (r *Reconciler) dispatchIncident(ctx context.Context, incident correlate.Incident) {
	incidentID, err := r.store.CreatePendingIncident(ctx, incident.Namespace, incident.Kind, incident.Name, incident.FirstSeen, incident.LastSeen)
	if err != nil {
		slog.Error("CreatePendingIncident failed", "namespace", incident.Namespace, "kind", incident.Kind, "name", incident.Name, "error", err)
		return
	}
	slog.Info("incident detected, dispatching for diagnosis", "incident_id", incidentID, "namespace", incident.Namespace, "kind", incident.Kind, "name", incident.Name)

	r.mu.Lock()
	r.awaiting[incidentID] = incident
	r.mu.Unlock()

	if err := r.publisher.PublishPendingIncident(ctx, incidentID, incident); err != nil {
		slog.Error("PublishPendingIncident failed", "incident_id", incidentID, "error", err)
	}
}

// OnDiagnosis resumes the pipeline for incidentID once ai/ has diagnosed it:
// restart-limit check, gate, execute, verify — the same logic the old
// synchronous analyzer used to trigger inline, now triggered by an HTTP
// callback instead (see httpserver's diagnosis route).
func (r *Reconciler) OnDiagnosis(ctx context.Context, incidentID string, diag analyze.Diagnosis) {
	r.mu.Lock()
	if r.diagnosed[incidentID] {
		r.mu.Unlock()
		slog.Info("duplicate diagnosis callback ignored", "incident_id", incidentID)
		return
	}
	incident, ok := r.awaiting[incidentID]
	if ok {
		delete(r.awaiting, incidentID)
		r.diagnosed[incidentID] = true
	}
	r.mu.Unlock()

	if !ok {
		r.handleOrphanedDiagnosis(ctx, incidentID, diag)
		return
	}

	if err := r.store.RecordDiagnosis(ctx, incidentID, diag.FailureMode); err != nil {
		slog.Error("RecordDiagnosis failed", "incident_id", incidentID, "error", err)
	}
	slog.Info("diagnosis received", "incident_id", incidentID, "failure_mode", diag.FailureMode, "recommended_action", diag.RecommendedAction, "confidence", diag.Confidence)

	if diag.RecommendedAction == "restart_pod" {
		attempts := r.recordRestartAttempt(incident.GroupKey)
		if attempts > maxAutoRestarts {
			// Checked before CreateRemediationAction on purpose — a
			// suppressed attempt was never decided on or executed, so it
			// shouldn't leave a dangling remediation_actions row behind.
			r.suppressRestart(ctx, incident, incidentID, &diag, attempts)
			return
		}
	}

	decision := gate.Evaluate(diag.RecommendedAction, r.mode)
	actionID, err := r.store.CreateRemediationAction(ctx, incidentID, diag.RecommendedAction, decision.RequiresApproval, decision.Reason)
	if err != nil {
		slog.Error("CreateRemediationAction failed", "incident_id", incidentID, "error", err)
		return
	}
	slog.Info("gate decision", "incident_id", incidentID, "action_id", actionID, "action", diag.RecommendedAction, "requires_approval", decision.RequiresApproval, "reason", decision.Reason)

	if decision.RequiresApproval {
		ts, err := r.slack.PostApproval(ctx, slackapproval.ApprovalRequest{
			IncidentID: incidentID, ActionID: actionID,
			FailureMode: diag.FailureMode, Action: diag.RecommendedAction,
			Namespace: incident.Namespace, Name: incident.Name,
		})
		if err != nil {
			slog.Error("PostApproval failed", "incident_id", incidentID, "action_id", actionID, "error", err)
		} else {
			slog.Info("approval request posted to Slack", "incident_id", incidentID, "action_id", actionID, "slack_ts", ts)
		}
		return // execution resumes from the Slack interaction handler in a later plan
	}

	r.executeAndVerify(ctx, incident, incidentID, actionID, &diag)
}

// handleOrphanedDiagnosis handles a diagnosis callback whose incident ID
// isn't in r.awaiting — almost always because backend restarted while ai/
// was still working. The Signals needed for verify.CheckPodHealthy are
// gone, so this deliberately does not attempt gate/execute — it records the
// diagnosis for the audit trail and asks a human to look, rather than
// silently dropping it or guessing at execution without the context to
// verify it worked. Same fail-closed instinct gate.go documents for its own
// unrecognized-mode branch.
func (r *Reconciler) handleOrphanedDiagnosis(ctx context.Context, incidentID string, diag analyze.Diagnosis) {
	slog.Warn("diagnosis callback for unknown/orphaned incident, escalating to Slack", "incident_id", incidentID, "failure_mode", diag.FailureMode)

	if err := r.store.RecordDiagnosis(ctx, incidentID, diag.FailureMode); err != nil {
		slog.Error("RecordDiagnosis failed", "incident_id", incidentID, "error", err)
	}
	if err := r.store.WriteAudit(ctx, incidentID, "diagnosis_orphaned", map[string]any{
		"reason":              "incident context lost, likely a backend restart while awaiting diagnosis",
		"failure_mode":        diag.FailureMode,
		"recommended_action":  diag.RecommendedAction,
	}); err != nil {
		slog.Error("WriteAudit failed", "incident_id", incidentID, "error", err)
	}

	ts, err := r.slack.PostNotification(ctx, slackapproval.NotificationRequest{
		IncidentID: incidentID, ActionID: "",
		FailureMode: diag.FailureMode,
		Action:      "none — incident context lost after a backend restart, manual review required for incident " + incidentID,
		Outcome:     "orphaned",
	})
	if err != nil {
		slog.Error("PostNotification failed", "incident_id", incidentID, "error", err)
	} else {
		slog.Info("orphaned-diagnosis alert posted to Slack", "incident_id", incidentID, "slack_ts", ts)
	}
}

// suppressRestart records that auto-remediation has given up on incident's
// object after maxAutoRestarts attempts, and alerts a human via Slack — a
// pod that still needs restarting after 5 tries has a permanent problem
// restarting can't fix, and silently continuing would just churn the
// cluster forever.
func (r *Reconciler) suppressRestart(ctx context.Context, incident correlate.Incident, incidentID string, diagnosis *analyze.Diagnosis, attempts int) {
	slog.Warn("restart limit exceeded, suppressing further auto-remediation",
		"incident_id", incidentID, "group_key", incident.GroupKey, "namespace", incident.Namespace, "name", incident.Name,
		"attempts", attempts, "limit", maxAutoRestarts)

	if err := r.store.WriteAudit(ctx, incidentID, "remediation_suppressed", map[string]any{
		"reason": "restart_limit_exceeded", "group_key": incident.GroupKey, "attempts": attempts, "limit": maxAutoRestarts,
	}); err != nil {
		slog.Error("WriteAudit failed", "incident_id", incidentID, "error", err)
	}

	ts, err := r.slack.PostNotification(ctx, slackapproval.NotificationRequest{
		IncidentID: incidentID, ActionID: "",
		FailureMode: diagnosis.FailureMode, Action: "none — restart limit reached, manual intervention required",
		Namespace: incident.Namespace, Name: incident.Name, Outcome: "suppressed",
	})
	if err != nil {
		slog.Error("PostNotification failed", "incident_id", incidentID, "error", err)
	} else {
		slog.Info("restart-limit alert posted to Slack", "incident_id", incidentID, "slack_ts", ts)
	}
}

// forgetObjectSignals removes every currently-pending signal belonging to the
// given object from r.pending, so it can't be re-correlated later.
func (r *Reconciler) forgetObjectSignals(namespace, kind, name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	remaining := make([]signal.Signal, 0, len(r.pending))
	for _, s := range r.pending {
		if s.Namespace == namespace && s.Kind == kind && s.Name == name {
			continue
		}
		remaining = append(remaining, s)
	}
	r.pending = remaining
}

func (r *Reconciler) executeAndVerify(ctx context.Context, incident correlate.Incident, incidentID, actionID string, diagnosis *analyze.Diagnosis) {
	if err := r.restarter.RestartPod(ctx, incident.Namespace, incident.Name); err != nil {
		slog.Error("RestartPod failed", "incident_id", incidentID, "action_id", actionID, "namespace", incident.Namespace, "name", incident.Name, "error", err)
		return
	}
	slog.Info("remediation executed", "incident_id", incidentID, "action_id", actionID, "action", diagnosis.RecommendedAction, "namespace", incident.Namespace, "name", incident.Name)
	if err := r.store.MarkExecuted(ctx, actionID); err != nil {
		slog.Error("MarkExecuted failed", "action_id", actionID, "error", err)
	}

	healthy, err := verify.CheckPodHealthy(ctx, r.clientset, incident.Namespace, incident.Signals[0].Labels, r.verifyTimeout, time.Second)
	if err != nil {
		slog.Error("CheckPodHealthy failed", "incident_id", incidentID, "action_id", actionID, "error", err)
		return
	}
	outcome := "resolved"
	if !healthy {
		outcome = "unresolved"
	}
	slog.Info("remediation verified", "incident_id", incidentID, "action_id", actionID, "outcome", outcome)
	if err := r.store.MarkVerified(ctx, actionID, outcome); err != nil {
		slog.Error("MarkVerified failed", "action_id", actionID, "error", err)
	}
	if err := r.store.WriteAudit(ctx, incidentID, "remediation_verified", map[string]any{"outcome": outcome}); err != nil {
		slog.Error("WriteAudit failed", "incident_id", incidentID, "error", err)
	}

	ts, err := r.slack.PostNotification(ctx, slackapproval.NotificationRequest{
		IncidentID: incidentID, ActionID: actionID,
		FailureMode: diagnosis.FailureMode, Action: diagnosis.RecommendedAction,
		Namespace: incident.Namespace, Name: incident.Name, Outcome: outcome,
	})
	if err != nil {
		slog.Error("PostNotification failed", "incident_id", incidentID, "action_id", actionID, "error", err)
	} else {
		slog.Info("auto-remediation notification posted to Slack", "incident_id", incidentID, "action_id", actionID, "slack_ts", ts)
	}
}
```

- [ ] **Step 2: Rewrite `reconcile_test.go`**

```go
// backend/tests/reconcile/reconcile_test.go
package reconcile_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sre-platform/backend/internal/analyze"
	"sre-platform/backend/internal/correlate"
	"sre-platform/backend/internal/execute"
	"sre-platform/backend/internal/gate"
	"sre-platform/backend/internal/k8swatch"
	"sre-platform/backend/internal/mcpauth"
	"sre-platform/backend/internal/mcpexecute"
	"sre-platform/backend/internal/reconcile"
	"sre-platform/backend/internal/signal"
	"sre-platform/backend/internal/slackapproval"
	"sre-platform/backend/internal/store"
)

// countingRestarter is a test-local reconcile.PodRestarter that records how
// many times RestartPod was invoked per (namespace, name).
type countingRestarter struct {
	mu     sync.Mutex
	counts map[string]int
}

func newCountingRestarter() *countingRestarter {
	return &countingRestarter{counts: make(map[string]int)}
}

func (c *countingRestarter) RestartPod(_ context.Context, namespace, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[namespace+"/"+name]++
	return nil
}

func (c *countingRestarter) count(namespace, name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[namespace+"/"+name]
}

// fakePublisher is a test-local reconcile.IncidentPublisher. Real NATS
// delivery is proven by backend/tests/incidentqueue; these tests only need
// to know which incident ID dispatchIncident assigned, so they can drive
// OnDiagnosis directly instead of standing up a real broker.
type fakePublisher struct {
	mu     sync.Mutex
	calls  int
	lastID string
}

func (f *fakePublisher) PublishPendingIncident(_ context.Context, incidentID string, _ correlate.Incident) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastID = incidentID
	return nil
}

func (f *fakePublisher) dispatchedID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastID
}

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
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

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

	r := reconcile.New(memStore, mcpClient, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)
	watcher := k8swatch.NewWatcher(clientset, func(s signal.Signal) { r.OnSignal(ctx, s) })

	watcher.HandleAddEvent(&corev1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "web-1"},
		Reason:         "BackOff",
		Message:        "Back-off restarting failed container",
		LastTimestamp:  metav1.NewTime(time.Now()),
	})

	if publisher.calls != 1 {
		t.Fatalf("expected exactly 1 incident dispatched to ai/, got %d", publisher.calls)
	}
	r.OnDiagnosis(ctx, publisher.dispatchedID(), analyze.Diagnosis{
		FailureMode: "CrashLoopBackOff", RecommendedAction: "restart_pod", Confidence: 0.9,
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

func TestReconciler_OnDiagnosis_SuppressesRestartAfterLimit(t *testing.T) {
	ctx := context.Background()
	healthyReplacement := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-healthy", Namespace: "team-a", Labels: map[string]string{"app": "worker"}},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	clientset := fake.NewSimpleClientset(healthyReplacement)
	memStore := store.NewMemoryStore()
	restarter := newCountingRestarter()
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	r := reconcile.New(memStore, restarter, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	const groupKey = "team-a/ReplicaSet/worker-rs"
	for i := 1; i <= 6; i++ {
		r.OnSignal(ctx, signal.Signal{
			Source: signal.SourceK8sEvent, Type: "CrashLoopBackOff",
			Namespace: "team-a", Kind: "Pod", Name: fmt.Sprintf("worker-rs-%d", i),
			Labels: map[string]string{"app": "worker"}, Timestamp: time.Now(),
			GroupKey: groupKey,
		})
		r.OnDiagnosis(ctx, publisher.dispatchedID(), analyze.Diagnosis{
			FailureMode: "CrashLoopBackOff", RecommendedAction: "restart_pod", Confidence: 0.9,
		})
	}

	totalRestarts := 0
	for i := 1; i <= 6; i++ {
		totalRestarts += restarter.count("team-a", fmt.Sprintf("worker-rs-%d", i))
	}
	if totalRestarts != 5 {
		t.Fatalf("expected exactly 5 restarts across all 6 recreations (6th suppressed), got %d", totalRestarts)
	}

	var suppressed int
	for _, entry := range memStore.AuditEntries {
		if entry.EventType == "remediation_suppressed" {
			suppressed++
			if entry.Detail["group_key"] != groupKey {
				t.Errorf("expected suppressed audit entry to reference group_key %q, got %v", groupKey, entry.Detail["group_key"])
			}
		}
	}
	if suppressed != 1 {
		t.Fatalf("expected exactly 1 remediation_suppressed audit entry, got %d", suppressed)
	}
	if len(memStore.Incidents) != 6 {
		t.Fatalf("expected all 6 incidents recorded (suppression still logs the incident), got %d", len(memStore.Incidents))
	}
}

func TestReconciler_OnDiagnosis_ManualModeRequestsApprovalAndDoesNotExecute(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default", Labels: map[string]string{"app": "web"}},
	}
	clientset := fake.NewSimpleClientset(pod)
	memStore := store.NewMemoryStore()
	executor := execute.NewExecutor(clientset)
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	r := reconcile.New(memStore, executor, publisher, slackClient, clientset, gate.ModeManual, 60*time.Second, 2*time.Second)

	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "CrashLoopBackOff",
		Namespace: "default", Kind: "Pod", Name: "web-1",
		Labels: map[string]string{"app": "web"}, Timestamp: time.Now(),
	})
	r.OnDiagnosis(ctx, publisher.dispatchedID(), analyze.Diagnosis{
		FailureMode: "CrashLoopBackOff", RecommendedAction: "restart_pod", Confidence: 0.9,
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

func TestReconciler_OnSignal_DoesNotReRemediateAlreadyHandledObject(t *testing.T) {
	ctx := context.Background()

	podXReplacement := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "api-2", Namespace: "team-a", Labels: map[string]string{"app": "api"}},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	podYReplacement := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-2", Namespace: "team-b", Labels: map[string]string{"app": "worker"}},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	clientset := fake.NewSimpleClientset(podXReplacement, podYReplacement)
	memStore := store.NewMemoryStore()
	restarter := newCountingRestarter()
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	r := reconcile.New(memStore, restarter, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "CrashLoopBackOff",
		Namespace: "team-a", Kind: "Pod", Name: "api-1",
		Labels: map[string]string{"app": "api"}, Timestamp: time.Now(),
	})
	r.OnDiagnosis(ctx, publisher.dispatchedID(), analyze.Diagnosis{
		FailureMode: "CrashLoopBackOff", RecommendedAction: "restart_pod", Confidence: 0.9,
	})

	if got := restarter.count("team-a", "api-1"); got != 1 {
		t.Fatalf("after first signal, expected pod X restarted exactly once, got %d", got)
	}
	incidentsAfterX := len(memStore.Incidents)
	actionsAfterX := len(memStore.Actions)
	if incidentsAfterX != 1 || actionsAfterX != 1 {
		t.Fatalf("expected exactly 1 incident and 1 action after handling pod X, got %d incidents / %d actions", incidentsAfterX, actionsAfterX)
	}

	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "CrashLoopBackOff",
		Namespace: "team-b", Kind: "Pod", Name: "worker-1",
		Labels: map[string]string{"app": "worker"}, Timestamp: time.Now(),
	})
	r.OnDiagnosis(ctx, publisher.dispatchedID(), analyze.Diagnosis{
		FailureMode: "CrashLoopBackOff", RecommendedAction: "restart_pod", Confidence: 0.9,
	})

	if got := restarter.count("team-a", "api-1"); got != 1 {
		t.Fatalf("pod X must NOT be re-remediated by an unrelated signal: want 1 restart, got %d", got)
	}
	if got := restarter.count("team-b", "worker-1"); got != 1 {
		t.Fatalf("expected pod Y restarted exactly once, got %d", got)
	}
	if got := len(memStore.Incidents) - incidentsAfterX; got != 1 {
		t.Fatalf("expected exactly 1 new incident (pod Y) from the second signal, got %d", got)
	}
	if got := len(memStore.Actions) - actionsAfterX; got != 1 {
		t.Fatalf("expected exactly 1 new action (pod Y) from the second signal, got %d", got)
	}
	if len(memStore.Incidents) != 2 {
		t.Fatalf("expected 2 incidents total (one each for X and Y), got %d", len(memStore.Incidents))
	}
}

// TestReconciler_OnDiagnosis_OrphanedIncidentEscalatesToSlack proves the
// backend-restart-mid-flight case: a diagnosis callback arrives for an
// incident ID the (fresh) Reconciler has never dispatched. It must not
// panic or silently drop the diagnosis — it records it for audit and lets
// the orphan path attempt a Slack alert (fire-and-forget, unasserted here
// same as every other Slack call in this file).
func TestReconciler_OnDiagnosis_OrphanedIncidentEscalatesToSlack(t *testing.T) {
	ctx := context.Background()
	clientset := fake.NewSimpleClientset()
	memStore := store.NewMemoryStore()
	restarter := newCountingRestarter()
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	r := reconcile.New(memStore, restarter, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	// No prior OnSignal/dispatch — "orphan-1" was never assigned by this process.
	r.OnDiagnosis(ctx, "orphan-1", analyze.Diagnosis{
		FailureMode: "CrashLoopBackOff", RecommendedAction: "restart_pod", Confidence: 0.9,
	})

	if restarter.count("", "") != 0 && len(restarter.counts) != 0 {
		t.Fatalf("orphaned diagnosis must never execute a restart, got counts: %+v", restarter.counts)
	}
	var orphaned int
	for _, entry := range memStore.AuditEntries {
		if entry.EventType == "diagnosis_orphaned" {
			orphaned++
		}
	}
	if orphaned != 1 {
		t.Fatalf("expected exactly 1 diagnosis_orphaned audit entry, got %d (entries: %+v)", orphaned, memStore.AuditEntries)
	}
}

// TestReconciler_OnDiagnosis_DuplicateCallbackIsNoOp proves NATS
// at-least-once redelivery (or an ai/-side retry after a slow HTTP response)
// can't double-execute a remediation.
func TestReconciler_OnDiagnosis_DuplicateCallbackIsNoOp(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default", Labels: map[string]string{"app": "web"}},
	}
	clientset := fake.NewSimpleClientset(pod)
	memStore := store.NewMemoryStore()
	restarter := newCountingRestarter()
	publisher := &fakePublisher{}
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	slackClient.APIBaseURL = "http://127.0.0.1:0"

	r := reconcile.New(memStore, restarter, publisher, slackClient, clientset, gate.ModeAuto, 60*time.Second, 2*time.Second)

	r.OnSignal(ctx, signal.Signal{
		Source: signal.SourceK8sEvent, Type: "CrashLoopBackOff",
		Namespace: "default", Kind: "Pod", Name: "web-1",
		Labels: map[string]string{"app": "web"}, Timestamp: time.Now(),
	})
	id := publisher.dispatchedID()
	diag := analyze.Diagnosis{FailureMode: "CrashLoopBackOff", RecommendedAction: "restart_pod", Confidence: 0.9}

	r.OnDiagnosis(ctx, id, diag)
	r.OnDiagnosis(ctx, id, diag) // redelivery of the same diagnosis

	if got := restarter.count("default", "web-1"); got != 1 {
		t.Fatalf("duplicate diagnosis callback must not re-execute: want 1 restart, got %d", got)
	}
	if len(memStore.Actions) != 1 {
		t.Fatalf("duplicate diagnosis callback must not create a second remediation action, got %d", len(memStore.Actions))
	}
}
```

- [ ] **Step 3: Run build and tests**

Run: `cd backend && go test ./tests/reconcile/... -v`
Expected: PASS on all 6 tests.

**This does NOT make the whole module build cleanly yet — and that's expected.** `reconcile.New`'s signature gains a new `publisher IncidentPublisher` parameter in this task, but `backend/cmd/backend/main.go` (not touched until Task 6) still calls the OLD signature — one argument short. This is the exact same "one file left broken on purpose between tasks" situation this plan already documents for Task 5→6 (`httpserver.NewRouter`'s signature). Confirm the *only* build errors anywhere in the module are inside `cmd/backend`:

Run: `cd backend && go build ./... 2>&1 | grep -v cmd/backend`
Expected: no output (confirms every package outside `cmd/backend` — including `reconcile` itself — builds clean; do not modify `main.go` in this task, that's Task 6's job).

- [ ] **Step 4: Stage**

```bash
git add backend/internal/reconcile/reconcile.go backend/tests/reconcile/reconcile_test.go
```

---

### Task 5: `httpserver` — diagnosis callback route

**Files:**
- Create: `backend/internal/httpserver/diagnosis.go`
- Modify: `backend/internal/httpserver/router.go`
- Modify: `backend/tests/httpserver/router_test.go`

**Interfaces:**
- Consumes: `analyze.Diagnosis` (Task 2), `mcpauth.RequireBearerToken` (existing, unchanged).
- Produces: `httpserver.NewRouter(slackClient *slackapproval.Client, s store.Store, diagnosisReceiver DiagnosisReceiver, diagnosisCallbackToken string) http.Handler` — note the two new trailing parameters. `httpserver.DiagnosisReceiver` is a small local interface (`OnDiagnosis(ctx context.Context, incidentID string, diag analyze.Diagnosis)`), matching this codebase's existing pattern of defining interfaces at the point of use rather than depending on `*reconcile.Reconciler` directly — `*reconcile.Reconciler` satisfies it structurally, no changes needed in `reconcile.go` for this.
- Route: `POST /internal/incidents/{id}/diagnosis`, bearer-token gated, body `{"failure_mode": string, "recommended_action": "restart_pod"|"none", "confidence": number}`. Rejects any other `recommended_action` with 400 — this is the concrete enforcement point for the spec's "action vocabulary stays narrow" rule (§5), not just a convention ai/ is expected to follow.

- [ ] **Step 1: Write the failing test**

Replace `backend/tests/httpserver/router_test.go`:

```go
// backend/tests/httpserver/router_test.go
package httpserver_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"sre-platform/backend/internal/analyze"
	"sre-platform/backend/internal/httpserver"
	"sre-platform/backend/internal/slackapproval"
	"sre-platform/backend/internal/store"
)

type fakeDiagnosisReceiver struct {
	mu         sync.Mutex
	calls      int
	incidentID string
	diag       analyze.Diagnosis
}

func (f *fakeDiagnosisReceiver) OnDiagnosis(_ context.Context, incidentID string, diag analyze.Diagnosis) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.incidentID = incidentID
	f.diag = diag
}

const testDiagnosisToken = "diagnosis-test-token"

func TestNewRouter_HealthzReturnsOK(t *testing.T) {
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	router := httpserver.NewRouter(slackClient, store.NewMemoryStore(), &fakeDiagnosisReceiver{}, testDiagnosisToken)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestNewRouter_RoutesSlackInteractionsUnderPrefix(t *testing.T) {
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	router := httpserver.NewRouter(slackClient, store.NewMemoryStore(), &fakeDiagnosisReceiver{}, testDiagnosisToken)

	req := httptest.NewRequest(http.MethodPost, "/slack/interactions", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code == http.StatusNotFound {
		t.Fatal("expected /slack/interactions to be routed, got 404 — check the /slack prefix group")
	}
}

func TestNewRouter_DiagnosisCallback_ValidRequestInvokesReceiver(t *testing.T) {
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	receiver := &fakeDiagnosisReceiver{}
	router := httpserver.NewRouter(slackClient, store.NewMemoryStore(), receiver, testDiagnosisToken)

	body := `{"failure_mode":"CrashLoopBackOff","recommended_action":"restart_pod","confidence":0.9}`
	req := httptest.NewRequest(http.MethodPost, "/internal/incidents/incident-1/diagnosis", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testDiagnosisToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if receiver.calls != 1 {
		t.Fatalf("expected OnDiagnosis called once, got %d", receiver.calls)
	}
	if receiver.incidentID != "incident-1" {
		t.Errorf("expected incident id %q, got %q", "incident-1", receiver.incidentID)
	}
	if receiver.diag.RecommendedAction != "restart_pod" {
		t.Errorf("expected recommended_action restart_pod, got %q", receiver.diag.RecommendedAction)
	}
}

func TestNewRouter_DiagnosisCallback_MissingTokenRejected(t *testing.T) {
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	receiver := &fakeDiagnosisReceiver{}
	router := httpserver.NewRouter(slackClient, store.NewMemoryStore(), receiver, testDiagnosisToken)

	body := `{"failure_mode":"CrashLoopBackOff","recommended_action":"restart_pod","confidence":0.9}`
	req := httptest.NewRequest(http.MethodPost, "/internal/incidents/incident-1/diagnosis", strings.NewReader(body))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with no bearer token, got %d", rec.Code)
	}
	if receiver.calls != 0 {
		t.Fatalf("expected OnDiagnosis not called when auth fails, got %d calls", receiver.calls)
	}
}

func TestNewRouter_DiagnosisCallback_RejectsOutOfVocabularyAction(t *testing.T) {
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	receiver := &fakeDiagnosisReceiver{}
	router := httpserver.NewRouter(slackClient, store.NewMemoryStore(), receiver, testDiagnosisToken)

	body := `{"failure_mode":"OOMKilled","recommended_action":"scale_up","confidence":0.7}`
	req := httptest.NewRequest(http.MethodPost, "/internal/incidents/incident-1/diagnosis", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testDiagnosisToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for an out-of-vocabulary recommended_action, got %d", rec.Code)
	}
	if receiver.calls != 0 {
		t.Fatalf("expected OnDiagnosis not called when the action is rejected, got %d calls", receiver.calls)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && go test ./tests/httpserver/... -v`
Expected: FAIL — `httpserver.NewRouter` doesn't accept the new parameters yet, `httpserver.DiagnosisReceiver` doesn't exist.

- [ ] **Step 3: Write `diagnosis.go`**

```go
// backend/internal/httpserver/diagnosis.go
package httpserver

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"sre-platform/backend/internal/analyze"
)

// DiagnosisReceiver is satisfied by *reconcile.Reconciler — defined here,
// at the point of use, rather than importing the concrete type, matching
// this codebase's existing pattern (see reconcile.PodRestarter).
type DiagnosisReceiver interface {
	OnDiagnosis(ctx context.Context, incidentID string, diag analyze.Diagnosis)
}

type diagnosisRequest struct {
	FailureMode       string  `json:"failure_mode"`
	RecommendedAction string  `json:"recommended_action"`
	Confidence        float64 `json:"confidence"`
}

// diagnosisHandler is deliberately the one place that enforces ai/'s output
// vocabulary — restart_pod or none, nothing else — rather than trusting
// ai/'s prompt/rules alone (see docs/superpowers/specs/2026-08-08-ai-diagnosis-service-design.md §5).
func diagnosisHandler(receiver DiagnosisReceiver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		incidentID := chi.URLParam(r, "id")
		if incidentID == "" {
			http.Error(w, "missing incident id", http.StatusBadRequest)
			return
		}

		var req diagnosisRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			slog.Warn("diagnosis callback: bad request body", "incident_id", incidentID, "error", err)
			http.Error(w, "bad request body", http.StatusBadRequest)
			return
		}
		if req.RecommendedAction != "restart_pod" && req.RecommendedAction != "none" {
			slog.Warn("diagnosis callback: rejected out-of-vocabulary recommended_action", "incident_id", incidentID, "recommended_action", req.RecommendedAction)
			http.Error(w, `recommended_action must be "restart_pod" or "none"`, http.StatusBadRequest)
			return
		}

		slog.Info("diagnosis callback received", "incident_id", incidentID, "failure_mode", req.FailureMode, "recommended_action", req.RecommendedAction, "confidence", req.Confidence)
		receiver.OnDiagnosis(r.Context(), incidentID, analyze.Diagnosis{
			FailureMode:       req.FailureMode,
			RecommendedAction: req.RecommendedAction,
			Confidence:        req.Confidence,
		})
		w.WriteHeader(http.StatusOK)
	}
}
```

- [ ] **Step 4: Update `router.go`**

```go
// backend/internal/httpserver/router.go
package httpserver

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"sre-platform/backend/internal/mcpauth"
	"sre-platform/backend/internal/slackapproval"
	"sre-platform/backend/internal/store"
)

// NewRouter builds backend/'s entire HTTP surface, grouped by prefix, so
// main.go never defines a route directly. New route groups attach here.
func NewRouter(slackClient *slackapproval.Client, s store.Store, diagnosisReceiver DiagnosisReceiver, diagnosisCallbackToken string) http.Handler {
	r := chi.NewRouter()

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	r.Route("/slack", func(r chi.Router) {
		r.Post("/interactions", slackClient.InteractionHandler(s))
	})

	// /internal is called by ai/ only, never by an external client — bearer
	// token checked the same way mcp-execute-server checks its own, an
	// independent layer rather than trusting network placement alone.
	r.Route("/internal", func(r chi.Router) {
		r.Use(func(next http.Handler) http.Handler {
			return mcpauth.RequireBearerToken(diagnosisCallbackToken, next)
		})
		r.Post("/incidents/{id}/diagnosis", diagnosisHandler(diagnosisReceiver))
	})

	return r
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd backend && go build ./... && go test ./tests/httpserver/... -v`
Expected: PASS (module still won't fully build yet — `cmd/backend/main.go` calls the old `NewRouter` signature; that's fixed in Task 6, same "one file left broken on purpose between tasks" situation as Task 2→4).

Run: `cd backend && go build ./... 2>&1 | grep -v cmd/backend`
Expected: no output (confirms nothing outside `cmd/backend` broke).

- [ ] **Step 6: Stage**

```bash
git add backend/internal/httpserver/diagnosis.go backend/internal/httpserver/router.go backend/tests/httpserver/router_test.go
```

---

### Task 6: `settings.go` + `cmd/backend/main.go` wiring

**Files:**
- Modify: `backend/internal/settings/settings.go`
- Modify: `backend/tests/settings/settings_test.go`
- Modify: `backend/cmd/backend/main.go`

**Interfaces:**
- Produces: `settings.Settings` gains `NATSURL string` (env `NATS_URL`) and `DiagnosisCallbackToken string` (env `DIAGNOSIS_CALLBACK_TOKEN`), both required by `Validate()`.

- [ ] **Step 1: Update the failing test**

In `backend/tests/settings/settings_test.go`, add the two new fields to `validSettings()`:

```go
func validSettings() settings.Settings {
	return settings.Settings{
		DatabaseURL:            "postgres://sre:sre@localhost:5432/sre_platform?sslmode=disable",
		Mode:                   gate.ModeManual,
		VerifyTimeout:          60 * time.Second,
		CorrelationWindow:      60 * time.Second,
		SlackBotToken:          "xoxb-real-token",
		SlackSigningSecret:     "real-signing-secret",
		SlackApprovalChannel:   "#sre-approvals",
		HTTPAddr:               ":8080",
		MCPExecuteAddr:         ":8090",
		MCPExecuteToken:        "real-shared-secret",
		MCPExecuteURL:          "http://localhost:8090",
		NATSURL:                "nats://localhost:4222",
		DiagnosisCallbackToken: "real-diagnosis-token",
	}
}
```

Add a new test:

```go
func TestSettings_Validate_RequiresNATSAndDiagnosisCallbackToken(t *testing.T) {
	s := validSettings()
	s.NATSURL = ""
	s.DiagnosisCallbackToken = ""

	err := s.Validate()
	if err == nil {
		t.Fatal("expected an error")
	}
	if !contains(err.Error(), "NATS_URL") || !contains(err.Error(), "DIAGNOSIS_CALLBACK_TOKEN") {
		t.Errorf("expected error to name both missing fields, got: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && go test ./tests/settings/... -v`
Expected: FAIL — `settings.Settings` has no field `NATSURL`/`DiagnosisCallbackToken`.

- [ ] **Step 3: Update `settings.go`**

```go
type Settings struct {
	DatabaseURL            string
	Kubeconfig             string
	Mode                   gate.Mode
	VerifyTimeout          time.Duration
	CorrelationWindow      time.Duration
	SlackBotToken          string
	SlackSigningSecret     string
	SlackApprovalChannel   string
	HTTPAddr               string
	MCPExecuteAddr         string
	MCPExecuteToken        string
	MCPExecuteURL          string
	NATSURL                string
	DiagnosisCallbackToken string
}

func Load() Settings {
	s := Settings{
		DatabaseURL:            getenv("DATABASE_URL", ""),
		Kubeconfig:             getenv("KUBECONFIG", ""),
		Mode:                   gate.Mode(getenv("REMEDIATION_MODE", "manual")),
		VerifyTimeout:          seconds(getenv("VERIFY_TIMEOUT_SECONDS", "60")),
		CorrelationWindow:      seconds(getenv("CORRELATION_WINDOW_SECONDS", "60")),
		SlackBotToken:          getenv("SLACK_BOT_TOKEN", ""),
		SlackSigningSecret:     getenv("SLACK_SIGNING_SECRET", ""),
		SlackApprovalChannel:   getenv("SLACK_APPROVAL_CHANNEL", "#sre-approvals"),
		HTTPAddr:               getenv("BACKEND_HTTP_ADDR", ":8080"),
		MCPExecuteAddr:         getenv("MCP_EXECUTE_ADDR", ":8090"),
		MCPExecuteToken:        getenv("MCP_EXECUTE_TOKEN", ""),
		MCPExecuteURL:          getenv("MCP_EXECUTE_URL", "http://localhost:8090"),
		NATSURL:                getenv("NATS_URL", ""),
		DiagnosisCallbackToken: getenv("DIAGNOSIS_CALLBACK_TOKEN", ""),
	}
	if err := s.Validate(); err != nil {
		slog.Error("invalid settings", "error", err)
		os.Exit(1)
	}
	return s
}

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
	if s.NATSURL == "" {
		missing = append(missing, "NATS_URL")
	}
	if s.DiagnosisCallbackToken == "" {
		missing = append(missing, "DIAGNOSIS_CALLBACK_TOKEN")
	}
	if s.Mode != gate.ModeAuto && s.Mode != gate.ModeManual {
		missing = append(missing, "REMEDIATION_MODE (invalid value)")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}
	return nil
}
```

(`getenv`/`seconds` helpers at the bottom of the file are unchanged.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && go test ./tests/settings/... -v`
Expected: PASS

- [ ] **Step 5: Wire `cmd/backend/main.go`**

```go
// backend/cmd/backend/main.go
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"sre-platform/backend/internal/httpserver"
	"sre-platform/backend/internal/incidentqueue"
	"sre-platform/backend/internal/k8swatch"
	"sre-platform/backend/internal/mcpexecute"
	"sre-platform/backend/internal/reconcile"
	"sre-platform/backend/internal/settings"
	"sre-platform/backend/internal/signal"
	"sre-platform/backend/internal/slackapproval"
	"sre-platform/backend/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	cfg := settings.Load()
	ctx := context.Background()

	clientset := buildClientset(cfg.Kubeconfig)
	pgStore, err := store.NewPostgresStore(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("connecting to Postgres", "error", err)
		os.Exit(1)
	}
	slackClient := slackapproval.NewClient(cfg.SlackBotToken, cfg.SlackApprovalChannel, cfg.SlackSigningSecret, http.DefaultClient)
	restarter, err := mcpexecute.NewClient(ctx, cfg.MCPExecuteURL, cfg.MCPExecuteToken)
	if err != nil {
		slog.Error("connecting to mcp-execute-server", "error", err)
		os.Exit(1)
	}
	publisher, err := incidentqueue.NewClient(ctx, cfg.NATSURL)
	if err != nil {
		slog.Error("connecting to NATS", "error", err)
		os.Exit(1)
	}

	reconciler := reconcile.New(pgStore, restarter, publisher, slackClient, clientset, cfg.Mode, cfg.CorrelationWindow, cfg.VerifyTimeout)
	watcher := k8swatch.NewWatcher(clientset, func(s signal.Signal) { reconciler.OnSignal(ctx, s) })

	router := httpserver.NewRouter(slackClient, pgStore, reconciler, cfg.DiagnosisCallbackToken)
	go func() {
		slog.Info("listening", "addr", cfg.HTTPAddr)
		if err := http.ListenAndServe(cfg.HTTPAddr, router); err != nil {
			slog.Error("http server exited", "error", err)
			os.Exit(1)
		}
	}()

	if err := watcher.Run(ctx); err != nil {
		slog.Error("watcher.Run", "error", err)
		os.Exit(1)
	}
}

func buildClientset(kubeconfig string) kubernetes.Interface {
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		slog.Error("building kubeconfig", "error", err)
		os.Exit(1)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		slog.Error("building clientset", "error", err)
		os.Exit(1)
	}
	return clientset
}
```

- [ ] **Step 6: Full module build**

Run: `cd backend && go build ./... && go vet ./... && go test ./...`
Expected: PASS, no build errors anywhere — this is the point where Phase A's split-build state (Tasks 2, 4, 5 each left one file broken on purpose) fully resolves.

- [ ] **Step 7: Stage**

```bash
git add backend/internal/settings/settings.go backend/tests/settings/settings_test.go backend/cmd/backend/main.go
```

---

### Task 7: `k8swatch` — detect more than one failure mode

Today `knownReasons` maps exactly one K8s event reason. Without more failure-mode diversity reaching ai/, every incident matches the same rule and the LLM fallback path (Phase C) is never exercised by real cluster traffic.

**Files:**
- Modify: `backend/internal/k8swatch/watcher.go`
- Modify: `backend/tests/k8swatch/watcher_test.go`

**Interfaces:**
- No signature changes — `signal.Signal.Type` just carries more possible values now (`"SchedulingFailed"`, `"ProbeFailure"`, `"ImagePullError"`, alongside the existing `"CrashLoopBackOff"`).

- [ ] **Step 1: Write the failing tests**

Add to `backend/tests/k8swatch/watcher_test.go`:

```go
func TestWatcher_HandleAddEvent_EmitsSchedulingFailedSignal(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	var got []signal.Signal
	w := k8swatch.NewWatcher(clientset, func(s signal.Signal) { got = append(got, s) })

	w.HandleAddEvent(&corev1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "web-1"},
		Reason:         "FailedScheduling",
		Message:        "0/3 nodes are available: insufficient cpu",
		LastTimestamp:  metav1.NewTime(time.Now()),
	})

	if len(got) != 1 || got[0].Type != "SchedulingFailed" {
		t.Fatalf("expected 1 SchedulingFailed signal, got %+v", got)
	}
}

func TestWatcher_HandleAddEvent_EmitsProbeFailureSignal(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	var got []signal.Signal
	w := k8swatch.NewWatcher(clientset, func(s signal.Signal) { got = append(got, s) })

	w.HandleAddEvent(&corev1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "web-1"},
		Reason:         "Unhealthy",
		Message:        "Readiness probe failed: HTTP probe failed with statuscode: 503",
		LastTimestamp:  metav1.NewTime(time.Now()),
	})

	if len(got) != 1 || got[0].Type != "ProbeFailure" {
		t.Fatalf("expected 1 ProbeFailure signal, got %+v", got)
	}
}

func TestWatcher_HandleAddEvent_EmitsImagePullErrorSignal(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	var got []signal.Signal
	w := k8swatch.NewWatcher(clientset, func(s signal.Signal) { got = append(got, s) })

	w.HandleAddEvent(&corev1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "web-1"},
		Reason:         "Failed",
		Message:        "Failed to pull image \"bad/image:latest\": rpc error: code = NotFound",
		LastTimestamp:  metav1.NewTime(time.Now()),
	})

	if len(got) != 1 || got[0].Type != "ImagePullError" {
		t.Fatalf("expected 1 ImagePullError signal, got %+v", got)
	}
}

func TestWatcher_HandleAddEvent_IgnoresUnrelatedFailedReason(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	var got []signal.Signal
	w := k8swatch.NewWatcher(clientset, func(s signal.Signal) { got = append(got, s) })

	// Reason "Failed" with a message that has nothing to do with image
	// pulls must NOT be swept into ImagePullError by the substring check.
	w.HandleAddEvent(&corev1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "web-1"},
		Reason:         "Failed",
		Message:        "Error: secret \"app-config\" not found",
		LastTimestamp:  metav1.NewTime(time.Now()),
	})

	if len(got) != 0 {
		t.Errorf("expected no signal for an unrelated Failed reason, got %+v", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd backend && go test ./tests/k8swatch/... -v`
Expected: FAIL on the 3 new positive-case tests (0 signals emitted instead of 1); the 4th passes already.

- [ ] **Step 3: Update `watcher.go`**

```go
// knownReasons maps a K8s Event's Reason field to our internal failure-mode
// taxonomy. Kubernetes itself emits "BackOff" (not "CrashLoopBackOff") for a
// crash-looping container; the pod status condition uses the longer name.
//
// "Failed" is deliberately not in this map: kubelet reuses that one Reason
// for many unrelated failures (image pull errors, missing secrets/configmaps,
// volume mount failures, ...), so it's handled separately below by
// inspecting the event Message instead of trusting Reason alone.
var knownReasons = map[string]string{
	"BackOff":          "CrashLoopBackOff",
	"FailedScheduling": "SchedulingFailed",
	"Unhealthy":        "ProbeFailure",
}

func (w *Watcher) HandleAddEvent(obj any) {
	ev, ok := obj.(*corev1.Event)
	if !ok {
		return
	}
	failureMode, known := knownReasons[ev.Reason]
	if !known && ev.Reason == "Failed" && strings.Contains(ev.Message, "pull image") {
		failureMode, known = "ImagePullError", true
	}
	if !known || ev.InvolvedObject.Kind != "Pod" {
		return
	}

	// ... rest of the function body is unchanged from today ...
```

Add `"strings"` to the import block.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd backend && go build ./... && go test ./tests/k8swatch/... -v`
Expected: PASS on all 6 tests.

- [ ] **Step 5: Stage**

```bash
git add backend/internal/k8swatch/watcher.go backend/tests/k8swatch/watcher_test.go
```

**Phase A checkpoint:** run `cd backend && go build ./... && go vet ./... && go test ./...` once more — the whole Go backend (detect → correlate → dispatch-to-NATS → [nothing consumes it yet, verified manually via Task 3's test] → HTTP callback → resume) is now internally consistent and fully tested, even though nothing actually calls the diagnosis callback in a running deployment yet. That's what Phase C's ai/ service does.

---

## Phase B — mcp-readonly-server (Go)

### Task 8: `introspect` package — read-only pod introspection

**Files:**
- Create: `backend/internal/introspect/introspect.go`
- Create: `backend/tests/introspect/introspect_test.go`

**Interfaces:**
- Produces: `introspect.GetPodLogs(ctx, clientset kubernetes.Interface, namespace, name string, tailLines int64) (string, error)`, `introspect.GetPodEvents(ctx, clientset, namespace, name string) ([]introspect.EventSummary, error)`, `introspect.DescribePod(ctx, clientset, namespace, name string) (introspect.PodSummary, error)`. All three take a plain `kubernetes.Interface` (same as `execute.Executor`) — this package has no write methods at all, which is itself part of the safety story (Task 9's server can only ever expose what's defined here).

- [ ] **Step 1: Write the failing tests**

```go
// backend/tests/introspect/introspect_test.go
package introspect_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sre-platform/backend/internal/introspect"
)

func TestGetPodLogs_ReturnsWithoutErrorForExistingPod(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default"}}
	clientset := fake.NewSimpleClientset(pod)

	// client-go's fake clientset has no real log backend behind GetLogs —
	// this only proves the call plumbs through end to end without an
	// unexpected error. Real log content is only ever exercised against a
	// live cluster (see this plan's Verification section).
	if _, err := introspect.GetPodLogs(context.Background(), clientset, "default", "web-1", 100); err != nil {
		t.Fatalf("GetPodLogs: %v", err)
	}
}

func TestGetPodEvents_FiltersToTheNamedPod(t *testing.T) {
	matching := &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: "web-1.abc", Namespace: "default"},
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "web-1"},
		Reason:         "BackOff", Message: "Back-off restarting failed container", Type: "Warning", Count: 3,
	}
	unrelated := &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: "other.abc", Namespace: "default"},
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "other-pod"},
		Reason:         "Scheduled", Type: "Normal",
	}
	clientset := fake.NewSimpleClientset(matching, unrelated)

	events, err := introspect.GetPodEvents(context.Background(), clientset, "default", "web-1")
	if err != nil {
		t.Fatalf("GetPodEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event for web-1, got %d: %+v", len(events), events)
	}
	if events[0].Reason != "BackOff" || events[0].Count != 3 {
		t.Errorf("unexpected event: %+v", events[0])
	}
}

func TestDescribePod_SummarizesContainerState(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "app", Ready: false, RestartCount: 4,
					State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
				},
			},
		},
	}
	clientset := fake.NewSimpleClientset(pod)

	summary, err := introspect.DescribePod(context.Background(), clientset, "default", "web-1")
	if err != nil {
		t.Fatalf("DescribePod: %v", err)
	}
	if summary.Phase != "Running" {
		t.Errorf("expected phase Running, got %q", summary.Phase)
	}
	if len(summary.Containers) != 1 || summary.Containers[0].RestartCount != 4 || summary.Containers[0].Reason != "CrashLoopBackOff" {
		t.Fatalf("unexpected container summary: %+v", summary.Containers)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd backend && go test ./tests/introspect/... -v`
Expected: FAIL — `package introspect` doesn't exist yet.

- [ ] **Step 3: Implement `introspect.go`**

```go
// backend/internal/introspect/introspect.go
//
// This package is read-only by construction — it has no method that calls
// Delete/Update/Patch/Create on anything. mcp-readonly-server (cmd/mcp-readonly-server)
// only ever exposes functions defined here as MCP tools, so this file is
// the actual enforcement point for "ai/ never mutates the cluster": there
// is simply nothing to call that would.
package introspect

import (
	"context"
	"io"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// GetPodLogs returns up to tailLines of the named pod's most recent log
// output.
func GetPodLogs(ctx context.Context, clientset kubernetes.Interface, namespace, name string, tailLines int64) (string, error) {
	req := clientset.CoreV1().Pods(namespace).GetLogs(name, &corev1.PodLogOptions{TailLines: &tailLines})
	stream, err := req.Stream(ctx)
	if err != nil {
		return "", err
	}
	defer stream.Close()

	data, err := io.ReadAll(stream)
	if err != nil {
		return string(data), err
	}
	return string(data), nil
}

type EventSummary struct {
	Reason    string `json:"reason"`
	Message   string `json:"message"`
	Type      string `json:"type"`
	Count     int32  `json:"count"`
	Timestamp string `json:"timestamp"`
}

// GetPodEvents returns every Event in namespace whose InvolvedObject is the
// named pod. Filtered client-side rather than via a field selector — field
// selector support for Events is inconsistent across API server versions
// and isn't implemented by the fake clientset used in tests.
func GetPodEvents(ctx context.Context, clientset kubernetes.Interface, namespace, name string) ([]EventSummary, error) {
	events, err := clientset.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var out []EventSummary
	for _, ev := range events.Items {
		if ev.InvolvedObject.Kind != "Pod" || ev.InvolvedObject.Name != name {
			continue
		}
		out = append(out, EventSummary{
			Reason: ev.Reason, Message: ev.Message, Type: ev.Type, Count: ev.Count,
			Timestamp: ev.LastTimestamp.String(),
		})
	}
	return out, nil
}

type PodSummary struct {
	Phase      string             `json:"phase"`
	Containers []ContainerSummary `json:"containers"`
}

type ContainerSummary struct {
	Name         string `json:"name"`
	Ready        bool   `json:"ready"`
	RestartCount int32  `json:"restart_count"`
	State        string `json:"state"`
	Reason       string `json:"reason"`
}

// DescribePod returns a compact summary of the named pod's current status —
// enough for an LLM investigation loop to reason about without needing the
// full Pod object.
func DescribePod(ctx context.Context, clientset kubernetes.Interface, namespace, name string) (PodSummary, error) {
	pod, err := clientset.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return PodSummary{}, err
	}
	summary := PodSummary{Phase: string(pod.Status.Phase)}
	for _, cs := range pod.Status.ContainerStatuses {
		c := ContainerSummary{Name: cs.Name, Ready: cs.Ready, RestartCount: cs.RestartCount}
		switch {
		case cs.State.Waiting != nil:
			c.State, c.Reason = "waiting", cs.State.Waiting.Reason
		case cs.State.Running != nil:
			c.State = "running"
		case cs.State.Terminated != nil:
			c.State, c.Reason = "terminated", cs.State.Terminated.Reason
		}
		summary.Containers = append(summary.Containers, c)
	}
	return summary, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd backend && go build ./... && go test ./tests/introspect/... -v`
Expected: PASS

- [ ] **Step 5: Stage**

```bash
git add backend/internal/introspect backend/tests/introspect
```

---

### Task 9: `cmd/mcp-readonly-server` — the read-only MCP server

Mirrors `backend/cmd/mcp-execute-server/main.go` exactly (same bearer-auth wrapper, same `/healthz` exemption, same Dockerfile shape) with three read-only tools instead of one write tool. Like `cmd/mcp-execute-server`, this binary has no dedicated test file — its tool-wiring pattern is already proven by the httptest-based MCP round-trip in `backend/tests/reconcile/reconcile_test.go` (Task 4), and its actual logic lives in `introspect` (Task 8), which is unit tested directly.

**Files:**
- Create: `backend/cmd/mcp-readonly-server/main.go`
- Create: `backend/cmd/mcp-readonly-server/Dockerfile`
- Modify: `backend/internal/settings/settings.go`
- Modify: `backend/tests/settings/settings_test.go`

**Interfaces:**
- Produces: a running MCP server exposing `get_pod_logs`, `get_pod_events`, `describe_pod` over Streamable HTTP, bearer-token gated. `settings.Settings` gains `MCPReadonlyAddr string` (env `MCP_READONLY_ADDR`, default `:8091`) and `MCPReadonlyToken string` (env `MCP_READONLY_TOKEN`, required).

- [ ] **Step 1: Extend `settings.go` again**

Add two fields to the `Settings` struct (after `MCPExecuteURL`):

```go
	MCPReadonlyAddr  string
	MCPReadonlyToken string
```

Add to `Load()`:

```go
		MCPReadonlyAddr:  getenv("MCP_READONLY_ADDR", ":8091"),
		MCPReadonlyToken: getenv("MCP_READONLY_TOKEN", ""),
```

Add to `Validate()`:

```go
	if s.MCPReadonlyToken == "" {
		missing = append(missing, "MCP_READONLY_TOKEN")
	}
```

Add `MCPReadonlyAddr: ":8091", MCPReadonlyToken: "real-readonly-token",` to `validSettings()` in `backend/tests/settings/settings_test.go`, and extend `TestSettings_Validate_RequiresNATSAndDiagnosisCallbackToken` (rename it `TestSettings_Validate_RequiresNATSDiagnosisAndReadonlyTokens`) to also blank `s.MCPReadonlyToken = ""` and assert `"MCP_READONLY_TOKEN"` appears in the error.

Run: `cd backend && go test ./tests/settings/... -v` — expect PASS.

- [ ] **Step 2: Write `main.go`**

```go
// backend/cmd/mcp-readonly-server/main.go
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"sre-platform/backend/internal/introspect"
	"sre-platform/backend/internal/mcpauth"
	"sre-platform/backend/internal/settings"
)

type GetPodLogsInput struct {
	Namespace string `json:"namespace" jsonschema:"the pod's namespace"`
	Name      string `json:"name" jsonschema:"the pod's name"`
	TailLines int64  `json:"tail_lines" jsonschema:"how many lines from the end of the log to return"`
}
type GetPodLogsOutput struct {
	Logs string `json:"logs"`
}

type GetPodEventsInput struct {
	Namespace string `json:"namespace" jsonschema:"the pod's namespace"`
	Name      string `json:"name" jsonschema:"the pod's name"`
}
type GetPodEventsOutput struct {
	Events []introspect.EventSummary `json:"events"`
}

type DescribePodInput struct {
	Namespace string `json:"namespace" jsonschema:"the pod's namespace"`
	Name      string `json:"name" jsonschema:"the pod's name"`
}
type DescribePodOutput struct {
	Summary introspect.PodSummary `json:"summary"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	s := settings.Load()

	restConfig, err := clientcmd.BuildConfigFromFlags("", s.Kubeconfig)
	if err != nil {
		slog.Error("building kubeconfig", "error", err)
		os.Exit(1)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		slog.Error("building clientset", "error", err)
		os.Exit(1)
	}

	getPodLogs := func(ctx context.Context, req *mcp.CallToolRequest, input GetPodLogsInput) (*mcp.CallToolResult, GetPodLogsOutput, error) {
		logs, err := introspect.GetPodLogs(ctx, clientset, input.Namespace, input.Name, input.TailLines)
		if err != nil {
			slog.Error("get_pod_logs failed", "namespace", input.Namespace, "name", input.Name, "error", err)
			return nil, GetPodLogsOutput{}, err
		}
		return nil, GetPodLogsOutput{Logs: logs}, nil
	}
	getPodEvents := func(ctx context.Context, req *mcp.CallToolRequest, input GetPodEventsInput) (*mcp.CallToolResult, GetPodEventsOutput, error) {
		events, err := introspect.GetPodEvents(ctx, clientset, input.Namespace, input.Name)
		if err != nil {
			slog.Error("get_pod_events failed", "namespace", input.Namespace, "name", input.Name, "error", err)
			return nil, GetPodEventsOutput{}, err
		}
		return nil, GetPodEventsOutput{Events: events}, nil
	}
	describePod := func(ctx context.Context, req *mcp.CallToolRequest, input DescribePodInput) (*mcp.CallToolResult, DescribePodOutput, error) {
		summary, err := introspect.DescribePod(ctx, clientset, input.Namespace, input.Name)
		if err != nil {
			slog.Error("describe_pod failed", "namespace", input.Namespace, "name", input.Name, "error", err)
			return nil, DescribePodOutput{}, err
		}
		return nil, DescribePodOutput{Summary: summary}, nil
	}

	server := mcp.NewServer(&mcp.Implementation{Name: "sre-readonly", Version: "v1.0.0"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "get_pod_logs", Description: "Returns the tail of a pod's container logs. Read-only."}, getPodLogs)
	mcp.AddTool(server, &mcp.Tool{Name: "get_pod_events", Description: "Returns Kubernetes Events involving a pod. Read-only."}, getPodEvents)
	mcp.AddTool(server, &mcp.Tool{Name: "describe_pod", Description: "Returns a compact status summary of a pod: phase, container states, restart counts. Read-only."}, describePod)

	handler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		return server
	}, nil)

	// /healthz stays outside the bearer-token wrapper, same reasoning as
	// mcp-execute-server: a kubelet probe can't present a token, and this
	// endpoint reveals nothing about the cluster.
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("/", mcpauth.RequireBearerToken(s.MCPReadonlyToken, handler))

	slog.Info("mcp-readonly-server listening", "addr", s.MCPReadonlyAddr)
	if err := http.ListenAndServe(s.MCPReadonlyAddr, mux); err != nil {
		slog.Error("http server exited", "error", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 3: Write the Dockerfile**

```dockerfile
# backend/cmd/mcp-readonly-server/Dockerfile
# Build context must be backend/ (the module root), e.g.:
#   docker build -f backend/cmd/mcp-readonly-server/Dockerfile -t sage-mcp-readonly-server backend/

FROM golang:1.26-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/mcp-readonly-server ./cmd/mcp-readonly-server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/mcp-readonly-server /sage-mcp-readonly-server
EXPOSE 8091
USER nonroot:nonroot
ENTRYPOINT ["/sage-mcp-readonly-server"]
```

- [ ] **Step 4: Full module build**

Run: `cd backend && go build ./... && go vet ./... && go test ./...`
Expected: PASS. Phase B is now complete — `mcp-readonly-server` builds as its own binary alongside `backend` and `mcp-execute-server`.

- [ ] **Step 5: Stage**

```bash
git add backend/cmd/mcp-readonly-server backend/internal/settings/settings.go backend/tests/settings/settings_test.go
```

---

## Phase C — ai/ Python service

**External API drift note (applies to every task in this phase):** this phase uses `langgraph`, `langchain-core`/`langchain-anthropic`/`langchain-openai`, `langchain-mcp-adapters`, and `nats-py` — all real, actively-maintained libraries, but exact method/constructor signatures below could have drifted from what `pip`/`uv` resolves at implementation time. If a call in a step below doesn't match installed behavior, check the installed package's own docs/source before guessing (`python -c "import <pkg>; help(<pkg>.<Symbol>)"`), and don't silently change the surrounding design to route around it.

### Task 10: `ai/` project setup — dependencies + settings

**Files:**
- Modify: `ai/pyproject.toml`
- Create: `ai/ai/settings.py`
- Create: `ai/tests/test_settings.py`
- Delete: `ai/tests/test_smoke.py` (its only job was proving pytest was wired before real code existed — Task 10 onward supersedes it)

**Interfaces:**
- Produces: `ai.settings.Settings` (a `pydantic_settings.BaseSettings`) with fields `nats_url`, `backend_callback_url`, `diagnosis_callback_token`, `mcp_readonly_url`, `mcp_readonly_token`, `llm_provider`, `llm_api_key`, `llm_model` — each sourced from the identically-named uppercase environment variable (`NATS_URL`, `BACKEND_CALLBACK_URL`, `DIAGNOSIS_CALLBACK_TOKEN`, `MCP_READONLY_URL`, `MCP_READONLY_TOKEN`, `LLM_PROVIDER`, `LLM_API_KEY`, `LLM_MODEL`) by pydantic-settings' default naming. `ai.settings.load_settings() -> Settings` is the one place this package reads the environment — mirrors `backend/internal/settings/settings.go`'s single-source-of-truth rule (Global Constraints).

- [ ] **Step 1: Update `pyproject.toml`**

```toml
# ai/pyproject.toml
[project]
name = "ai"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "langgraph>=0.2",
    "langchain-core>=0.3",
    "langchain-anthropic>=0.3",
    "langchain-openai>=0.2",
    "langchain-mcp-adapters>=0.1",
    "nats-py>=2.9",
    "httpx>=0.27",
    "pydantic>=2.9",
    "pydantic-settings>=2.5",
]

[dependency-groups]
dev = [
    "pytest>=8.3",
    "pytest-asyncio>=0.24",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
include = ["ai*"]
```

- [ ] **Step 2: Install dependencies**

The `[dependency-groups]` table above is PEP 735 dependency groups, not `[project.optional-dependencies]` extras — `pip install -e '.[dev]'`/`uv pip install -e '.[dev]'` silently skip it (extras and dependency-groups are two different mechanisms; `'.[dev]'` only ever resolves the former). Install both the project and the `dev` group explicitly:

```bash
cd ai
uv pip install -e . --group dev --python .venv/bin/python3
```

(`ai/.venv` already exists from the earlier scaffold. If this environment uses plain `pip` instead of `uv`, there's no single-command equivalent for PEP 735 groups on older pip — install the dev tools directly: `pip install -e . && pip install pytest pytest-asyncio` at minimum, matching the versions pinned in `[dependency-groups.dev]` above.)

- [ ] **Step 3: Write the failing test**

```python
# ai/tests/test_settings.py
import pytest
from pydantic import ValidationError

from ai.settings import Settings


def test_settings_requires_all_fields(monkeypatch):
    for key in [
        "NATS_URL", "BACKEND_CALLBACK_URL", "DIAGNOSIS_CALLBACK_TOKEN",
        "MCP_READONLY_URL", "MCP_READONLY_TOKEN", "LLM_PROVIDER", "LLM_API_KEY", "LLM_MODEL",
    ]:
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(ValidationError):
        Settings()


def test_settings_loads_from_environment(monkeypatch):
    monkeypatch.setenv("NATS_URL", "nats://localhost:4222")
    monkeypatch.setenv("BACKEND_CALLBACK_URL", "http://backend:8080")
    monkeypatch.setenv("DIAGNOSIS_CALLBACK_TOKEN", "token-a")
    monkeypatch.setenv("MCP_READONLY_URL", "http://mcp-readonly:8091")
    monkeypatch.setenv("MCP_READONLY_TOKEN", "token-b")
    monkeypatch.setenv("LLM_PROVIDER", "anthropic")
    monkeypatch.setenv("LLM_API_KEY", "sk-test")
    monkeypatch.setenv("LLM_MODEL", "claude-sonnet-4-5")

    settings = Settings()

    assert settings.nats_url == "nats://localhost:4222"
    assert settings.llm_provider == "anthropic"
    assert settings.llm_model == "claude-sonnet-4-5"
```

- [ ] **Step 4: Run test to verify it fails**

Run: `cd ai && pytest tests/test_settings.py -v`
Expected: FAIL — `ai.settings` doesn't exist yet.

- [ ] **Step 5: Implement `settings.py`**

```python
# ai/ai/settings.py
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Single source of truth for every environment-derived value used
    anywhere in ai/ — mirrors backend/internal/settings/settings.go's rule
    that no other module reads the environment directly."""

    model_config = SettingsConfigDict(extra="ignore")

    nats_url: str
    backend_callback_url: str
    diagnosis_callback_token: str
    mcp_readonly_url: str
    mcp_readonly_token: str

    llm_provider: str  # "anthropic" | "openai" | "openrouter" — see ai/llm.py
    llm_api_key: str
    llm_model: str


def load_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]  # fields are populated from the environment by pydantic-settings
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cd ai && pytest tests/test_settings.py -v`
Expected: PASS

- [ ] **Step 7: Delete the smoke test**

```bash
rm ai/tests/test_smoke.py
```

- [ ] **Step 8: Stage**

```bash
git add ai/pyproject.toml ai/ai/settings.py ai/tests/test_settings.py ai/tests/test_smoke.py
```

---

### Task 11: `models.py` — the Go↔Python wire contract

**Files:**
- Create: `ai/ai/models.py`
- Create: `ai/tests/test_models.py`

**Interfaces:**
- Consumes: nothing new — mirrors `backend/internal/incidentqueue.PendingIncident`/`SignalPayload` (Task 3) and the JSON body `backend/internal/httpserver/diagnosis.go` decodes (Task 5), field-for-field.
- Produces: `ai.models.SignalPayload`, `ai.models.PendingIncident`, `ai.models.Diagnosis` — every later Python task in this phase imports these.

- [ ] **Step 1: Write the failing test**

```python
# ai/tests/test_models.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ai && pytest tests/test_models.py -v`
Expected: FAIL — `ai.models` doesn't exist yet.

- [ ] **Step 3: Implement `models.py`**

```python
# ai/ai/models.py
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


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


class Diagnosis(BaseModel):
    """Mirrors the JSON body backend/internal/httpserver/diagnosis.go
    decodes on POST /internal/incidents/{id}/diagnosis."""

    failure_mode: str
    recommended_action: str  # constrained to "restart_pod" | "none" by ai/diagnose.py and investigate.py — see Global Constraints
    confidence: float
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd ai && pytest tests/test_models.py -v`
Expected: PASS

- [ ] **Step 5: Stage**

```bash
git add ai/ai/models.py ai/tests/test_models.py
```

---

### Task 12: Rule-based analyzers

**Files:**
- Create: `ai/ai/analyzers.py`
- Create: `ai/tests/test_analyzers.py`

**Interfaces:**
- Consumes: `ai.models.PendingIncident`/`Diagnosis` (Task 11).
- Produces: `ai.analyzers.run_rule_based_analyzers(incident: PendingIncident) -> Diagnosis | None` — Task 16's orchestrator calls this first, before ever touching the LLM.

- [ ] **Step 1: Write the failing tests**

```python
# ai/tests/test_analyzers.py
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ai && pytest tests/test_analyzers.py -v`
Expected: FAIL — `ai.analyzers` doesn't exist yet.

- [ ] **Step 3: Implement `analyzers.py`**

```python
# ai/ai/analyzers.py
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ai && pytest tests/test_analyzers.py -v`
Expected: PASS

- [ ] **Step 5: Stage**

```bash
git add ai/ai/analyzers.py ai/tests/test_analyzers.py
```

---

### Task 13: `llm.py` — pluggable LLM factory

**Files:**
- Create: `ai/ai/llm.py`
- Create: `ai/tests/test_llm.py`

**Interfaces:**
- Consumes: `ai.settings.Settings` (Task 10).
- Produces: `ai.llm.get_chat_model(settings: Settings) -> BaseChatModel` — Task 18's `__main__.py` calls this once at startup.

- [ ] **Step 1: Write the failing tests**

```python
# ai/tests/test_llm.py
import pytest
from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI

from ai.llm import get_chat_model
from ai.settings import Settings


def _settings(**overrides) -> Settings:
    base = dict(
        nats_url="nats://localhost:4222", backend_callback_url="http://backend:8080",
        diagnosis_callback_token="t", mcp_readonly_url="http://mcp:8091", mcp_readonly_token="t",
        llm_provider="anthropic", llm_api_key="sk-test", llm_model="claude-sonnet-4-5",
    )
    base.update(overrides)
    return Settings(**base)


def test_anthropic_provider_returns_chat_anthropic():
    model = get_chat_model(_settings(llm_provider="anthropic"))
    assert isinstance(model, ChatAnthropic)


def test_openai_provider_returns_chat_openai():
    model = get_chat_model(_settings(llm_provider="openai", llm_model="gpt-4o"))
    assert isinstance(model, ChatOpenAI)


def test_openrouter_provider_returns_chat_openai_pointed_at_openrouter():
    model = get_chat_model(_settings(llm_provider="openrouter", llm_model="deepseek/deepseek-chat"))
    assert isinstance(model, ChatOpenAI)
    # ChatOpenAI's base_url field name has moved before across langchain-openai
    # versions (openai_api_base vs base_url) — see this phase's API-drift note.
    assert "openrouter.ai" in str(getattr(model, "openai_api_base", None) or getattr(model, "base_url", ""))


def test_unknown_provider_raises():
    with pytest.raises(ValueError, match="unknown LLM_PROVIDER"):
        get_chat_model(_settings(llm_provider="bogus"))
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ai && pytest tests/test_llm.py -v`
Expected: FAIL — `ai.llm` doesn't exist yet.

- [ ] **Step 3: Implement `llm.py`**

```python
# ai/ai/llm.py
from __future__ import annotations

from langchain_core.language_models.chat_models import BaseChatModel

from ai.settings import Settings


def get_chat_model(settings: Settings) -> BaseChatModel:
    """Returns a LangChain chat model for whichever provider the deployer
    configured via LLM_PROVIDER — the seam that makes ai/ usable against a
    self-hosted OpenRouter endpoint instead of one hardcoded provider (see
    docs/superpowers/specs/2026-08-08-ai-diagnosis-service-design.md)."""
    if settings.llm_provider == "anthropic":
        from langchain_anthropic import ChatAnthropic

        return ChatAnthropic(model=settings.llm_model, api_key=settings.llm_api_key)
    if settings.llm_provider == "openai":
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(model=settings.llm_model, api_key=settings.llm_api_key)
    if settings.llm_provider == "openrouter":
        # OpenRouter speaks the OpenAI-compatible API — same client, just a
        # different base_url, so no separate SDK is needed for this branch.
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(
            model=settings.llm_model,
            api_key=settings.llm_api_key,
            base_url="https://openrouter.ai/api/v1",
        )
    raise ValueError(f"unknown LLM_PROVIDER {settings.llm_provider!r}: must be \"anthropic\", \"openai\", or \"openrouter\"")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ai && pytest tests/test_llm.py -v`
Expected: PASS. No network calls happen here — these LangChain chat model constructors are lazy and only reach the provider on `.invoke()`.

- [ ] **Step 5: Stage**

```bash
git add ai/ai/llm.py ai/tests/test_llm.py
```

---

### Task 14: `mcp_client.py` — read-only tools over MCP

**Files:**
- Create: `ai/ai/mcp_client.py`
- Create: `ai/tests/test_mcp_client.py`

**Interfaces:**
- Consumes: `ai.settings.Settings` (Task 10), `mcp-readonly-server`'s Streamable HTTP endpoint (Task 9) — never called for real in this task's test, only its URL/token/transport config is asserted.
- Produces: `ai.mcp_client.get_readonly_tools(settings: Settings) -> list[BaseTool]` — Task 18's `__main__.py` calls this once at startup and passes the result into `investigate()`.

- [ ] **Step 1: Write the failing test**

```python
# ai/tests/test_mcp_client.py
from unittest.mock import AsyncMock, patch

from ai.mcp_client import get_readonly_tools
from ai.settings import Settings


def _settings() -> Settings:
    return Settings(
        nats_url="nats://localhost:4222", backend_callback_url="http://backend:8080",
        diagnosis_callback_token="t", mcp_readonly_url="http://mcp-readonly:8091",
        mcp_readonly_token="readonly-token", llm_provider="anthropic", llm_api_key="sk-test", llm_model="claude-sonnet-4-5",
    )


async def test_get_readonly_tools_configures_streamable_http_with_bearer_token():
    fake_tools = ["get_pod_logs", "get_pod_events", "describe_pod"]
    with patch("ai.mcp_client.MultiServerMCPClient") as mock_client_cls:
        mock_client_cls.return_value.get_tools = AsyncMock(return_value=fake_tools)

        tools = await get_readonly_tools(_settings())

        assert tools == fake_tools
        config = mock_client_cls.call_args[0][0]
        assert config["sre-readonly"]["url"] == "http://mcp-readonly:8091"
        assert config["sre-readonly"]["headers"]["Authorization"] == "Bearer readonly-token"
        assert config["sre-readonly"]["transport"] == "streamable_http"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ai && pytest tests/test_mcp_client.py -v`
Expected: FAIL — `ai.mcp_client` doesn't exist yet.

- [ ] **Step 3: Implement `mcp_client.py`**

```python
# ai/ai/mcp_client.py
from __future__ import annotations

from langchain_core.tools import BaseTool
from langchain_mcp_adapters.client import MultiServerMCPClient

from ai.settings import Settings


async def get_readonly_tools(settings: Settings) -> list[BaseTool]:
    """Fetches the read-only tool set mcp-readonly-server exposes
    (get_pod_logs, get_pod_events, describe_pod — see
    backend/cmd/mcp-readonly-server/main.go) as LangChain tools the
    investigation graph (investigate.py) can call directly. ai/ never gets
    direct cluster credentials — this is its only path to cluster data."""
    client = MultiServerMCPClient(
        {
            "sre-readonly": {
                "transport": "streamable_http",
                "url": settings.mcp_readonly_url,
                "headers": {"Authorization": f"Bearer {settings.mcp_readonly_token}"},
            }
        }
    )
    return await client.get_tools()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd ai && pytest tests/test_mcp_client.py -v`
Expected: PASS

- [ ] **Step 5: Stage**

```bash
git add ai/ai/mcp_client.py ai/tests/test_mcp_client.py
```

---

### Task 15: `investigate.py` — LLM investigation loop

**Files:**
- Create: `ai/ai/investigate.py`
- Create: `ai/tests/test_investigate.py`

**Interfaces:**
- Consumes: `ai.models.PendingIncident`/`Diagnosis` (Task 11), a `BaseChatModel` (Task 13) and `list[BaseTool]` (Task 14) passed in by the caller.
- Produces: `ai.investigate.investigate(incident: PendingIncident, model: BaseChatModel, tools: list[BaseTool]) -> Diagnosis` and `ai.investigate._parse_diagnosis(text: str) -> Diagnosis` — Task 16's orchestrator calls `investigate()` only when no rule matched.

- [ ] **Step 1: Write the failing tests**

```python
# ai/tests/test_investigate.py
from datetime import datetime, timezone

from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage

from ai.investigate import _parse_diagnosis, investigate
from ai.models import PendingIncident, SignalPayload


def test_parse_diagnosis_extracts_trailing_json():
    text = 'I looked at the logs. Conclusion:\n{"failure_mode": "BadConfig", "recommended_action": "none", "confidence": 0.6}'
    diagnosis = _parse_diagnosis(text)
    assert diagnosis.failure_mode == "BadConfig"
    assert diagnosis.recommended_action == "none"
    assert diagnosis.confidence == 0.6


def test_parse_diagnosis_falls_back_safely_on_unparseable_text():
    diagnosis = _parse_diagnosis("I'm not sure what happened here.")
    assert diagnosis.recommended_action == "none"
    assert diagnosis.confidence == 0.0


def test_parse_diagnosis_rejects_out_of_vocabulary_action():
    text = '{"failure_mode": "X", "recommended_action": "scale_up", "confidence": 0.9}'
    diagnosis = _parse_diagnosis(text)
    assert diagnosis.recommended_action == "none"


def test_parse_diagnosis_falls_back_safely_on_non_string_content():
    # langchain-anthropic sets AIMessage.content to a list of content
    # blocks (not a str) for multi-block responses (e.g. extended
    # thinking + text, or citations) — must fall back, never raise.
    diagnosis = _parse_diagnosis([{"type": "text", "text": "irrelevant"}])
    assert diagnosis.recommended_action == "none"
    assert diagnosis.confidence == 0.0


async def test_investigate_returns_diagnosis_from_fake_model_final_answer():
    now = datetime.now(timezone.utc)
    incident = PendingIncident(
        incident_id="incident-1", namespace="default", kind="Pod", name="web-1",
        group_key="default/Pod/web-1",
        signals=[SignalPayload(type="ImagePullError", severity="warning", timestamp=now, raw="")],
        first_seen=now, last_seen=now,
    )
    fake_model = GenericFakeChatModel(
        messages=iter([AIMessage(content='{"failure_mode": "ImagePullError", "recommended_action": "none", "confidence": 0.8}')])
    )

    diagnosis = await investigate(incident, fake_model, tools=[])

    assert diagnosis.failure_mode == "ImagePullError"
    assert diagnosis.recommended_action == "none"
    assert diagnosis.confidence == 0.8
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ai && pytest tests/test_investigate.py -v`
Expected: FAIL — `ai.investigate` doesn't exist yet.

- [ ] **Step 3: Implement `investigate.py`**

```python
# ai/ai/investigate.py
from __future__ import annotations

import json
import re

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.tools import BaseTool
from langgraph.prebuilt import create_react_agent

from ai.models import Diagnosis, PendingIncident

SYSTEM_PROMPT = """You are SAGE's incident diagnosis agent. You investigate a
Kubernetes pod failure using read-only tools (get_pod_logs, get_pod_events,
describe_pod) and must end your investigation with exactly one JSON object
on its own line, matching this shape:

{"failure_mode": "<short name>", "recommended_action": "restart_pod" | "none", "confidence": <0.0-1.0>}

Rules:
- recommended_action MUST be exactly "restart_pod" or "none" — no other
  value is ever wired to an executor, so anything else is silently
  equivalent to guessing wrong.
- Use "none" whenever restarting the pod would not plausibly fix the
  underlying problem (a bad image reference, a missing secret/config, an
  unschedulable resource request) — do not default to "restart_pod" just
  because you're unsure; lower confidence instead.
- Investigate before concluding: call at least one tool unless the incident
  summary alone is unambiguous.
"""


async def investigate(incident: PendingIncident, model: BaseChatModel, tools: list[BaseTool]) -> Diagnosis:
    """LLM-driven fallback for incidents no rule-based analyzer matched.
    Uses LangGraph's prebuilt ReAct agent loop rather than a hand-built
    StateGraph — this fallback only needs "call tools, reason, repeat until
    done," which is exactly what create_react_agent already implements."""
    agent = create_react_agent(model, tools)
    incident_summary = (
        f"Incident {incident.incident_id}: {incident.kind} {incident.namespace}/{incident.name}\n"
        f"Signals: {[s.type for s in incident.signals]}\n"
        f"First seen: {incident.first_seen}, last seen: {incident.last_seen}"
    )
    result = await agent.ainvoke({"messages": [("system", SYSTEM_PROMPT), ("user", incident_summary)]})
    final_message = result["messages"][-1].content
    return _parse_diagnosis(final_message)


def _parse_diagnosis(text: str) -> Diagnosis:
    """Extracts the trailing JSON object the system prompt requires. Falls
    back to a safe "none" diagnosis with confidence 0 if the model didn't
    produce parseable JSON — a formatting slip must never be treated as
    license to restart a pod nobody actually recommended restarting. The
    regex search lives inside the try block because `text` isn't always a
    str in practice: langchain-anthropic sets AIMessage.content to a list
    of content blocks (not a plain string) when a response has multiple
    blocks (e.g. extended thinking + text) or citations, and that must fall
    back safely too, not raise past this function."""
    try:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            return Diagnosis(failure_mode="unrecognized", recommended_action="none", confidence=0.0)
        data = json.loads(match.group(0))
        action = data.get("recommended_action")
        return Diagnosis(
            failure_mode=str(data.get("failure_mode", "unrecognized")),
            recommended_action=action if action in ("restart_pod", "none") else "none",
            confidence=float(data.get("confidence", 0.0)),
        )
    except (json.JSONDecodeError, TypeError, ValueError):
        return Diagnosis(failure_mode="unrecognized", recommended_action="none", confidence=0.0)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ai && pytest tests/test_investigate.py -v`
Expected: PASS

- [ ] **Step 5: Stage**

```bash
git add ai/ai/investigate.py ai/tests/test_investigate.py
```

---

### Task 16: `diagnose.py` — rules-then-LLM orchestrator

**Files:**
- Create: `ai/ai/diagnose.py`
- Create: `ai/tests/test_diagnose.py`

**Interfaces:**
- Consumes: `ai.analyzers.run_rule_based_analyzers` (Task 12), `ai.investigate.investigate` (Task 15).
- Produces: `ai.diagnose.diagnose(incident: PendingIncident, model: BaseChatModel, tools: list[BaseTool]) -> Diagnosis` — Task 18's consumer calls this once per incident; it's the single entry point the rest of ai/ external to this module needs.

- [ ] **Step 1: Write the failing tests**

```python
# ai/tests/test_diagnose.py
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ai && pytest tests/test_diagnose.py -v`
Expected: FAIL — `ai.diagnose` doesn't exist yet.

- [ ] **Step 3: Implement `diagnose.py`**

```python
# ai/ai/diagnose.py
from __future__ import annotations

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.tools import BaseTool

from ai.analyzers import run_rule_based_analyzers
from ai.investigate import investigate
from ai.models import Diagnosis, PendingIncident


async def diagnose(incident: PendingIncident, model: BaseChatModel, tools: list[BaseTool]) -> Diagnosis:
    """Rules first (fast, free, deterministic), LLM investigation only when
    no rule matches — the diagnosis engine docs/overview.md originally
    described for ai/."""
    diagnosis = run_rule_based_analyzers(incident)
    if diagnosis is not None:
        return diagnosis
    return await investigate(incident, model, tools)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ai && pytest tests/test_diagnose.py -v`
Expected: PASS

- [ ] **Step 5: Stage**

```bash
git add ai/ai/diagnose.py ai/tests/test_diagnose.py
```

---

### Task 17: `callback.py` — POST diagnosis back to backend

**Files:**
- Create: `ai/ai/callback.py`
- Create: `ai/tests/test_callback.py`

**Interfaces:**
- Consumes: `ai.settings.Settings` (Task 10), `ai.models.Diagnosis` (Task 11), backend's `POST /internal/incidents/{id}/diagnosis` (Task 5).
- Produces: `ai.callback.post_diagnosis(settings: Settings, incident_id: str, diagnosis: Diagnosis, *, client: httpx.AsyncClient | None = None) -> None` — raises `httpx.HTTPStatusError` on a non-2xx response. Task 18's consumer calls this after every diagnosis.

- [ ] **Step 1: Write the failing tests**

```python
# ai/tests/test_callback.py
import httpx
import pytest

from ai.callback import post_diagnosis
from ai.models import Diagnosis
from ai.settings import Settings


def _settings() -> Settings:
    return Settings(
        nats_url="nats://localhost:4222", backend_callback_url="http://backend:8080",
        diagnosis_callback_token="cb-token", mcp_readonly_url="http://mcp:8091",
        mcp_readonly_token="t", llm_provider="anthropic", llm_api_key="sk-test", llm_model="claude-sonnet-4-5",
    )


async def test_post_diagnosis_sends_expected_request():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["auth"] = request.headers.get("Authorization")
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await post_diagnosis(
            _settings(), "incident-1",
            Diagnosis(failure_mode="CrashLoopBackOff", recommended_action="restart_pod", confidence=0.9),
            client=client,
        )

    assert captured["url"] == "http://backend:8080/internal/incidents/incident-1/diagnosis"
    assert captured["auth"] == "Bearer cb-token"


async def test_post_diagnosis_raises_on_error_response():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await post_diagnosis(
                _settings(), "incident-1",
                Diagnosis(failure_mode="X", recommended_action="none", confidence=0.1),
                client=client,
            )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ai && pytest tests/test_callback.py -v`
Expected: FAIL — `ai.callback` doesn't exist yet.

- [ ] **Step 3: Implement `callback.py`**

```python
# ai/ai/callback.py
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ai && pytest tests/test_callback.py -v`
Expected: PASS

- [ ] **Step 5: Stage**

```bash
git add ai/ai/callback.py ai/tests/test_callback.py
```

---

### Task 18: `consumer.py` + `__main__.py` — NATS consumer and entrypoint

**Files:**
- Create: `ai/ai/consumer.py`
- Create: `ai/ai/__main__.py`
- Create: `ai/tests/test_consumer.py`

**Interfaces:**
- Consumes: `ai.diagnose.diagnose` (Task 16), `ai.callback.post_diagnosis` (Task 17), `ai.models.PendingIncident` (Task 11), `ai.llm.get_chat_model` (Task 13), `ai.mcp_client.get_readonly_tools` (Task 14), `ai.settings.load_settings` (Task 10). Consumes the wire format `backend/internal/incidentqueue.Client` (Task 3) publishes — same `SAGE_INCIDENTS` stream name and `sage.incidents.pending` subject, durable consumer name `ai-diagnosis-worker`.
- Produces: `ai.consumer.run(settings, model, tools) -> None` (the forever loop) and `ai.consumer.handle_message(data: bytes, settings, model, tools) -> None` (the per-message unit, what tests actually exercise). `python -m ai` is the container entrypoint (used by Task 19's Dockerfile).

- [ ] **Step 1: Write the failing test**

```python
# ai/tests/test_consumer.py
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from ai.consumer import handle_message
from ai.models import PendingIncident, SignalPayload
from ai.settings import Settings


def _settings() -> Settings:
    return Settings(
        nats_url="nats://localhost:4222", backend_callback_url="http://backend:8080",
        diagnosis_callback_token="t", mcp_readonly_url="http://mcp:8091",
        mcp_readonly_token="t", llm_provider="anthropic", llm_api_key="sk-test", llm_model="claude-sonnet-4-5",
    )


async def test_handle_message_diagnoses_and_posts_back():
    now = datetime.now(timezone.utc)
    incident = PendingIncident(
        incident_id="incident-1", namespace="default", kind="Pod", name="web-1",
        group_key="default/Pod/web-1",
        signals=[SignalPayload(type="CrashLoopBackOff", severity="warning", timestamp=now, raw="")],
        first_seen=now, last_seen=now,
    )
    data = incident.model_dump_json().encode()

    with patch("ai.consumer.post_diagnosis", new=AsyncMock()) as mock_post:
        # model=None, tools=[] is safe here: CrashLoopBackOff matches a
        # rule in ai.analyzers, so diagnose() never touches the model.
        await handle_message(data, _settings(), model=None, tools=[])

    mock_post.assert_awaited_once()
    call_args = mock_post.call_args.args
    assert call_args[1] == "incident-1"
    assert call_args[2].failure_mode == "CrashLoopBackOff"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ai && pytest tests/test_consumer.py -v`
Expected: FAIL — `ai.consumer` doesn't exist yet.

- [ ] **Step 3: Implement `consumer.py`**

```python
# ai/ai/consumer.py
from __future__ import annotations

import logging

import nats
from nats.js.api import AckPolicy, ConsumerConfig

from ai.callback import post_diagnosis
from ai.diagnose import diagnose
from ai.models import PendingIncident
from ai.settings import Settings

logger = logging.getLogger("ai.consumer")

# Must match backend/internal/incidentqueue.StreamName / PendingSubject
# exactly — this and that Go file are the two ends of the same contract.
STREAM_NAME = "SAGE_INCIDENTS"
PENDING_SUBJECT = "sage.incidents.pending"
DURABLE_NAME = "ai-diagnosis-worker"


async def run(settings: Settings, model, tools) -> None:
    """Durable JetStream consumer loop: pulls pending incidents published by
    backend/, diagnoses each, POSTs the result back, then acks. Runs
    forever — called once from __main__.py at process startup."""
    nc = await nats.connect(settings.nats_url)
    js = nc.jetstream()
    sub = await js.pull_subscribe(
        PENDING_SUBJECT, durable=DURABLE_NAME, config=ConsumerConfig(ack_policy=AckPolicy.EXPLICIT)
    )

    logger.info("ai/ consumer started, subscribed to %s", PENDING_SUBJECT)
    while True:
        try:
            msgs = await sub.fetch(1, timeout=5)
        except TimeoutError:
            continue
        for msg in msgs:
            await handle_message(msg.data, settings, model, tools)
            await msg.ack()


async def handle_message(data: bytes, settings: Settings, model, tools) -> None:
    incident = PendingIncident.model_validate_json(data)
    logger.info("diagnosing incident %s (%s/%s)", incident.incident_id, incident.namespace, incident.name)
    diagnosis = await diagnose(incident, model, tools)
    logger.info(
        "diagnosis for %s: failure_mode=%s recommended_action=%s confidence=%s",
        incident.incident_id, diagnosis.failure_mode, diagnosis.recommended_action, diagnosis.confidence,
    )
    await post_diagnosis(settings, incident.incident_id, diagnosis)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd ai && pytest tests/test_consumer.py -v`
Expected: PASS

- [ ] **Step 5: Write `__main__.py`** (no dedicated test — pure wiring, same convention as `backend/cmd/*/main.go` being untested directly)

```python
# ai/ai/__main__.py
from __future__ import annotations

import asyncio
import logging

from ai import consumer
from ai.llm import get_chat_model
from ai.mcp_client import get_readonly_tools
from ai.settings import load_settings


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = load_settings()
    model = get_chat_model(settings)
    tools = await get_readonly_tools(settings)
    await consumer.run(settings, model, tools)


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 6: Run the full Python test suite**

Run: `cd ai && pytest -v`
Expected: PASS on every test across Tasks 10–18.

- [ ] **Step 7: Stage**

```bash
git add ai/ai/consumer.py ai/ai/__main__.py ai/tests/test_consumer.py
```

---

### Task 19: `ai/` Dockerfile

**Files:**
- Create: `ai/Dockerfile`

**Interfaces:** none — packaging only.

- [ ] **Step 1: Write the Dockerfile**

```dockerfile
# ai/Dockerfile
# Build context must be ai/, e.g.:
#   docker build -t sage-ai ai/
#
# Uses python:3.12-slim for both stages rather than
# gcr.io/distroless/python3-debian12 (the rest of this project's usual
# distroless pattern, see backend/cmd/*/Dockerfile) — distroless's bundled
# Python version isn't pinned to a version this Dockerfile controls, which
# risks a bytecode/ABI mismatch against compiled dependencies in the
# langchain/pydantic stack built in the separate builder stage. slim keeps
# the builder and runtime stages on the exact same interpreter.

FROM python:3.12-slim AS builder
WORKDIR /src
COPY pyproject.toml ./
COPY ai ./ai
RUN pip install --no-cache-dir --target=/deps .

FROM python:3.12-slim
WORKDIR /app
COPY --from=builder /deps /usr/local/lib/python3.12/site-packages
COPY --from=builder /src/ai ./ai
RUN useradd --uid 65532 --no-create-home nonroot
USER nonroot
ENTRYPOINT ["python", "-m", "ai"]
```

- [ ] **Step 2: Build it locally to confirm it compiles as an image**

Run: `docker build -t sage-ai:local ai/`
Expected: build succeeds (this doesn't run the service — `NATS_URL`/`LLM_API_KEY`/etc aren't set locally, so don't run the resulting image standalone; Helm — Phase D — is what actually deploys it with real config).

- [ ] **Step 3: Stage**

```bash
git add ai/Dockerfile
```

**Phase C checkpoint:** `cd ai && pytest -v` passes end to end. ai/ is now a real service: rule-based analyzers, an LLM-pluggable investigation fallback, a NATS consumer, and a callback poster — nothing in this phase has been deployed yet, that's Phase D.

---

## Phase D — Helm deployment

### Task 20: `nats` chart dependency + values scaffolding

**Version-pinning note:** this task adds `nats-io/k8s`'s official `nats` chart as a dependency. Its exact latest version and values schema (JetStream config keys in particular have changed shape across major versions of that chart) aren't verified here — Step 1 has you resolve the real current version, and Step 3's `helm template` run is what actually catches a values-schema mismatch, the same way Task 3's `go build` catches a Go API mismatch. Don't guess past what those commands tell you.

**Files:**
- Modify: `helm/Chart.yaml`
- Modify: `helm/values.yaml`
- Modify: `helm/templates/_helpers.tpl`

**Interfaces:**
- Produces: new `_helpers.tpl` templates `sage.natsUrl`, `sage.mcpReadonly.fullname`, `sage.mcpReadonly.selectorLabels`, `sage.rbac.mcpReadonly.name`, `sage.mcpReadonlyUrl`, `sage.ai.fullname`, `sage.ai.selectorLabels`, `sage.backendCallbackUrl` — Tasks 21 and 22 use these the same way existing templates use `sage.mcpExecute.fullname`/`sage.mcpExecuteUrl`.

- [ ] **Step 1: Resolve the current `nats` chart version**

```bash
helm repo add nats https://nats-io.github.io/k8s/helm/charts/
helm repo update
helm search repo nats/nats
```

Use whatever version that prints in place of the placeholder version below.

- [ ] **Step 2: Add the dependency to `Chart.yaml`**

```yaml
# helm/Chart.yaml
apiVersion: v2
name: sage
description: SAGE (Self-healing Autonomous Guardian Engine) — an autonomous SRE platform for Kubernetes
type: application
version: 0.1.0
appVersion: "0.1.0"

dependencies:
  - name: postgresql
    version: "18.8.1"
    repository: "oci://registry-1.docker.io/bitnamicharts"
    alias: postgresql
    condition: postgresql.enabled
  - name: nats
    version: "REPLACE_WITH_VERSION_FROM_STEP_1"
    repository: "https://nats-io.github.io/k8s/helm/charts/"
    alias: nats
    condition: nats.enabled
```

- [ ] **Step 3: Add `nats`/`mcpReadonly`/`ai` blocks to `values.yaml`**

Add after the existing `mcpExecute:` block:

```yaml
nats:
  enabled: true
  fullnameOverride: "sage-nats"
  # JetStream persistence for the pending-incidents stream — verify these
  # keys against `helm show values nats/nats --version <pinned>` (Step 1);
  # this chart's values schema has changed shape across major versions.
  config:
    jetstream:
      enabled: true
      fileStore:
        pvc:
          size: 1Gi

mcpReadonly:
  # Stateless and read-only — safely scalable if ever needed, same
  # reasoning as mcpExecute.replicaCount's comment, opposite conclusion
  # from backend's (which must stay 1).
  replicaCount: 1
  image:
    repository: ""
    tag: ""
    pullPolicy: IfNotPresent
  port: 8091
  resources: {}
  podAnnotations: {}
  nodeSelector: {}
  tolerations: []
  affinity: {}

ai:
  replicaCount: 1
  image:
    repository: ""
    tag: ""
    pullPolicy: IfNotPresent
  resources: {}
  podAnnotations: {}
  nodeSelector: {}
  tolerations: []
  affinity: {}

mcpReadonlyToken: ""
diagnosisCallbackToken: ""

llm:
  provider: ""   # "anthropic" | "openai" | "openrouter"
  model: ""
  apiKey: ""
```

- [ ] **Step 4: Add helpers to `_helpers.tpl`**

Add at the end of the file:

```
{{/*
Per-component names for mcp-readonly-server and ai/, following the exact
same pattern as sage.mcpExecute.fullname / sage.mcpExecute.selectorLabels.
*/}}
{{- define "sage.mcpReadonly.fullname" -}}
{{- printf "%s-mcp-readonly-server" (include "sage.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sage.mcpReadonly.selectorLabels" -}}
{{ include "sage.selectorLabels" . }}
app.kubernetes.io/component: mcp-readonly-server
{{- end -}}

{{- define "sage.rbac.mcpReadonly.name" -}}
{{- printf "%s-%s-mcp-readonly-server" (include "sage.fullname" .) .Release.Namespace | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sage.mcpReadonlyUrl" -}}
http://{{ include "sage.mcpReadonly.fullname" . }}:{{ .Values.mcpReadonly.port }}
{{- end -}}

{{- define "sage.ai.fullname" -}}
{{- printf "%s-ai" (include "sage.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sage.ai.selectorLabels" -}}
{{ include "sage.selectorLabels" . }}
app.kubernetes.io/component: ai
{{- end -}}

{{/*
In-cluster URL backend/ listens on — ai/ uses this to POST diagnoses back.
*/}}
{{- define "sage.backendCallbackUrl" -}}
http://{{ include "sage.backend.fullname" . }}:{{ .Values.backend.port }}
{{- end -}}

{{/*
In-cluster NATS URL. fullnameOverride pins this to a fixed name (same
approach as sage.postgresqlHost) so it doesn't depend on the nats
subchart's own fullname logic.
*/}}
{{- define "sage.natsUrl" -}}
nats://{{ .Values.nats.fullnameOverride }}:4222
{{- end -}}
```

- [ ] **Step 5: Fetch the dependency and confirm the chart still renders**

```bash
cd helm
helm dependency update
helm template sage . -f values.secret.yaml --set nats.enabled=true > /dev/null
```

Expected: renders without error. If `helm dependency update` or `helm template` errors on an unrecognized `nats.config.*` key, that's Step 1/3's version-pinning note in practice — check `helm show values nats/nats --version <pinned>` and adjust the `config:` block in `values.yaml` to match.

- [ ] **Step 6: Stage**

```bash
git add helm/Chart.yaml helm/Chart.lock helm/values.yaml helm/templates/_helpers.tpl
```

(`helm dependency update` also writes/updates `helm/charts/*.tgz` — that's gitignored on purpose, see `.gitignore`'s "Helm dependency chart archives" comment; don't force-add it.)

---

### Task 21: `mcp-readonly-server` Helm templates

Mirrors `helm/templates/deployment-mcp-execute.yaml`, `service-mcp-execute.yaml`, and `rbac-mcp-execute.yaml` (Task 9's Go binary already matches `mcp-execute-server`'s shape, so its Helm templates should too) — with one deliberate RBAC difference: `get`/`list`/`watch` instead of `delete`, and it needs a `pods/log` subresource grant `mcp-execute-server` never needed.

**Files:**
- Create: `helm/templates/deployment-mcp-readonly.yaml`
- Create: `helm/templates/service-mcp-readonly.yaml`
- Create: `helm/templates/rbac-mcp-readonly.yaml`

**Interfaces:** none new — consumes the helpers Task 20 added.

- [ ] **Step 1: Write `rbac-mcp-readonly.yaml`**

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "sage.mcpReadonly.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "sage.mcpReadonly.selectorLabels" . | nindent 4 }}
---
# Read-only, cluster-wide, no delete/update/patch/create anywhere — this is
# the infrastructure-level enforcement of introspect.go's own promise (see
# backend/internal/introspect/introspect.go's package comment): even if
# ai/'s LLM loop were somehow tricked into requesting a mutation, this
# identity has no permission to perform one.
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{ include "sage.rbac.mcpReadonly.name" . }}
  labels:
    {{- include "sage.mcpReadonly.selectorLabels" . | nindent 4 }}
rules:
  - apiGroups: [""]
    resources: ["pods", "events"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ include "sage.rbac.mcpReadonly.name" . }}
  labels:
    {{- include "sage.mcpReadonly.selectorLabels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {{ include "sage.rbac.mcpReadonly.name" . }}
subjects:
  - kind: ServiceAccount
    name: {{ include "sage.mcpReadonly.fullname" . }}
    namespace: {{ .Release.Namespace }}
```

- [ ] **Step 2: Write `deployment-mcp-readonly.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "sage.mcpReadonly.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "sage.mcpReadonly.selectorLabels" . | nindent 4 }}
spec:
  replicas: {{ .Values.mcpReadonly.replicaCount }}
  selector:
    matchLabels:
      {{- include "sage.mcpReadonly.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "sage.mcpReadonly.selectorLabels" . | nindent 8 }}
      {{- with .Values.mcpReadonly.podAnnotations }}
      annotations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
    spec:
      {{- with .Values.imagePullSecrets }}
      imagePullSecrets:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      serviceAccountName: {{ include "sage.mcpReadonly.fullname" . }}
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
      containers:
        - name: mcp-readonly-server
          image: "{{ .Values.mcpReadonly.image.repository }}:{{ .Values.mcpReadonly.image.tag | default .Chart.AppVersion }}"
          imagePullPolicy: {{ .Values.mcpReadonly.image.pullPolicy }}
          ports:
            - name: http
              containerPort: {{ .Values.mcpReadonly.port }}
          envFrom:
            - configMapRef:
                name: {{ include "sage.fullname" . }}-config
            - secretRef:
                name: {{ include "sage.secretName" . }}
          livenessProbe:
            httpGet:
              path: /healthz
              port: http
          readinessProbe:
            httpGet:
              path: /healthz
              port: http
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          {{- with .Values.mcpReadonly.resources }}
          resources:
            {{- toYaml . | nindent 12 }}
          {{- end }}
      {{- with .Values.mcpReadonly.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.mcpReadonly.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.mcpReadonly.affinity }}
      affinity:
        {{- toYaml . | nindent 8 }}
      {{- end }}
```

- [ ] **Step 3: Write `service-mcp-readonly.yaml`**

```yaml
# Unlike mcp-execute-server's Service, this one legitimately only needs to
# be reachable from inside the cluster (by ai/) — same ClusterIP-only
# reasoning, lower stakes (nothing here can mutate anything), so no
# service.type knob here either, for the same reason.
apiVersion: v1
kind: Service
metadata:
  name: {{ include "sage.mcpReadonly.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "sage.mcpReadonly.selectorLabels" . | nindent 4 }}
spec:
  type: ClusterIP
  ports:
    - name: http
      port: {{ .Values.mcpReadonly.port }}
      targetPort: http
  selector:
    {{- include "sage.mcpReadonly.selectorLabels" . | nindent 4 }}
```

- [ ] **Step 4: Stage**

```bash
git add helm/templates/deployment-mcp-readonly.yaml helm/templates/service-mcp-readonly.yaml helm/templates/rbac-mcp-readonly.yaml
```

---

### Task 22: `ai` Helm Deployment + Secret/ConfigMap wiring

**Files:**
- Create: `helm/templates/deployment-ai.yaml`
- Modify: `helm/templates/secret.yaml`
- Modify: `helm/templates/configmap.yaml`
- Modify: `helm/values.secret.yaml` (gitignored — real local values file, not committed; update it so the next `helm upgrade` has real values for the new keys)

**Interfaces:** none new — this is the last piece that makes Phases A–C actually reachable from a running cluster.

- [ ] **Step 1: Add the new secret keys to `secret.yaml`**

```yaml
{{- if not .Values.secrets.existingSecret }}
apiVersion: v1
kind: Secret
metadata:
  name: {{ include "sage.secretName" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "sage.labels" . | nindent 4 }}
type: Opaque
stringData:
  DATABASE_URL: {{ include "sage.databaseUrl" . | quote }}
  SLACK_BOT_TOKEN: {{ required "slack.botToken is required (set via --set/-f, or set secrets.existingSecret to a pre-existing Secret)" .Values.slack.botToken | quote }}
  SLACK_SIGNING_SECRET: {{ required "slack.signingSecret is required (set via --set/-f, or set secrets.existingSecret to a pre-existing Secret)" .Values.slack.signingSecret | quote }}
  MCP_EXECUTE_TOKEN: {{ required "mcpExecuteToken is required (set via --set/-f, or set secrets.existingSecret to a pre-existing Secret)" .Values.mcpExecuteToken | quote }}
  MCP_READONLY_TOKEN: {{ required "mcpReadonlyToken is required (set via --set/-f, or set secrets.existingSecret to a pre-existing Secret)" .Values.mcpReadonlyToken | quote }}
  DIAGNOSIS_CALLBACK_TOKEN: {{ required "diagnosisCallbackToken is required (set via --set/-f, or set secrets.existingSecret to a pre-existing Secret)" .Values.diagnosisCallbackToken | quote }}
  LLM_API_KEY: {{ required "llm.apiKey is required (set via --set/-f, or set secrets.existingSecret to a pre-existing Secret)" .Values.llm.apiKey | quote }}
{{- end }}
```

- [ ] **Step 2: Add the new non-secret keys to `configmap.yaml`**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "sage.fullname" . }}-config
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "sage.labels" . | nindent 4 }}
data:
  REMEDIATION_MODE: {{ include "sage.validatedMode" . | quote }}
  VERIFY_TIMEOUT_SECONDS: {{ .Values.timeouts.verifySeconds | quote }}
  CORRELATION_WINDOW_SECONDS: {{ .Values.timeouts.correlationWindowSeconds | quote }}
  SLACK_APPROVAL_CHANNEL: {{ .Values.slack.approvalChannel | quote }}
  BACKEND_HTTP_ADDR: {{ printf ":%d" (int .Values.backend.port) | quote }}
  MCP_EXECUTE_ADDR: {{ printf ":%d" (int .Values.mcpExecute.port) | quote }}
  MCP_EXECUTE_URL: {{ include "sage.mcpExecuteUrl" . | quote }}
  MCP_READONLY_ADDR: {{ printf ":%d" (int .Values.mcpReadonly.port) | quote }}
  MCP_READONLY_URL: {{ include "sage.mcpReadonlyUrl" . | quote }}
  NATS_URL: {{ include "sage.natsUrl" . | quote }}
  BACKEND_CALLBACK_URL: {{ include "sage.backendCallbackUrl" . | quote }}
  LLM_PROVIDER: {{ required "llm.provider is required (\"anthropic\", \"openai\", or \"openrouter\")" .Values.llm.provider | quote }}
  LLM_MODEL: {{ required "llm.model is required" .Values.llm.model | quote }}
  # KUBECONFIG is deliberately never set here. Leaving it unset is what makes
  # client-go fall back to in-cluster config automatically (it reads the
  # ServiceAccount token/CA cert Kubernetes already mounts into the pod) —
  # this is the entire mechanism that replaces mounting a kubeconfig Secret.
```

- [ ] **Step 3: Write `deployment-ai.yaml`**

```yaml
# ai/ has no Service — nothing calls into it. It only makes outbound calls
# (NATS, mcp-readonly-server, backend's callback endpoint, the LLM
# provider), so unlike every other component in this chart it needs no
# ServiceAccount/RBAC either — it never talks to the Kubernetes API
# directly, only through mcp-readonly-server.
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "sage.ai.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "sage.ai.selectorLabels" . | nindent 4 }}
spec:
  replicas: {{ .Values.ai.replicaCount }}
  selector:
    matchLabels:
      {{- include "sage.ai.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "sage.ai.selectorLabels" . | nindent 8 }}
      {{- with .Values.ai.podAnnotations }}
      annotations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
    spec:
      {{- with .Values.imagePullSecrets }}
      imagePullSecrets:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
      containers:
        - name: ai
          image: "{{ .Values.ai.image.repository }}:{{ .Values.ai.image.tag | default .Chart.AppVersion }}"
          imagePullPolicy: {{ .Values.ai.image.pullPolicy }}
          envFrom:
            - configMapRef:
                name: {{ include "sage.fullname" . }}-config
            - secretRef:
                name: {{ include "sage.secretName" . }}
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          {{- with .Values.ai.resources }}
          resources:
            {{- toYaml . | nindent 12 }}
          {{- end }}
      {{- with .Values.ai.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.ai.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.ai.affinity }}
      affinity:
        {{- toYaml . | nindent 8 }}
      {{- end }}
```

(No liveness/readiness probe — `ai` exposes no HTTP port at all, unlike every other Deployment in this chart. If that turns out to be operationally inconvenient — e.g. wanting `kubectl get pods` restart visibility on a wedged process — a future phase could add a trivial `/healthz` listener; not needed for this phase.)

- [ ] **Step 4: Update `values.secret.yaml`**

Add these keys (fill in real values — `mcpReadonlyToken`/`diagnosisCallbackToken`: `openssl rand -hex 32` each, same as the existing `mcpExecuteToken` comment already instructs; `llm.apiKey`: a real key from whichever provider `llm.provider` names):

```yaml
mcpReadonlyToken: ""   # generate: openssl rand -hex 32
diagnosisCallbackToken: ""   # generate: openssl rand -hex 32

llm:
  provider: ""   # "anthropic", "openai", or "openrouter"
  model: ""      # e.g. "claude-sonnet-4-5", "gpt-4o", "deepseek/deepseek-chat"
  apiKey: ""

ai:
  image:
    repository: ""   # e.g. ved104/sage-ai on Docker Hub, matching backend.image.repository's pattern
    tag: ""

mcpReadonly:
  image:
    repository: ""   # e.g. ved104/sage-mcp-readonly-server
    tag: ""
```

- [ ] **Step 5: Stage**

```bash
git add helm/templates/deployment-ai.yaml helm/templates/secret.yaml helm/templates/configmap.yaml
```

(`helm/values.secret.yaml` is gitignored — Step 4's edit is local-only, nothing to stage there. Tell the user directly which real values they still need to fill in.)

---

### Task 23: Full chart verification

**Files:** none created/modified — this task only runs checks.

- [ ] **Step 1: Lint**

```bash
cd helm
helm dependency update
helm lint . -f values.secret.yaml
```

Expected: `0 chart(s) failed`.

- [ ] **Step 2: Render and spot-check**

```bash
helm template sage . -f values.secret.yaml > /tmp/sage-rendered.yaml
grep -c "kind: Deployment" /tmp/sage-rendered.yaml   # expect 4: backend, mcp-execute-server, mcp-readonly-server, ai (plus postgresql/nats subchart Deployments/StatefulSets on top)
grep "MCP_READONLY_URL\|NATS_URL\|BACKEND_CALLBACK_URL\|LLM_PROVIDER" /tmp/sage-rendered.yaml
```

Expected: the new ConfigMap keys render with real (non-empty) values, and exactly 4 chart-native Deployments appear (`backend`, `mcp-execute-server`, `mcp-readonly-server`, `ai`) alongside whatever `postgresql`/`nats` subcharts add.

- [ ] **Step 3: Update `helm/templates/NOTES.txt`** if it references the component list (read it first — add a line noting `ai` and `mcp-readonly-server` alongside the existing `backend`/`mcp-execute-server` entries, matching whatever format is already there).

- [ ] **Step 4: Stage**

```bash
git add helm/templates/NOTES.txt
```

---

### Task 24: Update `.env.example`

Every task so far added a new required environment variable (`NATS_URL`, `DIAGNOSIS_CALLBACK_TOKEN` in Task 6; `MCP_READONLY_ADDR`/`MCP_READONLY_TOKEN` in Task 9) or a variable only `ai/` reads (`MCP_READONLY_URL`, `BACKEND_CALLBACK_URL`, `LLM_PROVIDER`, `LLM_API_KEY`, `LLM_MODEL` — see Task 10's `settings.py`). `.env.example` is this repo's single reference for "what do I need to set to run this locally," per this project's standing convention (real values stay placeholders here, never committed for real) — it needs every one of these added or it silently drifts out of sync with `settings.Load()`'s and `ai.settings.Settings`'s actual requirements.

**Files:**
- Modify: `.env.example`

**Interfaces:** none — documentation-only, no code change.

- [ ] **Step 1: Add the new backend variables**

Add after the existing `MCP_EXECUTE_URL` line:

```bash
# NATS (backend/ publishes pending incidents here; ai/ consumes them)
NATS_URL=nats://localhost:4222

# Diagnosis callback (ai/ POSTs its diagnosis back to backend/ here)
DIAGNOSIS_CALLBACK_TOKEN=placeholder-diagnosis-callback-token

# MCP read-only server (ai/'s only path to cluster data — logs/events/pod status, never mutates)
MCP_READONLY_ADDR=:8091
MCP_READONLY_TOKEN=placeholder-readonly-token
```

- [ ] **Step 2: Add the ai/-only variables**

Add a new section at the end of the file:

```bash
# --- ai/ (Python diagnosis service) ---
# ai/ reads these directly (see ai/ai/settings.py) — NATS_URL, DIAGNOSIS_CALLBACK_TOKEN,
# and MCP_READONLY_TOKEN above are shared with backend/ and must be the same values.

# Where ai/ reaches mcp-readonly-server (must match MCP_READONLY_ADDR's port above)
MCP_READONLY_URL=http://localhost:8091

# Where ai/ POSTs diagnoses back (must match BACKEND_HTTP_ADDR's port above)
BACKEND_CALLBACK_URL=http://localhost:8080

# LLM provider for the investigation fallback — "anthropic", "openai", or "openrouter"
LLM_PROVIDER=anthropic
LLM_API_KEY=placeholder-llm-api-key
LLM_MODEL=claude-sonnet-4-5
```

- [ ] **Step 3: Confirm every `settings.go`/`settings.py` field has a matching line**

Run: `grep -oE '"[A-Z_]+"' backend/internal/settings/settings.go | sort -u` and cross-check each against `.env.example` by eye — every env var name `getenv(...)` reads should appear in `.env.example`. Do the same by reading `ai/ai/settings.py`'s field list against `.env.example`'s new "ai/" section.
Expected: no gaps in either direction.

- [ ] **Step 4: Stage**

```bash
git add .env.example
```

**Phase D checkpoint — and the plan's overall Verification (from the spec, §9):**

1. `cd backend && go build ./... && go vet ./... && go test ./...` — PASS
2. `cd ai && pytest -v` — PASS
3. `helm lint helm/ -f helm/values.secret.yaml` — PASS
4. Build and push all three images (`sage-backend`, `sage-mcp-execute-server` unchanged; new `sage-mcp-readonly-server`, `sage-ai`), fill in `helm/values.secret.yaml`'s new tokens/LLM key, `helm upgrade sage helm/ -n sage -f helm/values.secret.yaml`.
5. End-to-end on the real cluster: trigger a `CrashLoopBackOff` test pod (same method used for v1's original smoke test) and confirm via Postgres (`incidents`/`remediation_actions`/`audit_log`) that it flows through `pending_diagnosis` → NATS → ai/ → callback → `diagnosed` → gate → executed → verified, without ever going through the deleted Go analyzer.
6. Trigger a genuinely unmatched failure mode (e.g. deploy a pod referencing a nonexistent image, so `ImagePullError` reaches ai/) and confirm it falls through both rule-based analyzers to the LLM investigation loop, and that the resulting diagnosis's `recommended_action` is either `restart_pod` or `none` — never anything else — and that `mcp-readonly-server`'s pods/events/logs tools were actually called (visible in its own `slog` output).
