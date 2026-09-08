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
	mu          sync.Mutex
	calls       int
	incidentID  string
	diag        analyze.Diagnosis
	receivedCtx context.Context
}

func (f *fakeDiagnosisReceiver) OnDiagnosis(ctx context.Context, incidentID string, diag analyze.Diagnosis) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.incidentID = incidentID
	f.diag = diag
	f.receivedCtx = ctx
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

// TestNewRouter_DiagnosisCallback_ReceiverContextSurvivesClientCancellation
// proves the C2 fix: OnDiagnosis can run for up to VERIFY_TIMEOUT_SECONDS
// inside executeAndVerify's health-check poll, which must not be aborted
// just because the HTTP client (ai/'s httpx.AsyncClient, with a much
// shorter timeout) gives up and disconnects, canceling the request's
// context. If the fix (context.WithoutCancel) were absent, the context
// OnDiagnosis received would already be canceled by the time it ran.
func TestNewRouter_DiagnosisCallback_ReceiverContextSurvivesClientCancellation(t *testing.T) {
	slackClient := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	receiver := &fakeDiagnosisReceiver{}
	router := httpserver.NewRouter(slackClient, store.NewMemoryStore(), receiver, testDiagnosisToken)

	body := `{"failure_mode":"CrashLoopBackOff","recommended_action":"restart_pod","confidence":0.9}`
	req := httptest.NewRequest(http.MethodPost, "/internal/incidents/incident-1/diagnosis", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testDiagnosisToken)

	ctx, cancel := context.WithCancel(req.Context())
	cancel()
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if receiver.calls != 1 {
		t.Fatalf("expected OnDiagnosis called once even with a canceled client context, got %d", receiver.calls)
	}
	if receiver.receivedCtx.Err() != nil {
		t.Fatalf("expected the context OnDiagnosis received to NOT be canceled (context.WithoutCancel should strip client cancellation), got err: %v", receiver.receivedCtx.Err())
	}
}
