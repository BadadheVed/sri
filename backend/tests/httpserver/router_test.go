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
