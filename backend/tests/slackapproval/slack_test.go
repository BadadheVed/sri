// backend/tests/slackapproval/slack_test.go
package slackapproval_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
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

func TestPostNotification_SendsMessageAndReturnsTimestamp(t *testing.T) {
	var gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat.postMessage" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true,"ts":"1700000000.000200"}`))
	}))
	defer server.Close()

	c := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", server.Client())
	c.APIBaseURL = server.URL

	ts, err := c.PostNotification(t.Context(), slackapproval.NotificationRequest{
		IncidentID: "incident-1", ActionID: "action-1",
		FailureMode: "CrashLoopBackOff", Action: "restart_pod",
		Namespace: "default", Name: "web-1", Outcome: "resolved",
	})
	if err != nil {
		t.Fatalf("PostNotification: %v", err)
	}
	if ts != "1700000000.000200" {
		t.Errorf("expected ts from Slack response, got %q", ts)
	}
	if !strings.Contains(gotBody, "auto-remediated") || !strings.Contains(gotBody, "resolved") {
		t.Errorf("expected notification text to mention auto-remediation and outcome, got body: %s", gotBody)
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

func TestInteractionHandler_RejectsStaleTimestamp(t *testing.T) {
	c := slackapproval.NewClient("xoxb-test", "#sre-approvals", "signing-secret", http.DefaultClient)
	memStore := store.NewMemoryStore()

	body := "payload=invalid"
	// More than 5 minutes in the past, so the timestamp-freshness check
	// should reject the request even though the signature is otherwise
	// correctly computed over that (stale) timestamp and body — this
	// simulates a captured request being replayed later.
	timestamp := strconv.FormatInt(time.Now().Add(-10*time.Minute).Unix(), 10)
	sig := signSlackRequest("signing-secret", timestamp, body)

	req := httptest.NewRequest(http.MethodPost, "/slack/interactions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Slack-Request-Timestamp", timestamp)
	req.Header.Set("X-Slack-Signature", sig)
	rec := httptest.NewRecorder()

	c.InteractionHandler(memStore)(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for stale timestamp, got %d: %s", rec.Code, rec.Body.String())
	}
}
