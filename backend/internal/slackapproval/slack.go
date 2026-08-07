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
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"sre-platform/backend/internal/store"
)

// maxRequestTimestampSkew is the maximum allowed difference between a Slack
// webhook request's timestamp and the current time. Slack recommends
// rejecting requests outside this window to prevent replay attacks:
// https://api.slack.com/authentication/verifying-requests-from-slack
const maxRequestTimestampSkew = 5 * time.Minute

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
	OK    bool   `json:"ok"`
	TS    string `json:"ts"`
	Error string `json:"error"`
}

func (c *Client) PostApproval(ctx context.Context, req ApprovalRequest) (string, error) {
	text := fmt.Sprintf(
		"Remediation approval needed\nIncident: %s/%s in %s\nDiagnosis: %s\nProposed action: %s\nAction ID: %s",
		req.Namespace, req.Name, req.Namespace, req.FailureMode, req.Action, req.ActionID,
	)
	return c.postMessage(ctx, text)
}

// NotificationRequest describes a non-blocking, FYI-only Slack message —
// unlike ApprovalRequest, nothing waits on a human response to this one. It's
// posted after auto mode has already executed and verified a remediation, so
// there's a visible record even when no approval round-trip ever happens.
type NotificationRequest struct {
	IncidentID  string
	ActionID    string
	FailureMode string
	Action      string
	Namespace   string
	Name        string
	Outcome     string
}

func (c *Client) PostNotification(ctx context.Context, req NotificationRequest) (string, error) {
	text := fmt.Sprintf(
		"SAGE auto-remediated an incident (no approval required)\nIncident: %s/%s in %s\nDiagnosis: %s\nAction taken: %s\nOutcome: %s\nAction ID: %s",
		req.Namespace, req.Name, req.Namespace, req.FailureMode, req.Action, req.Outcome, req.ActionID,
	)
	return c.postMessage(ctx, text)
}

// postMessage is the shared chat.postMessage call behind both PostApproval
// and PostNotification — the two differ only in the text they send.
func (c *Client) postMessage(ctx context.Context, text string) (string, error) {
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
		return "", fmt.Errorf("slack chat.postMessage failed: %s", parsed.Error)
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

// isTimestampFresh reports whether the given Slack request timestamp (Unix
// seconds, encoded as a string, per the X-Slack-Request-Timestamp header) is
// within maxRequestTimestampSkew of the current time. A stale or unparsable
// timestamp is treated as invalid; this guards against replay of a captured
// (signature, timestamp, body) tuple, since the signature alone never
// expires. See https://api.slack.com/authentication/verifying-requests-from-slack
func (c *Client) isTimestampFresh(timestamp string) bool {
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return false
	}
	diff := time.Now().Unix() - ts
	if diff < 0 {
		diff = -diff
	}
	return time.Duration(diff)*time.Second <= maxRequestTimestampSkew
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
			slog.Warn("slack interaction: failed to read body", "error", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		timestamp := r.Header.Get("X-Slack-Request-Timestamp")
		signature := r.Header.Get("X-Slack-Signature")
		// Check timestamp freshness before the more expensive HMAC
		// comparison, and fail both cases identically so a caller can't
		// distinguish "stale timestamp" from "bad signature".
		if !c.isTimestampFresh(timestamp) || !c.VerifySignature(timestamp, signature, bodyBytes) {
			slog.Warn("slack interaction: rejected invalid signature or stale timestamp", "remote_addr", r.RemoteAddr)
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}

		form, err := url.ParseQuery(string(bodyBytes))
		if err != nil {
			slog.Warn("slack interaction: failed to parse form body", "error", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		var payload interactionPayload
		if err := json.Unmarshal([]byte(form.Get("payload")), &payload); err != nil {
			slog.Warn("slack interaction: failed to unmarshal payload", "error", err)
			http.Error(w, "bad payload", http.StatusBadRequest)
			return
		}
		if len(payload.Actions) == 0 {
			slog.Warn("slack interaction: payload had no actions")
			http.Error(w, "no action in payload", http.StatusBadRequest)
			return
		}

		actionID := payload.Actions[0].Value
		decision := "denied"
		if payload.Actions[0].ActionID == "approve" {
			decision = "approved"
		}

		if err := s.RecordApprovalDecision(r.Context(), actionID, payload.User.Username, decision); err != nil {
			slog.Error("slack interaction: RecordApprovalDecision failed", "action_id", actionID, "error", err)
			http.Error(w, "failed to record decision", http.StatusInternalServerError)
			return
		}
		slog.Info("slack interaction: approval decision recorded", "action_id", actionID, "decision", decision, "user", payload.User.Username)

		w.WriteHeader(http.StatusOK)
	}
}
