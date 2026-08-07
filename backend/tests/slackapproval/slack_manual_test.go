package slackapproval_test

import (
	"context"
	"net/http"
	"os"
	"testing"

	"sre-platform/backend/internal/slackapproval"
)

// TestPostApproval_RealSlack_ManualVerification is a manual credentials
// check, not part of the regular test suite — it posts a real message to a
// real Slack workspace using live credentials, so it only runs when they're
// explicitly provided via environment variables. Everything else in this
// package tests against a fake in-process server; this is the one place
// that deliberately talks to the real Slack API, to verify a bot
// token/channel/invite setup actually works end to end.
//
// Run it with:
//
//	SLACK_BOT_TOKEN=xoxb-... SLACK_APPROVAL_CHANNEL=#your-channel \
//	  go test ./tests/slackapproval/... -run TestPostApproval_RealSlack_ManualVerification -v
func TestPostApproval_RealSlack_ManualVerification(t *testing.T) {
	token := os.Getenv("SLACK_BOT_TOKEN")
	channel := os.Getenv("SLACK_APPROVAL_CHANNEL")
	if token == "" || channel == "" {
		t.Skip("set SLACK_BOT_TOKEN and SLACK_APPROVAL_CHANNEL to run this manual Slack verification test")
	}

	client := slackapproval.NewClient(token, channel, "unused-for-this-test", http.DefaultClient)

	ts, err := client.PostApproval(context.Background(), slackapproval.ApprovalRequest{
		IncidentID:  "manual-test-incident",
		ActionID:    "manual-test-action",
		FailureMode: "ManualVerification",
		Action:      "none — this is only a credentials smoke test",
		Namespace:   "test",
		Name:        "slack-credentials-check",
	})
	if err != nil {
		t.Fatalf(
			"PostApproval failed — check that SLACK_BOT_TOKEN is valid, has the chat:write scope, "+
				"and the bot has been invited to %s (/invite @<your-app-name>): %v",
			channel, err,
		)
	}

	t.Logf("Message posted successfully to %s (Slack ts=%s) — check your Slack channel now.", channel, ts)
}
