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

// TestSettings_Validate_FailsWhenModeInvalid guards against
// settings.Load casting REMEDIATION_MODE straight to gate.Mode with no
// validation: an operator typo (or any value that isn't exactly "auto" or
// "manual") must fail fast at startup rather than reach gate.Evaluate.
func TestSettings_Validate_FailsWhenModeInvalid(t *testing.T) {
	invalidModes := []gate.Mode{"garbage", "Manual", ""}
	for _, mode := range invalidModes {
		t.Run(string(mode), func(t *testing.T) {
			s := validSettings()
			s.Mode = mode

			err := s.Validate()
			if err == nil {
				t.Fatalf("expected an error when Mode is %q", mode)
			}
			if !contains(err.Error(), "REMEDIATION_MODE") {
				t.Errorf("expected error to mention REMEDIATION_MODE, got: %v", err)
			}
		})
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
