// backend/internal/settings/settings.go
package settings

import (
	"fmt"
	"log/slog"
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
// has already exited via os.Exit, so nothing downstream (Postgres
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
		slog.Error("invalid settings", "error", err)
		os.Exit(1)
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
	if s.Mode != gate.ModeAuto && s.Mode != gate.ModeManual {
		missing = append(missing, "REMEDIATION_MODE (invalid value)")
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
