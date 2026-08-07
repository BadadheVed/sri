// backend/internal/mcpexecute/client.go
package mcpexecute

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type Client struct {
	mcpClient  *mcp.Client
	endpoint   string
	httpClient *http.Client

	mu      sync.Mutex
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

	c := &Client{mcpClient: mcpClient, endpoint: endpoint, httpClient: httpClient}
	session, err := c.connect(ctx)
	if err != nil {
		return nil, err
	}
	c.session = session
	return c, nil
}

// connect performs one fresh MCP handshake and returns the resulting
// session — used both by NewClient and by RestartPod's reconnect-on-failure
// path below, so a session lost server-side doesn't need a process restart
// to recover.
func (c *Client) connect(ctx context.Context) (*mcp.ClientSession, error) {
	// This client only ever issues request/response tool calls (RestartPod)
	// and never needs server-initiated pushes, so the standalone SSE stream
	// is disabled: by default the SDK keeps a long-lived GET connection open
	// for the life of the session, which would otherwise leak a connection
	// per Client and never resolve on its own.
	transport := &mcp.StreamableClientTransport{
		Endpoint:             c.endpoint,
		HTTPClient:           c.httpClient,
		DisableStandaloneSSE: true,
	}
	return c.mcpClient.Connect(ctx, transport, nil)
}

func (c *Client) RestartPod(ctx context.Context, namespace, name string) error {
	params := &mcp.CallToolParams{
		Name: "restart_pod",
		Arguments: map[string]any{
			"namespace": namespace,
			"name":      name,
		},
	}

	c.mu.Lock()
	session := c.session
	c.mu.Unlock()

	result, err := session.CallTool(ctx, params)
	if err != nil {
		// Observed in production: with DisableStandaloneSSE set, the server
		// can tear down an idle MCP session between sporadic calls (minutes
		// apart) even though SessionTimeout is unset — the client then has no
		// way to know until the next call fails with "session not found".
		// Reconnect once and retry rather than leaving every subsequent
		// RestartPod permanently broken for the rest of the process's life.
		slog.Warn("mcp session call failed, reconnecting and retrying once", "namespace", namespace, "name", name, "error", err)
		session, err = c.reconnect(ctx)
		if err != nil {
			return fmt.Errorf("restart_pod: session lost and reconnect failed: %w", err)
		}
		result, err = session.CallTool(ctx, params)
		if err != nil {
			return err
		}
	}
	// A tool-level failure (e.g. the executor's Kubernetes call failing) is
	// reported by the SDK via CallToolResult.IsError, not as a Go error from
	// CallTool — per the SDK's ToolHandlerFor contract, only transport/protocol
	// failures (auth, routing, etc.) come back as err above. Without this
	// check, a failed restart_pod call would be silently treated as success.
	if result.IsError {
		return fmt.Errorf("restart_pod: %s", toolErrorText(result))
	}
	return nil
}

func (c *Client) reconnect(ctx context.Context) (*mcp.ClientSession, error) {
	session, err := c.connect(ctx)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.session = session
	c.mu.Unlock()
	return session, nil
}

// toolErrorText extracts a human-readable message from a failed
// CallToolResult's content, which the SDK populates with the tool handler's
// error text.
func toolErrorText(result *mcp.CallToolResult) string {
	var parts []string
	for _, c := range result.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			parts = append(parts, tc.Text)
		}
	}
	if len(parts) == 0 {
		return "tool call failed"
	}
	return strings.Join(parts, "; ")
}
