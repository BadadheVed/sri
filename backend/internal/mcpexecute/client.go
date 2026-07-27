// backend/internal/mcpexecute/client.go
package mcpexecute

import (
	"context"
	"fmt"
	"net/http"
	"strings"

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
	// This client only ever issues request/response tool calls (RestartPod)
	// and never needs server-initiated pushes, so the standalone SSE stream
	// is disabled: by default the SDK keeps a long-lived GET connection open
	// for the life of the session, which would otherwise leak a connection
	// per Client and never resolve on its own.
	transport := &mcp.StreamableClientTransport{
		Endpoint:             endpoint,
		HTTPClient:           httpClient,
		DisableStandaloneSSE: true,
	}
	session, err := mcpClient.Connect(ctx, transport, nil)
	if err != nil {
		return nil, err
	}
	return &Client{session: session}, nil
}

func (c *Client) RestartPod(ctx context.Context, namespace, name string) error {
	result, err := c.session.CallTool(ctx, &mcp.CallToolParams{
		Name: "restart_pod",
		Arguments: map[string]any{
			"namespace": namespace,
			"name":      name,
		},
	})
	if err != nil {
		return err
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
