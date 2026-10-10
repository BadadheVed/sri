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

// callTool executes a tool call, transparently reconnecting once and
// retrying if the session was lost (see connect's comment on
// DisableStandaloneSSE) — shared by all four remediation methods.
func (c *Client) callTool(ctx context.Context, name string, args map[string]any) (*mcp.CallToolResult, error) {
	params := &mcp.CallToolParams{Name: name, Arguments: args}

	c.mu.Lock()
	session := c.session
	c.mu.Unlock()

	result, err := session.CallTool(ctx, params)
	if err != nil {
		slog.Warn("mcp session call failed, reconnecting and retrying once", "tool", name, "error", err)
		session, err = c.reconnect(ctx)
		if err != nil {
			return nil, fmt.Errorf("%s: session lost and reconnect failed: %w", name, err)
		}
		result, err = session.CallTool(ctx, params)
		if err != nil {
			return nil, err
		}
	}
	if result.IsError {
		return nil, fmt.Errorf("%s: %s", name, toolErrorText(result))
	}
	return result, nil
}

func (c *Client) RestartPod(ctx context.Context, namespace, name string) error {
	_, err := c.callTool(ctx, "restart_pod", map[string]any{"namespace": namespace, "name": name})
	return err
}

func (c *Client) ScaleDeployment(ctx context.Context, namespace, podName string, replicas int32) (string, error) {
	result, err := c.callTool(ctx, "scale_deployment", map[string]any{"namespace": namespace, "name": podName, "replicas": replicas})
	if err != nil {
		return "", err
	}
	return deploymentNameFromResult(result)
}

func (c *Client) PatchResources(ctx context.Context, namespace, podName, memoryLimit, cpuLimit string) (string, error) {
	args := map[string]any{"namespace": namespace, "name": podName}
	if memoryLimit != "" {
		args["memory_limit"] = memoryLimit
	}
	if cpuLimit != "" {
		args["cpu_limit"] = cpuLimit
	}
	result, err := c.callTool(ctx, "patch_resources", args)
	if err != nil {
		return "", err
	}
	return deploymentNameFromResult(result)
}

func (c *Client) RollbackDeployment(ctx context.Context, namespace, podName string) (string, error) {
	result, err := c.callTool(ctx, "rollback_deployment", map[string]any{"namespace": namespace, "name": podName})
	if err != nil {
		return "", err
	}
	return deploymentNameFromResult(result)
}

// deploymentNameFromResult extracts "deployment_name" out of a successful
// CallToolResult's StructuredContent. Verified against the pinned SDK
// (github.com/modelcontextprotocol/go-sdk v1.6.1, mcp/server.go +
// mcp/protocol.go): a ToolHandlerFor's typed Out value is marshaled
// server-side into StructuredContent as a json.RawMessage, and
// CallToolResult has no custom UnmarshalJSON for that field (only for the
// Content interface slice) — so on the client it decodes via plain
// encoding/json into an `any`, which for a JSON object is always
// map[string]any.
func deploymentNameFromResult(result *mcp.CallToolResult) (string, error) {
	sc, ok := result.StructuredContent.(map[string]any)
	if !ok {
		return "", fmt.Errorf("tool result missing structured content")
	}
	name, ok := sc["deployment_name"].(string)
	if !ok || name == "" {
		return "", fmt.Errorf("tool result missing deployment_name")
	}
	return name, nil
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
