// backend/tests/pxmetrics/client_test.go
package pxmetrics_test

import (
	"context"
	"testing"

	"sre-platform/backend/internal/pxmetrics"
)

func TestNewClient_RejectsUnknownConnMode(t *testing.T) {
	_, err := pxmetrics.NewClient(context.Background(), pxmetrics.Config{ConnMode: "bogus", VizierAddr: "vizier:443"})
	if err == nil {
		t.Fatal("expected an error for an unknown ConnMode")
	}
}

func TestNewClient_CloudModeRequiresAPIKeyAndClusterID(t *testing.T) {
	_, err := pxmetrics.NewClient(context.Background(), pxmetrics.Config{ConnMode: "cloud", VizierAddr: "cloud.example.com:443"})
	if err == nil {
		t.Fatal("expected an error when cloud mode is missing APIKey/ClusterID")
	}
}
