// backend/tests/metricsagg/noopsource_test.go
package metricsagg_test

import (
	"context"
	"testing"

	"sre-platform/backend/internal/metricsagg"
)

func TestNoopSource_PollReturnsEmptyMapAndNoError(t *testing.T) {
	got, err := (metricsagg.NoopSource{}).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Poll() = %v, want empty map", got)
	}
}
