// backend/internal/metricsagg/noopsource.go
package metricsagg

import (
	"context"

	"sre-platform/backend/internal/histogramquantile"
)

// NoopSource is a placeholder Source that reports zero series on every
// poll. It exists so main.go can wire up the full Aggregator -> Sink ->
// WebSocket-hub pipeline today, before a real Beyla-scraping Source
// exists — swapping it out later for the real implementation is a
// one-line change in main.go; nothing else changes.
type NoopSource struct{}

func (NoopSource) Poll(ctx context.Context) (map[SeriesKey]histogramquantile.Sample, error) {
	return map[SeriesKey]histogramquantile.Sample{}, nil
}
