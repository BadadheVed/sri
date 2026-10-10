// backend/internal/metricsagg/metricsagg.go
package metricsagg

import (
	"context"

	"sre-platform/backend/internal/histogramquantile"
)

// SeriesKey identifies one Beyla-exposed HTTP route's histogram series.
type SeriesKey struct {
	Namespace string
	Service   string
	Route     string
	Method    string
}

// Source produces one poll's worth of cumulative histogram samples, keyed
// by series. A future Beyla-scraping implementation satisfies this; this
// package has zero knowledge of Beyla, Prometheus text format, or HTTP.
type Source interface {
	Poll(ctx context.Context) (map[SeriesKey]histogramquantile.Sample, error)
}

// Sink receives one series' newly aggregated Result. httpserver.MetricsHub
// implements this in production.
type Sink interface {
	Publish(key SeriesKey, result histogramquantile.Result)
}
