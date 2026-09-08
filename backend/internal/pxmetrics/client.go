// backend/internal/pxmetrics/client.go
package pxmetrics

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"px.dev/pxapi"
	"px.dev/pxapi/types"
)

// Config selects how this package connects to Pixie's Vizier. "cloud" is
// used for both hosted Community Cloud and self-hosted Pixie Cloud — from
// the client's perspective they're the same wire protocol, just a different
// VizierAddr (see docs/superpowers/specs/2026-08-12-pixie-observability-design.md §3).
type Config struct {
	ConnMode              string // "direct" | "cloud"
	VizierAddr            string
	APIKey                string // cloud: required. direct: optional bearer token — if empty, connects unauthenticated
	ClusterID             string // required when ConnMode == "cloud"
	InsecureSkipTLSVerify bool   // cloud only — for self-hosted Pixie Cloud using a self-signed/local CA (e.g. mkcert) this process doesn't trust by default
}

type Client struct {
	vizier *pxapi.VizierClient
}

// NewClient dials Vizier per cfg. queryTimeout bounds every subsequent
// ExecuteScript call — new relative to introspect.go's client-go calls,
// because a hung Pixie call would otherwise stall the whole ReAct loop.
const queryTimeout = 20 * time.Second

func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	var opts []pxapi.ClientOption
	switch cfg.ConnMode {
	case "direct":
		opts = []pxapi.ClientOption{pxapi.WithDirectAddr(cfg.VizierAddr)}
		if cfg.APIKey != "" {
			opts = append(opts, pxapi.WithBearerAuth(cfg.APIKey))
		} else {
			opts = append(opts, pxapi.WithDirectCredsInsecure())
		}
	case "cloud":
		if cfg.APIKey == "" || cfg.ClusterID == "" {
			return nil, fmt.Errorf("pxmetrics: cloud connMode requires both APIKey and ClusterID")
		}
		opts = []pxapi.ClientOption{pxapi.WithCloudAddr(cfg.VizierAddr), pxapi.WithAPIKey(cfg.APIKey)}
		if cfg.InsecureSkipTLSVerify {
			// pxapi.WithDisableTLSVerification's real behavior, confirmed by
			// reading px.dev/pxapi@v0.5.0/opts.go directly (go doc's
			// rendered signature — func WithDisableTLSVerification(cloudAddr
			// string) ClientOption — doesn't show this): it does NOT
			// unconditionally disable certificate verification. It only
			// sets disableTLSVerification=true when (a) cloudAddr contains
			// the literal substring "cluster.local" AND (b) the
			// PX_DISABLE_TLS process env var is exactly "1"; otherwise it's
			// a no-op (verification stays on), and if cloudAddr contains
			// "cluster.local" while PX_DISABLE_TLS isn't "1" it calls
			// log.Fatalf, hard-crashing the process. We set PX_DISABLE_TLS=1
			// ourselves so that gate is always satisfied and the Fatalf
			// branch can never trigger.
			//
			// KNOWN LIMITATION: this means InsecureSkipTLSVerify only
			// actually takes effect when VizierAddr is a
			// "*cluster.local*"-style internal address. For
			// helm/README.md's documented self-hosted Pixie Cloud setup
			// (mkcert against a real external-looking domain, not
			// cluster.local), this option is a silent no-op and TLS
			// verification stays on — pxapi v0.5.0 exposes no general
			// "skip verification for any address" option (confirmed: the
			// full ClientOption list is WithAPIKey, WithBearerAuth,
			// WithCloudAddr, WithDirectAddr, WithDirectCredsInsecure,
			// WithDisableTLSVerification, WithE2EEncryption — no custom
			// CA/TLS config or grpc.DialOption escape hatch exists). See
			// final-review-fix-report.md Fix 5 for the full go doc /
			// source-reading transcript.
			os.Setenv("PX_DISABLE_TLS", "1")
			opts = append(opts, pxapi.WithDisableTLSVerification(cfg.VizierAddr))
		}
	default:
		return nil, fmt.Errorf("pxmetrics: unknown ConnMode %q: must be \"direct\" or \"cloud\"", cfg.ConnMode)
	}

	client, err := pxapi.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("pxmetrics: connecting to Pixie: %w", err)
	}
	vizier, err := client.NewVizierClient(ctx, cfg.ClusterID)
	if err != nil {
		return nil, fmt.Errorf("pxmetrics: creating Vizier client: %w", err)
	}
	return &Client{vizier: vizier}, nil
}

type CPUSample struct {
	WindowStart time.Time `json:"window_start"`
	CPUCores    float64   `json:"cpu_cores"`
}

type TrafficSample struct {
	WindowStart    time.Time `json:"window_start"`
	RequestsPerSec float64   `json:"requests_per_sec"`
	ErrorsPerSec   float64   `json:"errors_per_sec"`
	LatencyP50Ms   float64   `json:"latency_p50_ms"`
	LatencyP90Ms   float64   `json:"latency_p90_ms"`
	LatencyP99Ms   float64   `json:"latency_p99_ms"`
}

// maxSamples bounds how many window buckets a query can return — a large
// lookback shouldn't be able to blow up the LLM's context with an
// unbounded time series.
const maxSamples = 20

// maxLookback bounds how far back a query can look — prevents a
// pathological LLM-supplied value from producing a huge lookback that
// would otherwise force silent sample truncation, and guards against the
// int64-nanosecond-overflow risk of multiplying an unbounded
// LookbackSeconds into a time.Duration in main.go.
const maxLookback = 24 * time.Hour

// windowSecondsFor picks a bucket width wide enough that the full lookback
// fits within maxSamples buckets, so the two query methods below don't
// have to silently drop data the way a fixed 60s window would for any
// lookback beyond maxSamples minutes. Stays at the original 60s default
// for any lookback that already fits (≤ 20 minutes), so short-lookback
// callers see identical behavior to before this fix.
func windowSecondsFor(lookback time.Duration) int {
	w := int(lookback.Seconds()) / maxSamples
	if w < 60 {
		return 60
	}
	return w
}

// columnRecordHandler adapts pxapi's per-record callback style into a
// simple "read named columns as strings" interface, using types.Datum's
// confirmed String() method rather than its concrete per-type struct
// fields. Confirmed live via `go doc px.dev/pxapi/types Record`/`Datum`
// against the installed px.dev/pxapi@v0.5.0 (Task 2 Step 1): GetDatum(name
// string) types.Datum returns a literal nil (a true nil interface, not a
// wrapped nil pointer) when the column isn't found, so the `d == nil` check
// below is safe.
type columnRecordHandler struct {
	onRecord func(col func(name string) string) error
	err      error
}

func (h *columnRecordHandler) HandleInit(ctx context.Context, metadata types.TableMetadata) error {
	return nil
}
func (h *columnRecordHandler) HandleRecord(ctx context.Context, record *types.Record) error {
	err := h.onRecord(func(name string) string {
		d := record.GetDatum(name)
		if d == nil {
			return ""
		}
		return d.String()
	})
	if err != nil {
		h.err = err
	}
	return err
}
func (h *columnRecordHandler) HandleDone(ctx context.Context) error { return nil }

type singleHandlerMuxer struct {
	handler pxapi.TableRecordHandler
}

func (m *singleHandlerMuxer) AcceptTable(ctx context.Context, metadata types.TableMetadata) (pxapi.TableRecordHandler, error) {
	return m.handler, nil
}

// pixieTimeLayout is the format px.dev/pxapi@v0.5.0's Time64NSValue.String()
// actually emits, confirmed by reading types/types.go directly (Task 2 Step
// 1 discovery — go doc's rendered signatures don't show String()'s method
// body): Time64NSValue.ScanInt64 stores the wire int64-nanosecond count as
// time.Unix(0, data) (Local location), and String() returns
// fmt.Sprintf("%v", v.val), which for a time.Time value is Go's default
// t.Format("2006-01-02 15:04:05.999999999 -0700 MST"). This is neither of
// the two formats guessed during design (raw integer nanoseconds,
// RFC3339Nano) — see task-2-report.md for the full go doc / source
// transcript.
const pixieTimeLayout = "2006-01-02 15:04:05.999999999 -0700 MST"

// parseTimeDatum parses a Time64NSValue's String() output per pixieTimeLayout.
// Normalized to UTC — pxapi's own String() carries the backend process's
// Local zone (via time.Unix(0, data) with no location conversion), which
// would otherwise make WindowStart's zone offset vary by deployment.
func parseTimeDatum(s string) (time.Time, error) {
	t, err := time.Parse(pixieTimeLayout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("pxmetrics: unrecognized time datum format: %q: %w", s, err)
	}
	t = t.UTC()
	return t, nil
}

// runCollectingScript executes script against Vizier, calling onRecord once
// per result row (bounded by maxSamples, checked by the caller) until the
// query completes.
func (c *Client) runCollectingScript(ctx context.Context, script string, onRecord func(col func(name string) string) error) error {
	handler := &columnRecordHandler{onRecord: onRecord}
	muxer := &singleHandlerMuxer{handler: handler}
	results, err := c.vizier.ExecuteScript(ctx, script, muxer)
	if err != nil {
		return fmt.Errorf("pxmetrics: ExecuteScript: %w", err)
	}
	if err := results.Stream(); err != nil {
		results.Close()
		return fmt.Errorf("pxmetrics: streaming results: %w", err)
	}
	results.Close()
	return handler.err
}

// GetPodCPUUsage's live behavior (does ExecuteScript actually return the
// time_/cpu_cores columns BuildPodCPUScript's px.display names, does
// parseTimeDatum's format hold) is not verifiable without a real Vizier —
// see the Prerequisite section. Treat running this against a real Vizier as
// a required manual step before trusting it in production, same precedent
// introspect_test.go already sets for GetPodLogs.
func (c *Client) GetPodCPUUsage(ctx context.Context, namespace, name string, lookback time.Duration) ([]CPUSample, bool, error) {
	if lookback <= 0 {
		return nil, false, fmt.Errorf("pxmetrics: lookback must be positive, got %s", lookback)
	}
	if lookback > maxLookback {
		lookback = maxLookback
	}
	script, err := BuildPodCPUScript(namespace, name, lookback, windowSecondsFor(lookback))
	if err != nil {
		return nil, false, err
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	var samples []CPUSample
	var truncated bool
	err = c.runCollectingScript(ctx, script, func(col func(string) string) error {
		if len(samples) >= maxSamples {
			truncated = true
			return nil
		}
		ts, err := parseTimeDatum(col("time_"))
		if err != nil {
			return err
		}
		cpu, err := strconv.ParseFloat(col("cpu_cores"), 64)
		if err != nil {
			return fmt.Errorf("pxmetrics: parsing cpu_cores: %w", err)
		}
		samples = append(samples, CPUSample{WindowStart: ts, CPUCores: cpu})
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return samples, truncated, nil
}

// GetPodTrafficStats — same live-verification caveat as GetPodCPUUsage.
func (c *Client) GetPodTrafficStats(ctx context.Context, namespace, name string, lookback time.Duration) ([]TrafficSample, bool, error) {
	if lookback <= 0 {
		return nil, false, fmt.Errorf("pxmetrics: lookback must be positive, got %s", lookback)
	}
	if lookback > maxLookback {
		lookback = maxLookback
	}
	script, err := BuildPodTrafficScript(namespace, name, lookback, windowSecondsFor(lookback))
	if err != nil {
		return nil, false, err
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	parseMs := func(col func(string) string, name string) (float64, error) {
		// latency_p50/p90/p99 are px.DurationNanos(...) values — confirmed
		// via px.dev/pxapi@v0.5.0's vizierpb.DataType enum (Task 2 Step 1)
		// to have no separate DURATION64NS wire type; Duration is carried
		// as DataType_INT64 with SemanticType=ST_DURATION_NS, so these
		// decode as Int64Value and stringify as a plain integer-nanosecond
		// count. Convert to milliseconds for the output struct.
		ns, err := strconv.ParseInt(col(name), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("pxmetrics: parsing %s: %w", name, err)
		}
		return float64(ns) / 1e6, nil
	}

	var samples []TrafficSample
	var truncated bool
	err = c.runCollectingScript(ctx, script, func(col func(string) string) error {
		if len(samples) >= maxSamples {
			truncated = true
			return nil
		}
		ts, err := parseTimeDatum(col("time_"))
		if err != nil {
			return err
		}
		rps, err := strconv.ParseFloat(col("requests_per_s"), 64)
		if err != nil {
			return fmt.Errorf("pxmetrics: parsing requests_per_s: %w", err)
		}
		eps, err := strconv.ParseFloat(col("errors_per_s"), 64)
		if err != nil {
			return fmt.Errorf("pxmetrics: parsing errors_per_s: %w", err)
		}
		p50, err := parseMs(col, "latency_p50")
		if err != nil {
			return err
		}
		p90, err := parseMs(col, "latency_p90")
		if err != nil {
			return err
		}
		p99, err := parseMs(col, "latency_p99")
		if err != nil {
			return err
		}
		samples = append(samples, TrafficSample{
			WindowStart: ts, RequestsPerSec: rps, ErrorsPerSec: eps,
			LatencyP50Ms: p50, LatencyP90Ms: p90, LatencyP99Ms: p99,
		})
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return samples, truncated, nil
}
