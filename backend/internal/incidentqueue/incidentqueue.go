// backend/internal/incidentqueue/incidentqueue.go
package incidentqueue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"sre-platform/backend/internal/correlate"
)

const StreamName = "SAGE_INCIDENTS"
const PendingSubject = "sage.incidents.pending"

// PendingIncident is the JSON payload published to PendingSubject. ai/'s
// consumer (ai/ai/models.py) must keep its field names in sync with the
// `json` tags here — there is no shared schema, this struct and its Python
// mirror are the contract.
type PendingIncident struct {
	IncidentID string          `json:"incident_id"`
	Namespace  string          `json:"namespace"`
	Kind       string          `json:"kind"`
	Name       string          `json:"name"`
	GroupKey   string          `json:"group_key"`
	Signals    []SignalPayload `json:"signals"`
	FirstSeen  time.Time       `json:"first_seen"`
	LastSeen   time.Time       `json:"last_seen"`
}

type SignalPayload struct {
	Type      string            `json:"type"`
	Severity  string            `json:"severity"`
	Labels    map[string]string `json:"labels"`
	Timestamp time.Time         `json:"timestamp"`
	Raw       string            `json:"raw"`
}

type Client struct {
	nc *nats.Conn
	js jetstream.JetStream
}

// NewClient connects to NATS and ensures StreamName exists (idempotent —
// CreateOrUpdateStream is safe to call on every process start).
func NewClient(ctx context.Context, url string) (*Client, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamName,
		Subjects: []string{PendingSubject},
		Storage:  jetstream.FileStorage,
	}); err != nil {
		nc.Close()
		return nil, err
	}
	return &Client{nc: nc, js: js}, nil
}

func (c *Client) PublishPendingIncident(ctx context.Context, incidentID string, incident correlate.Incident) error {
	payload := PendingIncident{
		IncidentID: incidentID,
		Namespace:  incident.Namespace,
		Kind:       incident.Kind,
		Name:       incident.Name,
		GroupKey:   incident.GroupKey,
		FirstSeen:  incident.FirstSeen,
		LastSeen:   incident.LastSeen,
	}
	for _, s := range incident.Signals {
		payload.Signals = append(payload.Signals, SignalPayload{
			Type: s.Type, Severity: s.Severity, Labels: s.Labels, Timestamp: s.Timestamp, Raw: s.Raw,
		})
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = c.js.Publish(ctx, PendingSubject, data)
	return err
}

func (c *Client) Close() {
	c.nc.Close()
}
