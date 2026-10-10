// backend/tests/incidentqueue/incidentqueue_test.go
package incidentqueue_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"

	"sre-platform/backend/internal/correlate"
	"sre-platform/backend/internal/incidentqueue"
	"sre-platform/backend/internal/signal"
)

func startTestNATS(t *testing.T) string {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	var srv *natsserver.Server
	srv = natstest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

func TestClient_PublishPendingIncident_DeliversToConsumer(t *testing.T) {
	ctx := context.Background()
	url := startTestNATS(t)

	client, err := incidentqueue.NewClient(ctx, url)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	inc := correlate.Incident{
		Namespace: "default", Kind: "Pod", Name: "web-1", GroupKey: "default/Pod/web-1",
		Signals:   []signal.Signal{{Type: "CrashLoopBackOff", Severity: "warning", Raw: "Back-off restarting"}},
		FirstSeen: time.Now(), LastSeen: time.Now(),
	}
	if err := client.PublishPendingIncident(ctx, "incident-1", inc); err != nil {
		t.Fatalf("PublishPendingIncident: %v", err)
	}

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, incidentqueue.StreamName, jetstream.ConsumerConfig{
		Durable: "test-consumer", AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer: %v", err)
	}
	msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	var got incidentqueue.PendingIncident
	count := 0
	for msg := range msgs.Messages() {
		count++
		if err := json.Unmarshal(msg.Data(), &got); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		msg.Ack()
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 message delivered, got %d", count)
	}
	if got.IncidentID != "incident-1" || got.Namespace != "default" || got.GroupKey != "default/Pod/web-1" {
		t.Fatalf("unexpected payload: %+v", got)
	}
	if len(got.Signals) != 1 || got.Signals[0].Type != "CrashLoopBackOff" {
		t.Fatalf("expected 1 CrashLoopBackOff signal in payload, got %+v", got.Signals)
	}
}
