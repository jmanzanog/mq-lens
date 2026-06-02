package app

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/jmanzano/mq-lens/internal/broker/jolokia"
	"github.com/jmanzano/mq-lens/internal/config"
	"github.com/jmanzano/mq-lens/internal/domain"
)

func TestTopologyIncludesAuditEdges(t *testing.T) {
	application, err := New(config.Config{
		DBPath:      filepath.Join(t.TempDir(), "test.db"),
		Mode:        "hybrid",
		AuditPrefix: "LENS.AUDIT.",
		AuditQueues: []string{"ORDER.CREATED"},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New() error=%v", err)
	}
	defer func() { _ = application.Close() }()

	application.mu.Lock()
	application.snapshot = domain.BrokerSnapshot{
		BrokerName:  "localhost",
		CollectedAt: time.Now().UTC(),
		Queues: []domain.DestinationSnapshot{
			{Name: "ORDER.CREATED", Type: domain.DestinationQueue, ConsumerCount: 1},
			{Name: "LENS.AUDIT.ORDER.CREATED", Type: domain.DestinationQueue},
		},
	}
	application.mu.Unlock()

	topology := application.Topology()
	if !hasEdge(topology, "queue:ORDER.CREATED", "queue:LENS.AUDIT.ORDER.CREATED", "audit-copy") {
		t.Fatalf("missing audit edge: %+v", topology.Edges)
	}
	if !hasEdge(topology, "queue:LENS.AUDIT.ORDER.CREATED", "inspector:mq-lens", "observes") {
		t.Fatalf("missing inspector edge: %+v", topology.Edges)
	}
}

func TestTopologyNormalizesPrefixedAuditQueues(t *testing.T) {
	application, err := New(config.Config{
		DBPath:      filepath.Join(t.TempDir(), "test.db"),
		Mode:        "hybrid",
		AuditPrefix: "LENS.AUDIT.",
		AuditQueues: []string{"LENS.AUDIT.ORDER.CREATED"},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New() error=%v", err)
	}
	defer func() { _ = application.Close() }()

	topology := application.Topology()
	if !hasEdge(topology, "queue:ORDER.CREATED", "queue:LENS.AUDIT.ORDER.CREATED", "audit-copy") {
		t.Fatalf("missing normalized audit edge: %+v", topology.Edges)
	}
}

func TestTopologyInfersServiceFromVirtualTopicConsumer(t *testing.T) {
	application, err := New(config.Config{
		DBPath: filepath.Join(t.TempDir(), "test.db"),
		Mode:   "hybrid",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New() error=%v", err)
	}
	defer func() { _ = application.Close() }()

	application.mu.Lock()
	application.snapshot = domain.BrokerSnapshot{
		BrokerName: "localhost",
		Queues: []domain.DestinationSnapshot{
			{Name: "Consumer.my-service.VirtualTopic.my-topic", Type: domain.DestinationQueue, ConsumerCount: 1},
		},
		Topics: []domain.DestinationSnapshot{
			{Name: "VirtualTopic.my-topic", Type: domain.DestinationTopic},
		},
	}
	application.mu.Unlock()

	topology := application.Topology()
	if !hasEdge(topology, "topic:VirtualTopic.my-topic", "queue:Consumer.my-service.VirtualTopic.my-topic", "routes") {
		t.Errorf("missing routes edge")
	}
	if !hasEdge(topology, "queue:Consumer.my-service.VirtualTopic.my-topic", "service:my-service", "consumes") {
		t.Errorf("missing consumes edge")
	}
}

func TestRefreshSnapshotPreservesLastGoodSnapshotOnJolokiaError(t *testing.T) {
	fail := false
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		if fail {
			return testResponse(http.StatusServiceUnavailable, "temporary outage"), nil
		}
		return testResponse(http.StatusOK, `{"status":200,"value":{"org.apache.activemq:type=Broker,brokerName=localhost":{"BrokerName":"localhost"}}}`), nil
	})}

	application, err := New(config.Config{
		DBPath:      filepath.Join(t.TempDir(), "test.db"),
		Mode:        "hybrid",
		AuditPrefix: "LENS.AUDIT.",
		JolokiaURL:  "http://jolokia",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New() error=%v", err)
	}
	defer func() { _ = application.Close() }()
	application.jolokia = jolokia.NewWithHTTPClient("http://jolokia", "", "", client)

	application.refreshSnapshot(context.Background())
	application.mu.Lock()
	application.snapshot.Queues = []domain.DestinationSnapshot{{Name: "ORDER.CREATED", Type: domain.DestinationQueue}}
	application.mu.Unlock()

	fail = true
	application.refreshSnapshot(context.Background())
	snapshot := application.Snapshot()
	if snapshot.Available {
		t.Fatalf("snapshot should be marked unavailable")
	}
	if len(snapshot.Queues) != 1 || snapshot.Queues[0].Name != "ORDER.CREATED" {
		t.Fatalf("last good topology was not preserved: %+v", snapshot.Queues)
	}
	if snapshot.Error == "" {
		t.Fatalf("expected error message")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func testResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Header:     make(http.Header),
	}
}

func hasEdge(topology domain.Topology, source, target, kind string) bool {
	for _, edge := range topology.Edges {
		if edge.Source == source && edge.Target == target && edge.Type == kind {
			return true
		}
	}
	return false
}
