package jolokia

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/jmanzano/mq-lens/internal/domain"
)

func TestSnapshotParsesBrokerAndDestinations(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost {
			t.Fatalf("method=%s, want POST", r.Method)
		}
		if r.Header.Get("Origin") != "http://jolokia" {
			t.Fatalf("Origin=%q, want http://jolokia", r.Header.Get("Origin"))
		}
		request := readRequest(t, r)
		var body string
		switch {
		case strings.Contains(request.MBean, "destinationType=Queue"):
			body = `{"status":200,"value":{"org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=ORDER.CREATED":{"Name":"ORDER.CREATED","QueueSize":2,"EnqueueCount":5,"DequeueCount":3,"DispatchCount":3,"ConsumerCount":1,"ProducerCount":1,"ExpiredCount":0}}}`
		case strings.Contains(request.MBean, "destinationType=Topic"):
			body = `{"status":200,"value":{"org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Topic,destinationName=PAYMENT.EVENTS":{"Name":"PAYMENT.EVENTS","EnqueueCount":7,"ConsumerCount":2}}}`
		default:
			body = `{"status":200,"value":{"org.apache.activemq:type=Broker,brokerName=localhost":{"BrokerName":"localhost","BrokerVersion":"5.19.7","Uptime":"1 hour","MemoryPercentUsage":10,"StorePercentUsage":20,"TempPercentUsage":30}}}`
		}
		return testResponse(http.StatusOK, body), nil
	})}

	snapshot, err := NewWithHTTPClient("http://jolokia", "", "", client).Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error=%v", err)
	}
	if !snapshot.Available {
		t.Fatalf("snapshot should be available")
	}
	if snapshot.BrokerName != "localhost" || snapshot.BrokerVersion != "5.19.7" {
		t.Fatalf("broker fields not parsed: %+v", snapshot)
	}
	if len(snapshot.Queues) != 1 || snapshot.Queues[0].Name != "ORDER.CREATED" || snapshot.Queues[0].QueueSize != 2 {
		t.Fatalf("queues not parsed: %+v", snapshot.Queues)
	}
	if len(snapshot.Topics) != 1 || snapshot.Topics[0].Name != "PAYMENT.EVENTS" {
		t.Fatalf("topics not parsed: %+v", snapshot.Topics)
	}
}

func TestSnapshotReturnsUnavailableOnHTTPError(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return testResponse(http.StatusServiceUnavailable, "nope"), nil
	})}

	c := NewWithHTTPClient("http://jolokia", "", "", client)
	snapshot, err := c.Snapshot(context.Background())
	if err == nil {
		t.Fatalf("expected error")
	}
	assertSnapshotUnavailable(t, snapshot)
}

func assertSnapshotUnavailable(t *testing.T, snapshot domain.BrokerSnapshot) {
	if snapshot.Available {
		t.Fatalf("snapshot should be unavailable")
	}
	if snapshot.Error == "" {
		t.Fatalf("expected error message")
	}
}

type jolokiaRequest struct {
	Type  string `json:"type"`
	MBean string `json:"mbean"`
}

func readRequest(t *testing.T, req *http.Request) jolokiaRequest {
	t.Helper()
	var out jolokiaRequest
	if err := json.NewDecoder(req.Body).Decode(&out); err != nil {
		t.Fatalf("decode request body: %v", err)
	}
	if out.Type != "read" {
		t.Fatalf("request type=%q, want read", out.Type)
	}
	return out
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
