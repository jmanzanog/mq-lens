package jolokia

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestSnapshotParsesBrokerAndDestinations(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		path := r.URL.Path
		var body string
		switch {
		case strings.Contains(path, "destinationType=Queue") || strings.Contains(path, "destinationType%3DQueue"):
			body = `{"status":200,"value":{"org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=ORDER.CREATED":{"Name":"ORDER.CREATED","QueueSize":2,"EnqueueCount":5,"DequeueCount":3,"DispatchCount":3,"ConsumerCount":1,"ProducerCount":1,"ExpiredCount":0}}}`
		case strings.Contains(path, "destinationType=Topic") || strings.Contains(path, "destinationType%3DTopic"):
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

	snapshot, err := NewWithHTTPClient("http://jolokia", "", "", client).Snapshot(context.Background())
	if err == nil {
		t.Fatalf("expected error")
	}
	if snapshot.Available {
		t.Fatalf("snapshot should be unavailable")
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
