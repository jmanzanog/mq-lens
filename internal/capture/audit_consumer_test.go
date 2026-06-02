package capture_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jmanzano/mq-lens/internal/capture"
	"github.com/jmanzano/mq-lens/internal/config"
	"github.com/jmanzano/mq-lens/internal/stream"
)

func TestAuditConsumer_FailFast(t *testing.T) {
	// Configure short reconnect time to speed up tests
	cfg := config.Config{
		Mode:              "full",
		AuditQueues:       []string{"ALL"},
		STOMPReconnectMin: 1 * time.Millisecond,
		STOMPAddr:         "invalid:1234", // Force connection failure
	}

	events := stream.NewBroker()
	_, eventCh := events.Subscribe()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	consumer := capture.NewAuditConsumer(cfg, nil, events, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	panicked := false
	done := make(chan struct{})

	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
			close(done)
		}()
		consumer.Run(ctx)
	}()

	// Wait for the panic and event
	select {
	case event := <-eventCh:
		if event.Type != "error" {
			t.Errorf("expected topic error, got %s", event.Type)
		}
		if msgMap, ok := event.Data.(map[string]string); ok {
			if msgMap["message"] != "Fail-fast triggered: max STOMP retries reached without successful subscription" {
				t.Errorf("unexpected message payload: %v", msgMap)
			}
		} else {
			t.Errorf("unexpected payload type: %T", event.Data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event")
	}

	<-done

	if !panicked {
		t.Error("expected consumer.Run to panic")
	}
}
