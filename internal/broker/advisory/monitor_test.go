package advisory

import (
	"testing"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
	"github.com/jmanzano/mq-lens/internal/domain"
)

func TestEventFromMessage(t *testing.T) {
	msg := &gostomp.Message{
		Header: frame.NewHeader(
			"destination", "/queue/ORDER.CREATED",
			"consumer-id", "consumer-1",
		),
		Body: []byte(`{"event":"consumer"}`),
	}

	event := EventFromMessage("ActiveMQ.Advisory.Consumer.Queue.>", msg)
	if event.EventType != "consumer.changed" {
		t.Fatalf("EventType=%q", event.EventType)
	}
	if event.DestinationType != domain.DestinationQueue {
		t.Fatalf("DestinationType=%q", event.DestinationType)
	}
	if event.DestinationName != "ORDER.CREATED" {
		t.Fatalf("DestinationName=%q", event.DestinationName)
	}
	if event.ConsumerID != "consumer-1" {
		t.Fatalf("ConsumerID=%q", event.ConsumerID)
	}
	if event.BodyPreview == "" {
		t.Fatalf("expected body preview")
	}
}

func TestEventFromMessageTruncatesPreview(t *testing.T) {
	body := make([]byte, 3000)
	for i := range body {
		body[i] = 'x'
	}

	event := EventFromMessage("ActiveMQ.Advisory.Producer.Topic.>", &gostomp.Message{
		Header: frame.NewHeader("destination", "/topic/PAYMENT.EVENTS"),
		Body:   body,
	})
	if len(event.BodyPreview) != 2048 {
		t.Fatalf("BodyPreview length=%d", len(event.BodyPreview))
	}
	if event.DestinationType != domain.DestinationTopic {
		t.Fatalf("DestinationType=%q", event.DestinationType)
	}
}
