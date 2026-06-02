package stream

import "testing"

func TestBrokerPublishSubscribeUnsubscribe(t *testing.T) {
	broker := NewBroker()
	id, ch := broker.Subscribe()

	broker.Publish("message.captured", map[string]string{"id": "msg-1"})

	event := <-ch
	if event.Type != "message.captured" {
		t.Fatalf("Type=%q", event.Type)
	}
	if string(event.JSON()) != `{"id":"msg-1"}` {
		t.Fatalf("JSON()=%s", event.JSON())
	}

	broker.Unsubscribe(id)
	broker.Publish("message.captured", map[string]string{"id": "msg-2"})
	if _, ok := <-ch; ok {
		t.Fatalf("expected closed channel")
	}
}
