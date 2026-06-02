package stream

import (
	"encoding/json"
	"sync"
)

type Event struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

type Broker struct {
	mu      sync.Mutex
	nextID  int
	clients map[int]chan Event
}

func NewBroker() *Broker {
	return &Broker{clients: make(map[int]chan Event)}
}

func (b *Broker) Subscribe() (int, <-chan Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := b.nextID
	b.nextID++
	ch := make(chan Event, 32)
	b.clients[id] = ch
	return id, ch
}

func (b *Broker) Unsubscribe(id int) {
	b.mu.Lock()
	ch, ok := b.clients[id]
	if ok {
		delete(b.clients, id)
		close(ch)
	}
	b.mu.Unlock()
}

func (b *Broker) Publish(eventType string, data any) {
	b.mu.Lock()
	defer b.mu.Unlock()
	event := Event{Type: eventType, Data: data}
	for _, ch := range b.clients {
		select {
		case ch <- event:
		default:
		}
	}
}

func (e Event) JSON() []byte {
	data, err := json.Marshal(e.Data)
	if err != nil {
		return []byte(`{"error":"event encoding failed"}`)
	}
	return data
}
