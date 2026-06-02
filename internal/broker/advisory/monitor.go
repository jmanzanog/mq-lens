package advisory

import (
	"context"
	"log/slog"
	"strings"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/google/uuid"
	brokerstomp "github.com/jmanzano/mq-lens/internal/broker/stomp"
	"github.com/jmanzano/mq-lens/internal/config"
	"github.com/jmanzano/mq-lens/internal/domain"
	"github.com/jmanzano/mq-lens/internal/stream"
)

type Monitor struct {
	cfg    config.Config
	events *stream.Broker
	logger *slog.Logger
}

func New(cfg config.Config, events *stream.Broker, logger *slog.Logger) *Monitor {
	return &Monitor{cfg: cfg, events: events, logger: logger}
}

func (m *Monitor) Run(ctx context.Context) {
	if !m.cfg.AdvisoryEnabled || m.cfg.Mode == "audit" || len(m.cfg.AdvisoryTopics) == 0 {
		return
	}
	backoff := m.cfg.STOMPReconnectMin
	for {
		if ctx.Err() != nil {
			return
		}
		connected, err := m.runOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if connected {
			backoff = m.cfg.STOMPReconnectMin
		}
		m.logger.Warn("advisory monitor disconnected", "error", err, "retryIn", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > m.cfg.STOMPReconnectMax {
			backoff = m.cfg.STOMPReconnectMax
		}
	}
}

func (m *Monitor) runOnce(ctx context.Context) (bool, error) {
	conn, err := gostomp.Dial("tcp", m.cfg.STOMPAddr, gostomp.ConnOpt.Login(m.cfg.STOMPUser, m.cfg.STOMPPassword))
	if err != nil {
		return false, err
	}
	defer conn.Disconnect()

	for _, topic := range m.cfg.AdvisoryTopics {
		destination := brokerstomp.STOMPDestination(domain.DestinationTopic, topic)
		sub, err := conn.Subscribe(destination, gostomp.AckAuto)
		if err != nil {
			return true, err
		}
		go m.consume(ctx, sub, topic)
		m.logger.Info("subscribed advisory topic", "destination", destination)
	}

	<-ctx.Done()
	return true, ctx.Err()
}

func (m *Monitor) consume(ctx context.Context, sub *gostomp.Subscription, topic string) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-sub.C:
			if msg == nil {
				return
			}
			if msg.Err != nil {
				m.logger.Warn("advisory message error", "error", msg.Err)
				time.Sleep(250 * time.Millisecond)
				continue
			}
			event := EventFromMessage(topic, msg)
			m.events.Publish("topology.changed", event)
		}
	}
}

func EventFromMessage(topic string, msg *gostomp.Message) domain.TopologyEvent {
	headers := map[string]string{}
	if msg.Header != nil {
		for i := 0; i < msg.Header.Len(); i++ {
			key, value := msg.Header.GetAt(i)
			headers[key] = value
		}
	}
	return domain.TopologyEvent{
		ID:              uuid.NewString(),
		EventAt:         time.Now().UTC(),
		EventType:       eventType(topic),
		DestinationType: destinationType(topic),
		DestinationName: destinationName(topic, headers),
		ClientID:        firstHeader(headers, "client-id", "JMSActiveMQBrokerInTime"),
		ConnectionID:    firstHeader(headers, "connection-id", "JMSActiveMQBrokerPath"),
		ProducerID:      firstHeader(headers, "producer-id"),
		ConsumerID:      firstHeader(headers, "consumer-id", "subscription"),
		Headers:         headers,
		BodyPreview:     preview(msg.Body, 2048),
	}
}

func eventType(topic string) string {
	switch {
	case strings.Contains(topic, ".Consumer."):
		return "consumer.changed"
	case strings.Contains(topic, ".Producer."):
		return "producer.changed"
	case strings.Contains(topic, ".Connection"):
		return "connection.changed"
	case strings.Contains(topic, ".Queue"):
		return "queue.changed"
	case strings.Contains(topic, ".Topic"):
		return "topic.changed"
	default:
		return "topology.changed"
	}
}

func destinationType(topic string) domain.DestinationType {
	if strings.Contains(topic, ".Topic") {
		return domain.DestinationTopic
	}
	if strings.Contains(topic, ".Queue") {
		return domain.DestinationQueue
	}
	return ""
}

func destinationName(topic string, headers map[string]string) string {
	for _, key := range []string{"destination", "physicalName"} {
		if value := headers[key]; value != "" {
			return strings.TrimPrefix(strings.TrimPrefix(value, "/queue/"), "/topic/")
		}
	}
	if idx := strings.LastIndex(topic, ".Queue."); idx >= 0 {
		return strings.TrimSuffix(topic[idx+len(".Queue."):], ">")
	}
	if idx := strings.LastIndex(topic, ".Topic."); idx >= 0 {
		return strings.TrimSuffix(topic[idx+len(".Topic."):], ">")
	}
	return ""
}

func firstHeader(headers map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := headers[key]; value != "" {
			return value
		}
	}
	return ""
}

func preview(body []byte, limit int) string {
	if len(body) > limit {
		body = body[:limit]
	}
	return string(body)
}
