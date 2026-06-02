package capture

import (
	"context"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/google/uuid"
	brokerstomp "github.com/jmanzano/mq-lens/internal/broker/stomp"
	"github.com/jmanzano/mq-lens/internal/config"
	"github.com/jmanzano/mq-lens/internal/domain"
	"github.com/jmanzano/mq-lens/internal/stream"
)

type Store interface {
	SaveMessage(context.Context, domain.CapturedMessage) error
}

type AuditConsumer struct {
	cfg       config.Config
	store     Store
	events    *stream.Broker
	logger    *slog.Logger
	connected atomic.Bool
}

func NewAuditConsumer(cfg config.Config, store Store, events *stream.Broker, logger *slog.Logger) *AuditConsumer {
	return &AuditConsumer{
		cfg:    cfg,
		store:  store,
		events: events,
		logger: logger,
	}
}

func (c *AuditConsumer) Connected() bool {
	return c.connected.Load()
}

func (c *AuditConsumer) Run(ctx context.Context) {
	if c.cfg.Mode == "observe" || len(c.cfg.AuditQueues) == 0 {
		c.setConnected(false)
		return
	}
	backoff := c.cfg.STOMPReconnectMin
	retries := 0
	const maxInitialRetries = 10

	for {
		if ctx.Err() != nil {
			return
		}
		connected, err := c.runOnce(ctx)
		c.setConnected(false)
		if ctx.Err() != nil {
			return
		}
		if connected {
			backoff = c.cfg.STOMPReconnectMin
			retries = 0 // Reset retries on successful connection
		} else {
			retries++
			if retries > maxInitialRetries {
				c.logger.Error("Fail-fast triggered: max STOMP retries reached without successful subscription")
				c.events.Publish("error", map[string]string{"message": "Fail-fast triggered: max STOMP retries reached without successful subscription"})
				time.Sleep(100 * time.Millisecond) // Allow event to be flushed
				panic("Fail-fast triggered: max STOMP retries reached without successful subscription")
			}
		}
		c.logger.Warn("stomp disconnected", "error", err, "retryIn", backoff, "retryCount", retries)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > c.cfg.STOMPReconnectMax {
			backoff = c.cfg.STOMPReconnectMax
		}
	}
}

func (c *AuditConsumer) runOnce(ctx context.Context) (bool, error) {
	conn, err := gostomp.Dial(
		"tcp",
		c.cfg.STOMPAddr,
		gostomp.ConnOpt.Login(c.cfg.STOMPUser, c.cfg.STOMPPassword),
		gostomp.ConnOpt.HeartBeat(5*time.Second, 5*time.Second),
	)
	if err != nil {
		return false, err
	}
	defer func() { _ = conn.Disconnect() }()
	c.setConnected(true)
	c.logger.Info("stomp connected", "addr", c.cfg.STOMPAddr)

	for _, queue := range c.cfg.AuditQueues {
		auditName := brokerstomp.AuditQueueName(c.cfg.AuditPrefix, queue)
		destination := brokerstomp.STOMPDestination(domain.DestinationQueue, auditName)
		sub, err := conn.Subscribe(destination, ackMode(c.cfg.STOMPSubscription))
		if err != nil {
			return true, err
		}
		go c.consume(ctx, sub, auditName, queue)
		c.logger.Info("subscribed audit queue", "destination", destination)
	}

	<-ctx.Done()
	return true, ctx.Err()
}

func (c *AuditConsumer) consume(ctx context.Context, sub *gostomp.Subscription, auditName, original string) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-sub.C:
			if msg == nil {
				return
			}
			if msg.Err != nil {
				c.logger.Warn("stomp message error", "error", msg.Err)
				return
			}
			captured := c.toCapturedMessage(msg, auditName, original)
			if err := c.store.SaveMessage(ctx, captured); err != nil {
				c.logger.Error("save captured message", "error", err)
				c.events.Publish("error", map[string]string{"message": err.Error()})
				continue
			}
			c.events.Publish("message.captured", map[string]any{
				"id":          captured.ID,
				"destination": captured.OriginalDestination,
				"capturedAt":  captured.CapturedAt,
			})
		}
	}
}

func (c *AuditConsumer) toCapturedMessage(msg *gostomp.Message, auditName, original string) domain.CapturedMessage {
	headers := map[string]string{}
	properties := map[string]string{}

	allowlist := make(map[string]bool)
	for _, k := range c.cfg.PropertiesAllowlist {
		allowlist[k] = true
	}

	for i := 0; i < msg.Header.Len(); i++ {
		k, v := msg.Header.GetAt(i)
		switch k {
		case "message-id", "destination", "content-type", "content-length", "subscription":
			headers[k] = v
		default:
			if allowlist[k] {
				properties[k] = v
			}
		}
	}

	if c.cfg.Redaction {
		headersChanged := false
		propertiesChanged := false
		headers, headersChanged = RedactMap(headers)
		properties, propertiesChanged = RedactMap(properties)
		if headersChanged || propertiesChanged {
			c.logger.Info("sensitive headers or properties redacted")
		}
	}
	actualOriginal := original
	if val, ok := properties["LENS_OriginalDestination"]; ok && val != "" {
		actualOriginal = val
	}

	msgType := domain.DestinationQueue
	if val, ok := properties["LENS_DestinationType"]; ok {
		if val == "Topic" {
			msgType = domain.DestinationTopic
		}
	}

	body := ProcessBody(msg.Body, c.cfg.MaxBodyBytes, c.cfg.Redaction)
	return domain.CapturedMessage{
		ID:                  uuid.NewString(),
		CapturedAt:          time.Now().UTC(),
		Broker:              c.cfg.STOMPAddr,
		OriginalDestination: actualOriginal,
		AuditDestination:    auditName,
		DestinationType:     msgType,
		MessageID:           firstNonEmpty(properties["LENS_OriginalMessageId"], msg.Header.Get("message-id"), msg.Header.Get("JMSMessageID")),
		CorrelationID:       firstNonEmpty(msg.Header.Get("correlation-id"), msg.Header.Get("JMSCorrelationID")),
		ReplyTo:             msg.Header.Get("reply-to"),
		Type:                msg.Header.Get("type"),
		Persistent:          parseBool(msg.Header.Get("persistent")),
		Priority:            parseInt(msg.Header.Get("priority")),
		Headers:             headers,
		Properties:          properties,
		BodyFormat:          body.Format,
		BodyText:            body.Text,
		BodyBytes:           body.Bytes,
		BodySize:            body.Size,
		BodySHA256:          body.SHA256,
		Truncated:           body.Truncated,
		Redacted:            body.Redacted,
	}
}

func (c *AuditConsumer) setConnected(value bool) {
	c.connected.Store(value)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func parseBool(value string) bool {
	parsed, _ := strconv.ParseBool(value)
	return parsed
}

func parseInt(value string) int {
	parsed, _ := strconv.Atoi(value)
	return parsed
}

func ackMode(value string) gostomp.AckMode {
	switch value {
	case "client":
		return gostomp.AckClient
	case "client-individual":
		return gostomp.AckClientIndividual
	default:
		return gostomp.AckAuto
	}
}
