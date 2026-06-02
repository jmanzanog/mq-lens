package domain

import "time"

type DestinationType string

const (
	DestinationQueue DestinationType = "queue"
	DestinationTopic DestinationType = "topic"
)

type BodyFormat string

const (
	BodyJSON    BodyFormat = "json"
	BodyXML     BodyFormat = "xml"
	BodyText    BodyFormat = "text"
	BodyBytes   BodyFormat = "bytes"
	BodyEmpty   BodyFormat = "empty"
	BodyUnknown BodyFormat = "unknown"
)

type CapturedMessage struct {
	ID                  string            `json:"id"`
	CapturedAt          time.Time         `json:"capturedAt"`
	Broker              string            `json:"broker"`
	OriginalDestination string            `json:"originalDestination"`
	AuditDestination    string            `json:"auditDestination"`
	DestinationType     DestinationType   `json:"destinationType"`
	MessageID           string            `json:"messageId,omitempty"`
	CorrelationID       string            `json:"correlationId,omitempty"`
	ReplyTo             string            `json:"replyTo,omitempty"`
	Type                string            `json:"type,omitempty"`
	Persistent          bool              `json:"persistent"`
	Priority            int               `json:"priority"`
	Timestamp           *time.Time        `json:"timestamp,omitempty"`
	Expiration          *time.Time        `json:"expiration,omitempty"`
	Headers             map[string]string `json:"headers"`
	Properties          map[string]string `json:"properties"`
	BodyFormat          BodyFormat        `json:"bodyFormat"`
	BodyText            string            `json:"bodyText,omitempty"`
	BodyBytes           []byte            `json:"bodyBytes,omitempty"`
	BodySize            int64             `json:"bodySize"`
	BodySHA256          string            `json:"bodySha256"`
	Truncated           bool              `json:"truncated"`
	Redacted            bool              `json:"redacted"`
}

type MessageNote struct {
	ID        string    `json:"id"`
	MessageID string    `json:"messageId"`
	Note      string    `json:"note"`
	CreatedAt time.Time `json:"createdAt"`
}

type DestinationSnapshot struct {
	Name          string          `json:"name"`
	Type          DestinationType `json:"type"`
	QueueSize     int64           `json:"queueSize"`
	EnqueueCount  int64           `json:"enqueueCount"`
	DequeueCount  int64           `json:"dequeueCount"`
	DispatchCount int64           `json:"dispatchCount"`
	ConsumerCount int64           `json:"consumerCount"`
	ProducerCount int64           `json:"producerCount"`
	ExpiredCount  int64           `json:"expiredCount"`
}

type BrokerSnapshot struct {
	BrokerName    string                `json:"brokerName"`
	BrokerVersion string                `json:"brokerVersion"`
	Uptime        string                `json:"uptime"`
	MemoryPercent int                   `json:"memoryPercent"`
	StorePercent  int                   `json:"storePercent"`
	TempPercent   int                   `json:"tempPercent"`
	Queues        []DestinationSnapshot `json:"queues"`
	Topics        []DestinationSnapshot `json:"topics"`
	CollectedAt   time.Time             `json:"collectedAt"`
	Available     bool                  `json:"available"`
	Error         string                `json:"error,omitempty"`
}

type Health struct {
	Status           string `json:"status"`
	BrokerConnected  bool   `json:"brokerConnected"`
	JolokiaAvailable bool   `json:"jolokiaAvailable"`
	STOMPConnected   bool   `json:"stompConnected"`
	Mode             string `json:"mode"`
}

type Topology struct {
	Nodes []TopologyNode `json:"nodes"`
	Edges []TopologyEdge `json:"edges"`
}

type TopologyNode struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Label string `json:"label"`
	Meta  any    `json:"meta,omitempty"`
}

type TopologyEdge struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Type   string `json:"type"`
}

type TopologyEvent struct {
	ID              string            `json:"id"`
	EventAt         time.Time         `json:"eventAt"`
	EventType       string            `json:"eventType"`
	DestinationType DestinationType   `json:"destinationType,omitempty"`
	DestinationName string            `json:"destinationName,omitempty"`
	ClientID        string            `json:"clientId,omitempty"`
	ConnectionID    string            `json:"connectionId,omitempty"`
	ProducerID      string            `json:"producerId,omitempty"`
	ConsumerID      string            `json:"consumerId,omitempty"`
	Headers         map[string]string `json:"headers,omitempty"`
	BodyPreview     string            `json:"bodyPreview,omitempty"`
}

type MessageFilter struct {
	Destination     string
	DestinationType string
	CorrelationID   string
	MessageID       string
	Contains        string
	HeaderKey       string
	HeaderValue     string
	PropertyKey     string
	PropertyValue   string
	Limit           int
	Offset          int
	SortAsc         bool
}
