package jolokia

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jmanzano/mq-lens/internal/domain"
)

type Client struct {
	baseURL  string
	user     string
	password string
	client   *http.Client
}

func New(baseURL, user, password string) *Client {
	return NewWithHTTPClient(baseURL, user, password, &http.Client{Timeout: 5 * time.Second})
}

func NewWithHTTPClient(baseURL, user, password string, client *http.Client) *Client {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &Client{
		baseURL:  strings.TrimRight(baseURL, "/"),
		user:     user,
		password: password,
		client:   client,
	}
}

func (c *Client) Snapshot(ctx context.Context) (domain.BrokerSnapshot, error) {
	now := time.Now().UTC()
	broker, err := c.read(ctx, "org.apache.activemq:type=Broker,brokerName=*")
	if err != nil {
		return domain.BrokerSnapshot{CollectedAt: now, Available: false, Error: err.Error()}, err
	}
	snapshot := domain.BrokerSnapshot{CollectedAt: now, Available: true}
	if value, ok := broker.ValueMap(); ok {
		for name, raw := range value {
			attrs, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			snapshot.BrokerName = stringValue(attrs["BrokerName"], name)
			snapshot.BrokerVersion = stringValue(attrs["BrokerVersion"], "")
			snapshot.Uptime = stringValue(attrs["Uptime"], "")
			snapshot.MemoryPercent = intValue(attrs["MemoryPercentUsage"])
			snapshot.StorePercent = intValue(attrs["StorePercentUsage"])
			snapshot.TempPercent = intValue(attrs["TempPercentUsage"])
			break
		}
	}
	snapshot.Queues = c.readDestinations(ctx, domain.DestinationQueue)
	snapshot.Topics = c.readDestinations(ctx, domain.DestinationTopic)
	return snapshot, nil
}

func (c *Client) readDestinations(ctx context.Context, kind domain.DestinationType) []domain.DestinationSnapshot {
	objectName := "org.apache.activemq:type=Broker,brokerName=*,destinationType=Queue,destinationName=*"
	if kind == domain.DestinationTopic {
		objectName = "org.apache.activemq:type=Broker,brokerName=*,destinationType=Topic,destinationName=*"
	}
	response, err := c.read(ctx, objectName)
	if err != nil {
		return nil
	}
	value, ok := response.ValueMap()
	if !ok {
		return nil
	}
	items := make([]domain.DestinationSnapshot, 0, len(value))
	for objectName, raw := range value {
		attrs, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		items = append(items, domain.DestinationSnapshot{
			Name:          destinationName(objectName, attrs),
			Type:          kind,
			QueueSize:     int64Value(attrs["QueueSize"]),
			EnqueueCount:  int64Value(attrs["EnqueueCount"]),
			DequeueCount:  int64Value(attrs["DequeueCount"]),
			DispatchCount: int64Value(attrs["DispatchCount"]),
			ConsumerCount: int64Value(attrs["ConsumerCount"]),
			ProducerCount: int64Value(attrs["ProducerCount"]),
			ExpiredCount:  int64Value(attrs["ExpiredCount"]),
		})
	}
	return items
}

func (c *Client) read(ctx context.Context, mbean string) (response, error) {
	endpoint := c.baseURL + "/read/" + url.PathEscape(mbean)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return response{}, err
	}
	if c.user != "" || c.password != "" {
		req.SetBasicAuth(c.user, c.password)
	}
	res, err := c.client.Do(req)
	if err != nil {
		return response{}, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode >= 400 {
		return response{}, fmt.Errorf("jolokia returned %s", res.Status)
	}
	var out response
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		return response{}, err
	}
	if out.Status >= 400 {
		return response{}, fmt.Errorf("jolokia status %d", out.Status)
	}
	return out, nil
}

type response struct {
	Status int `json:"status"`
	Value  any `json:"value"`
}

func (r response) ValueMap() (map[string]any, bool) {
	value, ok := r.Value.(map[string]any)
	return value, ok
}

func destinationName(objectName string, attrs map[string]any) string {
	if name := stringValue(attrs["Name"], ""); name != "" {
		return name
	}
	const marker = "destinationName="
	idx := strings.Index(objectName, marker)
	if idx < 0 {
		return objectName
	}
	name := objectName[idx+len(marker):]
	if comma := strings.IndexByte(name, ','); comma >= 0 {
		name = name[:comma]
	}
	return strings.Trim(name, `"`)
}

func stringValue(value any, fallback string) string {
	if s, ok := value.(string); ok && s != "" {
		return s
	}
	return fallback
}

func intValue(value any) int {
	return int(int64Value(value))
}

func int64Value(value any) int64 {
	switch typed := value.(type) {
	case float64:
		return int64(typed)
	case int64:
		return typed
	case int:
		return int64(typed)
	default:
		return 0
	}
}
