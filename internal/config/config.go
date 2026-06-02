package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	AppName        string
	PackageName    string
	HTTPAddr       string
	Mode           string
	DBPath         string
	MaxBodyBytes   int64
	MaxMessages    int
	RetentionHours int
	Redaction      bool
	DevTools       bool

	STOMPAddr           string
	STOMPUser           string
	STOMPPassword       string
	STOMPReconnectMin   time.Duration
	STOMPReconnectMax   time.Duration
	STOMPSubscription   string
	AuditPrefix         string
	AuditQueues         []string
	AuditTopics         []string
	VirtualTopicMode    bool
	PropertiesAllowlist []string
	AdvisoryEnabled     bool
	AdvisoryTopics      []string
	JolokiaURL          string
	JolokiaUser         string
	JolokiaPassword     string
	JolokiaPoll         time.Duration
	CleanupInterval     time.Duration
}

func Load() Config {
	virtualTopicMode := envBool("LENS_VIRTUAL_TOPIC_MODE", false)
	propertiesAllowlist := envList("LENS_PROPERTIES_ALLOWLIST", "correlation-id,reply-to,type,persistent,priority,timestamp,expires,eventId,bbEventType,sourceEventTopic,LENS_OriginalDestination,LENS_DestinationType")

	return Config{
		AppName:             "MQ Lens",
		PackageName:         "com.jmanzano.mqlens",
		HTTPAddr:            env("LENS_HTTP_ADDR", "127.0.0.1:8080"),
		Mode:                env("LENS_MODE", "hybrid"),
		DBPath:              env("LENS_DB_PATH", "./data/mq-lens.db"),
		MaxBodyBytes:        int64(envInt("LENS_MAX_BODY_BYTES", 262144)),
		MaxMessages:         envInt("LENS_MAX_MESSAGES", 10000),
		RetentionHours:      envInt("LENS_RETENTION_HOURS", 24),
		Redaction:           envBool("LENS_ENABLE_REDACTION", true),
		DevTools:            envBool("LENS_DEV_TOOLS_ENABLED", false),
		STOMPAddr:           env("ACTIVEMQ_STOMP_ADDR", "127.0.0.1:61613"),
		STOMPUser:           env("ACTIVEMQ_STOMP_USER", "admin"),
		STOMPPassword:       env("ACTIVEMQ_STOMP_PASSWORD", "admin"),
		STOMPReconnectMin:   envDuration("LENS_STOMP_RECONNECT_MIN", time.Second),
		STOMPReconnectMax:   envDuration("LENS_STOMP_RECONNECT_MAX", 30*time.Second),
		STOMPSubscription:   env("LENS_STOMP_SUBSCRIPTION_ACK", "auto"),
		AuditPrefix:         env("LENS_AUDIT_PREFIX", "LENS.AUDIT."),
		AuditQueues:         envList("LENS_AUDIT_QUEUES", "ALL"),
		AuditTopics:         envList("LENS_AUDIT_TOPICS", ""),
		VirtualTopicMode:    virtualTopicMode,
		PropertiesAllowlist: propertiesAllowlist,
		AdvisoryEnabled:     envBool("LENS_ADVISORY_ENABLED", true),
		AdvisoryTopics: envList("LENS_ADVISORY_TOPICS", strings.Join([]string{
			"ActiveMQ.Advisory.Connection",
			"ActiveMQ.Advisory.Consumer.Queue.>",
			"ActiveMQ.Advisory.Consumer.Topic.>",
			"ActiveMQ.Advisory.Producer.Queue.>",
			"ActiveMQ.Advisory.Producer.Topic.>",
			"ActiveMQ.Advisory.Queue",
			"ActiveMQ.Advisory.Topic",
		}, ",")),
		JolokiaURL:      env("ACTIVEMQ_JOLOKIA_URL", "http://127.0.0.1:8161/api/jolokia"),
		JolokiaUser:     env("ACTIVEMQ_JOLOKIA_USER", "admin"),
		JolokiaPassword: env("ACTIVEMQ_JOLOKIA_PASSWORD", "admin"),
		JolokiaPoll:     envDuration("LENS_JOLOKIA_POLL_INTERVAL", 5*time.Second),
		CleanupInterval: envDuration("LENS_CLEANUP_INTERVAL", 10*time.Minute),
	}
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envList(key, fallback string) []string {
	raw := env(key, fallback)
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value != "" {
			values = append(values, value)
		}
	}
	return values
}
