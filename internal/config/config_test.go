package config

import (
	"reflect"
	"testing"
	"time"
)

func TestLoadReadsAdvisoryConfig(t *testing.T) {
	t.Setenv("LENS_ADVISORY_ENABLED", "false")
	t.Setenv("LENS_ADVISORY_TOPICS", "ActiveMQ.Advisory.Connection, ActiveMQ.Advisory.Queue")
	t.Setenv("LENS_STOMP_RECONNECT_MIN", "2s")

	cfg := Load()
	if cfg.AdvisoryEnabled {
		t.Fatalf("AdvisoryEnabled=true")
	}
	if !reflect.DeepEqual(cfg.AdvisoryTopics, []string{"ActiveMQ.Advisory.Connection", "ActiveMQ.Advisory.Queue"}) {
		t.Fatalf("AdvisoryTopics=%v", cfg.AdvisoryTopics)
	}
	if cfg.STOMPReconnectMin != 2*time.Second {
		t.Fatalf("STOMPReconnectMin=%s", cfg.STOMPReconnectMin)
	}
}
