package stomp

import (
	"testing"

	"github.com/jmanzano/mq-lens/internal/domain"
)

func TestSTOMPDestination(t *testing.T) {
	tests := []struct {
		name string
		kind domain.DestinationType
		dest string
		want string
	}{
		{name: "queue", kind: domain.DestinationQueue, dest: "ORDER.CREATED", want: "/queue/ORDER.CREATED"},
		{name: "topic", kind: domain.DestinationTopic, dest: "PAYMENT.EVENTS", want: "/topic/PAYMENT.EVENTS"},
		{name: "already prefixed", kind: domain.DestinationQueue, dest: "/queue/LENS.AUDIT.ORDER.CREATED", want: "/queue/LENS.AUDIT.ORDER.CREATED"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := STOMPDestination(tt.kind, tt.dest); got != tt.want {
				t.Fatalf("STOMPDestination()=%q want %q", got, tt.want)
			}
		})
	}
}

func TestAuditQueueNameDoesNotDoublePrefix(t *testing.T) {
	tests := []struct {
		name     string
		original string
		want     string
	}{
		{name: "business queue", original: "ORDER.CREATED", want: "LENS.AUDIT.ORDER.CREATED"},
		{name: "already audit queue", original: "LENS.AUDIT.ORDER.CREATED", want: "LENS.AUDIT.ORDER.CREATED"},
		{name: "already audit queue with stomp prefix", original: "/queue/LENS.AUDIT.ORDER.CREATED", want: "LENS.AUDIT.ORDER.CREATED"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AuditQueueName("LENS.AUDIT.", tt.original); got != tt.want {
				t.Fatalf("AuditQueueName()=%q want %q", got, tt.want)
			}
		})
	}
}

func TestOriginalFromAudit(t *testing.T) {
	tests := []struct {
		name  string
		audit string
		want  string
	}{
		{name: "audit queue", audit: "LENS.AUDIT.ORDER.CREATED", want: "ORDER.CREATED"},
		{name: "stomp audit queue", audit: "/queue/LENS.AUDIT.ORDER.CREATED", want: "ORDER.CREATED"},
		{name: "business queue", audit: "ORDER.CREATED", want: "ORDER.CREATED"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := OriginalFromAudit("LENS.AUDIT.", tt.audit); got != tt.want {
				t.Fatalf("OriginalFromAudit()=%q want %q", got, tt.want)
			}
		})
	}
}
