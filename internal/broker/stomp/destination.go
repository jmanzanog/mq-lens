package stomp

import (
	"strings"

	"github.com/jmanzano/mq-lens/internal/domain"
)

func STOMPDestination(kind domain.DestinationType, name string) string {
	clean := cleanDestinationName(name)
	if kind == domain.DestinationTopic {
		return "/topic/" + clean
	}
	return "/queue/" + clean
}

func AuditQueueName(prefix, original string) string {
	prefix = strings.TrimSpace(prefix)
	original = cleanDestinationName(original)
	if strings.HasPrefix(original, prefix) {
		return original
	}
	return prefix + original
}

func OriginalFromAudit(prefix, audit string) string {
	audit = cleanDestinationName(audit)
	return strings.TrimPrefix(audit, strings.TrimSpace(prefix))
}

func cleanDestinationName(name string) string {
	clean := strings.TrimSpace(name)
	for {
		switch {
		case strings.HasPrefix(clean, "/queue/"):
			clean = strings.TrimPrefix(clean, "/queue/")
		case strings.HasPrefix(clean, "/topic/"):
			clean = strings.TrimPrefix(clean, "/topic/")
		default:
			return clean
		}
	}
}
