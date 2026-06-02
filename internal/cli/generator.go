package cli

import (
	"errors"
	"fmt"
	"html"
	"os"
	"strings"

	"github.com/jmanzano/mq-lens/internal/config"
)

func GenerateActiveMQConfig(cfg config.Config, outputPath string) error {
	var builder strings.Builder

	builder.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	builder.WriteString("<beans xmlns=\"http://www.springframework.org/schema/beans\"\n")
	builder.WriteString("       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n")
	builder.WriteString("       xsi:schemaLocation=\"http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd\n")
	builder.WriteString("                           http://activemq.apache.org/schema/core http://activemq.apache.org/schema/core/activemq-core.xsd\">\n\n")

	builder.WriteString("    <broker xmlns=\"http://activemq.apache.org/schema/core\">\n")
	builder.WriteString("        <destinationInterceptors>\n")
	builder.WriteString("            <virtualDestinationInterceptor>\n")
	builder.WriteString("                <virtualDestinations>\n")

	for _, dest := range cfg.AuditQueues {
		auditName := html.EscapeString(cfg.AuditPrefix + dest)
		destEscaped := html.EscapeString(dest)
		if cfg.VirtualTopicMode {
			topicName := "VirtualTopic." + destEscaped
			builder.WriteString(fmt.Sprintf("                    <compositeTopic name=\"%s\">\n", topicName))
			builder.WriteString("                        <forwardTo>\n")
			builder.WriteString(fmt.Sprintf("                            <queue physicalName=\"%s\" />\n", auditName))
			builder.WriteString("                        </forwardTo>\n")
			builder.WriteString("                    </compositeTopic>\n")
		} else {
			builder.WriteString(fmt.Sprintf("                    <compositeQueue name=\"%s\">\n", destEscaped))
			builder.WriteString("                        <forwardTo>\n")
			builder.WriteString(fmt.Sprintf("                            <queue physicalName=\"%s\" />\n", auditName))
			builder.WriteString("                        </forwardTo>\n")
			builder.WriteString("                    </compositeQueue>\n")
		}
	}

	builder.WriteString("                </virtualDestinations>\n")
	builder.WriteString("            </virtualDestinationInterceptor>\n")
	builder.WriteString("        </destinationInterceptors>\n")
	builder.WriteString("    </broker>\n")
	builder.WriteString("</beans>\n")

	if outputPath == "" || outputPath == "-" {
		fmt.Print(builder.String())
		return nil
	}

	if _, err := os.Stat(outputPath); err == nil {
		return errors.New("output file already exists, please remove it or use another path")
	}

	return os.WriteFile(outputPath, []byte(builder.String()), 0644)
}
