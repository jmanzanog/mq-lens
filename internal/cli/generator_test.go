package cli_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jmanzano/mq-lens/internal/cli"
	"github.com/jmanzano/mq-lens/internal/config"
)

func TestGenerateActiveMQConfig(t *testing.T) {
	tests := []struct {
		name      string
		cfg       config.Config
		wantMatch []string
	}{
		{
			name: "flat queue mode",
			cfg: config.Config{
				AuditPrefix:      "LENS.AUDIT.",
				AuditQueues:      []string{"ORDER.CREATED"},
				VirtualTopicMode: false,
			},
			wantMatch: []string{
				`<compositeQueue name="ORDER.CREATED">`,
				`<queue physicalName="LENS.AUDIT.ORDER.CREATED" />`,
			},
		},
		{
			name: "virtual topic mode",
			cfg: config.Config{
				AuditPrefix:      "LENS.AUDIT.",
				AuditQueues:      []string{"PAYMENT.EVENT"},
				VirtualTopicMode: true,
			},
			wantMatch: []string{
				`<compositeTopic name="VirtualTopic.PAYMENT.EVENT">`,
				`<queue physicalName="LENS.AUDIT.PAYMENT.EVENT" />`,
			},
		},
		{
			name: "xml escaping",
			cfg: config.Config{
				AuditPrefix:      "LENS.AUDIT.",
				AuditQueues:      []string{"FOO&BAR<BAZ>"},
				VirtualTopicMode: false,
			},
			wantMatch: []string{
				`<compositeQueue name="FOO&amp;BAR&lt;BAZ&gt;">`,
				`<queue physicalName="LENS.AUDIT.FOO&amp;BAR&lt;BAZ&gt;" />`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "activemq.xml")
			if err := cli.GenerateActiveMQConfig(tt.cfg, path); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read file: %v", err)
			}
			content := string(data)
			for _, match := range tt.wantMatch {
				if !strings.Contains(content, match) {
					t.Errorf("expected generated output to contain %q, got: %s", match, content)
				}
			}
		})
	}
}

func TestGenerateActiveMQConfig_FileExists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "activemq.xml")
	if err := os.WriteFile(path, []byte("existing"), 0644); err != nil {
		t.Fatal(err)
	}
	err := cli.GenerateActiveMQConfig(config.Config{}, path)
	if err == nil {
		t.Fatal("expected error when file exists")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("expected 'already exists' error, got: %v", err)
	}
}

func TestGenerateActiveMQConfig_Stdout(t *testing.T) {
	// Re-route stdout temporarily
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := cli.GenerateActiveMQConfig(config.Config{
		AuditQueues: []string{"TEST"},
	}, "-")

	w.Close()
	os.Stdout = old

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	if !strings.Contains(buf.String(), `<?xml version="1.0"`) {
		t.Errorf("expected xml header in stdout, got: %s", buf.String())
	}
}
