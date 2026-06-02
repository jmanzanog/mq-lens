package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/jmanzano/mq-lens/internal/domain"
)

func TestRepositorySaveAndGetMessage(t *testing.T) {
	repo, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open() error=%v", err)
	}
	defer repo.Close()

	want := domain.CapturedMessage{
		ID:                  "msg-1",
		CapturedAt:          time.Now().UTC(),
		Broker:              "localhost",
		OriginalDestination: "ORDER.CREATED",
		AuditDestination:    "LENS.AUDIT.ORDER.CREATED",
		DestinationType:     domain.DestinationQueue,
		MessageID:           "ID:1",
		Headers:             map[string]string{"content-type": "application/json"},
		Properties:          map[string]string{"tenant": "local"},
		BodyFormat:          domain.BodyJSON,
		BodyText:            `{"ok":true}`,
		BodySize:            11,
		BodySHA256:          "hash",
	}
	if err := repo.SaveMessage(context.Background(), want); err != nil {
		t.Fatalf("SaveMessage() error=%v", err)
	}
	got, err := repo.GetMessage(context.Background(), want.ID)
	if err != nil {
		t.Fatalf("GetMessage() error=%v", err)
	}
	if got.OriginalDestination != want.OriginalDestination {
		t.Fatalf("OriginalDestination=%q", got.OriginalDestination)
	}
	if got.Headers["content-type"] != "application/json" {
		t.Fatalf("headers not loaded")
	}
	if got.Properties["tenant"] != "local" {
		t.Fatalf("properties not loaded")
	}
}
