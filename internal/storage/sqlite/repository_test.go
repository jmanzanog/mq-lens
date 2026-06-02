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
	defer func() { _ = repo.Close() }()

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

func TestRecentCorrelationIDsAndClearMessages(t *testing.T) {
	repo, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open() error=%v", err)
	}
	defer func() { _ = repo.Close() }()

	ctx := context.Background()
	msg1 := domain.CapturedMessage{ID: "m1", CorrelationID: "corr-A", CapturedAt: time.Now().UTC()}
	msg2 := domain.CapturedMessage{ID: "m2", CorrelationID: "corr-B", CapturedAt: time.Now().UTC().Add(time.Second)}
	msg3 := domain.CapturedMessage{ID: "m3", CorrelationID: "corr-A", CapturedAt: time.Now().UTC().Add(2 * time.Second)}
	msg4 := domain.CapturedMessage{ID: "m4"} // No correlation ID

	_ = repo.SaveMessage(ctx, msg1)
	_ = repo.SaveMessage(ctx, msg2)
	_ = repo.SaveMessage(ctx, msg3)
	_ = repo.SaveMessage(ctx, msg4)

	ids, err := repo.RecentCorrelationIDs(ctx, 10)
	if err != nil {
		t.Fatalf("RecentCorrelationIDs() error=%v", err)
	}
	// Should be distinct and ordered by MAX(captured_at) DESC -> corr-A (m3), corr-B (m2)
	if len(ids) != 2 || ids[0] != "corr-A" || ids[1] != "corr-B" {
		t.Fatalf("unexpected recent IDs: %v", ids)
	}

	if err := repo.ClearMessages(ctx); err != nil {
		t.Fatalf("ClearMessages() error=%v", err)
	}
	count, _ := repo.CountMessages(ctx)
	if count != 0 {
		t.Fatalf("expected 0 messages after clear, got %d", count)
	}
}
