package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jmanzano/mq-lens/internal/config"
	"github.com/jmanzano/mq-lens/internal/domain"
	"github.com/jmanzano/mq-lens/internal/stream"
)

func TestHealth(t *testing.T) {
	handler := newTestHandler()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"mode":"hybrid"`) {
		t.Fatalf("unexpected body=%s", rec.Body.String())
	}
}

func TestDestinationsFiltersAuditQueues(t *testing.T) {
	handler := newTestHandler()
	req := httptest.NewRequest(http.MethodGet, "/destinations?type=queue&auditOnly=true", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	var got []domain.DestinationSnapshot
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(got) != 1 || got[0].Name != "LENS.AUDIT.ORDER.CREATED" {
		t.Fatalf("destinations=%v", got)
	}
}

func TestMessagesPassesFilter(t *testing.T) {
	store := &fakeStore{}
	handler := New(testConfig(), store, fakeStatus{}, stream.NewBroker(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodGet, "/messages?destination=ORDER.CREATED&contains=secret&limit=10&offset=5", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	if store.lastFilter.Destination != "ORDER.CREATED" || store.lastFilter.Contains != "secret" {
		t.Fatalf("filter=%+v", store.lastFilter)
	}
	if store.lastFilter.Limit != 10 || store.lastFilter.Offset != 5 {
		t.Fatalf("pagination=%+v", store.lastFilter)
	}
}

func TestAddNoteRequiresBody(t *testing.T) {
	handler := newTestHandler()
	req := httptest.NewRequest(http.MethodPost, "/messages/msg-1/notes", bytes.NewBufferString(`{}`))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestDevSendTestMessageDisabledByDefault(t *testing.T) {
	handler := newTestHandler()
	req := httptest.NewRequest(http.MethodPost, "/dev/send-test-message", bytes.NewBufferString(`{"destination":"ORDER.CREATED"}`))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestDevSendTestMessageValidatesDestination(t *testing.T) {
	cfg := testConfig()
	cfg.DevTools = true
	handler := New(cfg, &fakeStore{}, fakeStatus{}, stream.NewBroker(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, "/dev/send-test-message", bytes.NewBufferString(`{"body":"{}"}`))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", rec.Code)
	}
}

func newTestHandler() http.Handler {
	return New(testConfig(), &fakeStore{}, fakeStatus{}, stream.NewBroker(), slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func testConfig() config.Config {
	return config.Config{Mode: "hybrid", AuditPrefix: "LENS.AUDIT."}
}

type fakeStatus struct{}

func (fakeStatus) Health() domain.Health {
	return domain.Health{Status: "ok", Mode: "hybrid", BrokerConnected: true}
}

func (fakeStatus) Snapshot() domain.BrokerSnapshot {
	return domain.BrokerSnapshot{
		BrokerName:  "localhost",
		CollectedAt: time.Now().UTC(),
		Available:   true,
		Queues: []domain.DestinationSnapshot{
			{Name: "ORDER.CREATED", Type: domain.DestinationQueue, QueueSize: 1},
			{Name: "LENS.AUDIT.ORDER.CREATED", Type: domain.DestinationQueue, QueueSize: 1},
		},
		Topics: []domain.DestinationSnapshot{
			{Name: "PAYMENT.EVENTS", Type: domain.DestinationTopic},
		},
	}
}

func (fakeStatus) Topology() domain.Topology {
	return domain.Topology{Nodes: []domain.TopologyNode{{ID: "broker:localhost", Type: "broker", Label: "localhost"}}}
}

type fakeStore struct {
	lastFilter domain.MessageFilter
}

func (s *fakeStore) ListMessages(_ context.Context, filter domain.MessageFilter) ([]domain.CapturedMessage, error) {
	s.lastFilter = filter
	return []domain.CapturedMessage{{ID: "msg-1", CapturedAt: time.Now().UTC(), Headers: map[string]string{}, Properties: map[string]string{}}}, nil
}

func (*fakeStore) GetMessage(_ context.Context, id string) (domain.CapturedMessage, error) {
	if id == "missing" {
		return domain.CapturedMessage{}, sql.ErrNoRows
	}
	return domain.CapturedMessage{ID: id, CapturedAt: time.Now().UTC(), Headers: map[string]string{}, Properties: map[string]string{}}, nil
}

func (*fakeStore) CountMessages(context.Context) (int64, error) {
	return 1, nil
}

func (*fakeStore) AddNote(_ context.Context, messageID, note string) (domain.MessageNote, error) {
	return domain.MessageNote{ID: "note-1", MessageID: messageID, Note: note, CreatedAt: time.Now().UTC()}, nil
}

func (*fakeStore) DeleteNote(context.Context, string) error {
	return nil
}
