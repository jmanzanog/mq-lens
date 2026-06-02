package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	brokerstomp "github.com/jmanzano/mq-lens/internal/broker/stomp"
	"github.com/jmanzano/mq-lens/internal/config"
	"github.com/jmanzano/mq-lens/internal/domain"
	"github.com/jmanzano/mq-lens/internal/stream"
)

type Store interface {
	ListMessages(context.Context, domain.MessageFilter) ([]domain.CapturedMessage, error)
	GetMessage(context.Context, string) (domain.CapturedMessage, error)
	CountMessages(context.Context) (int64, error)
	AddNote(context.Context, string, string) (domain.MessageNote, error)
	DeleteNote(context.Context, string) error
}

type StatusProvider interface {
	Health() domain.Health
	Snapshot() domain.BrokerSnapshot
	Topology() domain.Topology
}

type Router struct {
	cfg    config.Config
	store  Store
	status StatusProvider
	events *stream.Broker
	logger *slog.Logger
}

func New(cfg config.Config, store Store, status StatusProvider, events *stream.Broker, logger *slog.Logger) http.Handler {
	router := &Router{cfg: cfg, store: store, status: status, events: events, logger: logger}
	r := chi.NewRouter()
	r.Get("/health", router.health)
	r.Get("/broker/status", router.brokerStatus)
	r.Get("/destinations", router.destinations)
	r.Get("/destinations/{type}/{name}", router.destination)
	r.Get("/topology", router.topology)
	r.Get("/messages", router.messages)
	r.Get("/messages/{id}", router.message)
	r.Get("/messages/{id}/download", router.downloadMessage)
	r.Post("/messages/{id}/notes", router.addNote)
	r.Delete("/messages/{id}/notes/{noteId}", router.deleteNote)
	if cfg.DevTools {
		r.Post("/dev/send-test-message", router.sendTestMessage)
	}
	r.Get("/events", router.eventsStream)
	return r
}

func (r *Router) health(w http.ResponseWriter, req *http.Request) {
	writeJSON(w, http.StatusOK, r.status.Health())
}

func (r *Router) brokerStatus(w http.ResponseWriter, req *http.Request) {
	writeJSON(w, http.StatusOK, r.status.Snapshot())
}

func (r *Router) destinations(w http.ResponseWriter, req *http.Request) {
	snapshot := r.status.Snapshot()
	destinations := append([]domain.DestinationSnapshot{}, snapshot.Queues...)
	destinations = append(destinations, snapshot.Topics...)
	filterType := req.URL.Query().Get("type")
	q := req.URL.Query().Get("q")
	activeOnly := req.URL.Query().Get("activeOnly") == "true"
	auditOnly := req.URL.Query().Get("auditOnly") == "true"
	out := destinations[:0]
	for _, destination := range destinations {
		if filterType != "" && string(destination.Type) != filterType {
			continue
		}
		if q != "" && !containsFold(destination.Name, q) {
			continue
		}
		if activeOnly && destination.QueueSize == 0 && destination.ConsumerCount == 0 && destination.ProducerCount == 0 {
			continue
		}
		if auditOnly && !containsFold(destination.Name, r.cfg.AuditPrefix) {
			continue
		}
		out = append(out, destination)
	}
	writeJSON(w, http.StatusOK, out)
}

func (r *Router) destination(w http.ResponseWriter, req *http.Request) {
	kind := chi.URLParam(req, "type")
	name := chi.URLParam(req, "name")
	snapshot := r.status.Snapshot()
	for _, destination := range append(snapshot.Queues, snapshot.Topics...) {
		if string(destination.Type) == kind && destination.Name == name {
			writeJSON(w, http.StatusOK, destination)
			return
		}
	}
	writeError(w, http.StatusNotFound, "destination not found")
}

func (r *Router) topology(w http.ResponseWriter, req *http.Request) {
	writeJSON(w, http.StatusOK, r.status.Topology())
}

func (r *Router) messages(w http.ResponseWriter, req *http.Request) {
	filter := domain.MessageFilter{
		Destination:     req.URL.Query().Get("destination"),
		DestinationType: req.URL.Query().Get("destinationType"),
		CorrelationID:   req.URL.Query().Get("correlationId"),
		MessageID:       req.URL.Query().Get("messageId"),
		Contains:        req.URL.Query().Get("contains"),
		HeaderKey:       req.URL.Query().Get("headerKey"),
		HeaderValue:     req.URL.Query().Get("headerValue"),
		PropertyKey:     req.URL.Query().Get("propertyKey"),
		PropertyValue:   req.URL.Query().Get("propertyValue"),
		Limit:           queryInt(req, "limit", 100),
		Offset:          queryInt(req, "offset", 0),
	}
	messages, err := r.store.ListMessages(req.Context(), filter)
	if err != nil {
		r.logger.Error("list messages", "error", err)
		writeError(w, http.StatusInternalServerError, "list messages failed")
		return
	}
	if total, err := r.store.CountMessages(req.Context()); err == nil {
		w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	}
	writeJSON(w, http.StatusOK, messages)
}

func (r *Router) message(w http.ResponseWriter, req *http.Request) {
	message, err := r.store.GetMessage(req.Context(), chi.URLParam(req, "id"))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "message not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "load message failed")
		return
	}
	writeJSON(w, http.StatusOK, message)
}

func (r *Router) downloadMessage(w http.ResponseWriter, req *http.Request) {
	message, err := r.store.GetMessage(req.Context(), chi.URLParam(req, "id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "message not found")
		return
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.json"`, message.ID))
	writeJSON(w, http.StatusOK, message)
}

func (r *Router) addNote(w http.ResponseWriter, req *http.Request) {
	var input struct {
		Note string `json:"note"`
	}
	if err := json.NewDecoder(req.Body).Decode(&input); err != nil || input.Note == "" {
		writeError(w, http.StatusBadRequest, "note is required")
		return
	}
	note, err := r.store.AddNote(req.Context(), chi.URLParam(req, "id"), input.Note)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "add note failed")
		return
	}
	writeJSON(w, http.StatusCreated, note)
}

func (r *Router) deleteNote(w http.ResponseWriter, req *http.Request) {
	if err := r.store.DeleteNote(req.Context(), chi.URLParam(req, "noteId")); err != nil {
		writeError(w, http.StatusInternalServerError, "delete note failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (r *Router) sendTestMessage(w http.ResponseWriter, req *http.Request) {
	var input brokerstomp.SendRequest
	if err := json.NewDecoder(req.Body).Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if strings.TrimSpace(input.Destination) == "" {
		writeError(w, http.StatusBadRequest, "destination is required")
		return
	}
	if input.Headers == nil {
		input.Headers = map[string]string{}
	}
	if err := brokerstomp.SendTestMessage(req.Context(), r.cfg, input); err != nil {
		r.logger.Error("send test message", "error", err)
		writeError(w, http.StatusBadGateway, "send test message failed")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "sent"})
}

func (r *Router) eventsStream(w http.ResponseWriter, req *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	_, _ = fmt.Fprint(w, "retry: 5000\n\n")
	flusher.Flush()
	id, ch := r.events.Subscribe()
	defer r.events.Unsubscribe(id)
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-req.Context().Done():
			return
		case event := <-ch:
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Type, event.JSON())
			flusher.Flush()
		case <-ticker.C:
			_, _ = fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func queryInt(req *http.Request, key string, fallback int) int {
	value := req.URL.Query().Get(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func containsFold(value, needle string) bool {
	if needle == "" {
		return true
	}
	return len(value) >= len(needle) && (value == needle || containsFoldSlow(value, needle))
}

func containsFoldSlow(value, needle string) bool {
	value = strings.ToLower(value)
	needle = strings.ToLower(needle)
	return strings.Contains(value, needle)
}
