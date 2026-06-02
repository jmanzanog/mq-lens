package app

import (
	"context"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jmanzano/mq-lens/internal/api"
	"github.com/jmanzano/mq-lens/internal/broker/advisory"
	"github.com/jmanzano/mq-lens/internal/broker/jolokia"
	brokerstomp "github.com/jmanzano/mq-lens/internal/broker/stomp"
	"github.com/jmanzano/mq-lens/internal/capture"
	"github.com/jmanzano/mq-lens/internal/config"
	"github.com/jmanzano/mq-lens/internal/domain"
	"github.com/jmanzano/mq-lens/internal/storage/sqlite"
	"github.com/jmanzano/mq-lens/internal/stream"
	"github.com/jmanzano/mq-lens/internal/ui"
)

type App struct {
	cfg      config.Config
	logger   *slog.Logger
	repo     *sqlite.Repository
	events   *stream.Broker
	jolokia  *jolokia.Client
	consumer *capture.AuditConsumer
	advisory *advisory.Monitor
	mu       sync.RWMutex
	wg       sync.WaitGroup
	snapshot domain.BrokerSnapshot
}

func New(cfg config.Config, logger *slog.Logger) (*App, error) {
	repo, err := sqlite.Open(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	events := stream.NewBroker()
	application := &App{
		cfg:      cfg,
		logger:   logger,
		repo:     repo,
		events:   events,
		jolokia:  jolokia.New(cfg.JolokiaURL, cfg.JolokiaUser, cfg.JolokiaPassword),
		snapshot: domain.BrokerSnapshot{CollectedAt: time.Now().UTC(), Available: false},
	}
	application.consumer = capture.NewAuditConsumer(cfg, repo, events, logger)
	application.advisory = advisory.New(cfg, events, logger)
	return application, nil
}

func (a *App) Handler() http.Handler {
	r := chi.NewRouter()
	r.Mount("/api", api.New(a.cfg, a.repo, a, a.events, a.logger))
	r.Mount("/", ui.Handler())
	return r
}

func (a *App) Run(ctx context.Context) {
	a.runWorker(ctx, a.consumer.Run)
	a.runWorker(ctx, a.advisory.Run)
	a.runWorker(ctx, a.pollJolokia)
	a.runWorker(ctx, a.cleanup)
}

func (a *App) Close() error {
	a.wg.Wait()
	return a.repo.Close()
}

func (a *App) Health() domain.Health {
	snapshot := a.Snapshot()
	stompConnected := a.consumer.Connected()
	brokerConnected := snapshot.Available || stompConnected
	return domain.Health{
		Status:           "ok",
		BrokerConnected:  brokerConnected,
		JolokiaAvailable: snapshot.Available,
		STOMPConnected:   stompConnected,
		Mode:             a.cfg.Mode,
	}
}

func (a *App) Snapshot() domain.BrokerSnapshot {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.snapshot
}

var consumerRegex = regexp.MustCompile(`^Consumer\.([^.]+)\.VirtualTopic\.(.+)$`)

func (a *App) Topology() domain.Topology {
	snapshot := a.Snapshot()
	nodes := []domain.TopologyNode{{
		ID:    "broker:" + valueOr(snapshot.BrokerName, "activemq"),
		Type:  "broker",
		Label: valueOr(snapshot.BrokerName, "ActiveMQ"),
	}}
	edges := []domain.TopologyEdge{}
	brokerID := nodes[0].ID
	inspectorID := "inspector:mq-lens"
	nodes = append(nodes, domain.TopologyNode{ID: inspectorID, Type: "inspector", Label: "MQ Lens"})

	for _, destination := range append(snapshot.Queues, snapshot.Topics...) {
		id := string(destination.Type) + ":" + destination.Name
		nodeType := string(destination.Type)
		if nodeType == "queue" && strings.HasPrefix(destination.Name, "Consumer.") {
			nodeType = "consumer-queue"
		}
		nodes = append(nodes, domain.TopologyNode{ID: id, Type: nodeType, Label: destination.Name, Meta: destination})
		edges = append(edges, domain.TopologyEdge{Source: brokerID, Target: id, Type: "owns"})

		if destination.Type == "queue" {
			if match := consumerRegex.FindStringSubmatch(destination.Name); match != nil {
				serviceName := match[1]
				topicName := match[2]
				topicID := "topic:VirtualTopic." + topicName

				// Draw edge from VirtualTopic -> Consumer Queue (Routing)
				edges = append(edges, domain.TopologyEdge{Source: topicID, Target: id, Type: "routes"})

				// Add inferred service if the queue has active consumers
				if destination.ConsumerCount > 0 {
					serviceNodeID := "service:" + serviceName
					nodes = append(nodes, domain.TopologyNode{ID: serviceNodeID, Type: "service", Label: serviceName})
					edges = append(edges, domain.TopologyEdge{Source: id, Target: serviceNodeID, Type: "consumes"})
				}
				continue
			}
		}

		if destination.ConsumerCount > 0 {
			consumerID := "consumer:" + destination.Name
			nodes = append(nodes, domain.TopologyNode{ID: consumerID, Type: "consumer", Label: "Consumers"})
			edges = append(edges, domain.TopologyEdge{Source: id, Target: consumerID, Type: "consumes"})
		}
	}
	for _, queue := range a.cfg.AuditQueues {
		original := brokerstomp.OriginalFromAudit(a.cfg.AuditPrefix, queue)
		auditName := brokerstomp.AuditQueueName(a.cfg.AuditPrefix, queue)
		originalID := "queue:" + original
		auditID := "queue:" + auditName
		edges = append(edges,
			domain.TopologyEdge{Source: originalID, Target: auditID, Type: "audit-copy"},
			domain.TopologyEdge{Source: auditID, Target: inspectorID, Type: "observes"},
		)
	}
	return domain.Topology{Nodes: nodes, Edges: edges}
}

func (a *App) pollJolokia(ctx context.Context) {
	ticker := time.NewTicker(a.cfg.JolokiaPoll)
	defer ticker.Stop()
	a.refreshSnapshot(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.refreshSnapshot(ctx)
		}
	}
}

func (a *App) refreshSnapshot(ctx context.Context) {
	snapshot, err := a.jolokia.Snapshot(ctx)
	if err != nil {
		a.logger.Warn("jolokia unavailable", "error", err)
		a.mu.Lock()
		snapshot = a.snapshot
		snapshot.Available = false
		snapshot.Error = err.Error()
		snapshot.CollectedAt = time.Now().UTC()
		a.snapshot = snapshot
		a.mu.Unlock()
		a.events.Publish("broker.status.changed", snapshot)
		return
	}
	a.mu.Lock()
	a.snapshot = snapshot
	a.mu.Unlock()
	a.events.Publish("broker.status.changed", snapshot)
}

func (a *App) runWorker(ctx context.Context, fn func(context.Context)) {
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		fn(ctx)
	}()
}

func (a *App) cleanup(ctx context.Context) {
	ticker := time.NewTicker(a.cfg.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := a.repo.Cleanup(ctx, a.cfg.RetentionHours, a.cfg.MaxMessages); err != nil {
				a.logger.Warn("cleanup failed", "error", err)
			}
		}
	}
}

func valueOr(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
