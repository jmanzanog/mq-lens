package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jmanzano/mq-lens/internal/app"
	"github.com/jmanzano/mq-lens/internal/cli"
	"github.com/jmanzano/mq-lens/internal/config"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := config.Load()

	if len(os.Args) > 1 && os.Args[1] == "generate-activemq-config" {
		generateCmd := flag.NewFlagSet("generate-activemq-config", flag.ExitOnError)
		outputPtr := generateCmd.String("output", "-", "Output path for the generated activemq.xml snippet")
		_ = generateCmd.Parse(os.Args[2:])

		if err := cli.GenerateActiveMQConfig(cfg, *outputPtr); err != nil {
			logger.Error("generate config failed", "error", err)
			os.Exit(1)
		}
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	application, err := app.New(cfg, logger)
	if err != nil {
		logger.Error("initialize app", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := application.Close(); err != nil {
			logger.Error("close app", "error", err)
		}
	}()

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           application.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go application.Run(ctx)

	go func() {
		logger.Info("MQ Lens started", "addr", cfg.HTTPAddr, "mode", cfg.Mode)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server stopped", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown", "error", err)
	}
}
