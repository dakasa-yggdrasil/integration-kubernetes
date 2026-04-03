package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/dakasa-yggdrasil/integration-kubernetes/controllers/message"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()

	brokerURL := strings.TrimSpace(os.Getenv("BROKER_URL"))
	if brokerURL == "" {
		logger.Fatal("BROKER_URL is not set")
	}

	conn, err := amqp.Dial(brokerURL)
	if err != nil {
		logger.Fatal("connect rabbitmq", zap.Error(err))
	}
	defer conn.Close()

	if err := message.RegisterAllConsumers(conn, logger); err != nil {
		logger.Fatal("register consumers", zap.Error(err))
	}

	server := newHealthServer(conn, logger)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("serve health endpoints", zap.Error(err))
		}
	}()

	logger.Info("integration-kubernetes worker started",
		zap.Strings("queues", message.Queues()),
		zap.String("health_port", envOrDefault("HEALTHCHECK_PORT", "8080")),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	closeCh := make(chan *amqp.Error, 1)
	conn.NotifyClose(closeCh)

	select {
	case <-ctx.Done():
		logger.Info("shutting down integration-kubernetes worker", zap.String("reason", fmt.Sprint(ctx.Err())))
	case err := <-closeCh:
		if err != nil {
			logger.Error("rabbitmq connection closed", zap.Error(err))
		} else {
			logger.Warn("rabbitmq connection closed")
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Warn("shutdown health server", zap.Error(err))
	}
}

func newHealthServer(conn *amqp.Connection, logger *zap.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if conn == nil || conn.IsClosed() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("rabbitmq_unavailable"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})

	return &http.Server{
		Addr:              ":" + envOrDefault("HEALTHCHECK_PORT", "8080"),
		Handler:           mux,
		ReadHeaderTimeout: durationFromEnv("HTTP_READ_HEADER_TIMEOUT_SECONDS", 10*time.Second, logger),
		ReadTimeout:       durationFromEnv("HTTP_READ_TIMEOUT_SECONDS", 30*time.Second, logger),
		WriteTimeout:      durationFromEnv("HTTP_WRITE_TIMEOUT_SECONDS", 30*time.Second, logger),
		IdleTimeout:       durationFromEnv("HTTP_IDLE_TIMEOUT_SECONDS", 120*time.Second, logger),
	}
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func durationFromEnv(name string, fallback time.Duration, logger *zap.Logger) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err == nil {
		return duration
	}
	logger.Warn("invalid duration, using fallback",
		zap.String("env", name),
		zap.String("value", value),
		zap.Duration("fallback", fallback),
		zap.Error(err),
	)
	return fallback
}
