package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/adapter"
	"go.uber.org/zap"

	"github.com/dakasa-yggdrasil/integration-kubernetes/controllers/message"
	ad "github.com/dakasa-yggdrasil/integration-kubernetes/internal/adapter"
)

// main bootstraps the kubernetes adapter with transport selected at
// runtime:
//
//   - YGGDRASIL_TRANSPORT=http (default) → ListenHTTP on ADAPTER_PORT.
//   - YGGDRASIL_TRANSPORT=amqp → ListenAMQP dialing BROKER_URL.
//
// A separate health server on HEALTHCHECK_PORT exposes /healthz and
// /readyz for Kubernetes liveness/readiness probes regardless of the
// RPC transport chosen.
func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()

	a := adapter.New(adapter.Config{
		Provider:       ad.Provider,
		Version:        ad.AdapterVersion,
		DefaultTimeout: 30 * time.Second,
		Concurrency:    5,
	}).
		Register("describe", message.DescribeHandler(logger)).
		Register("execute", message.ExecuteHandler(logger))

	switch transport := strings.ToLower(strings.TrimSpace(os.Getenv("YGGDRASIL_TRANSPORT"))); transport {
	case "", "http", "http_json":
		addr := ":" + envOrDefault("ADAPTER_PORT", "8081")
		a.ListenHTTP(addr)
		logger.Info("integration-kubernetes adapter starting on HTTP", zap.String("addr", addr))
	case "amqp", "rabbitmq":
		brokerURL := strings.TrimSpace(os.Getenv("BROKER_URL"))
		if brokerURL == "" {
			logger.Fatal("YGGDRASIL_TRANSPORT=amqp but BROKER_URL is empty")
		}
		a.ListenAMQP(brokerURL)
		logger.Info("integration-kubernetes adapter starting on AMQP")
	default:
		logger.Fatal("unsupported YGGDRASIL_TRANSPORT", zap.String("value", transport))
	}

	healthSrv := newHealthServer()
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("health server", zap.Error(err))
		}
	}()

	ctx := adapter.WithSignalHandler(context.Background())
	if err := a.Run(ctx); err != nil {
		logger.Fatal("adapter run", zap.Error(err))
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := healthSrv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Warn("shutdown health server", zap.Error(err))
	}
}

// newHealthServer exposes Kubernetes-oriented probes on a separate port
// so they stay reachable even when the RPC transport is degraded
// (e.g. an AMQP reconnect loop). Readiness is intentionally permissive
// here — the upstream workflow engine's describe-handshake catches
// real adapter degradation, so we only signal "pod process is up".
func newHealthServer() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	return &http.Server{
		Addr:              ":" + envOrDefault("HEALTHCHECK_PORT", "8080"),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
