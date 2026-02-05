// cmd/api/main.go
package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"asana-extractor/internal/asana"
	"asana-extractor/internal/cache"
	"asana-extractor/internal/config"
	"asana-extractor/internal/handlers"
	"asana-extractor/internal/limiter"
	"asana-extractor/internal/scheduler"
	"asana-extractor/internal/storage"
	"asana-extractor/internal/worker"
	"asana-extractor/pkg/logger"
)

func main() {
	cfg := config.NewDefaultConfig()

	if err := cfg.LoadFromFile("config.json"); err != nil {
		logger.Info("No config file found, using defaults")
	}

	localCache := cache.NewLocalCache(
		cache.WithCleanupInterval(10*time.Minute),
		cache.WithPersistence("cache.gob"),
	)
	defer localCache.Close()

	tracker := cache.NewProcessedItemsTracker(
		localCache,
		"fetcher",
		24*time.Hour,
	)

	perSecond, burst := cfg.GetRateLimits()
	rateLimiter := limiter.NewAdaptiveRateLimiter(perSecond, burst)

	apiClient := asana.NewClient(cfg, rateLimiter)

	fileStorage, err := storage.NewFileStorage(cfg.ExportsBasePath)
	if err != nil {
		logger.Error("Failed to initialize storage", "error", err)
		os.Exit(1)
	}

	workerPool := worker.NewWorkerPool(
		cfg.MaxConcurrentFetch,
		fileStorage,
		tracker,
		apiClient,
	)

	sched := scheduler.NewScheduler(
		cfg,
		apiClient,
		fileStorage,
		tracker,
		workerPool,
	)

	h := handlers.NewHandlers(
		cfg,
		sched,
		rateLimiter,
		fileStorage,
		localCache,
		workerPool,
	)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", h.HealthCheck)
	mux.HandleFunc("GET /config", h.GetConfig)
	mux.HandleFunc("PUT /api/token", h.UpdateToken)
	mux.HandleFunc("GET /api/token/status", h.GetTokenStatus)
	mux.HandleFunc("PUT /api/cron/interval", h.UpdateCronInterval)
	mux.HandleFunc("GET /api/cron/intervals", h.GetCronIntervals)
	mux.HandleFunc("PUT /api/rate-limit", h.UpdateRateLimit)
	mux.HandleFunc("GET /api/rate-limit", h.GetRateLimits)
	mux.HandleFunc("POST /api/jobs/trigger", h.TriggerJob)
	mux.HandleFunc("GET /api/jobs/status", h.GetJobStatus)
	mux.HandleFunc("GET /api/stats/storage", h.GetStorageStats)
	mux.HandleFunc("GET /api/stats/workers", h.GetWorkerStats)

	server := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sched.Start(ctx)

	go func() {
		logger.Info("Starting HTTP server", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down...")

	sched.Stop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("Server shutdown error", "error", err)
	}

	if err := cfg.SaveToFile("config.json"); err != nil {
		logger.Warn("Failed to save config", "error", err)
	}

	logger.Info("Shutdown complete")
}
