package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/akin01/reschedule/internal/scheduler"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	cfg := scheduler.DefaultConfig()

	if addr := os.Getenv("REDIS_URL"); addr != "" {
		cfg.RedisAddr = addr
	}
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		cfg.RedisAddr = addr
	}
	if interval := os.Getenv("POLL_INTERVAL"); interval != "" {
		if d, err := time.ParseDuration(interval); err == nil {
			cfg.PollInterval = d
		}
	}
	if batchSize := os.Getenv("BATCH_SIZE"); batchSize != "" {
		fmt.Sscanf(batchSize, "%d", &cfg.BatchSize)
	}
	if lockTTL := os.Getenv("LOCK_TTL"); lockTTL != "" {
		if d, err := time.ParseDuration(lockTTL); err == nil {
			cfg.LockTTL = d
		}
	}
	if httpAddr := os.Getenv("HTTP_ADDR"); httpAddr != "" {
		cfg.HTTPAddr = httpAddr
	}
	if authToken := os.Getenv("AUTH_TOKEN"); authToken != "" {
		cfg.AuthToken = authToken
	}
	if maxRetries := os.Getenv("MAX_RETRIES"); maxRetries != "" {
		fmt.Sscanf(maxRetries, "%d", &cfg.MaxRetries)
	}
	if useCluster := os.Getenv("USE_REDIS_CLUSTER"); useCluster == "true" {
		cfg.UseCluster = true
	}

	handlers := map[string]scheduler.HandlerFunc{
		"example": func(ctx context.Context, taskID string, payload map[string]string) error {
			log.Printf("INFO: "("executing example handler", "taskID", taskID, "payload", payload)
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	}

	sched := scheduler.NewScheduler(cfg, handlers)
	sched.Start()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("POST /tasks/{id}/retry", authMiddleware(cfg.AuthToken, handleRetryTask(sched)))
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	})

	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: mux,
	}

	go func() {
		log.Printf("INFO: "("starting HTTP server", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("ERROR: "("HTTP server failed", "err", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Printf("INFO: "("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("ERROR: "("server shutdown error", "err", err)
	}

	sched.Stop()
	log.Printf("INFO: "("graceful shutdown complete")
}

func authMiddleware(token string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if token == "" {
			next(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "missing authorization header", http.StatusUnauthorized)
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || parts[0] != "Bearer" || parts[1] != token {
			http.Error(w, "invalid authorization", http.StatusUnauthorized)
			return
		}

		next(w, r)
	}
}

func handleRetryTask(sched *scheduler.Scheduler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		taskID := strings.TrimPrefix(path, "/tasks/")
		taskID = strings.TrimSuffix(taskID, "/retry")

		if taskID == "" {
			http.Error(w, "invalid taskID", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		if err := sched.RetryTask(ctx, taskID); err != nil {
			http.Error(w, fmt.Sprintf("retry failed: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"retried": true})
	}
}
