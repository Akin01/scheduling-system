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

	"github.com/akin01/reschedule/internal/common"
	"github.com/akin01/reschedule/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	apiRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "api_requests_total"},
		[]string{"method", "endpoint", "status"},
	)
	taskCreateTotal = promauto.NewCounter(prometheus.CounterOpts{Name: "api_tasks_created_total"})
	taskRetryTotal  = promauto.NewCounter(prometheus.CounterOpts{Name: "api_tasks_retried_total"})
)

type API struct {
	store      *store.Store
	config     common.Config
	authToken  string
	httpServer *http.Server
}

func NewAPI(cfg common.Config, store *store.Store) *API {
	return &API{
		store:     store,
		config:    cfg,
		authToken: cfg.AuthToken,
	}
}

func (a *API) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /tasks", a.handleCreateTask)
	mux.HandleFunc("GET /tasks/{id}", a.handleGetTask)
	mux.HandleFunc("POST /tasks/{id}/retry", a.authMiddleware(a.handleRetryTask))
	mux.HandleFunc("GET /health", a.handleHealth)
	mux.Handle("GET /metrics", promhttp.Handler())

	a.httpServer = &http.Server{
		Addr:    a.config.HTTPAddr,
		Handler: mux,
	}

	log.Printf("[API] Starting HTTP server on %s", a.config.HTTPAddr)
	return a.httpServer.ListenAndServe()
}

func (a *API) Stop(ctx context.Context) error {
	if a.httpServer != nil {
		return a.httpServer.Shutdown(ctx)
	}
	return nil
}

func (a *API) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	status := "200"
	defer func() {
		apiRequestsTotal.WithLabelValues(r.Method, "/tasks", status).Inc()
	}()

	var req struct {
		Handler     string            `json:"handler"`
		Payload     map[string]string `json:"payload"`
		ScheduledAt *time.Time        `json:"scheduled_at,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		status = "400"
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}

	if req.Handler == "" {
		status = "400"
		http.Error(w, "handler is required", http.StatusBadRequest)
		return
	}

	scheduledAt := time.Now()
	if req.ScheduledAt != nil {
		scheduledAt = *req.ScheduledAt
	}

	task, err := a.store.CreateTask(r.Context(), req.Handler, req.Payload, scheduledAt)
	if err != nil {
		status = "500"
		http.Error(w, fmt.Sprintf("failed to create task: %v", err), http.StatusInternalServerError)
		return
	}

	taskCreateTotal.Inc()
	log.Printf("[API] Task created: %s (handler=%s, scheduled=%v)", task.ID, task.Handler, scheduledAt)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(task)
}

func (a *API) handleGetTask(w http.ResponseWriter, r *http.Request) {
	status := "200"
	defer func() {
		apiRequestsTotal.WithLabelValues(r.Method, "/tasks/{id}", status).Inc()
	}()

	// Extract task ID from URL path /tasks/{id}
	path := strings.TrimPrefix(r.URL.Path, "/tasks/")
	taskID := strings.TrimSpace(path)
	if taskID == "" {
		status = "400"
		http.Error(w, "task ID required", http.StatusBadRequest)
		return
	}

	task, err := a.store.GetTask(r.Context(), taskID)
	if err != nil {
		status = "500"
		http.Error(w, fmt.Sprintf("failed to get task: %v", err), http.StatusInternalServerError)
		return
	}

	if task == nil {
		status = "404"
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(task)
}

func (a *API) handleRetryTask(w http.ResponseWriter, r *http.Request) {
	status := "200"
	defer func() {
		apiRequestsTotal.WithLabelValues(r.Method, "/tasks/{id}/retry", status).Inc()
	}()

	// Extract task ID from URL path /tasks/{id}/retry
	path := strings.TrimPrefix(r.URL.Path, "/tasks/")
	path = strings.TrimSuffix(path, "/retry")
	taskID := strings.TrimSpace(path)
	if taskID == "" {
		status = "400"
		http.Error(w, "task ID required", http.StatusBadRequest)
		return
	}

	if err := a.store.RetryTask(r.Context(), taskID); err != nil {
		status = "400"
		http.Error(w, fmt.Sprintf("failed to retry task: %v", err), http.StatusBadRequest)
		return
	}

	taskRetryTotal.Inc()
	log.Printf("[API] Task retried: %s", taskID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"retried": true})
}

func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
		"time":   time.Now().Format(time.RFC3339),
	})
}

func (a *API) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.authToken == "" {
			next(w, r)
			return
		}

		token := r.Header.Get("Authorization")
		if token == "" {
			token = r.URL.Query().Get("token")
		}

		expected := "Bearer " + a.authToken
		if token != expected && token != a.authToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		next(w, r)
	}
}

func main() {
	cfg := common.LoadConfig("api")

	client, err := store.NewRedisClient(cfg.GetRedisAddr(), cfg.UseCluster)
	if err != nil {
		log.Fatalf("[API] Failed to connect to Redis: %v", err)
	}
	defer client.Close()

	taskStore := store.NewStore(client, cfg.TaskTTL)
	api := NewAPI(cfg, taskStore)

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	errChan := make(chan error, 1)
	go func() {
		errChan <- api.Start()
	}()

	select {
	case err := <-errChan:
		if err != http.ErrServerClosed {
			log.Fatalf("[API] Server failed: %v", err)
		}
	case sig := <-sigChan:
		log.Printf("[API] Received signal %v, shutting down...", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := api.Stop(ctx); err != nil {
			log.Printf("[API] Shutdown error: %v", err)
		}
	}

	log.Println("[API] Server stopped")
}
