package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/akin01/reschedule/internal/common"
	"github.com/akin01/reschedule/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	tasksExecuted = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "worker_tasks_executed_total"},
		[]string{"worker_id", "handler", "success"},
	)
	tasksFailed = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "worker_tasks_failed_total"},
		[]string{"worker_id", "handler"},
	)
	tasksDeadLettered = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "worker_tasks_deadlettered_total"},
		[]string{"worker_id"},
	)
	pollDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{Name: "worker_poll_duration_seconds"},
		[]string{"worker_id"},
	)
	activeLocks = promauto.NewGaugeVec(
		prometheus.GaugeOpts{Name: "worker_active_locks"},
		[]string{"worker_id"},
	)
	handlerDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{Name: "worker_handler_duration_seconds"},
		[]string{"worker_id", "handler"},
	)
)

// HandlerFunc defines the signature for task handlers
type HandlerFunc func(ctx context.Context, payload map[string]string) error

// Worker processes tasks from the queue
type Worker struct {
	store     *store.Store
	config    common.Config
	workerID  string
	handlers  map[string]HandlerFunc
	done      chan struct{}
	wg        sync.WaitGroup
	pollCount int32
}

func NewWorker(cfg common.Config, store *store.Store, handlers map[string]HandlerFunc) *Worker {
	hostname, _ := os.Hostname()
	pid := os.Getpid()
	workerID := fmt.Sprintf("worker-%s-%d", hostname, pid)

	return &Worker{
		store:    store,
		config:   cfg,
		workerID: workerID,
		handlers: handlers,
		done:     make(chan struct{}),
	}
}

func (w *Worker) Start() {
	log.Printf("[WORKER] Starting worker service (worker_id=%s)", w.workerID)

	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case <-ticker.C:
				w.pollAndProcess()
			case <-w.done:
				return
			}
		}
	}()
}

func (w *Worker) Stop() {
	log.Printf("[WORKER] Stopping worker service...")
	close(w.done)
	w.wg.Wait()
	log.Printf("[WORKER] Worker stopped")
}

func (w *Worker) pollAndProcess() {
	start := time.Now()
	ctx := context.Background()

	taskIDs, err := w.store.GetDueTasks(ctx, w.config.BatchSize)
	if err != nil {
		log.Printf("[WORKER] Error polling tasks: %v", err)
		return
	}

	pollDuration.WithLabelValues(w.workerID).Observe(time.Since(start).Seconds())

	var wg sync.WaitGroup
	for _, taskID := range taskIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			w.processTask(ctx, id)
		}(taskID)
	}

	wg.Wait()

	// Report metrics periodically
	if atomic.AddInt32(&w.pollCount, 1)%10 == 0 {
		queueLen, _ := w.store.QueueLength(ctx)
		deadLen, _ := w.store.DeadLetterLength(ctx)
		log.Printf("[WORKER] Queue length: %d, Dead letter: %d", queueLen, deadLen)
	}
}

func (w *Worker) processTask(ctx context.Context, taskID string) {
	// Try to acquire lock
	acquired, err := w.store.AcquireLock(ctx, taskID, w.config.LockTTL)
	if err != nil {
		log.Printf("[WORKER] Error acquiring lock for task %s: %v", taskID, err)
		return
	}
	if !acquired {
		return // Another worker got it
	}

	activeLocks.WithLabelValues(w.workerID).Inc()
	defer activeLocks.WithLabelValues(w.workerID).Dec()

	// Setup lock renewal
	renewCtx, renewCancel := context.WithCancel(ctx)
	defer renewCancel()
	go w.renewLock(renewCtx, taskID)

	// Get task
	task, err := w.store.GetTask(ctx, taskID)
	if err != nil {
		log.Printf("[WORKER] Error getting task %s: %v", taskID, err)
		w.store.ReleaseLock(ctx, taskID)
		return
	}
	if task == nil {
		w.store.ReleaseLock(ctx, taskID)
		return
	}

	handlerName := task.Handler
	log.Printf("[WORKER] Processing task %s (handler=%s, retries=%d/%d)", 
		taskID, handlerName, task.RetryCount, task.MaxRetries)

	// Execute handler
	handler, exists := w.handlers[handlerName]
	if !exists {
		log.Printf("[WORKER] Handler %s not found for task %s", handlerName, taskID)
		w.handleFailure(ctx, task, fmt.Errorf("handler not found: %s", handlerName))
		return
	}

	start := time.Now()
	err = handler(ctx, task.Payload)
	handlerDuration.WithLabelValues(w.workerID, handlerName).Observe(time.Since(start).Seconds())

	if err != nil {
		log.Printf("[WORKER] Task %s failed: %v", taskID, err)
		w.handleFailure(ctx, task, err)
	} else {
		log.Printf("[WORKER] Task %s completed successfully", taskID)
		w.handleSuccess(ctx, task)
	}

	w.store.ReleaseLock(ctx, taskID)
}

func (w *Worker) renewLock(ctx context.Context, taskID string) {
	ticker := time.NewTicker(w.config.LockTTL / 3)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.store.RenewLock(ctx, taskID, w.config.LockTTL); err != nil {
				log.Printf("[WORKER] Error renewing lock for task %s: %v", taskID, err)
				return
			}
		}
	}
}

func (w *Worker) handleSuccess(ctx context.Context, task *store.Task) {
	tasksExecuted.WithLabelValues(w.workerID, task.Handler, "true").Inc()
	w.store.DeleteTask(ctx, task.ID)
}

func (w *Worker) handleFailure(ctx context.Context, task *store.Task, err error) {
	tasksFailed.WithLabelValues(w.workerID, task.Handler).Inc()
	tasksExecuted.WithLabelValues(w.workerID, task.Handler, "false").Inc()

	task.RetryCount++
	task.LastError = err.Error()

	if task.RetryCount >= task.MaxRetries {
		log.Printf("[WORKER] Task %s exceeded max retries, moving to dead letter", task.ID)
		w.store.UpdateTask(ctx, task)
		w.store.MoveToDeadLetter(ctx, task.ID)
		tasksDeadLettered.WithLabelValues(w.workerID).Inc()
	} else {
		// Reschedule with exponential backoff
		backoff := time.Duration(1<<uint(task.RetryCount)) * time.Second
		if backoff > time.Minute {
			backoff = time.Minute
		}
		rescheduleAt := time.Now().Add(backoff)
		log.Printf("[WORKER] Rescheduling task %s for %v (attempt %d/%d)", 
			task.ID, rescheduleAt, task.RetryCount, task.MaxRetries)
		w.store.UpdateTask(ctx, task)
		w.store.MoveToQueue(ctx, task.ID, rescheduleAt)
	}
}

// RegisterHandler registers a handler function for a given handler name
func (w *Worker) RegisterHandler(name string, fn HandlerFunc) {
	w.handlers[name] = fn
	log.Printf("[WORKER] Registered handler: %s", name)
}

// Example handlers for demonstration
func exampleHandler(ctx context.Context, payload map[string]string) error {
	log.Printf("[HANDLER] Executing example handler with payload: %v", payload)
	// Simulate work
	time.Sleep(100 * time.Millisecond)
	return nil
}

func failingHandler(ctx context.Context, payload map[string]string) error {
	log.Printf("[HANDLER] Executing failing handler")
	return fmt.Errorf("intentional failure for testing")
}

func main() {
	cfg := common.LoadConfig("worker")

	client, err := store.NewRedisClient(cfg.GetRedisAddr(), cfg.UseCluster)
	if err != nil {
		log.Fatalf("[WORKER] Failed to connect to Redis: %v", err)
	}
	defer client.Close()

	taskStore := store.NewStore(client, cfg.TaskTTL)

	// Create worker with default handlers
	handlers := map[string]HandlerFunc{
		"example": exampleHandler,
		"failing": failingHandler,
	}
	worker := NewWorker(cfg, taskStore, handlers)

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	worker.Start()

	sig := <-sigChan
	log.Printf("[WORKER] Received signal %v, shutting down...", sig)
	worker.Stop()

	log.Println("[WORKER] Worker service stopped")
}
