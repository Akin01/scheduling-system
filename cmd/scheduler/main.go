package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/akin01/reschedule/internal/common"
	"github.com/akin01/reschedule/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	tasksScheduled = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "scheduler_tasks_scheduled_total"},
		[]string{"worker_id"},
	)
	tasksMoved = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "scheduler_tasks_moved_total"},
		[]string{"worker_id"},
	)
	schedulerErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "scheduler_errors_total"},
		[]string{"worker_id", "operation"},
	)
	queueLengthGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{Name: "scheduler_queue_length"},
		[]string{"worker_id"},
	)
	scheduledLengthGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{Name: "scheduler_scheduled_length"},
		[]string{"worker_id"},
	)
)

type Scheduler struct {
	store    *store.Store
	config   common.Config
	workerID string
	done     chan struct{}
	wg       sync.WaitGroup
}

func NewScheduler(cfg common.Config, store *store.Store) *Scheduler {
	hostname, _ := os.Hostname()
	pid := os.Getpid()
	workerID := fmt.Sprintf("scheduler-%s-%d", hostname, pid)

	return &Scheduler{
		store:    store,
		config:   cfg,
		workerID: workerID,
		done:     make(chan struct{}),
	}
}

func (s *Scheduler) Start() {
	log.Printf("[SCHEDULER] Starting scheduler service (worker_id=%s)", s.workerID)

	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-ticker.C:
				s.pollAndMove()
			case <-s.done:
				return
			}
		}
	}()

	// Metrics reporting
	metricsTicker := time.NewTicker(10 * time.Second)
	defer metricsTicker.Stop()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-metricsTicker.C:
				s.reportMetrics()
			case <-s.done:
				return
			}
		}
	}()
}

func (s *Scheduler) Stop() {
	log.Printf("[SCHEDULER] Stopping scheduler service...")
	close(s.done)
	s.wg.Wait()
	log.Printf("[SCHEDULER] Scheduler stopped")
}

func (s *Scheduler) pollAndMove() {
	ctx := context.Background()

	// Get tasks from scheduled queue that are due
	now := time.Now()
	taskIDs, err := s.store.GetDueTasks(ctx, s.config.BatchSize)
	if err != nil {
		log.Printf("[SCHEDULER] Error getting scheduled tasks: %v", err)
		schedulerErrors.WithLabelValues(s.workerID, "get_due_tasks").Inc()
		return
	}

	for _, taskID := range taskIDs {
		task, err := s.store.GetTask(ctx, taskID)
		if err != nil {
			log.Printf("[SCHEDULER] Error getting task %s: %v", taskID, err)
			continue
		}
		if task == nil {
			continue
		}

		// Move to active queue for execution
		if err := s.store.MoveToQueue(ctx, taskID, now); err != nil {
			log.Printf("[SCHEDULER] Error moving task %s to queue: %v", taskID, err)
			schedulerErrors.WithLabelValues(s.workerID, "move_to_queue").Inc()
			continue
		}

		tasksScheduled.WithLabelValues(s.workerID).Inc()
		tasksMoved.WithLabelValues(s.workerID).Inc()
		log.Printf("[SCHEDULER] Moved task %s to active queue (handler=%s)", taskID, task.Handler)
	}
}

func (s *Scheduler) reportMetrics() {
	ctx := context.Background()

	scheduledLen, err := s.store.ScheduledLength(ctx)
	if err == nil {
		scheduledLengthGauge.WithLabelValues(s.workerID).Set(float64(scheduledLen))
	}

	queueLen, err := s.store.QueueLength(ctx)
	if err == nil {
		queueLengthGauge.WithLabelValues(s.workerID).Set(float64(queueLen))
	}
}

func main() {
	cfg := common.LoadConfig("scheduler")

	client, err := store.NewRedisClient(cfg.GetRedisAddr(), cfg.UseCluster)
	if err != nil {
		log.Fatalf("[SCHEDULER] Failed to connect to Redis: %v", err)
	}
	defer client.Close()

	taskStore := store.NewStore(client, cfg.TaskTTL)
	scheduler := NewScheduler(cfg, taskStore)

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	scheduler.Start()

	sig := <-sigChan
	log.Printf("[SCHEDULER] Received signal %v, shutting down...", sig)
	scheduler.Stop()

	log.Println("[SCHEDULER] Scheduler service stopped")
}
