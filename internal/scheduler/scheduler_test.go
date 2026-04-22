package scheduler

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func getTestRedisAddr() string {
	if addr := os.Getenv("REDIS_URL"); addr != "" {
		return addr
	}
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

func setupTestScheduler(t *testing.T, handlers map[string]HandlerFunc) (*Scheduler, func()) {
	t.Helper()

	cfg := DefaultConfig()
	cfg.RedisAddr = getTestRedisAddr()
	cfg.PollInterval = 50 * time.Millisecond
	cfg.LockTTL = 30 * time.Second
	cfg.MaxRetries = 3

	sched := NewScheduler(cfg, handlers)

	cleanup := func() {
		ctx := context.Background()
		sched.GetRedisClient().FlushDB(ctx)
		sched.Stop()
	}

	return sched, cleanup
}

func TestNewScheduler(t *testing.T) {
	handlers := map[string]HandlerFunc{
		"test": func(ctx context.Context, taskID string, payload map[string]string) error {
			return nil
		},
	}

	sched, cleanup := setupTestScheduler(t, handlers)
	defer cleanup()

	if sched == nil {
		t.Fatal("expected scheduler to be created")
	}

	if sched.workerID == "" {
		t.Error("expected workerID to be set")
	}

	if sched.GetRedisClient() == nil {
		t.Error("expected redis client to be initialized")
	}
}

func TestSchedule(t *testing.T) {
	handlers := map[string]HandlerFunc{
		"test_handler": func(ctx context.Context, taskID string, payload map[string]string) error {
			return nil
		},
	}

	sched, cleanup := setupTestScheduler(t, handlers)
	defer cleanup()

	ctx := context.Background()
	taskID := "test-task-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	executeAt := time.Now().Add(100 * time.Millisecond)

	err := sched.Schedule(ctx, taskID, "test_handler", `{"key":"value"}`, executeAt)
	if err != nil {
		t.Fatalf("unexpected error scheduling task: %v", err)
	}

	// Verify task exists in Redis
	taskKey := fmt.Sprintf("scheduler:task:%s", taskID)
	exists, err := sched.GetRedisClient().Exists(ctx, taskKey).Result()
	if err != nil {
		t.Fatalf("error checking task existence: %v", err)
	}
	if exists == 0 {
		t.Error("expected task to exist in Redis")
	}

	// Verify task is in queue
	score, err := sched.GetRedisClient().ZScore(ctx, "scheduler:queue", taskID).Result()
	if err != nil {
		t.Fatalf("error getting queue score: %v", err)
	}
	if score == 0 {
		t.Error("expected task to be in queue")
	}
}

func TestScheduleDuplicate(t *testing.T) {
	handlers := map[string]HandlerFunc{
		"test_handler": func(ctx context.Context, taskID string, payload map[string]string) error {
			return nil
		},
	}

	sched, cleanup := setupTestScheduler(t, handlers)
	defer cleanup()

	ctx := context.Background()
	taskID := "test-task-dup-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	executeAt := time.Now().Add(100 * time.Millisecond)

	// Schedule first task
	err := sched.Schedule(ctx, taskID, "test_handler", `{"key":"value"}`, executeAt)
	if err != nil {
		t.Fatalf("unexpected error scheduling first task: %v", err)
	}

	// Try to schedule duplicate
	err = sched.Schedule(ctx, taskID, "test_handler", `{"key":"value2"}`, executeAt)
	if err == nil {
		t.Error("expected error when scheduling duplicate task")
	}
}

func TestProcessTaskSuccess(t *testing.T) {
	var handlerCalled bool
	var capturedTaskID string
	var capturedPayload map[string]string

	handlers := map[string]HandlerFunc{
		"success_handler": func(ctx context.Context, taskID string, payload map[string]string) error {
			handlerCalled = true
			capturedTaskID = taskID
			capturedPayload = payload
			return nil
		},
	}

	sched, cleanup := setupTestScheduler(t, handlers)
	defer cleanup()

	ctx := context.Background()
	taskID := "test-task-success-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	// Manually create task in Redis (bypass Schedule to test processTask directly)
	taskKey := fmt.Sprintf("scheduler:task:%s", taskID)
	sched.GetRedisClient().HSet(ctx, taskKey, map[string]interface{}{
		"handler":     "success_handler",
		"payload":     `{"test":"data"}`,
		"retry_count": "0",
	})

	// Add to queue with immediate execution time
	sched.GetRedisClient().ZAdd(ctx, "scheduler:queue", &redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: taskID,
	})

	// Give scheduler time to poll and process
	sched.Start()
	time.Sleep(200 * time.Millisecond)
	sched.Stop()

	if !handlerCalled {
		t.Error("expected handler to be called")
	}

	if capturedTaskID != taskID {
		t.Errorf("expected taskID %s, got %s", taskID, capturedTaskID)
	}

	// Verify task was removed after successful completion
	exists, _ := sched.GetRedisClient().Exists(ctx, taskKey).Result()
	if exists > 0 {
		t.Error("expected task to be removed after successful completion")
	}
}

func TestProcessTaskFailure(t *testing.T) {
	attemptCount := 0

	handlers := map[string]HandlerFunc{
		"fail_handler": func(ctx context.Context, taskID string, payload map[string]string) error {
			attemptCount++
			return fmt.Errorf("intentional failure")
		},
	}

	sched, cleanup := setupTestScheduler(t, handlers)
	defer cleanup()

	ctx := context.Background()
	taskID := "test-task-fail-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	// Manually create task in Redis
	taskKey := fmt.Sprintf("scheduler:task:%s", taskID)
	sched.GetRedisClient().HSet(ctx, taskKey, map[string]interface{}{
		"handler":     "fail_handler",
		"payload":     `{"test":"data"}`,
		"retry_count": "0",
	})

	// Add to queue with immediate execution time
	sched.GetRedisClient().ZAdd(ctx, "scheduler:queue", &redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: taskID,
	})

	// Start scheduler and wait for retries
	sched.Start()
	time.Sleep(500 * time.Millisecond)
	sched.Stop()

	// Should have attempted multiple times (initial + retries)
	if attemptCount < 2 {
		t.Errorf("expected at least 2 attempts, got %d", attemptCount)
	}
}

func TestDeadLetter(t *testing.T) {
	handlers := map[string]HandlerFunc{
		"always_fail": func(ctx context.Context, taskID string, payload map[string]string) error {
			return fmt.Errorf("always fails")
		},
	}

	sched, cleanup := setupTestScheduler(t, handlers)
	defer cleanup()

	ctx := context.Background()
	taskID := "test-task-dead-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	// Create task with max retries already reached
	taskKey := fmt.Sprintf("scheduler:task:%s", taskID)
	sched.GetRedisClient().HSet(ctx, taskKey, map[string]interface{}{
		"handler":     "always_fail",
		"payload":     `{"test":"data"}`,
		"retry_count": strconv.Itoa(sched.config.MaxRetries),
	})

	// Add to queue
	sched.GetRedisClient().ZAdd(ctx, "scheduler:queue", &redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: taskID,
	})

	sched.Start()
	time.Sleep(300 * time.Millisecond)
	sched.Stop()

	// Verify task moved to dead letter queue
	inDead, _ := sched.GetRedisClient().ZScore(ctx, "scheduler:dead", taskID).Result()
	if inDead == 0 {
		t.Error("expected task to be in dead letter queue")
	}

	// Verify task removed from main queue
	inQueue, _ := sched.GetRedisClient().ZScore(ctx, "scheduler:queue", taskID).Result()
	if inQueue != 0 {
		t.Error("expected task to be removed from main queue")
	}
}

func TestRetryTask(t *testing.T) {
	handlers := map[string]HandlerFunc{
		"test": func(ctx context.Context, taskID string, payload map[string]string) error {
			return nil
		},
	}

	sched, cleanup := setupTestScheduler(t, handlers)
	defer cleanup()

	ctx := context.Background()
	taskID := "test-task-retry-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	// Create task in dead letter queue
	taskKey := fmt.Sprintf("scheduler:task:%s", taskID)
	sched.GetRedisClient().HSet(ctx, taskKey, map[string]interface{}{
		"handler":     "test",
		"payload":     `{"test":"data"}`,
		"retry_count": "5",
	})

	sched.GetRedisClient().ZAdd(ctx, "scheduler:dead", &redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: taskID,
	})

	// Retry the task
	err := sched.RetryTask(ctx, taskID)
	if err != nil {
		t.Fatalf("unexpected error retrying task: %v", err)
	}

	// Verify task moved back to queue
	inQueue, _ := sched.GetRedisClient().ZScore(ctx, "scheduler:queue", taskID).Result()
	if inQueue == 0 {
		t.Error("expected task to be in main queue after retry")
	}

	// Verify task removed from dead letter queue
	inDead, _ := sched.GetRedisClient().ZScore(ctx, "scheduler:dead", taskID).Result()
	if inDead != 0 {
		t.Error("expected task to be removed from dead letter queue")
	}

	// Verify retry_count reset
	retryCount, _ := sched.GetRedisClient().HGet(ctx, taskKey, "retry_count").Result()
	if retryCount != "0" {
		t.Errorf("expected retry_count to be 0, got %s", retryCount)
	}
}

func TestAtomicLockAndPop(t *testing.T) {
	handlers := map[string]HandlerFunc{
		"test": func(ctx context.Context, taskID string, payload map[string]string) error {
			return nil
		},
	}

	sched, cleanup := setupTestScheduler(t, handlers)
	defer cleanup()

	ctx := context.Background()
	taskID := "test-task-lock-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	// Add task to queue
	sched.GetRedisClient().ZAdd(ctx, "scheduler:queue", &redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: taskID,
	})

	// First lock should succeed
	locked := sched.atomicLockAndPop(ctx, taskID)
	if !locked {
		t.Error("expected first lock to succeed")
	}

	// Second lock should fail (task already popped from queue)
	lockedAgain := sched.atomicLockAndPop(ctx, taskID)
	if lockedAgain {
		t.Error("expected second lock to fail")
	}
}

func TestConcurrentLocking(t *testing.T) {
	handlers := map[string]HandlerFunc{
		"test": func(ctx context.Context, taskID string, payload map[string]string) error {
			return nil
		},
	}

	sched, cleanup := setupTestScheduler(t, handlers)
	defer cleanup()

	ctx := context.Background()
	taskID := "test-task-concurrent-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	// Add task to queue
	sched.GetRedisClient().ZAdd(ctx, "scheduler:queue", &redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: taskID,
	})

	var successCount int
	var mu sync.Mutex

	// Try to lock from multiple goroutines
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if sched.atomicLockAndPop(ctx, taskID) {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	if successCount != 1 {
		t.Errorf("expected exactly 1 successful lock, got %d", successCount)
	}
}

func TestPoll(t *testing.T) {
	processedTasks := make(map[string]bool)
	var mu sync.Mutex

	handlers := map[string]HandlerFunc{
		"poll_test": func(ctx context.Context, taskID string, payload map[string]string) error {
			mu.Lock()
			processedTasks[taskID] = true
			mu.Unlock()
			return nil
		},
	}

	sched, cleanup := setupTestScheduler(t, handlers)
	defer cleanup()

	ctx := context.Background()

	// Create multiple tasks
	for i := 0; i < 5; i++ {
		taskID := fmt.Sprintf("poll-task-%d-%d", i, time.Now().UnixNano())
		taskKey := fmt.Sprintf("scheduler:task:%s", taskID)
		sched.GetRedisClient().HSet(ctx, taskKey, map[string]interface{}{
			"handler":     "poll_test",
			"payload":     fmt.Sprintf(`{"index":%d}`, i),
			"retry_count": "0",
		})

		sched.GetRedisClient().ZAdd(ctx, "scheduler:queue", &redis.Z{
			Score:  float64(time.Now().UnixNano()),
			Member: taskID,
		})
	}

	sched.Start()
	time.Sleep(500 * time.Millisecond)
	sched.Stop()

	mu.Lock()
	defer mu.Unlock()

	if len(processedTasks) != 5 {
		t.Errorf("expected 5 tasks to be processed, got %d", len(processedTasks))
	}
}

func TestRenewLock(t *testing.T) {
	handlers := map[string]HandlerFunc{
		"test": func(ctx context.Context, taskID string, payload map[string]string) error {
			return nil
		},
	}

	sched, cleanup := setupTestScheduler(t, handlers)
	defer cleanup()

	ctx := context.Background()
	lockKey := "scheduler:lock:test-renew-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	// Set initial lock with short TTL
	sched.GetRedisClient().Set(ctx, lockKey, "1", 2*time.Second)

	// Start renewal goroutine
	renewCtx, cancel := context.WithCancel(ctx)
	go sched.renewLock(renewCtx, lockKey)

	// Wait longer than initial TTL
	time.Sleep(3 * time.Second)

	// Lock should still exist due to renewal
	ttl, _ := sched.GetRedisClient().PTTL(ctx, lockKey).Result()
	if ttl <= 0 {
		t.Error("expected lock to be renewed")
	}

	cancel()
	time.Sleep(1 * time.Second)
}

func TestStartStop(t *testing.T) {
	handlers := map[string]HandlerFunc{
		"test": func(ctx context.Context, taskID string, payload map[string]string) error {
			return nil
		},
	}

	sched, cleanup := setupTestScheduler(t, handlers)
	defer cleanup()

	// Start and stop multiple times
	sched.Start()
	time.Sleep(100 * time.Millisecond)
	sched.Stop()

	sched.Start()
	time.Sleep(100 * time.Millisecond)
	sched.Stop()
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.RedisAddr != "localhost:6379" {
		t.Errorf("expected RedisAddr localhost:6379, got %s", cfg.RedisAddr)
	}

	if cfg.PollInterval != 100*time.Millisecond {
		t.Errorf("expected PollInterval 100ms, got %v", cfg.PollInterval)
	}

	if cfg.BatchSize != 1000 {
		t.Errorf("expected BatchSize 1000, got %d", cfg.BatchSize)
	}

	if cfg.LockTTL != 5*time.Minute {
		t.Errorf("expected LockTTL 5m, got %v", cfg.LockTTL)
	}

	if cfg.MaxRetries != 5 {
		t.Errorf("expected MaxRetries 5, got %d", cfg.MaxRetries)
	}
}
