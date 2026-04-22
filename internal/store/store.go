package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Task represents a scheduled task
type Task struct {
	ID          string            `json:"id"`
	Handler     string            `json:"handler"`
	Payload     map[string]string `json:"payload"`
	ScheduledAt int64             `json:"scheduled_at"` // Unix nano
	CreatedAt   int64             `json:"created_at"`
	RetryCount  int               `json:"retry_count"`
	MaxRetries  int               `json:"max_retries"`
	LastError   string            `json:"last_error,omitempty"`
}

// Store provides Redis-backed storage for tasks
type Store struct {
	client   redis.UniversalClient
	taskTTL  time.Duration
	queueKey string
	schedKey string
	deadKey  string
	prefix   string
}

// NewStore creates a new Redis store
func NewStore(client redis.UniversalClient, taskTTL time.Duration) *Store {
	return &Store{
		client:   client,
		taskTTL:  taskTTL,
		queueKey: "scheduler:queue",
		schedKey: "scheduler:scheduled",
		deadKey:  "scheduler:dead",
		prefix:   "scheduler:task:",
	}
}

// CreateTask creates a new task in the scheduled queue
func (s *Store) CreateTask(ctx context.Context, handler string, payload map[string]string, scheduledAt time.Time) (*Task, error) {
	task := &Task{
		ID:          uuid.New().String(),
		Handler:     handler,
		Payload:     payload,
		ScheduledAt: scheduledAt.UnixNano(),
		CreatedAt:   time.Now().UnixNano(),
		RetryCount:  0,
		MaxRetries:  5,
	}

	// Store task metadata
	taskKey := s.taskKey(task.ID)
	data, err := json.Marshal(task)
	if err != nil {
		return nil, fmt.Errorf("marshal task: %w", err)
	}

	pipe := s.client.TxPipeline()
	pipe.Set(ctx, taskKey, string(data), s.taskTTL)
	pipe.ZAdd(ctx, s.schedKey, redis.Z{
		Score:  float64(task.ScheduledAt),
		Member: task.ID,
	})
	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("redis transaction: %w", err)
	}

	log.Printf("[STORE] Created task %s for handler %s at %v", task.ID, handler, scheduledAt)
	return task, nil
}

// GetTask retrieves a task by ID
func (s *Store) GetTask(ctx context.Context, taskID string) (*Task, error) {
	taskKey := s.taskKey(taskID)
	data, err := s.client.Get(ctx, taskKey).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get task: %w", err)
	}

	var task Task
	if err := json.Unmarshal([]byte(data), &task); err != nil {
		return nil, fmt.Errorf("unmarshal task: %w", err)
	}

	return &task, nil
}

// GetDueTasks returns tasks that are due for execution (up to limit)
func (s *Store) GetDueTasks(ctx context.Context, limit int) ([]string, error) {
	nowNs := time.Now().UnixNano()
	opts := redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%d", nowNs),
		Count: int64(limit),
	}

	taskIDs, err := s.client.ZRangeByScore(ctx, s.queueKey, &opts).Result()
	if err != nil {
		return nil, fmt.Errorf("get due tasks: %w", err)
	}

	return taskIDs, nil
}

// MoveToQueue moves a task from scheduled to active queue
func (s *Store) MoveToQueue(ctx context.Context, taskID string, executeAt time.Time) error {
	pipe := s.client.TxPipeline()
	pipe.ZRem(ctx, s.schedKey, taskID)
	pipe.ZAdd(ctx, s.queueKey, redis.Z{
		Score:  float64(executeAt.UnixNano()),
		Member: taskID,
	})
	_, err := pipe.Exec(ctx)
	return err
}

// AcquireLock attempts to acquire a lock for a task atomically
func (s *Store) AcquireLock(ctx context.Context, taskID string, ttl time.Duration) (bool, error) {
	lockKey := s.lockKey(taskID)
	nowNs := time.Now().UnixNano()

	// Check if task is due
	score, err := s.client.ZScore(ctx, s.queueKey, taskID).Result()
	if err == redis.Nil || score > float64(nowNs) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check score: %w", err)
	}

	// Try to acquire lock
	acquired, err := s.client.SetNX(ctx, lockKey, "1", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("set lock: %w", err)
	}

	if !acquired {
		return false, nil
	}

	// Remove from queue atomically after lock
	_, err = s.client.ZRem(ctx, s.queueKey, taskID).Result()
	if err != nil {
		// Release lock if ZREM fails
		s.client.Del(ctx, lockKey)
		return false, fmt.Errorf("remove from queue: %w", err)
	}

	return true, nil
}

// RenewLock extends the lock TTL
func (s *Store) RenewLock(ctx context.Context, taskID string, ttl time.Duration) error {
	lockKey := s.lockKey(taskID)
	return s.client.PExpire(ctx, lockKey, ttl).Err()
}

// ReleaseLock removes the lock for a task
func (s *Store) ReleaseLock(ctx context.Context, taskID string) error {
	lockKey := s.lockKey(taskID)
	return s.client.Del(ctx, lockKey).Err()
}

// UpdateTask persists task changes
func (s *Store) UpdateTask(ctx context.Context, task *Task) error {
	taskKey := s.taskKey(task.ID)
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}
	return s.client.Set(ctx, taskKey, string(data), s.taskTTL).Err()
}

// MoveToDeadLetter moves a task to the dead letter queue
func (s *Store) MoveToDeadLetter(ctx context.Context, taskID string) error {
	nowNs := time.Now().UnixNano()
	pipe := s.client.TxPipeline()
	pipe.ZRem(ctx, s.queueKey, taskID)
	pipe.ZAdd(ctx, s.deadKey, redis.Z{
		Score:  float64(nowNs),
		Member: taskID,
	})
	_, err := pipe.Exec(ctx)
	return err
}

// RetryTask moves a task from dead letter back to queue
func (s *Store) RetryTask(ctx context.Context, taskID string) error {
	task, err := s.GetTask(ctx, taskID)
	if err != nil {
		return err
	}
	if task == nil {
		return fmt.Errorf("task not found")
	}

	// Check if in dead letter
	_, err = s.client.ZScore(ctx, s.deadKey, taskID).Result()
	if err == redis.Nil {
		return fmt.Errorf("task not in dead letter queue")
	}
	if err != nil {
		return fmt.Errorf("check dead letter: %w", err)
	}

	pipe := s.client.TxPipeline()
	pipe.ZRem(ctx, s.deadKey, taskID)
	pipe.ZAdd(ctx, s.queueKey, redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: taskID,
	})
	pipe.HSet(ctx, s.taskKey(taskID), "retry_count", "0")
	_, err = pipe.Exec(ctx)
	return err
}

// DeleteTask removes a task completely
func (s *Store) DeleteTask(ctx context.Context, taskID string) error {
	pipe := s.client.TxPipeline()
	pipe.Del(ctx, s.taskKey(taskID))
	pipe.ZRem(ctx, s.queueKey, taskID)
	pipe.ZRem(ctx, s.schedKey, taskID)
	pipe.ZRem(ctx, s.deadKey, taskID)
	_, err := pipe.Exec(ctx)
	return err
}

// QueueLength returns the number of tasks in the active queue
func (s *Store) QueueLength(ctx context.Context) (int64, error) {
	return s.client.ZCard(ctx, s.queueKey).Result()
}

// DeadLetterLength returns the number of tasks in the dead letter queue
func (s *Store) DeadLetterLength(ctx context.Context) (int64, error) {
	return s.client.ZCard(ctx, s.deadKey).Result()
}

// ScheduledLength returns the number of tasks in the scheduled queue
func (s *Store) ScheduledLength(ctx context.Context) (int64, error) {
	return s.client.ZCard(ctx, s.schedKey).Result()
}

func (s *Store) taskKey(taskID string) string {
	return s.prefix + taskID
}

func (s *Store) lockKey(taskID string) string {
	return "scheduler:lock:" + taskID
}

// NewRedisClient creates a Redis client from config
func NewRedisClient(addr string, useCluster bool) (redis.UniversalClient, error) {
	if useCluster {
		addrs := strings.Split(addr, ",")
		client := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: addrs,
		})
		if err := client.Ping(context.Background()).Err(); err != nil {
			return nil, fmt.Errorf("cluster ping failed: %w", err)
		}
		return client, nil
	}

	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}
	return client, nil
}
