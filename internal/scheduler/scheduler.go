package scheduler

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
)

// HandlerFunc defines the signature for task handlers
type HandlerFunc func(ctx context.Context, taskID string, payload map[string]string) error

type Config struct {
	RedisAddr     string        `json:"redis_addr"`
	PollInterval  time.Duration `json:"poll_interval"`
	BatchSize     int           `json:"batch_size"`
	LockTTL       time.Duration `json:"lock_ttl"`
	HTTPAddr      string        `json:"http_addr"`
	TaskTTL       time.Duration `json:"task_ttl"`
	AuthToken     string        `json:"auth_token"`
	MaxRetries    int           `json:"max_retries"`
	UseCluster    bool          `json:"use_redis_cluster"`
}

func DefaultConfig() Config {
	return Config{
		RedisAddr:    "localhost:6379",
		PollInterval: 100 * time.Millisecond,
		BatchSize:    1000,
		LockTTL:      5 * time.Minute,
		HTTPAddr:     ":8080",
		TaskTTL:      30 * 24 * time.Hour,
		AuthToken:    "",
		MaxRetries:   5,
		UseCluster:   false,
	}
}



var (
	tasksScheduled    = promauto.NewCounterVec(prometheus.CounterOpts{Name: "scheduler_tasks_scheduled_total"}, []string{"worker_id"})
	tasksExecuted     = promauto.NewCounterVec(prometheus.CounterOpts{Name: "scheduler_tasks_executed_total"}, []string{"worker_id", "handler", "success"})
	tasksFailed       = promauto.NewCounterVec(prometheus.CounterOpts{Name: "scheduler_tasks_failed_total"}, []string{"worker_id", "handler"})
	tasksDeadLettered = promauto.NewCounterVec(prometheus.CounterOpts{Name: "scheduler_tasks_deadlettered_total"}, []string{"worker_id"})
	pollsTotal        = promauto.NewCounterVec(prometheus.CounterOpts{Name: "scheduler_polls_total"}, []string{"worker_id"})
	pollDuration      = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "scheduler_poll_duration_seconds"}, []string{"worker_id"})
	queueLength       = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "scheduler_queue_length"}, []string{"worker_id"})
	deadLength        = promauto.NewGauge(prometheus.GaugeOpts{Name: "scheduler_dead_length"})
	locksActive       = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "scheduler_locks_active"}, []string{"worker_id"})
	handlerDuration   = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "scheduler_handler_duration_seconds"}, []string{"worker_id", "handler"})
)

type Scheduler struct {
	rdb        redis.UniversalClient
	handlers   map[string]HandlerFunc
	config     Config
	workerID   string
	metrics    *promMetrics
	luaScripts struct {
		lockAndPop string
	}
	pollCount int32
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

type promMetrics struct {
	tasksScheduled    prometheus.Counter
	tasksExecuted     *prometheus.CounterVec
	tasksFailed       *prometheus.CounterVec
	tasksDeadLettered prometheus.Counter
	pollsTotal        prometheus.Counter
	pollDuration      prometheus.Observer
	queueLength       prometheus.Gauge
	locksActive       prometheus.Gauge
	handlerDuration   *prometheus.HistogramVec
	workerID          string
}

func NewScheduler(cfg Config, handlers map[string]HandlerFunc) *Scheduler {
	hostname, _ := os.Hostname()
	pid := os.Getpid()
	workerID := fmt.Sprintf("%s-%d-%s", hostname, pid, uuid.New().String()[:8])
	log.Printf("INFO: worker init, worker_id=%s",("worker init", "worker_id", workerID)

	var rdb redis.UniversalClient
	if cfg.UseCluster {
		rdb = redis.NewClusterClient(&redis.ClusterOptions{Addrs: splitAddresses(cfg.RedisAddr)})
	} else {
		rdb = redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	}

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		panic(fmt.Sprintf("redis ping failed: %v", err))
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &Scheduler{
		rdb:      rdb,
		handlers: handlers,
		config:   cfg,
		workerID: workerID,
		ctx:      ctx,
		cancel:   cancel,
	}

	s.metrics = &promMetrics{
		tasksScheduled:    tasksScheduled.WithLabelValues(workerID),
		tasksExecuted:     tasksExecuted.MustCurryWith(map[string]string{"worker_id": workerID}),
		tasksFailed:       tasksFailed.MustCurryWith(map[string]string{"worker_id": workerID}),
		tasksDeadLettered: tasksDeadLettered.WithLabelValues(workerID),
		pollsTotal:        pollsTotal.WithLabelValues(workerID),
		pollDuration:      pollDuration.WithLabelValues(workerID),
		queueLength:       queueLength.WithLabelValues(workerID),
		locksActive:       locksActive.WithLabelValues(workerID),
		handlerDuration:   handlerDuration.MustCurryWith(map[string]string{"worker_id": workerID}),
		workerID:          workerID,
	}

	s.loadLuaScripts()
	queueLength.WithLabelValues(workerID).Set(0)
	return s
}

func splitAddresses(addr string) []string {
	result := make([]string, 0)
	for _, a := range splitByComma(addr) {
		trimmed := trimSpace(a)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func splitByComma(s string) []string {
	result := make([]string, 0)
	current := ""
	for _, c := range s {
		if c == ',' {
			result = append(result, current)
			current = ""
		} else {
			current += string(c)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}

func trimSpace(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}

func (s *Scheduler) loadLuaScripts() {
	script := `
local lock_key = KEYS[1]
local queue_key = KEYS[2]
local task_id = ARGV[1]
local now_ns = ARGV[2]
local ttl_ms = ARGV[3]
local zscore = redis.call('ZSCORE', queue_key, task_id)
if not zscore or tonumber(zscore) > tonumber(now_ns) then
	return 0
end
if redis.call('SET', lock_key, '1', 'PX', ttl_ms, 'NX') then
	redis.call('ZREM', queue_key, task_id)
	return 1
else
	return 0
end
	`
	s.luaScripts.lockAndPop = script
}

func (s *Scheduler) atomicLockAndPop(ctx context.Context, taskID string) bool {
	nowNs := time.Now().UnixNano()
	res, err := s.rdb.EvalSha(ctx, s.rdb.ScriptLoad(ctx, s.luaScripts.lockAndPop).Val(), []string{
		fmt.Sprintf("scheduler:lock:%s", taskID),
		"scheduler:queue",
	}, taskID, strconv.FormatInt(nowNs, 10), strconv.FormatInt(int64(s.config.LockTTL.Milliseconds()), 10)).Result()

	if err != nil {
		log.Printf("ERROR: ("lua lock-pop failed", "taskID", taskID, "err", err)
		return false
	}

	val, ok := res.(int64)
	return ok && val == 1
}

func (s *Scheduler) processTask(ctx context.Context, taskID string) {
	if !s.atomicLockAndPop(ctx, taskID) {
		return
	}

	s.metrics.locksActive.Inc()
	defer func() {
		lockKey := fmt.Sprintf("scheduler:lock:%s", taskID)
		s.rdb.Del(ctx, lockKey)
		s.metrics.locksActive.Dec()
	}()

	renewCtx, renewCancel := context.WithCancel(ctx)
	defer renewCancel()
	go s.renewLock(renewCtx, fmt.Sprintf("scheduler:lock:%s", taskID))

	taskKey := fmt.Sprintf("scheduler:task:%s", taskID)
	hash, err := s.rdb.HGetAll(ctx, taskKey).Result()
	if err != nil || len(hash) == 0 {
		return
	}

	handlerName := hash["handler"]
	payload := hash["payload"]
	retryCount := 0
	if rc := hash["retry_count"]; rc != "" {
		if parsed, parseErr := strconv.Atoi(rc); parseErr == nil {
			retryCount = parsed
		}
	}

	log.Printf("INFO: worker init, worker_id=%s",("processing", "worker_id", s.workerID, "taskID", taskID, "handler", handlerName)

	start := time.Now()
	var handlerErr error
	if handler, exists := s.handlers[handlerName]; exists {
		handlerErr = handler(ctx, taskID, map[string]string{"payload": payload})
	} else {
		handlerErr = fmt.Errorf("handler not found: %s", handlerName)
	}
	duration := time.Since(start)

	s.metrics.handlerDuration.WithLabelValues(handlerName).Observe(duration.Seconds())

	if handlerErr != nil {
		log.Printf("ERROR: ("handler failed", "worker_id", s.workerID, "taskID", taskID, "handler", handlerName, "err", handlerErr)
		s.metrics.tasksFailed.WithLabelValues(handlerName).Inc()
		s.metrics.tasksExecuted.WithLabelValues(handlerName, "false").Inc()

		if retryCount < s.config.MaxRetries {
			s.handleRetry(ctx, taskID, retryCount+1)
		} else {
			s.deadLetter(ctx, taskID)
		}
	} else {
		log.Printf("INFO: worker init, worker_id=%s",("task completed", "worker_id", s.workerID, "taskID", taskID, "handler", handlerName)
		s.metrics.tasksExecuted.WithLabelValues(handlerName, "true").Inc()
		s.rdb.Del(ctx, taskKey)
	}
}

func (s *Scheduler) renewLock(ctx context.Context, lockKey string) {
	ticker := time.NewTicker(s.config.LockTTL / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.rdb.PExpire(ctx, lockKey, s.config.LockTTL)
		}
	}
}

func (s *Scheduler) handleRetry(ctx context.Context, taskID string, retryCount int) {
	taskKey := fmt.Sprintf("scheduler:task:%s", taskID)
	s.rdb.HSet(ctx, taskKey, "retry_count", strconv.Itoa(retryCount))
	s.rdb.ZAdd(ctx, "scheduler:queue", &redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: taskID,
	})
}

func (s *Scheduler) deadLetter(ctx context.Context, taskID string) {
	log.Printf("WARN: ("dead lettering task", "worker_id", s.workerID, "taskID", taskID)
	s.metrics.tasksDeadLettered.Inc()

	taskKey := fmt.Sprintf("scheduler:task:%s", taskID)
	s.rdb.ZAdd(ctx, "scheduler:dead", &redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: taskID,
	})
	s.rdb.Del(ctx, taskKey)
}

func (s *Scheduler) poll(ctx context.Context) {
	start := time.Now()
	defer func() {
		s.metrics.pollDuration.Observe(time.Since(start).Seconds())
		s.metrics.pollsTotal.Inc()
	}()

	nowNs := time.Now().UnixNano()
	opts := &redis.ZRangeByScore{Min: "-inf", Max: strconv.FormatInt(nowNs, 10), Count: int64(s.config.BatchSize)}
	taskIDs, err := s.rdb.ZRangeByScore(ctx, "scheduler:queue", opts).Result()
	if err != nil {
		log.Printf("ERROR: ("poll failed", "worker_id", s.workerID, "err", err)
		return
	}

	for _, taskID := range taskIDs {
		go s.processTask(ctx, taskID)
	}

	if atomic.AddInt32(&s.pollCount, 1)%10 == 0 {
		ql, _ := s.rdb.ZCard(ctx, "scheduler:queue").Result()
		dl, _ := s.rdb.ZCard(ctx, "scheduler:dead").Result()
		s.metrics.queueLength.Set(float64(ql) / float64(runtime.NumCPU()))
		deadLength.Set(float64(dl))
	}
}

func (s *Scheduler) Schedule(ctx context.Context, taskID string, handlerName string, payload string, executeAt time.Time) error {
	taskKey := fmt.Sprintf("scheduler:task:%s", taskID)
	exists, _ := s.rdb.Exists(ctx, taskKey).Result()
	if exists > 0 {
		return fmt.Errorf("task already exists: %s", taskID)
	}

	s.rdb.HSet(ctx, taskKey, map[string]interface{}{
		"handler":    handlerName,
		"payload":    payload,
		"created_at": time.Now().Format(time.RFC3339),
		"retry_count": "0",
	})

	s.rdb.ZAdd(ctx, "scheduler:queue", &redis.Z{
		Score:  float64(executeAt.UnixNano()),
		Member: taskID,
	})

	s.metrics.tasksScheduled.Inc()
	log.Printf("INFO: worker init, worker_id=%s",("task scheduled", "worker_id", s.workerID, "taskID", taskID, "handler", handlerName, "execute_at", executeAt)
	return nil
}

func (s *Scheduler) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.config.PollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				s.poll(s.ctx)
			}
		}
	}()
}

func (s *Scheduler) Stop() {
	s.cancel()
	s.wg.Wait()
}

func (s *Scheduler) RetryTask(ctx context.Context, taskID string) error {
	pipe := s.rdb.TxPipeline()
	pipe.ZRem(ctx, "scheduler:dead", taskID)
	pipe.ZAdd(ctx, "scheduler:queue", &redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: taskID,
	})
	pipe.HSet(ctx, fmt.Sprintf("scheduler:task:%s", taskID), "retry_count", "0")
	_, err := pipe.Exec(ctx)
	return err
}

func (s *Scheduler) GetRedisClient() redis.UniversalClient {
	return s.rdb
}
