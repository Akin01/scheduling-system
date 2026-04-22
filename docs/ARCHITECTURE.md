# Distributed Task Scheduler Architecture

## Overview

This system implements a **decoupled, microservices-based architecture** for distributed task scheduling. The system is split into three distinct services that communicate exclusively through Redis, enabling independent scaling, deployment, and failure isolation.

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   API       │      │  Scheduler  │      │   Worker    │
│  Service    │      │   Service   │      │   Service   │
│             │      │             │      │             │
│ - HTTP REST │      │ - Cron Jobs │      │ - Polling   │
│ - Auth      │      │ - Queue Mgmt│      │ - Execution │
│ - Retry UI  │      │ - Locking   │      │ - Handlers  │
└──────┬──────┘      └──────┬──────┘      └──────┬──────┘
       │                    │                    │
       └────────────────────┼────────────────────┘
                            │
                   ┌────────▼────────┐
                   │     Redis       │
                   │  (Shared State) │
                   │                 │
                   │ - Queue (ZSET)  │
                   │ - Tasks (HASH)  │
                   │ - Locks (KEYS)  │
                   │ - Dead Letter   │
                   └─────────────────┘
```

## Services

### 1. API Service (`cmd/api`)

**Responsibility**: External interface for task management and monitoring.

**Features**:
- RESTful HTTP API for task CRUD operations
- Authentication middleware
- Manual retry endpoint for dead-lettered tasks
- Health check endpoint
- Prometheus metrics exposure

**Endpoints**:
- `POST /tasks` - Schedule a new task
- `GET /tasks/:id` - Get task details
- `POST /tasks/:id/retry` - Retry a failed task (auth protected)
- `GET /health` - Health check
- `GET /metrics` - Prometheus metrics

**Scaling**: Stateless, can be scaled horizontally behind a load balancer.

### 2. Scheduler Service (`cmd/scheduler`)

**Responsibility**: Time-based task orchestration and queue management.

**Features**:
- Cron-based job scheduling
- Moves due tasks from scheduled set to active queue
- Atomic locking with Lua scripts
- Lock renewal for long-running tasks
- No task execution logic (pure coordinator)

**Key Operations**:
- Scans for tasks where `scheduled_at <= now`
- Uses atomic Lua script to lock and move tasks
- Maintains distributed locks with TTL
- Handles task rescheduling based on cron expressions

**Scaling**: Can run multiple instances; atomic locking prevents duplicate processing.

### 3. Worker Service (`cmd/worker`)

**Responsibility**: Task execution and result handling.

**Features**:
- Polls active queue for locked tasks
- Executes registered handlers
- Manages retry logic with exponential backoff
- Moves failed tasks to dead letter queue after max retries
- Reports execution metrics

**Execution Flow**:
1. Poll Redis for available tasks
2. Acquire lock via atomic operation
3. Execute handler function
4. On success: cleanup task
5. On failure: increment retry count or move to DLQ

**Scaling**: Scale based on task volume and handler complexity. Each worker is independent.

## Data Model (Redis)

### Keys Structure

| Key Pattern | Type | Description |
|-------------|------|-------------|
| `scheduler:queue` | ZSET | Active task queue (score = execution time ns) |
| `scheduler:scheduled` | ZSET | Future tasks (score = scheduled time ns) |
| `scheduler:dead` | ZSET | Dead letter queue |
| `scheduler:task:{id}` | HASH | Task metadata and payload |
| `scheduler:lock:{id}` | STRING | Distributed lock (TTL-based) |

### Task Hash Structure

```json
{
  "id": "uuid",
  "handler": "handler_name",
  "payload": "{\"key\": \"value\"}",
  "scheduled_at": "1234567890",
  "created_at": "1234567890",
  "retry_count": "0",
  "max_retries": "5",
  "last_error": "error message"
}
```

## Communication Patterns

### Producer-Consumer (API → Scheduler → Worker)

1. **API** creates task in `scheduler:scheduled` ZSET
2. **Scheduler** moves due tasks to `scheduler:queue`
3. **Worker** polls `scheduler:queue`, executes, and cleans up

### Distributed Locking

```lua
-- Atomic lock acquisition and queue removal
local lock_key = KEYS[1]
local queue_key = KEYS[2]
local task_id = ARGV[1]
local now_ns = ARGV[2]
local ttl_ms = ARGV[3]

-- Check if task is due
local zscore = redis.call('ZSCORE', queue_key, task_id)
if not zscore or tonumber(zscore) > tonumber(now_ns) then
    return 0
end

-- Try to acquire lock
if redis.call('SET', lock_key, '1', 'PX', ttl_ms, 'NX') then
    redis.call('ZREM', queue_key, task_id)
    return 1
else
    return 0
end
```

### Lock Renewal

Workers spawn a goroutine to renew locks every `LockTTL/3`:
```go
ticker := time.NewTicker(s.config.LockTTL / 3)
for {
    select {
    case <-ctx.Done():
        return
    case <-ticker.C:
        redis.PExpire(lockKey, LockTTL)
    }
}
```

## Configuration

All services share common configuration via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_URL` | Redis connection URL | `redis://localhost:6379` |
| `REDIS_ADDR` | Legacy Redis address (fallback) | `localhost:6379` |
| `HTTP_ADDR` | API service bind address | `:8080` |
| `AUTH_TOKEN` | API authentication token | `` |
| `LOG_LEVEL` | Logging level | `info` |

Service-specific variables:

| Variable | Service | Description | Default |
|----------|---------|-------------|---------|
| `POLL_INTERVAL` | Worker | Queue polling frequency | `100ms` |
| `BATCH_SIZE` | Worker | Tasks per poll cycle | `100` |
| `LOCK_TTL` | Scheduler/Worker | Lock expiration time | `5m` |
| `TASK_TTL` | All | Task retention period | `30d` |
| `MAX_RETRIES` | Worker | Max retry attempts | `5` |
| `USE_REDIS_CLUSTER` | All | Enable cluster mode | `false` |

## Deployment Topology

### Development (Docker Compose)

```yaml
services:
  redis:
    image: redis:7-alpine
    
  api:
    build: ./cmd/api
    ports: ["8080:8080"]
    depends_on: [redis]
    
  scheduler:
    build: ./cmd/scheduler
    depends_on: [redis]
    
  worker:
    build: ./cmd/worker
    replicas: 3
    depends_on: [redis]
```

### Production (Kubernetes)

```yaml
# API Deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
spec:
  replicas: 2
  template:
    spec:
      containers:
      - name: api
        image: akin01/reschedule-api:latest
        env:
        - name: REDIS_URL
          valueFrom:
            secretKeyRef:
              name: redis-secret
              key: url

# Scheduler Deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: scheduler
spec:
  replicas: 2  # Multiple for HA

# Worker Deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: worker
spec:
  replicas: 10  # Scale based on queue depth
  autoscaling:
    minReplicas: 2
    maxReplicas: 50
    targetCPUUtilization: 70%
```

## Failure Modes & Recovery

### Service Failures

| Service | Impact | Recovery |
|---------|--------|----------|
| API | Cannot create/retry tasks | Restart instance, LB redirects |
| Scheduler | Tasks not moved to queue | Restart, locks expire automatically |
| Worker | Tasks not executed | Scale up workers, tasks remain in queue |

### Redis Failure

- **Impact**: Complete system outage
- **Mitigation**: Use Redis Cluster or Sentinel
- **Recovery**: Automatic reconnection with exponential backoff

### Network Partitions

- Workers hold locks until TTL expires
- Scheduler detects partition and pauses scheduling
- API returns 503 for write operations

## Observability

### Metrics (Prometheus)

All services expose:
- `scheduler_tasks_scheduled_total{service,worker_id}`
- `scheduler_tasks_executed_total{service,worker_id,handler,success}`
- `scheduler_tasks_failed_total{service,worker_id,handler}`
- `scheduler_tasks_deadlettered_total{service,worker_id}`
- `scheduler_poll_duration_seconds{service,worker_id}`
- `scheduler_queue_length{service,worker_id}`
- `scheduler_locks_active{service,worker_id}`

### Logging

Structured JSON logs with fields:
- `service`: api|scheduler|worker
- `worker_id`: Unique instance identifier
- `task_id`: Task being processed
- `handler`: Handler name
- `level`: info|warn|error

### Tracing

Future enhancement: OpenTelemetry integration for distributed tracing across services.

## Security Considerations

1. **Authentication**: API endpoints protected by Bearer token
2. **Network Isolation**: Services communicate only via Redis (private network)
3. **Least Privilege**: Redis ACLs for service-specific permissions
4. **Secrets Management**: Environment variables or Kubernetes Secrets for sensitive config

## Performance Characteristics

| Metric | Target | Notes |
|--------|--------|-------|
| Task Latency | < 1s | From due time to execution start |
| Throughput | 10k tasks/s | With 10 workers, batch size 100 |
| Lock Contention | < 1% | With proper batch sizing |
| Memory Usage | < 100MB/service | Depends on batch size |

## Future Enhancements

1. **Priority Queues**: Add priority field to task scoring
2. **Rate Limiting**: Per-handler rate limits in worker
3. **Task Dependencies**: DAG-based task execution
4. **Webhooks**: Callback URLs on task completion
5. **Persistent Storage**: Archive completed tasks to PostgreSQL
6. **UI Dashboard**: Real-time queue monitoring and task inspection
