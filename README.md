# Go Scheduler

A distributed task scheduler built with Go, Redis, and Prometheus metrics.

## Features

- **Distributed Task Scheduling**: Schedule tasks to be executed at specific times
- **Redis Backend**: Uses Redis sorted sets for efficient task queue management
- **Prometheus Metrics**: Comprehensive monitoring with worker-level metrics
- **HTTP API**: RESTful endpoints for task management and health checks
- **Graceful Shutdown**: Proper cleanup on SIGINT/SIGTERM
- **Lock Renewal**: Automatic lock renewal for long-running tasks
- **Dead Letter Queue**: Failed tasks are moved to dead letter queue after max retries
- **Redis Cluster Support**: Experimental support for Redis cluster mode

## Architecture

The scheduler uses a poll-based architecture where workers continuously poll Redis for due tasks. Tasks are stored in Redis sorted sets with execution time as the score, allowing efficient retrieval of due tasks.

### Key Components

1. **Scheduler**: Main component that polls for tasks and executes handlers
2. **Handlers**: User-defined functions that process tasks
3. **Redis**: Stores task data and manages the queue
4. **HTTP Server**: Exposes metrics and management endpoints

## Quick Start

### Prerequisites

- Go 1.21+
- Docker and Docker Compose (for development)
- Redis 7+ (or Redis Cluster)

### Development with Docker Compose

```bash
# Start Redis and the scheduler
docker-compose up -d

# View logs
docker-compose logs -f scheduler

# Stop services
docker-compose down
```

### Local Development

```bash
# Start Redis (required)
docker run -d --name redis -p 6379:6379 redis:7-alpine

# Build and run
go mod tidy
go run ./cmd/scheduler
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_ADDR` | `localhost:6379` | Redis address (comma-separated for cluster) |
| `POLL_INTERVAL` | `100ms` | How often to poll for due tasks |
| `BATCH_SIZE` | `1000` | Maximum tasks to fetch per poll |
| `LOCK_TTL` | `5m` | Lock timeout for task execution |
| `HTTP_ADDR` | `:8080` | HTTP server bind address |
| `AUTH_TOKEN` | `` | Bearer token for protected endpoints |
| `MAX_RETRIES` | `5` | Maximum retry attempts before dead letter |
| `USE_REDIS_CLUSTER` | `false` | Enable Redis cluster mode |

## API Endpoints

### Health Check
```bash
GET /health
```

### Prometheus Metrics
```bash
GET /metrics
```

### Retry Dead-Lettered Task
```bash
POST /tasks/{id}/retry
Authorization: Bearer <token>
```

## Usage Example

```go
package main

import (
    "context"
    "time"
    "github.com/example/scheduler/internal/scheduler"
)

func main() {
    cfg := scheduler.DefaultConfig()
    cfg.RedisAddr = "localhost:6379"

    // Define custom handlers
    handlers := map[string]scheduler.HandlerFunc{
        "send_email": func(ctx context.Context, taskID string, payload map[string]string) error {
            // Your email sending logic here
            return nil
        },
        "process_payment": func(ctx context.Context, taskID string, payload map[string]string) error {
            // Your payment processing logic here
            return nil
        },
    }

    // Create and start scheduler
    sched := scheduler.NewScheduler(cfg, handlers)
    sched.Start()
    defer sched.Stop()

    // Schedule a task
    ctx := context.Background()
    err := sched.Schedule(
        ctx,
        "task-123",
        "send_email",
        `{"to": "user@example.com"}`,
        time.Now().Add(5*time.Minute),
    )
    if err != nil {
        panic(err)
    }

    // Keep running
    select {}
}
```

## Metrics

The scheduler exposes the following Prometheus metrics:

- `scheduler_tasks_scheduled_total`: Total tasks scheduled
- `scheduler_tasks_executed_total`: Total tasks executed (with success/failure label)
- `scheduler_tasks_failed_total`: Total failed tasks
- `scheduler_tasks_deadlettered_total`: Tasks moved to dead letter queue
- `scheduler_polls_total`: Total poll operations
- `scheduler_poll_duration_seconds`: Poll operation duration
- `scheduler_queue_length`: Current queue length
- `scheduler_dead_length`: Dead letter queue length
- `scheduler_locks_active`: Active locks held
- `scheduler_handler_duration_seconds`: Handler execution duration

All metrics include a `worker_id` label for multi-worker deployments.

## Redis Data Structures

- `scheduler:queue`: Sorted set of pending tasks (score = execution time in nanoseconds)
- `scheduler:dead`: Sorted set of dead-lettered tasks
- `scheduler:task:{id}`: Hash containing task data (handler, payload, retry_count, etc.)
- `scheduler:lock:{id}`: Lock key for task execution (prevents duplicate processing)

## Building

```bash
# Build binary
go build -o scheduler ./cmd/scheduler

# Build Docker image
docker build -t scheduler .
```

## Running in Production

### Single Instance

```bash
REDIS_ADDR=redis-cluster:6379 \
AUTH_TOKEN=your-secret-token \
./scheduler
```

### Multiple Workers

Run multiple instances with the same Redis backend. Each worker will have a unique ID and will coordinate through Redis locks.

```bash
# Worker 1
REDIS_ADDR=redis:6379 HTTP_ADDR=:8081 ./scheduler

# Worker 2
REDIS_ADDR=redis:6379 HTTP_ADDR=:8082 ./scheduler
```

### Kubernetes

Use the provided Dockerfile and deploy with appropriate environment variables. Consider using a StatefulSet or Deployment with multiple replicas.

## License

MIT
