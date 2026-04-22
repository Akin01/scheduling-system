package common

import (
	"fmt"
	"os"
	"time"
)

// Config holds shared configuration for all services
type Config struct {
	RedisAddr     string        `json:"redis_addr"`
	RedisURL      string        `json:"redis_url"`
	PollInterval  time.Duration `json:"poll_interval"`
	BatchSize     int           `json:"batch_size"`
	LockTTL       time.Duration `json:"lock_ttl"`
	HTTPAddr      string        `json:"http_addr"`
	TaskTTL       time.Duration `json:"task_ttl"`
	AuthToken     string        `json:"auth_token"`
	MaxRetries    int           `json:"max_retries"`
	UseCluster    bool          `json:"use_redis_cluster"`
	LogLevel      string        `json:"log_level"`
	ServiceName   string        `json:"service_name"`
}

// DefaultConfig returns default configuration values
func DefaultConfig() Config {
	return Config{
		RedisAddr:    "localhost:6379",
		RedisURL:     "",
		PollInterval: 100 * time.Millisecond,
		BatchSize:    100,
		LockTTL:      5 * time.Minute,
		HTTPAddr:     ":8080",
		TaskTTL:      30 * 24 * time.Hour,
		AuthToken:    "",
		MaxRetries:   5,
		UseCluster:   false,
		LogLevel:     "info",
		ServiceName:  "unknown",
	}
}

// LoadConfig loads configuration from environment variables
func LoadConfig(serviceName string) Config {
	cfg := DefaultConfig()
	cfg.ServiceName = serviceName

	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		cfg.RedisAddr = addr
	}
	if url := os.Getenv("REDIS_URL"); url != "" {
		cfg.RedisURL = url
	}
	if interval := os.Getenv("POLL_INTERVAL"); interval != "" {
		if d, err := time.ParseDuration(interval); err == nil {
			cfg.PollInterval = d
		}
	}
	if batchSize := os.Getenv("BATCH_SIZE"); batchSize != "" {
		if n, err := fmt.Sscanf(batchSize, "%d", &cfg.BatchSize); n == 1 && err == nil {
			// parsed successfully
		}
	}
	if ttl := os.Getenv("LOCK_TTL"); ttl != "" {
		if d, err := time.ParseDuration(ttl); err == nil {
			cfg.LockTTL = d
		}
	}
	if addr := os.Getenv("HTTP_ADDR"); addr != "" {
		cfg.HTTPAddr = addr
	}
	if ttl := os.Getenv("TASK_TTL"); ttl != "" {
		if d, err := time.ParseDuration(ttl); err == nil {
			cfg.TaskTTL = d
		}
	}
	if token := os.Getenv("AUTH_TOKEN"); token != "" {
		cfg.AuthToken = token
	}
	if retries := os.Getenv("MAX_RETRIES"); retries != "" {
		if n, err := fmt.Sscanf(retries, "%d", &cfg.MaxRetries); n == 1 && err == nil {
			// parsed successfully
		}
	}
	if cluster := os.Getenv("USE_REDIS_CLUSTER"); cluster == "true" || cluster == "1" {
		cfg.UseCluster = true
	}
	if level := os.Getenv("LOG_LEVEL"); level != "" {
		cfg.LogLevel = level
	}

	return cfg
}

// GetRedisAddr returns the Redis connection address, preferring REDIS_URL
func (c *Config) GetRedisAddr() string {
	if c.RedisURL != "" {
		return c.RedisURL
	}
	return c.RedisAddr
}
