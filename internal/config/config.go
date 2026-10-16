// Package config loads settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"
)

// Config is the service configuration. See .env.example for each variable.
type Config struct {
	DatabaseURL     string
	HTTPAddr        string
	LogLevel        slog.Level
	ShutdownTimeout time.Duration
	SourcesFile     string
	IngestSecret    string
	QueueEndpoint   string
	QueueRegion     string
	QueueAccessKey  string
	QueueSecretKey  string
	QueueName       string
	DLQName         string
	QueueVisibility int
}

// Load reads the environment. DATABASE_URL is required.
func Load() (Config, error) {
	c := Config{
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		HTTPAddr:     envOr("HTTP_ADDR", ":8080"),
		SourcesFile:  envOr("SOURCES_FILE", "sources.json"),
		IngestSecret: os.Getenv("INGEST_SECRET"),
	}
	if c.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is required")
	}
	if err := c.LogLevel.UnmarshalText([]byte(envOr("LOG_LEVEL", "info"))); err != nil {
		return Config{}, fmt.Errorf("LOG_LEVEL: %w", err)
	}
	timeout, err := time.ParseDuration(envOr("SHUTDOWN_TIMEOUT", "10s"))
	if err != nil {
		return Config{}, fmt.Errorf("SHUTDOWN_TIMEOUT: %w", err)
	}
	c.ShutdownTimeout = timeout
	c.QueueEndpoint = os.Getenv("AWS_ENDPOINT_URL")
	c.QueueRegion = envOr("AWS_REGION", "us-east-1")
	c.QueueAccessKey = os.Getenv("AWS_ACCESS_KEY_ID")
	c.QueueSecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	c.QueueName = envOr("QUEUE_NAME", "pgland-sync")
	c.DLQName = envOr("DLQ_NAME", "pgland-sync-dlq")
	visibility, err := strconv.Atoi(envOr("QUEUE_VISIBILITY_SECONDS", "60"))
	if err != nil || visibility < 1 {
		return Config{}, errors.New("QUEUE_VISIBILITY_SECONDS must be a positive integer")
	}
	c.QueueVisibility = visibility
	if c.QueueEndpoint == "" || c.QueueAccessKey == "" || c.QueueSecretKey == "" {
		return Config{}, errors.New("AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY are required")
	}
	return c, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
