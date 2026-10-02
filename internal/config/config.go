// Package config loads settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
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
	return c, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
