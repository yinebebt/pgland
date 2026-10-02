// Command pgland syncs event data from HTTP APIs and push clients into Postgres.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yinebebt/pgland/internal/config"
	"github.com/yinebebt/pgland/internal/httpapi"
	"github.com/yinebebt/pgland/internal/source"
	"github.com/yinebebt/pgland/internal/store"
)

var version = "0.1.0"

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	defs, err := source.Load(cfg.SourcesFile)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 30 * time.Second}
	pullers := make([]httpapi.Puller, 0, len(defs))
	for _, def := range defs {
		pullers = append(pullers, source.New(def, client))
	}
	log.Info("sources loaded", "file", cfg.SourcesFile, "count", len(pullers))

	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()

	if cfg.IngestSecret == "" {
		log.Warn("INGEST_SECRET is empty; POST /events accepts unsigned requests")
	}

	srv := &http.Server{
		Addr: cfg.HTTPAddr,
		Handler: httpapi.Handler(httpapi.Config{
			Log:          log,
			Store:        st,
			Sources:      pullers,
			IngestSecret: cfg.IngestSecret,
			Version:      version,
		}),
		ReadHeaderTimeout: 10 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		log.Info("http listening", "addr", cfg.HTTPAddr, "version", version)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	log.Info("shutdown complete")
	return nil
}
