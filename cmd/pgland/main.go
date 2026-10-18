// Command pgland syncs event data from HTTP APIs and push clients into Postgres.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/yinebebt/pgland/internal/httpapi"
)

var version = "0.1.0"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "worker" {
		if err := runWorker(); err != nil {
			slog.Error("fatal", "err", err)
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	rt, err := newRuntime()
	if err != nil {
		return err
	}
	defer rt.close()

	rt.log.Info("sources loaded", "file", rt.cfg.SourcesFile, "count", len(rt.sources))

	if rt.cfg.IngestSecret == "" {
		rt.log.Warn("INGEST_SECRET is empty; POST /events accepts unsigned requests")
	}

	pullers := make([]httpapi.Puller, 0, len(rt.sources))
	for _, src := range rt.sources {
		pullers = append(pullers, src)
	}

	srv := &http.Server{
		Addr: rt.cfg.HTTPAddr,
		Handler: httpapi.Handler(httpapi.Config{
			Log:          rt.log,
			Store:        rt.store,
			Queue:        rt.queue,
			Sources:      pullers,
			IngestSecret: rt.cfg.IngestSecret,
			Version:      version,
		}),
		ReadHeaderTimeout: 10 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		rt.log.Info("http listening", "addr", rt.cfg.HTTPAddr, "version", version)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case err := <-serveErr:
		return err
	case <-rt.ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), rt.cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	rt.log.Info("shutdown complete")
	return nil
}
