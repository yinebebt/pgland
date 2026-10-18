package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yinebebt/pgland/internal/config"
	"github.com/yinebebt/pgland/internal/source"
	"github.com/yinebebt/pgland/internal/sqsqueue"
	"github.com/yinebebt/pgland/internal/store"
)

const httpClientTimeout = 30 * time.Second

// runtime is the config, store, queue, and pull sources shared by the API and the worker.
type runtime struct {
	ctx     context.Context
	stop    context.CancelFunc
	cfg     config.Config
	log     *slog.Logger
	store   *store.Store
	queue   *sqsqueue.Queue
	sources []*source.HTTP
}

func newRuntime() (*runtime, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	rt := &runtime{ctx: ctx, stop: stop, cfg: cfg, log: log}

	defs, err := source.Load(cfg.SourcesFile)
	if err != nil {
		stop()
		return nil, err
	}
	client := &http.Client{Timeout: httpClientTimeout}
	rt.sources = make([]*source.HTTP, 0, len(defs))
	for _, def := range defs {
		rt.sources = append(rt.sources, source.New(def, client))
	}

	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		stop()
		return nil, err
	}
	rt.store = st

	q, err := sqsqueue.Open(ctx, sqsqueue.Config{
		Endpoint:          cfg.QueueEndpoint,
		Region:            cfg.QueueRegion,
		AccessKey:         cfg.QueueAccessKey,
		SecretKey:         cfg.QueueSecretKey,
		QueueName:         cfg.QueueName,
		DLQName:           cfg.DLQName,
		VisibilitySeconds: cfg.QueueVisibility,
	})
	if err != nil {
		st.Close()
		stop()
		return nil, err
	}
	rt.queue = q
	return rt, nil
}

func (rt *runtime) close() {
	rt.store.Close()
	rt.stop()
}
