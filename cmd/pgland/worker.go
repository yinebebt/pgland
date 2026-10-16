package main

import (
	"context"
	"fmt"
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
	"github.com/yinebebt/pgland/internal/syncjob"
)

func runWorker() error {
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
	pullers := map[string]*source.HTTP{}
	for _, def := range defs {
		p := source.New(def, client)
		pullers[p.Name()] = p
	}

	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()

	q, err := openQueue(ctx, cfg)
	if err != nil {
		return err
	}
	log.Info("worker consuming", "queue", cfg.QueueName)

	for {
		delivery, ok, err := q.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Error("receive", "err", err)
			if !sleep(ctx, time.Second) {
				return nil
			}
			continue
		}
		if !ok {
			continue
		}

		runErr := finishDelivery(ctx, st, pullers, delivery.RunID)
		if runErr != nil {
			if delivery.ReceiveCount >= sqsqueue.MaxReceives {
				if err := st.FailSync(ctx, delivery.RunID, "retries exhausted"); err != nil {
					log.Error("fail sync", "run", delivery.RunID, "err", err)
				}
				log.Error("sync failed; message left for the dead-letter queue", "run", delivery.RunID, "receives", delivery.ReceiveCount, "err", runErr)
			} else {
				log.Error("sync failed; message left for retry", "run", delivery.RunID, "receives", delivery.ReceiveCount, "err", runErr)
			}
			continue
		}
		if err := q.Delete(ctx, delivery.Receipt); err != nil {
			log.Error("delete", "run", delivery.RunID, "err", err)
			continue
		}
		log.Info("sync finished", "run", delivery.RunID)
	}
}

func finishDelivery(ctx context.Context, st *store.Store, pullers map[string]*source.HTTP, runID string) error {
	run, err := st.GetSync(ctx, runID)
	if err != nil {
		return err
	}
	if run.Status != store.SyncAccepted {
		return nil
	}
	puller, ok := pullers[run.Source]
	if !ok {
		return fmt.Errorf("unknown source %s", run.Source)
	}
	return syncjob.Run(ctx, st, puller.Fetch, run)
}

func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
