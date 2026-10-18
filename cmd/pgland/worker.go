package main

import (
	"fmt"
	"time"

	"github.com/yinebebt/pgland/internal/source"
	"github.com/yinebebt/pgland/internal/sqsqueue"
	"github.com/yinebebt/pgland/internal/store"
	"github.com/yinebebt/pgland/internal/syncjob"
)

func runWorker() error {
	rt, err := newRuntime()
	if err != nil {
		return err
	}
	defer rt.close()

	pullers := make(map[string]*source.HTTP, len(rt.sources))
	for _, p := range rt.sources {
		pullers[p.Name()] = p
	}
	rt.log.Info("worker consuming", "queue", rt.cfg.QueueName)

	// Each pass takes one run id and fetches one batch, then waits again.
	for {
		delivery, ok, err := rt.queue.Receive(rt.ctx)
		if err != nil {
			if rt.ctx.Err() != nil {
				return nil
			}
			rt.log.Error("receive", "err", err)
			timer := time.NewTimer(time.Second)
			select {
			case <-rt.ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
			}
			continue
		}
		if !ok {
			continue
		}

		var runErr error
		run, err := rt.store.GetSync(rt.ctx, delivery.RunID)
		if err != nil {
			runErr = err
		} else if run.Status == store.SyncAccepted {
			puller, found := pullers[run.Source]
			if !found {
				runErr = fmt.Errorf("unknown source %s", run.Source)
			} else {
				runErr = syncjob.Run(rt.ctx, rt.store, puller.Fetch, run)
			}
		}
		if runErr != nil {
			if delivery.ReceiveCount >= sqsqueue.MaxReceives {
				if err := rt.store.FailSync(rt.ctx, delivery.RunID, "retries exhausted"); err != nil {
					rt.log.Error("fail sync", "run", delivery.RunID, "err", err)
				}
				rt.log.Error("sync failed; message left for the dead-letter queue",
					"run", delivery.RunID, "receives", delivery.ReceiveCount, "err", runErr)
			} else {
				rt.log.Error("sync failed; message left for retry",
					"run", delivery.RunID, "receives", delivery.ReceiveCount, "err", runErr)
			}
			continue
		}
		if err := rt.queue.Delete(rt.ctx, delivery.Receipt); err != nil {
			rt.log.Error("delete", "run", delivery.RunID, "err", err)
			continue
		}
		rt.log.Info("sync finished", "run", delivery.RunID)
	}
}
