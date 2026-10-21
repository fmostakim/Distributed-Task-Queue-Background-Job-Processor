package worker

import (
	"context"
	"sync"
	"time"

	"github.com/goqueue/goqueue/internal/broker"
)

// Reclaimer periodically scans the active lease ZSET for expired tasks whose workers died.
type Reclaimer struct {
	broker       broker.Broker
	queues       []string
	scanInterval time.Duration
	batchSize    int64
	logger       func(string, ...interface{})
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

// NewReclaimer creates a new lease reclaimer.
func NewReclaimer(b broker.Broker, queues []string, interval time.Duration, logger func(string, ...interface{})) *Reclaimer {
	if interval <= 0 {
		interval = 1 * time.Second
	}
	if len(queues) == 0 {
		queues = []string{"default"}
	}
	if logger == nil {
		logger = func(string, ...interface{}) {}
	}

	return &Reclaimer{
		broker:       b,
		queues:       queues,
		scanInterval: interval,
		batchSize:    100,
		logger:       logger,
		stopCh:       make(chan struct{}),
	}
}

// Start launches the reclaimer sweeper goroutine.
func (r *Reclaimer) Start(ctx context.Context) {
	r.wg.Add(1)
	go r.run(ctx)
}

// Stop signals the reclaimer to terminate and waits for completion.
func (r *Reclaimer) Stop() {
	close(r.stopCh)
	r.wg.Wait()
}

func (r *Reclaimer) run(ctx context.Context) {
	defer r.wg.Done()
	ticker := time.NewTicker(r.scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.sweep(ctx)
		}
	}
}

func (r *Reclaimer) sweep(ctx context.Context) {
	for _, q := range r.queues {
		reclaimed, deadlettered, err := r.broker.ReclaimExpiredLeases(ctx, q, r.batchSize)
		if err != nil {
			r.logger("[RECLAIMER ERROR] queue=%s err=%v", q, err)
			continue
		}
		if reclaimed > 0 || deadlettered > 0 {
			r.logger("[RECLAIMER] queue=%s reclaimed=%d deadlettered=%d", q, reclaimed, deadlettered)
		}
	}
}
