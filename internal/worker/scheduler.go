package worker

import (
	"context"
	"sync"
	"time"

	"github.com/goqueue/goqueue/internal/broker"
)

// Scheduler periodically scans the scheduled ZSET for due jobs and moves them to ready queues.
type Scheduler struct {
	broker       broker.Broker
	queues       []string
	pollInterval time.Duration
	batchSize    int64
	logger       func(string, ...interface{})
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

// NewScheduler creates a new background scheduler.
func NewScheduler(b broker.Broker, queues []string, interval time.Duration, logger func(string, ...interface{})) *Scheduler {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	if len(queues) == 0 {
		queues = []string{"default"}
	}
	if logger == nil {
		logger = func(string, ...interface{}) {}
	}

	return &Scheduler{
		broker:       b,
		queues:       queues,
		pollInterval: interval,
		batchSize:    100,
		logger:       logger,
		stopCh:       make(chan struct{}),
	}
}

// Start launches the scheduler goroutine.
func (s *Scheduler) Start(ctx context.Context) {
	s.wg.Add(1)
	go s.run(ctx)
}

// Stop signals the scheduler to terminate and waits for completion.
func (s *Scheduler) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

func (s *Scheduler) run(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.poll(ctx)
		}
	}
}

func (s *Scheduler) poll(ctx context.Context) {
	for _, q := range s.queues {
		moved, err := s.broker.ScheduleDueTasks(ctx, q, s.batchSize)
		if err != nil {
			s.logger("[SCHEDULER ERROR] queue=%s err=%v", q, err)
			continue
		}
		if moved > 0 {
			s.logger("[SCHEDULER] queue=%s moved %d due tasks to ready queue", q, moved)
		}
	}
}
