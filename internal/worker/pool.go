package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goqueue/goqueue/internal/broker"
	"github.com/goqueue/goqueue/internal/task"
)

// PoolConfig specifies runtime tuning parameters for the worker pool.
type PoolConfig struct {
	Concurrency       int
	Queues            []string
	VisibilityTimeout time.Duration
	GracePeriod       time.Duration
	SchedulerInterval time.Duration
	ReclaimerInterval time.Duration
	RetryPolicy       *task.RetryPolicy
	Logger            func(string, ...interface{})
	OnTaskComplete    func(task *task.Task, duration time.Duration, err error)
}

// DefaultPoolConfig returns sensible production defaults.
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		Concurrency:       10,
		Queues:            []string{"default"},
		VisibilityTimeout: 60 * time.Second,
		GracePeriod:       15 * time.Second,
		SchedulerInterval: 500 * time.Millisecond,
		ReclaimerInterval: 1 * time.Second,
		RetryPolicy:       task.DefaultRetryPolicy(),
		Logger: func(format string, args ...interface{}) {
			fmt.Printf(format+"\n", args...)
		},
	}
}

// Pool manages a concurrent fleet of workers, scheduler, and reclaimer.
type Pool struct {
	broker    broker.Broker
	registry  *HandlerRegistry
	limiter   *RateLimiterRegistry
	cfg       PoolConfig
	scheduler *Scheduler
	reclaimer *Reclaimer
	stopCh    chan struct{}
	wg        sync.WaitGroup
	inFlight  int32
	running   int32
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewPool initializes a new worker pool.
func NewPool(b broker.Broker, cfg PoolConfig) *Pool {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	if len(cfg.Queues) == 0 {
		cfg.Queues = []string{"default"}
	}
	if cfg.VisibilityTimeout <= 0 {
		cfg.VisibilityTimeout = 60 * time.Second
	}
	if cfg.GracePeriod <= 0 {
		cfg.GracePeriod = 15 * time.Second
	}
	if cfg.RetryPolicy == nil {
		cfg.RetryPolicy = task.DefaultRetryPolicy()
	}
	if cfg.Logger == nil {
		cfg.Logger = func(string, ...interface{}) {}
	}

	registry := NewHandlerRegistry()
	registry.Use(PanicRecoveryMiddleware())

	limiter := NewRateLimiterRegistry()

	scheduler := NewScheduler(b, cfg.Queues, cfg.SchedulerInterval, cfg.Logger)
	reclaimer := NewReclaimer(b, cfg.Queues, cfg.ReclaimerInterval, cfg.Logger)

	return &Pool{
		broker:    b,
		registry:  registry,
		limiter:   limiter,
		cfg:       cfg,
		scheduler: scheduler,
		reclaimer: reclaimer,
		stopCh:    make(chan struct{}),
	}
}

// Registry returns the handler registry.
func (p *Pool) Registry() *HandlerRegistry {
	return p.registry
}

// Limiter returns the rate limiter registry.
func (p *Pool) Limiter() *RateLimiterRegistry {
	return p.limiter
}

// RegisterHandler registers a task handler function.
func (p *Pool) RegisterHandler(taskType string, h HandlerFunc) {
	p.registry.Register(taskType, h)
}

// SetRateLimit sets dynamic rate limit per task type.
func (p *Pool) SetRateLimit(taskType string, rate float64, burst int) {
	p.limiter.SetLimit(taskType, rate, burst)
}

// InFlightCount returns current number of actively executing tasks.
func (p *Pool) InFlightCount() int32 {
	return atomic.LoadInt32(&p.inFlight)
}

// Concurrency returns configured pool concurrency.
func (p *Pool) Concurrency() int {
	return p.cfg.Concurrency
}

// Start spawns the worker routines, scheduler, and visibility lease reclaimer.
func (p *Pool) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&p.running, 0, 1) {
		return fmt.Errorf("worker pool is already running")
	}

	p.ctx, p.cancel = context.WithCancel(ctx)

	// Start background daemons
	p.scheduler.Start(p.ctx)
	p.reclaimer.Start(p.ctx)

	p.cfg.Logger("[POOL] Starting worker pool with concurrency=%d queues=%v", p.cfg.Concurrency, p.cfg.Queues)

	// Spawn worker goroutines
	for i := 1; i <= p.cfg.Concurrency; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		w := newWorker(
			workerID,
			p.broker,
			p.registry,
			p.limiter,
			p.cfg.RetryPolicy,
			p.cfg.Queues,
			p.cfg.VisibilityTimeout,
			&p.inFlight,
			p.cfg.Logger,
			p.cfg.OnTaskComplete,
		)

		p.wg.Add(1)
		go func(wrk *worker) {
			defer p.wg.Done()
			wrk.run(p.ctx, p.stopCh)
		}(w)
	}

	return nil
}

// Stop initiates graceful shutdown, waiting up to GracePeriod for in-flight tasks to complete.
func (p *Pool) Stop() error {
	if !atomic.CompareAndSwapInt32(&p.running, 1, 0) {
		return nil
	}

	p.cfg.Logger("[POOL] Initiating graceful shutdown (grace period: %v)...", p.cfg.GracePeriod)

	// Signal workers to stop taking new tasks
	close(p.stopCh)

	// Wait for workers to finish current in-flight tasks or timeout
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.cfg.Logger("[POOL] All workers drained successfully.")
	case <-time.After(p.cfg.GracePeriod):
		p.cfg.Logger("[POOL WARNING] Grace period expired with %d in-flight tasks remaining. Forcing shutdown.", atomic.LoadInt32(&p.inFlight))
		p.cancel() // Cancel remaining context
		<-done
	}

	// Stop background daemons
	p.scheduler.Stop()
	p.reclaimer.Stop()

	p.cfg.Logger("[POOL] Worker pool shutdown complete.")
	return nil
}
