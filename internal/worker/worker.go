package worker

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/goqueue/goqueue/internal/broker"
	"github.com/goqueue/goqueue/internal/task"
)

// worker represents a single consumer thread running inside the pool.
type worker struct {
	id                string
	broker            broker.Broker
	registry          *HandlerRegistry
	limiter           *RateLimiterRegistry
	retryPolicy       *task.RetryPolicy
	queues            []string
	visibilityTimeout time.Duration
	logger            func(string, ...interface{})
	inFlight          *int32
	onTaskComplete    func(task *task.Task, duration time.Duration, err error)
}

func newWorker(
	id string,
	b broker.Broker,
	reg *HandlerRegistry,
	lim *RateLimiterRegistry,
	rp *task.RetryPolicy,
	queues []string,
	visTimeout time.Duration,
	inFlight *int32,
	logger func(string, ...interface{}),
	onTaskComplete func(task *task.Task, duration time.Duration, err error),
) *worker {
	if visTimeout <= 0 {
		visTimeout = 60 * time.Second
	}
	if rp == nil {
		rp = task.DefaultRetryPolicy()
	}
	if len(queues) == 0 {
		queues = []string{"default"}
	}
	if logger == nil {
		logger = func(string, ...interface{}) {}
	}

	return &worker{
		id:                id,
		broker:            b,
		registry:          reg,
		limiter:           lim,
		retryPolicy:       rp,
		queues:            queues,
		visibilityTimeout: visTimeout,
		logger:            logger,
		inFlight:          inFlight,
		onTaskComplete:    onTaskComplete,
	}
}

// run executes the worker polling and processing loop until stopCh or ctx triggers.
func (w *worker) run(ctx context.Context, stopCh <-chan struct{}) {
	idleDelay := 50 * time.Millisecond
	maxIdleDelay := 500 * time.Millisecond

	for {
		select {
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		default:
		}

		claimedAny := false
		for _, q := range w.queues {
			t, err := w.broker.Claim(ctx, q, w.id, w.visibilityTimeout)
			if err != nil {
				if err == broker.ErrNoTaskAvailable || err == broker.ErrQueuePaused {
					continue
				}
				// Transient error reading from Redis
				time.Sleep(100 * time.Millisecond)
				continue
			}

			if t != nil {
				claimedAny = true
				idleDelay = 50 * time.Millisecond // reset idle delay on work found
				w.processTask(ctx, t)
				break
			}
		}

		if !claimedAny {
			// Exponential backoff during idle to prevent Redis CPU spinning
			select {
			case <-ctx.Done():
				return
			case <-stopCh:
				return
			case <-time.After(idleDelay):
				idleDelay = time.Duration(float64(idleDelay) * 1.5)
				if idleDelay > maxIdleDelay {
					idleDelay = maxIdleDelay
				}
			}
		}
	}
}

func (w *worker) processTask(ctx context.Context, t *task.Task) {
	atomic.AddInt32(w.inFlight, 1)
	defer atomic.AddInt32(w.inFlight, -1)

	// Heartbeat goroutine: periodically extends visibility timeout lease
	heartbeatDone := make(chan struct{})
	heartbeatInterval := t.VisibilityTimeout / 3
	if heartbeatInterval <= 0 {
		heartbeatInterval = w.visibilityTimeout / 3
	}
	if heartbeatInterval < 2*time.Second {
		heartbeatInterval = 2 * time.Second
	}

	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatDone:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				ext := t.VisibilityTimeout
				if ext <= 0 {
					ext = w.visibilityTimeout
				}
				if err := w.broker.Heartbeat(ctx, t.ID, t.Queue, ext); err != nil {
					w.logger("[WORKER %s] Heartbeat failed for task=%s: %v", w.id, t.ID, err)
					return
				}
			}
		}
	}()
	defer close(heartbeatDone)

	// Rate limiting wait
	if w.limiter != nil {
		if err := w.limiter.Wait(ctx, t.Type); err != nil {
			w.logger("[WORKER %s] Rate limiter wait cancelled: %v", w.id, err)
			return
		}
	}

	// Execution timeout
	taskTimeout := t.Timeout
	if taskTimeout <= 0 {
		taskTimeout = 30 * time.Second
	}
	taskCtx, taskCancel := context.WithTimeout(ctx, taskTimeout)
	defer taskCancel()

	start := time.Now()
	var execErr error
	var stackTrace string

	handler, exists := w.registry.Get(t.Type)
	if !exists {
		execErr = fmt.Errorf("no handler registered for task type: %s", t.Type)
	} else {
		func() {
			defer func() {
				if r := recover(); r != nil {
					stackTrace = string(debug.Stack())
					execErr = fmt.Errorf("panic during task execution: %v", r)
				}
			}()
			execErr = handler(taskCtx, t)
		}()
	}

	duration := time.Since(start)

	if execErr == nil {
		if err := w.broker.Ack(ctx, t.ID, t.Queue); err != nil {
			w.logger("[WORKER %s] Failed to ACK task=%s: %v", w.id, t.ID, err)
		}
	} else {
		backoff := w.retryPolicy.CalculateBackoff(t.CurrentRetry + 1)
		if stackTrace == "" {
			stackTrace = fmt.Sprintf("failed at attempt %d: %v", t.CurrentRetry+1, execErr)
		}
		if err := w.broker.Nack(ctx, t.ID, t.Queue, execErr.Error(), stackTrace, backoff); err != nil {
			w.logger("[WORKER %s] Failed to NACK task=%s: %v", w.id, t.ID, err)
		}
	}

	if w.onTaskComplete != nil {
		w.onTaskComplete(t, duration, execErr)
	}
}
