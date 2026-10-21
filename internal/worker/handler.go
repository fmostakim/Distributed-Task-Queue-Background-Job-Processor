package worker

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/goqueue/goqueue/internal/task"
)

// HandlerFunc defines the function signature for executing a background task.
type HandlerFunc func(ctx context.Context, t *task.Task) error

// Middleware wraps a HandlerFunc with cross-cutting concerns (logging, recovery, metrics).
type Middleware func(HandlerFunc) HandlerFunc

// HandlerRegistry manages task handlers and applies global middleware.
type HandlerRegistry struct {
	mu          sync.RWMutex
	handlers    map[string]HandlerFunc
	middlewares []Middleware
}

// NewHandlerRegistry creates an empty registry.
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers:    make(map[string]HandlerFunc),
		middlewares: make([]Middleware, 0),
	}
}

// Use adds a global middleware to the chain.
func (r *HandlerRegistry) Use(m ...Middleware) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.middlewares = append(r.middlewares, m...)
}

// Register maps a task type string to its handler implementation.
func (r *HandlerRegistry) Register(taskType string, h HandlerFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[taskType] = h
}

// Get retrieves the wrapped handler for a given task type.
func (r *HandlerRegistry) Get(taskType string) (HandlerFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	h, exists := r.handlers[taskType]
	if !exists {
		return nil, false
	}

	// Chain middlewares in FIFO order: outermost first
	wrapped := h
	for i := len(r.middlewares) - 1; i >= 0; i-- {
		wrapped = r.middlewares[i](wrapped)
	}

	return wrapped, true
}

// RegisteredTypes returns a list of all currently registered task types.
func (r *HandlerRegistry) RegisteredTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	return types
}

// PanicRecoveryMiddleware captures panics in task handlers and converts them into errors with stack traces.
func PanicRecoveryMiddleware() Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, t *task.Task) (err error) {
			defer func() {
				if r := recover(); r != nil {
					stack := string(debug.Stack())
					err = fmt.Errorf("task panicked: %v\nstack:\n%s", r, stack)
				}
			}()
			return next(ctx, t)
		}
	}
}

// LoggingMiddleware logs task execution start, completion, and duration.
func LoggingMiddleware(logger func(format string, args ...interface{})) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, t *task.Task) error {
			start := time.Now()
			logger("[TASK START] id=%s type=%s queue=%s priority=%s attempt=%d/%d",
				t.ID, t.Type, t.Queue, t.Priority, t.CurrentRetry+1, t.MaxRetries)

			err := next(ctx, t)
			duration := time.Since(start)

			if err != nil {
				logger("[TASK FAILED] id=%s type=%s duration=%v err=%v", t.ID, t.Type, duration, err)
			} else {
				logger("[TASK COMPLETED] id=%s type=%s duration=%v", t.ID, t.Type, duration)
			}
			return err
		}
	}
}
