package task

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// State represents the current lifecycle state of a task.
type State string

const (
	StatePending      State = "pending"
	StateActive       State = "active"
	StateCompleted    State = "completed"
	StateFailed       State = "failed"
	StateDeadLettered State = "dead_lettered"
)

// Priority represents the execution priority level.
type Priority string

const (
	PriorityCritical Priority = "critical"
	PriorityHigh     Priority = "high"
	PriorityDefault  Priority = "default"
	PriorityLow      Priority = "low"
)

// PriorityOrder defines the multi-tiered precedence of execution.
var PriorityOrder = []Priority{
	PriorityCritical,
	PriorityHigh,
	PriorityDefault,
	PriorityLow,
}

// PriorityLevel returns the numeric weight of a priority (higher number = higher priority).
func (p Priority) Level() int {
	switch p {
	case PriorityCritical:
		return 4
	case PriorityHigh:
		return 3
	case PriorityDefault:
		return 2
	case PriorityLow:
		return 1
	default:
		return 2
	}
}

// IsValid checks if the priority is recognized.
func (p Priority) IsValid() bool {
	switch p {
	case PriorityCritical, PriorityHigh, PriorityDefault, PriorityLow:
		return true
	default:
		return false
	}
}

// Task represents a discrete unit of work processed by GoQueue.
type Task struct {
	ID                string        `json:"id"`
	Queue             string        `json:"queue"`
	Type              string        `json:"type"`
	Payload           []byte        `json:"payload"`
	Priority          Priority      `json:"priority"`
	State             State         `json:"state"`
	MaxRetries        int           `json:"max_retries"`
	CurrentRetry      int           `json:"current_retry"`
	Timeout           time.Duration `json:"timeout"`
	VisibilityTimeout time.Duration `json:"visibility_timeout"`
	CreatedAt         time.Time     `json:"created_at"`
	ScheduledAt       time.Time     `json:"scheduled_at"`
	LastFailedAt      *time.Time    `json:"last_failed_at,omitempty"`
	LastError         string        `json:"last_error,omitempty"`
	StackTrace        string        `json:"stack_trace,omitempty"`
	WorkerID          string        `json:"worker_id,omitempty"`
	HeartbeatAt       *time.Time    `json:"heartbeat_at,omitempty"`
	CompletedAt       *time.Time    `json:"completed_at,omitempty"`
}

// NewTask creates a new Task with sensible defaults.
func NewTask(taskType string, payload []byte, opts ...Option) *Task {
	t := &Task{
		ID:                uuid.New().String(),
		Queue:             "default",
		Type:              taskType,
		Payload:           payload,
		Priority:          PriorityDefault,
		State:             StatePending,
		MaxRetries:        3,
		CurrentRetry:      0,
		Timeout:           30 * time.Second,
		VisibilityTimeout: 60 * time.Second,
		CreatedAt:         time.Now().UTC(),
		ScheduledAt:       time.Now().UTC(),
	}

	for _, opt := range opts {
		opt(t)
	}

	return t
}

// Option configures task parameters.
type Option func(*Task)

// WithQueue specifies the target queue.
func WithQueue(queue string) Option {
	return func(t *Task) {
		if queue != "" {
			t.Queue = queue
		}
	}
}

// WithID overrides the task ID (useful for idempotency keys).
func WithID(id string) Option {
	return func(t *Task) {
		if id != "" {
			t.ID = id
		}
	}
}

// WithPriority specifies the execution priority.
func WithPriority(p Priority) Option {
	return func(t *Task) {
		if p.IsValid() {
			t.Priority = p
		}
	}
}

// WithMaxRetries sets the maximum retry threshold.
func WithMaxRetries(retries int) Option {
	return func(t *Task) {
		if retries >= 0 {
			t.MaxRetries = retries
		}
	}
}

// WithTimeout sets the maximum execution duration for a single attempt.
func WithTimeout(d time.Duration) Option {
	return func(t *Task) {
		if d > 0 {
			t.Timeout = d
		}
	}
}

// WithVisibilityTimeout sets the lease duration before re-queuing if unacknowledged.
func WithVisibilityTimeout(d time.Duration) Option {
	return func(t *Task) {
		if d > 0 {
			t.VisibilityTimeout = d
		}
	}
}

// WithDelay schedules the task to run after the specified delay from now.
func WithDelay(delay time.Duration) Option {
	return func(t *Task) {
		if delay > 0 {
			t.ScheduledAt = time.Now().UTC().Add(delay)
		}
	}
}

// WithScheduleAt schedules the task to run at an explicit future timestamp.
func WithScheduleAt(at time.Time) Option {
	return func(t *Task) {
		t.ScheduledAt = at.UTC()
	}
}

// Marshal serializes the Task to JSON.
func (t *Task) Marshal() ([]byte, error) {
	return json.Marshal(t)
}

// UnmarshalTask deserializes a Task from JSON.
func UnmarshalTask(data []byte) (*Task, error) {
	var t Task
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task: %w", err)
	}
	return &t, nil
}
