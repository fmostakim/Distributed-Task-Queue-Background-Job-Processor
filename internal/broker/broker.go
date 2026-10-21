package broker

import (
	"context"
	"errors"
	"time"

	"github.com/goqueue/goqueue/internal/task"
)

var (
	// ErrNoTaskAvailable is returned when a claim poll yields no available work.
	ErrNoTaskAvailable = errors.New("no task available")
	// ErrTaskNotFound is returned when querying a task ID that does not exist.
	ErrTaskNotFound = errors.New("task not found")
	// ErrQueuePaused is returned when attempting to pop or process a paused queue.
	ErrQueuePaused = errors.New("queue is paused")
	// ErrLeaseLost is returned when a heartbeat cannot extend the lease because it was already reclaimed.
	ErrLeaseLost = errors.New("task lease lost or expired")
)

// QueueStats captures point-in-time metrics for a specific queue.
type QueueStats struct {
	QueueName      string           `json:"queue_name"`
	IsPaused       bool             `json:"is_paused"`
	ReadyByPriority map[string]int64 `json:"ready_by_priority"`
	TotalReady     int64            `json:"total_ready"`
	Active         int64            `json:"active"`
	Scheduled      int64            `json:"scheduled"`
	DeadLetter     int64            `json:"dead_letter"`
	TotalProcessed int64            `json:"total_processed"`
	TotalFailed    int64            `json:"total_failed"`
}

// Broker abstracts queue storage, atomic claims, leases, and lifecycle management.
type Broker interface {
	// Enqueue stores task state and inserts it into either ready or scheduled queue.
	Enqueue(ctx context.Context, t *task.Task) error

	// EnqueueBatch enqueues multiple tasks atomically using pipelines.
	EnqueueBatch(ctx context.Context, tasks []*task.Task) error

	// Claim atomically claims the highest priority ready task, moving it to active with a visibility lease.
	Claim(ctx context.Context, queue string, workerID string, visibilityTimeout time.Duration) (*task.Task, error)

	// Ack marks the task completed and removes it from the active lease set.
	Ack(ctx context.Context, taskID string, queue string) error

	// Nack increments the retry count and moves the task to scheduled (with backoff) or DLQ.
	Nack(ctx context.Context, taskID string, queue string, errReason string, stackTrace string, backoffDelay time.Duration) error

	// Heartbeat extends the active visibility timeout lease for a task.
	Heartbeat(ctx context.Context, taskID string, queue string, extendBy time.Duration) error

	// ScheduleDueTasks moves delayed/scheduled tasks whose timestamp has arrived into the ready queue.
	ScheduleDueTasks(ctx context.Context, queue string, limit int64) (int64, error)

	// ReclaimExpiredLeases scans active tasks whose lease has expired and either retries them or moves to DLQ.
	ReclaimExpiredLeases(ctx context.Context, queue string, limit int64) (int64, int64, error)

	// GetTask retrieves task details by ID.
	GetTask(ctx context.Context, taskID string) (*task.Task, error)

	// GetQueueStats returns depths and health for a single queue.
	GetQueueStats(ctx context.Context, queue string) (*QueueStats, error)

	// GetAllQueueStats returns stats across all registered queues.
	GetAllQueueStats(ctx context.Context) (map[string]*QueueStats, error)

	// PauseQueue temporarily halts worker claims on the queue.
	PauseQueue(ctx context.Context, queue string) error

	// ResumeQueue resumes worker claims on the queue.
	ResumeQueue(ctx context.Context, queue string) error

	// IsQueuePaused checks whether the queue is paused.
	IsQueuePaused(ctx context.Context, queue string) (bool, error)

	// GetDLQTasks returns paginated tasks from the Dead-Letter Queue.
	GetDLQTasks(ctx context.Context, queue string, offset, limit int64) ([]*task.Task, int64, error)

	// RetryDLQTask moves a dead-lettered task back into the ready queue for execution.
	RetryDLQTask(ctx context.Context, taskID string, queue string) error

	// RetryAllDLQTasks moves all tasks from DLQ back into their ready queues.
	RetryAllDLQTasks(ctx context.Context, queue string) (int64, error)

	// PurgeDLQ deletes all tasks currently in the DLQ.
	PurgeDLQ(ctx context.Context, queue string) (int64, error)

	// Close cleanly terminates the broker connection.
	Close() error

	// Ping verifies broker connectivity.
	Ping(ctx context.Context) error
}
