package integration

import (
	"context"
	"testing"
	"time"

	"github.com/goqueue/goqueue/internal/broker"
	"github.com/goqueue/goqueue/internal/task"
)

func TestDelayedAndScheduledJobs(t *testing.T) {
	b, _ := setupTestBroker(t)
	defer b.Close()

	ctx := context.Background()
	queue := "test_delayed"

	// 1. Enqueue job scheduled 2 seconds in the future
	delay := 2 * time.Second
	tsk := task.NewTask(
		"delayed_job",
		[]byte("future_payload"),
		task.WithQueue(queue),
		task.WithDelay(delay),
	)

	if err := b.Enqueue(ctx, tsk); err != nil {
		t.Fatalf("failed to enqueue delayed task: %v", err)
	}

	// 2. Immediate claim attempt should return ErrNoTaskAvailable because job is not due
	claimed, err := b.Claim(ctx, queue, "worker-1", 30*time.Second)
	if err != broker.ErrNoTaskAvailable {
		t.Fatalf("expected ErrNoTaskAvailable for future job, got task: %v, err: %v", claimed, err)
	}

	// 3. Fast-forward / wait for scheduled time to arrive
	time.Sleep(2100 * time.Millisecond)

	// 4. Run ScheduleDueTasks poller
	moved, err := b.ScheduleDueTasks(ctx, queue, 100)
	if err != nil {
		t.Fatalf("ScheduleDueTasks failed: %v", err)
	}
	if moved != 1 {
		t.Fatalf("expected 1 task moved to ready queue, got: %d", moved)
	}

	// 5. Now claim should succeed
	claimed, err = b.Claim(ctx, queue, "worker-1", 30*time.Second)
	if err != nil {
		t.Fatalf("failed to claim task after scheduling: %v", err)
	}
	if claimed.ID != tsk.ID {
		t.Fatalf("claimed wrong task ID: expected %s, got %s", tsk.ID, claimed.ID)
	}
}
