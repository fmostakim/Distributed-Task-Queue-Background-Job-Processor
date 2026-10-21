package integration

import (
	"context"
	"testing"
	"time"

	"github.com/goqueue/goqueue/internal/task"
)

func TestDeadLetterQueueLifecycle(t *testing.T) {
	b, _ := setupTestBroker(t)
	defer b.Close()

	ctx := context.Background()
	queue := "test_dlq"

	// Create task with max retries = 1
	tsk := task.NewTask(
		"payment_webhook",
		[]byte(`{"invoice":"inv_456"}`),
		task.WithQueue(queue),
		task.WithPriority(task.PriorityHigh),
		task.WithMaxRetries(1),
	)

	if err := b.Enqueue(ctx, tsk); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Attempt 1: Claim and Nack with 1s backoff
	claimed, err := b.Claim(ctx, queue, "worker-1", 30*time.Second)
	if err != nil {
		t.Fatalf("attempt 1 claim failed: %v", err)
	}

	err = b.Nack(ctx, claimed.ID, queue, "connection refused", "trace_attempt_1", 1*time.Second)
	if err != nil {
		t.Fatalf("attempt 1 nack failed: %v", err)
	}

	// Verify it's scheduled for retry (retry 1 of 1)
	stats, err := b.GetQueueStats(ctx, queue)
	if err != nil {
		t.Fatalf("get stats failed: %v", err)
	}
	if stats.Scheduled != 1 || stats.DeadLetter != 0 {
		t.Fatalf("expected 1 scheduled, 0 DLQ; got %d scheduled, %d DLQ", stats.Scheduled, stats.DeadLetter)
	}

	// Fast forward 1.1s and move due task to ready
	time.Sleep(1100 * time.Millisecond)
	moved, err := b.ScheduleDueTasks(ctx, queue, 10)
	if err != nil || moved != 1 {
		t.Fatalf("expected 1 task moved, got %d (err: %v)", moved, err)
	}

	// Attempt 2: Claim and Nack again -> Should exceed max retries (current_retry 2 > max_retries 1)
	claimed2, err := b.Claim(ctx, queue, "worker-1", 30*time.Second)
	if err != nil {
		t.Fatalf("attempt 2 claim failed: %v", err)
	}

	err = b.Nack(ctx, claimed2.ID, queue, "fatal bank rejection (402)", "stacktrace: line 42", 1*time.Second)
	if err != nil {
		t.Fatalf("attempt 2 nack failed: %v", err)
	}

	// Verify task is now in DLQ
	dlqTasks, total, err := b.GetDLQTasks(ctx, queue, 0, 10)
	if err != nil {
		t.Fatalf("GetDLQTasks failed: %v", err)
	}
	if total != 1 || len(dlqTasks) != 1 {
		t.Fatalf("expected total=1 and 1 task in DLQ, got total=%d, count=%d", total, len(dlqTasks))
	}

	dlqTask := dlqTasks[0]
	if dlqTask.ID != tsk.ID {
		t.Fatalf("DLQ task ID mismatch: expected %s, got %s", tsk.ID, dlqTask.ID)
	}
	if dlqTask.State != task.StateDeadLettered {
		t.Fatalf("expected StateDeadLettered, got %s", dlqTask.State)
	}
	if dlqTask.LastError != "fatal bank rejection (402)" {
		t.Fatalf("unexpected last error: %s", dlqTask.LastError)
	}
	if dlqTask.StackTrace != "stacktrace: line 42" {
		t.Fatalf("unexpected stack trace: %s", dlqTask.StackTrace)
	}

	// Manual Retry from DLQ
	if err := b.RetryDLQTask(ctx, dlqTask.ID, queue); err != nil {
		t.Fatalf("RetryDLQTask failed: %v", err)
	}

	// After retry, DLQ should be empty and task should be ready to claim
	dlqTasksAfter, totalAfter, _ := b.GetDLQTasks(ctx, queue, 0, 10)
	if totalAfter != 0 || len(dlqTasksAfter) != 0 {
		t.Fatalf("DLQ should be empty after retry, got %d", totalAfter)
	}

	retriedClaim, err := b.Claim(ctx, queue, "worker-2", 30*time.Second)
	if err != nil {
		t.Fatalf("failed to claim retried task: %v", err)
	}
	if retriedClaim.ID != tsk.ID {
		t.Fatalf("claimed wrong task: %s", retriedClaim.ID)
	}
	if retriedClaim.CurrentRetry != 0 {
		t.Fatalf("expected retry counter reset to 0, got %d", retriedClaim.CurrentRetry)
	}

	// Ack retried task
	if err := b.Ack(ctx, retriedClaim.ID, queue); err != nil {
		t.Fatalf("Ack failed: %v", err)
	}
}

func TestDLQPurge(t *testing.T) {
	b, _ := setupTestBroker(t)
	defer b.Close()

	ctx := context.Background()
	queue := "test_purge"

	// Create and fail 3 tasks directly into DLQ by setting max_retries = 0
	for i := 0; i < 3; i++ {
		tsk := task.NewTask("failing_job", []byte("bad"), task.WithQueue(queue), task.WithMaxRetries(0))
		_ = b.Enqueue(ctx, tsk)
		c, _ := b.Claim(ctx, queue, "w1", 10*time.Second)
		_ = b.Nack(ctx, c.ID, queue, "fail", "trace", 1*time.Second)
	}

	_, total, err := b.GetDLQTasks(ctx, queue, 0, 10)
	if err != nil || total != 3 {
		t.Fatalf("expected 3 tasks in DLQ, got %d", total)
	}

	purged, err := b.PurgeDLQ(ctx, queue)
	if err != nil {
		t.Fatalf("PurgeDLQ failed: %v", err)
	}
	if purged != 3 {
		t.Fatalf("expected 3 tasks purged, got %d", purged)
	}

	_, totalAfter, _ := b.GetDLQTasks(ctx, queue, 0, 10)
	if totalAfter != 0 {
		t.Fatalf("expected 0 tasks after purge, got %d", totalAfter)
	}
}
