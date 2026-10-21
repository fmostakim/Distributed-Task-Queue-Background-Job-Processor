package integration

import (
	"context"
	"testing"
	"time"

	"github.com/goqueue/goqueue/internal/task"
)

func TestFailoverAndVisibilityTimeoutRecovery(t *testing.T) {
	b, _ := setupTestBroker(t)
	defer b.Close()

	ctx := context.Background()
	queue := "test_failover"

	tsk := task.NewTask(
		"critical_order",
		[]byte(`{"order_id":999}`),
		task.WithQueue(queue),
		task.WithPriority(task.PriorityCritical),
		task.WithMaxRetries(3),
		task.WithVisibilityTimeout(1*time.Second),
	)

	if err := b.Enqueue(ctx, tsk); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// 1. Worker-1 claims the task with 1-second visibility lease
	claimed, err := b.Claim(ctx, queue, "worker-1", 1*time.Second)
	if err != nil {
		t.Fatalf("worker-1 failed to claim: %v", err)
	}
	if claimed.ID != tsk.ID {
		t.Fatalf("expected task %s, got %s", tsk.ID, claimed.ID)
	}

	// Verify task is currently active
	stats, err := b.GetQueueStats(ctx, queue)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if stats.Active != 1 {
		t.Fatalf("expected 1 active task, got %d", stats.Active)
	}

	// 2. Simulate worker-1 crash: worker-1 stops responding, sends no heartbeat, no ACK.
	// Wait for the 1-second visibility timeout to expire
	time.Sleep(1200 * time.Millisecond)

	// 3. Lease reclaimer runs
	reclaimed, deadlettered, err := b.ReclaimExpiredLeases(ctx, queue, 10)
	if err != nil {
		t.Fatalf("ReclaimExpiredLeases failed: %v", err)
	}
	if reclaimed != 1 || deadlettered != 0 {
		t.Fatalf("expected reclaimed=1, deadlettered=0; got reclaimed=%d, deadlettered=%d", reclaimed, deadlettered)
	}

	// Reclaimed task is placed in scheduled with base backoff (5s in Lua script)
	// Fast forward time to schedule it ready
	time.Sleep(5100 * time.Millisecond)
	moved, err := b.ScheduleDueTasks(ctx, queue, 10)
	if err != nil {
		t.Fatalf("ScheduleDueTasks failed: %v", err)
	}
	if moved != 1 {
		t.Fatalf("expected 1 task moved to ready, got %d", moved)
	}

	// 4. Worker-2 picks up the recovered task
	worker2Claim, err := b.Claim(ctx, queue, "worker-2", 30*time.Second)
	if err != nil {
		t.Fatalf("worker-2 failed to claim recovered task: %v", err)
	}
	if worker2Claim.ID != tsk.ID {
		t.Fatalf("worker-2 claimed wrong task ID: %s", worker2Claim.ID)
	}
	if worker2Claim.CurrentRetry != 1 {
		t.Fatalf("expected retry count 1, got %d", worker2Claim.CurrentRetry)
	}

	// 5. Worker-2 successfully completes and ACKs the task
	if err := b.Ack(ctx, worker2Claim.ID, queue); err != nil {
		t.Fatalf("worker-2 failed to ACK: %v", err)
	}

	// Final verification: active should be 0
	finalStats, err := b.GetQueueStats(ctx, queue)
	if err != nil {
		t.Fatalf("failed to get final stats: %v", err)
	}
	if finalStats.Active != 0 {
		t.Fatalf("expected 0 active tasks, got %d", finalStats.Active)
	}
	if finalStats.TotalProcessed != 1 {
		t.Fatalf("expected 1 total processed, got %d", finalStats.TotalProcessed)
	}
}

func TestHeartbeatLeaseExtension(t *testing.T) {
	b, _ := setupTestBroker(t)
	defer b.Close()

	ctx := context.Background()
	queue := "test_heartbeat"

	tsk := task.NewTask(
		"long_running_job",
		[]byte(`{"video_id":"vid_123"}`),
		task.WithQueue(queue),
		task.WithVisibilityTimeout(2*time.Second),
	)

	if err := b.Enqueue(ctx, tsk); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	claimed, err := b.Claim(ctx, queue, "worker-1", 2*time.Second)
	if err != nil {
		t.Fatalf("failed to claim: %v", err)
	}

	// Sleep 1 second, then heartbeat to extend lease by 3 more seconds
	time.Sleep(1 * time.Second)
	err = b.Heartbeat(ctx, claimed.ID, queue, 3*time.Second)
	if err != nil {
		t.Fatalf("heartbeat extension failed: %v", err)
	}

	// Sleep another 1.5 seconds. The original 2s lease would have expired,
	// but because we heartbeated with +3s, it must NOT be expired yet!
	time.Sleep(1500 * time.Millisecond)

	reclaimed, _, err := b.ReclaimExpiredLeases(ctx, queue, 10)
	if err != nil {
		t.Fatalf("reclaim call failed: %v", err)
	}
	if reclaimed != 0 {
		t.Fatalf("task was prematurely reclaimed despite active heartbeat extension!")
	}

	// Clean up: worker-1 ACKs task
	if err := b.Ack(ctx, claimed.ID, queue); err != nil {
		t.Fatalf("failed to ACK: %v", err)
	}
}
