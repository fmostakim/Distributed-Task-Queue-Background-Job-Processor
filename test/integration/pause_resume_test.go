package integration

import (
	"context"
	"testing"
	"time"

	"github.com/goqueue/goqueue/internal/broker"
	"github.com/goqueue/goqueue/internal/task"
	"github.com/goqueue/goqueue/internal/worker"
)

func TestPauseAndResumeQueue(t *testing.T) {
	b, _ := setupTestBroker(t)
	defer b.Close()

	ctx := context.Background()
	queue := "test_pause"

	tsk := task.NewTask("sample_task", []byte("data"), task.WithQueue(queue))
	if err := b.Enqueue(ctx, tsk); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// 1. Pause queue
	if err := b.PauseQueue(ctx, queue); err != nil {
		t.Fatalf("PauseQueue failed: %v", err)
	}

	isPaused, err := b.IsQueuePaused(ctx, queue)
	if err != nil || !isPaused {
		t.Fatalf("expected isPaused=true, got %v (err: %v)", isPaused, err)
	}

	// 2. Claim attempt MUST return ErrNoTaskAvailable because queue is paused
	claimed, err := b.Claim(ctx, queue, "w1", 30*time.Second)
	if err != broker.ErrNoTaskAvailable {
		t.Fatalf("expected ErrNoTaskAvailable while paused, got task: %v, err: %v", claimed, err)
	}

	// 3. Resume queue
	if err := b.ResumeQueue(ctx, queue); err != nil {
		t.Fatalf("ResumeQueue failed: %v", err)
	}

	isPaused, _ = b.IsQueuePaused(ctx, queue)
	if isPaused {
		t.Fatalf("expected isPaused=false after resume")
	}

	// 4. Now claim should succeed
	claimed, err = b.Claim(ctx, queue, "w1", 30*time.Second)
	if err != nil {
		t.Fatalf("claim failed after resume: %v", err)
	}
	if claimed.ID != tsk.ID {
		t.Fatalf("claimed wrong task: %s", claimed.ID)
	}
}

func TestDynamicRateLimiter(t *testing.T) {
	reg := worker.NewRateLimiterRegistry()

	// 10 ops/sec, burst 2
	reg.SetLimit("heavy_task", 10.0, 2)

	// First 2 calls should be allowed immediately by burst
	if !reg.Allow("heavy_task") {
		t.Fatalf("expected token 1 allowed")
	}
	if !reg.Allow("heavy_task") {
		t.Fatalf("expected token 2 allowed")
	}

	// 3rd call immediately should be denied
	if reg.Allow("heavy_task") {
		t.Fatalf("expected token 3 to be denied by rate limiter")
	}

	// Unregistered task type should always be allowed
	if !reg.Allow("unmetered_task") {
		t.Fatalf("expected unmetered task to be allowed")
	}
}
