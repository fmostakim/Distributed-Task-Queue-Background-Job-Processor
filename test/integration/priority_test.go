package integration

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/goqueue/goqueue/internal/broker"
	"github.com/goqueue/goqueue/internal/task"
	"github.com/redis/go-redis/v9"
)

func setupTestBroker(t *testing.T) (*broker.RedisBroker, *miniredis.Miniredis) {
	mr := miniredis.RunT(t)

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	b := broker.NewRedisBrokerFromClient(client, "test_gq:")
	return b, mr
}

func TestPriorityQueueOrdering(t *testing.T) {
	b, _ := setupTestBroker(t)
	defer b.Close()

	ctx := context.Background()
	queue := "test_priority"

	// Enqueue in reverse order: Low, Default, High, Critical
	priorities := []task.Priority{
		task.PriorityLow,
		task.PriorityDefault,
		task.PriorityHigh,
		task.PriorityCritical,
	}

	for _, p := range priorities {
		tsk := task.NewTask(
			"test_job",
			[]byte("payload_"+string(p)),
			task.WithQueue(queue),
			task.WithPriority(p),
		)
		if err := b.Enqueue(ctx, tsk); err != nil {
			t.Fatalf("failed to enqueue task with priority %s: %v", p, err)
		}
	}

	// Now claim tasks sequentially; they MUST be claimed in order:
	// Critical -> High -> Default -> Low
	expectedOrder := []task.Priority{
		task.PriorityCritical,
		task.PriorityHigh,
		task.PriorityDefault,
		task.PriorityLow,
	}

	for i, expected := range expectedOrder {
		claimed, err := b.Claim(ctx, queue, "worker-1", 30*time.Second)
		if err != nil {
			t.Fatalf("step %d: expected to claim task, got err: %v", i+1, err)
		}
		if claimed.Priority != expected {
			t.Fatalf("step %d: expected priority %s, got %s (task ID: %s)",
				i+1, expected, claimed.Priority, claimed.ID)
		}
	}

	// Next claim should yield ErrNoTaskAvailable
	_, err := b.Claim(ctx, queue, "worker-1", 30*time.Second)
	if err != broker.ErrNoTaskAvailable {
		t.Fatalf("expected ErrNoTaskAvailable, got: %v", err)
	}
}
