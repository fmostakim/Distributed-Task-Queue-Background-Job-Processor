package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goqueue/goqueue/internal/task"
	"github.com/goqueue/goqueue/internal/worker"
)

func TestWorkerPoolConcurrentExecution(t *testing.T) {
	b, _ := setupTestBroker(t)
	defer b.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queue := "test_concurrent"
	totalTasks := 40
	concurrency := 5

	poolCfg := worker.DefaultPoolConfig()
	poolCfg.Concurrency = concurrency
	poolCfg.Queues = []string{queue}
	poolCfg.SchedulerInterval = 100 * time.Millisecond
	poolCfg.ReclaimerInterval = 200 * time.Millisecond

	pool := worker.NewPool(b, poolCfg)

	var processedCount int64
	var mu sync.Mutex
	processedIDs := make(map[string]bool)

	pool.RegisterHandler("compute_task", func(ctx context.Context, t *task.Task) error {
		// Simulate slight work
		time.Sleep(15 * time.Millisecond)
		mu.Lock()
		processedIDs[t.ID] = true
		mu.Unlock()
		atomic.AddInt64(&processedCount, 1)
		return nil
	})

	// Start pool
	if err := pool.Start(ctx); err != nil {
		t.Fatalf("failed to start pool: %v", err)
	}

	// Enqueue tasks with varying priorities
	priorities := []task.Priority{
		task.PriorityCritical,
		task.PriorityHigh,
		task.PriorityDefault,
		task.PriorityLow,
	}

	for i := 0; i < totalTasks; i++ {
		prio := priorities[i%len(priorities)]
		tsk := task.NewTask(
			"compute_task",
			[]byte(fmt.Sprintf(`{"index":%d}`, i)),
			task.WithQueue(queue),
			task.WithPriority(prio),
		)
		if err := b.Enqueue(ctx, tsk); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}
	}

	// Wait for workers to process all tasks
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&processedCount) == int64(totalTasks) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if atomic.LoadInt64(&processedCount) != int64(totalTasks) {
		t.Fatalf("timeout waiting for tasks: processed %d / %d",
			atomic.LoadInt64(&processedCount), totalTasks)
	}

	mu.Lock()
	distinct := len(processedIDs)
	mu.Unlock()

	if distinct != totalTasks {
		t.Fatalf("expected %d distinct processed tasks, got %d", totalTasks, distinct)
	}

	// Test Graceful Shutdown
	if err := pool.Stop(); err != nil {
		t.Fatalf("pool Stop failed: %v", err)
	}
}
