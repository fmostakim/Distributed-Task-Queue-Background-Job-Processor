package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/goqueue/goqueue/internal/broker"
	"github.com/goqueue/goqueue/internal/task"
)

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	redisPassword := os.Getenv("REDIS_PASSWORD")

	countFlag := flag.Int("count", 1000, "Total number of tasks to enqueue (0 for continuous mode)")
	batchSizeFlag := flag.Int("batch", 50, "Batch size per pipeline write")
	rateFlag := flag.Int("rate", 200, "Target enqueue rate in tasks/sec")
	failRateFlag := flag.Float64("fail-rate", 0.08, "Fraction of tasks that are programmed to fail (0.0 to 1.0)")
	delayRateFlag := flag.Float64("delay-rate", 0.15, "Fraction of tasks that have scheduled delay (0.0 to 1.0)")
	flag.Parse()

	log.Printf("[LOADGEN] Target Redis: %s | Count: %d | Batch: %d | Rate: %d/s",
		redisAddr, *countFlag, *batchSizeFlag, *rateFlag)

	cfg := broker.DefaultRedisConfig()
	cfg.Addr = redisAddr
	cfg.Password = redisPassword
	cfg.PoolSize = 50

	b, err := broker.NewRedisBroker(cfg)
	if err != nil {
		log.Fatalf("[FATAL] Failed to connect to Redis: %v", err)
	}
	defer b.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("[LOADGEN] Received interrupt. Stopping load generator...")
		cancel()
	}()

	taskTypes := []string{"send_email", "process_payment", "generate_pdf", "sync_webhook"}
	priorities := []task.Priority{
		task.PriorityCritical,
		task.PriorityHigh,
		task.PriorityDefault,
		task.PriorityLow,
	}

	var totalEnqueued int64
	start := time.Now()
	ticker := time.NewTicker(time.Duration(float64(time.Second) * float64(*batchSizeFlag) / float64(*rateFlag)))
	defer ticker.Stop()

	var wg sync.WaitGroup

	log.Println("[LOADGEN] Starting synthetic workload injection...")

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			printSummary(totalEnqueued, start)
			return
		case <-ticker.C:
			if *countFlag > 0 && atomic.LoadInt64(&totalEnqueued) >= int64(*countFlag) {
				wg.Wait()
				printSummary(totalEnqueued, start)
				return
			}

			// Prepare batch
			batch := make([]*task.Task, 0, *batchSizeFlag)
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			now := time.Now().UTC()

			for i := 0; i < *batchSizeFlag; i++ {
				tType := taskTypes[rng.Intn(len(taskTypes))]
				prio := priorities[rng.Intn(len(priorities))]

				if rng.Float64() < *failRateFlag {
					tType = "failing_task"
				}

				payload := fmt.Sprintf(`{"client_id":"cli_%d","amount":%d,"ts":"%s"}`,
					rng.Intn(10000), rng.Intn(500)+1, now.Format(time.RFC3339))

				t := task.NewTask(
					tType,
					[]byte(payload),
					task.WithQueue("default"),
					task.WithPriority(prio),
					task.WithMaxRetries(2),
					task.WithTimeout(15*time.Second),
					task.WithVisibilityTimeout(20*time.Second),
				)

				if rng.Float64() < *delayRateFlag {
					delaySec := rng.Intn(8) + 2
					t.ScheduledAt = now.Add(time.Duration(delaySec) * time.Second)
				}

				batch = append(batch, t)
			}

			wg.Add(1)
			go func(tasks []*task.Task) {
				defer wg.Done()
				pipeCtx, pipeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer pipeCancel()

				if err := b.EnqueueBatch(pipeCtx, tasks); err != nil {
					log.Printf("[LOADGEN ERROR] Batch write failed: %v", err)
					return
				}
				enq := atomic.AddInt64(&totalEnqueued, int64(len(tasks)))
				if enq%500 == 0 || (*countFlag > 0 && enq >= int64(*countFlag)) {
					elapsed := time.Since(start).Seconds()
					rate := float64(enq) / elapsed
					log.Printf("[LOADGEN PROGRESS] Enqueued %d tasks (%.1f tasks/sec)", enq, rate)
				}
			}(batch)
		}
	}
}

func printSummary(total int64, start time.Time) {
	elapsed := time.Since(start).Seconds()
	rate := float64(total) / elapsed
	log.Printf("[LOADGEN SUMMARY] Finished. Total tasks: %d | Elapsed: %.2fs | Throughput: %.1f tasks/sec",
		total, elapsed, rate)
}
