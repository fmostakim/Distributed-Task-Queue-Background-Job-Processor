package main

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/goqueue/goqueue/internal/broker"
	"github.com/goqueue/goqueue/internal/task"
	"github.com/goqueue/goqueue/internal/worker"
	"github.com/goqueue/goqueue/pkg/api"
	"github.com/prometheus/client_golang/prometheus"
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func main() {
	log.Println("==================================================================")
	log.Println("  GoQueue — Distributed Asynchronous Task Engine & Job Processor  ")
	log.Println("==================================================================")

	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	redisPassword := getEnv("REDIS_PASSWORD", "")
	httpPort := getEnv("HTTP_PORT", "8080")
	concurrency := getEnvInt("CONCURRENCY", 10)
	gracePeriodSec := getEnvInt("GRACE_PERIOD_SEC", 15)
	queuesStr := getEnv("QUEUES", "default,critical,emails,payments")
	queues := strings.Split(queuesStr, ",")
	for i := range queues {
		queues[i] = strings.TrimSpace(queues[i])
	}

	log.Printf("[CONFIG] Redis: %s | Concurrency: %d | Queues: %v | Port: :%s", redisAddr, concurrency, queues, httpPort)

	// 1. Initialize Redis Broker
	redisCfg := broker.DefaultRedisConfig()
	redisCfg.Addr = redisAddr
	redisCfg.Password = redisPassword
	redisCfg.PoolSize = concurrency * 4

	b, err := broker.NewRedisBroker(redisCfg)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize Redis broker: %v", err)
	}
	defer b.Close()
	log.Printf("[REDIS] Connected successfully to Redis at %s", redisAddr)

	// 2. Metrics & Instrumentation
	promMetrics := api.NewMetrics(prometheus.DefaultRegisterer)

	// 3. Worker Pool Setup
	poolCfg := worker.DefaultPoolConfig()
	poolCfg.Concurrency = concurrency
	poolCfg.Queues = queues
	poolCfg.GracePeriod = time.Duration(gracePeriodSec) * time.Second
	poolCfg.VisibilityTimeout = 45 * time.Second
	poolCfg.OnTaskComplete = func(t *task.Task, duration time.Duration, execErr error) {
		status := "completed"
		if execErr != nil {
			status = "failed"
		}
		promMetrics.TasksProcessed.WithLabelValues(t.Queue, t.Type, status).Inc()
		promMetrics.TaskDuration.WithLabelValues(t.Queue, t.Type).Observe(duration.Seconds())
	}
	poolCfg.Logger = func(format string, args ...interface{}) {
		log.Printf(format, args...)
	}

	pool := worker.NewPool(b, poolCfg)

	// Register Task Handlers
	registerEnterpriseHandlers(pool)

	// Configure Dynamic Rate Limiters
	pool.SetRateLimit("send_email", 30.0, 50)       // 30 emails/sec, burst 50
	pool.SetRateLimit("process_payment", 15.0, 20)  // 15 payments/sec, burst 20
	pool.SetRateLimit("sync_webhook", 40.0, 60)     // 40 webhooks/sec, burst 60

	// 4. Start Worker Pool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		log.Fatalf("[FATAL] Failed to start worker pool: %v", err)
	}

	// 5. Start HTTP & Web UI Server
	serverAPI := api.NewServerAPI(b, pool, promMetrics)
	server := api.NewServer(":"+httpPort, serverAPI)

	go func() {
		log.Printf("[HTTP] Web Dashboard & REST API listening on http://0.0.0.0:%s", httpPort)
		if err := server.Start(); err != nil {
			log.Printf("[HTTP] Server stopped: %v", err)
		}
	}()

	// 6. Graceful Shutdown Signal Handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	sig := <-sigCh
	log.Printf("[SHUTDOWN] Received OS signal (%s). Initiating graceful shutdown...", sig)

	// Step A: Stop HTTP Server (reject new incoming HTTP requests)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := server.Stop(shutdownCtx); err != nil {
		log.Printf("[HTTP] Error shutting down HTTP server: %v", err)
	}

	// Step B: Stop Worker Pool (wait for in-flight jobs to finish or re-enqueue)
	if err := pool.Stop(); err != nil {
		log.Printf("[POOL] Error stopping pool: %v", err)
	}

	log.Println("[SHUTDOWN] GoQueue terminated cleanly. All resources released.")
}

func registerEnterpriseHandlers(pool *worker.Pool) {
	// Sample handler 1: Email dispatch
	pool.RegisterHandler("send_email", func(ctx context.Context, t *task.Task) error {
		time.Sleep(time.Duration(20+rand.Intn(40)) * time.Millisecond)
		return nil
	})

	// Sample handler 2: Payment processing
	pool.RegisterHandler("process_payment", func(ctx context.Context, t *task.Task) error {
		time.Sleep(time.Duration(50+rand.Intn(60)) * time.Millisecond)
		return nil
	})

	// Sample handler 3: PDF / Report Generation
	pool.RegisterHandler("generate_pdf", func(ctx context.Context, t *task.Task) error {
		time.Sleep(time.Duration(80+rand.Intn(80)) * time.Millisecond)
		return nil
	})

	// Sample handler 4: Webhook synchronization
	pool.RegisterHandler("sync_webhook", func(ctx context.Context, t *task.Task) error {
		time.Sleep(time.Duration(15+rand.Intn(30)) * time.Millisecond)
		return nil
	})

	// Sample handler 5: Failing Task (exercises exponential backoff and DLQ routing)
	pool.RegisterHandler("failing_task", func(ctx context.Context, t *task.Task) error {
		time.Sleep(10 * time.Millisecond)
		return errors.New("upstream service downstream timeout (503 Service Unavailable)")
	})

	// Sample handler 6: Panicking Task (exercises panic recovery middleware)
	pool.RegisterHandler("panicking_task", func(ctx context.Context, t *task.Task) error {
		panic("runtime memory corruption / nil pointer dereference")
	})
}
