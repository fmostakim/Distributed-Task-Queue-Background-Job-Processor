package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/goqueue/goqueue/internal/broker"
	"github.com/goqueue/goqueue/internal/task"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	redisPassword := os.Getenv("REDIS_PASSWORD")

	cfg := broker.DefaultRedisConfig()
	cfg.Addr = redisAddr
	cfg.Password = redisPassword

	b, err := broker.NewRedisBroker(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to Redis at %s: %v\n", redisAddr, err)
		os.Exit(1)
	}
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := os.Args[1]
	switch cmd {
	case "status":
		handleStatus(ctx, b)
	case "pause":
		handlePause(ctx, b)
	case "resume":
		handleResume(ctx, b)
	case "dlq":
		handleDLQ(ctx, b)
	case "enqueue":
		handleEnqueue(ctx, b)
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`GoQueue CLI — Operational Tool for GoQueue Distributed Engine

Usage:
  goqueue-cli <command> [arguments]

Commands:
  status                     Display queue status, multi-tier depths, and DLQ counts
  pause <queue>              Pause consumption on a queue
  resume <queue>             Resume consumption on a queue
  dlq list [queue]           List failed tasks in Dead-Letter Queue
  dlq retry <task_id> [q]    Retry a specific task from DLQ
  dlq retry-all [queue]      Retry all tasks in DLQ
  dlq purge [queue]          Purge all tasks from DLQ
  enqueue [flags]            Enqueue a new task

Environment Variables:
  REDIS_ADDR                 Redis address (default: localhost:6379)
  REDIS_PASSWORD             Redis auth password (optional)`)
}

func handleStatus(ctx context.Context, b broker.Broker) {
	stats, err := b.GetAllQueueStats(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting stats: %v\n", err)
		os.Exit(1)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "QUEUE\tSTATUS\tCRITICAL\tHIGH\tDEFAULT\tLOW\tACTIVE\tSCHED\tDLQ\tPROCESSED\tFAILED")
	fmt.Fprintln(w, "-----\t------\t--------\t----\t-------\t---\t------\t-----\t---\t---------\t------")

	for name, qs := range stats {
		status := "RUNNING"
		if qs.IsPaused {
			status = "PAUSED"
		}
		crit := qs.ReadyByPriority[string(task.PriorityCritical)]
		high := qs.ReadyByPriority[string(task.PriorityHigh)]
		def := qs.ReadyByPriority[string(task.PriorityDefault)]
		low := qs.ReadyByPriority[string(task.PriorityLow)]

		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
			name, status, crit, high, def, low, qs.Active, qs.Scheduled, qs.DeadLetter, qs.TotalProcessed, qs.TotalFailed)
	}
	w.Flush()
}

func handlePause(ctx context.Context, b broker.Broker) {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "Usage: goqueue-cli pause <queue_name>")
		os.Exit(1)
	}
	queue := os.Args[2]
	if err := b.PauseQueue(ctx, queue); err != nil {
		fmt.Fprintf(os.Stderr, "Error pausing queue %s: %v\n", queue, err)
		os.Exit(1)
	}
	fmt.Printf("Queue '%s' is now PAUSED.\n", queue)
}

func handleResume(ctx context.Context, b broker.Broker) {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "Usage: goqueue-cli resume <queue_name>")
		os.Exit(1)
	}
	queue := os.Args[2]
	if err := b.ResumeQueue(ctx, queue); err != nil {
		fmt.Fprintf(os.Stderr, "Error resuming queue %s: %v\n", queue, err)
		os.Exit(1)
	}
	fmt.Printf("Queue '%s' is now RESUMED.\n", queue)
}

func handleDLQ(ctx context.Context, b broker.Broker) {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "Usage: goqueue-cli dlq <list|retry|retry-all|purge> [args]")
		os.Exit(1)
	}

	action := os.Args[2]
	switch action {
	case "list":
		queue := "default"
		if len(os.Args) >= 4 {
			queue = os.Args[3]
		}
		tasks, total, err := b.GetDLQTasks(ctx, queue, 0, 50)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting DLQ tasks: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("DLQ Tasks for queue '%s' (Total: %d):\n\n", queue, total)
		if len(tasks) == 0 {
			fmt.Println("No tasks in DLQ.")
			return
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "TASK ID\tTYPE\tPRIORITY\tRETRIES\tLAST ERROR")
		fmt.Fprintln(w, "-------\t----\t--------\t-------\t----------")
		for _, t := range tasks {
			errMsg := t.LastError
			if len(errMsg) > 40 {
				errMsg = errMsg[:37] + "..."
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%d/%d\t%s\n", t.ID, t.Type, t.Priority, t.CurrentRetry, t.MaxRetries, errMsg)
		}
		w.Flush()

	case "retry":
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "Usage: goqueue-cli dlq retry <task_id> [queue]")
			os.Exit(1)
		}
		taskID := os.Args[3]
		queue := "default"
		if len(os.Args) >= 5 {
			queue = os.Args[4]
		}
		if err := b.RetryDLQTask(ctx, taskID, queue); err != nil {
			fmt.Fprintf(os.Stderr, "Error retrying task %s: %v\n", taskID, err)
			os.Exit(1)
		}
		fmt.Printf("Task '%s' successfully requeued from DLQ into '%s' queue.\n", taskID, queue)

	case "retry-all":
		queue := "default"
		if len(os.Args) >= 4 {
			queue = os.Args[3]
		}
		count, err := b.RetryAllDLQTasks(ctx, queue)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error retrying all DLQ tasks: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Requeued %d tasks from DLQ into '%s' queue.\n", count, queue)

	case "purge":
		queue := "default"
		if len(os.Args) >= 4 {
			queue = os.Args[3]
		}
		count, err := b.PurgeDLQ(ctx, queue)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error purging DLQ: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Purged %d tasks from DLQ for queue '%s'.\n", count, queue)

	default:
		fmt.Fprintf(os.Stderr, "Unknown DLQ action: %s\n", action)
		os.Exit(1)
	}
}

func handleEnqueue(ctx context.Context, b broker.Broker) {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	qFlag := fs.String("queue", "default", "Target queue name")
	tFlag := fs.String("type", "", "Task type (required)")
	pFlag := fs.String("payload", "{}", "JSON task payload")
	prioFlag := fs.String("priority", "default", "Priority (critical, high, default, low)")
	retriesFlag := fs.Int("retries", 3, "Max retry attempts")
	delayFlag := fs.Int("delay", 0, "Delay in seconds before execution")

	_ = fs.Parse(os.Args[2:])

	if *tFlag == "" {
		fmt.Fprintln(os.Stderr, "Error: --type is required")
		fs.Usage()
		os.Exit(1)
	}

	prio := task.Priority(strings.ToLower(*prioFlag))
	if !prio.IsValid() {
		prio = task.PriorityDefault
	}

	t := task.NewTask(
		*tFlag,
		[]byte(*pFlag),
		task.WithQueue(*qFlag),
		task.WithPriority(prio),
		task.WithMaxRetries(*retriesFlag),
	)

	if *delayFlag > 0 {
		t.ScheduledAt = time.Now().UTC().Add(time.Duration(*delayFlag) * time.Second)
	}

	if err := b.Enqueue(ctx, t); err != nil {
		fmt.Fprintf(os.Stderr, "Error enqueuing task: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Task enqueued successfully!\nID: %s\nQueue: %s\nType: %s\nPriority: %s\n",
		t.ID, t.Queue, t.Type, t.Priority)
}
