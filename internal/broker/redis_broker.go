package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/goqueue/goqueue/internal/task"
	"github.com/redis/go-redis/v9"
)

// RedisConfig holds connection and configuration parameters for Redis.
type RedisConfig struct {
	Addr         string        `json:"addr"`
	Password     string        `json:"password"`
	DB           int           `json:"db"`
	KeyPrefix    string        `json:"key_prefix"`
	DialTimeout  time.Duration `json:"dial_timeout"`
	ReadTimeout  time.Duration `json:"read_timeout"`
	WriteTimeout time.Duration `json:"write_timeout"`
	PoolSize     int           `json:"pool_size"`
}

// DefaultRedisConfig provides default settings for local/containerized Redis.
func DefaultRedisConfig() RedisConfig {
	return RedisConfig{
		Addr:         "localhost:6379",
		Password:     "",
		DB:           0,
		KeyPrefix:    "goqueue:",
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     50,
	}
}

// RedisBroker implements the Broker interface using Redis as the state store.
type RedisBroker struct {
	client  redis.UniversalClient
	scripts *ScriptManager
	prefix  string
}

// NewRedisBroker creates a new RedisBroker instance with connected client.
func NewRedisBroker(cfg RedisConfig) (*RedisBroker, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		PoolSize:     cfg.PoolSize,
	})

	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping redis at %s: %w", cfg.Addr, err)
	}

	prefix := cfg.KeyPrefix
	if prefix == "" {
		prefix = "goqueue:"
	}

	return &RedisBroker{
		client:  client,
		scripts: NewScriptManager(),
		prefix:  prefix,
	}, nil
}

// NewRedisBrokerFromClient allows wrapping an existing redis.UniversalClient (e.g. miniredis in tests).
func NewRedisBrokerFromClient(client redis.UniversalClient, prefix string) *RedisBroker {
	if prefix == "" {
		prefix = "goqueue:"
	}
	return &RedisBroker{
		client:  client,
		scripts: NewScriptManager(),
		prefix:  prefix,
	}
}

// Key helper methods
func (b *RedisBroker) taskKey(taskID string) string {
	return b.prefix + "task:" + taskID
}

func (b *RedisBroker) taskKeyPrefix() string {
	return b.prefix + "task:"
}

func (b *RedisBroker) queueReadyKey(queue string, priority task.Priority) string {
	return fmt.Sprintf("%squeue:%s:%s", b.prefix, queue, priority)
}

func (b *RedisBroker) queueActiveKey(queue string) string {
	return fmt.Sprintf("%squeue:%s:active", b.prefix, queue)
}

func (b *RedisBroker) queueScheduledKey(queue string) string {
	return fmt.Sprintf("%squeue:%s:scheduled", b.prefix, queue)
}

func (b *RedisBroker) queueDLQKey(queue string) string {
	return fmt.Sprintf("%squeue:%s:dlq", b.prefix, queue)
}

func (b *RedisBroker) queuePausedKey(queue string) string {
	return fmt.Sprintf("%squeue:%s:paused", b.prefix, queue)
}

func (b *RedisBroker) queueStatsKey(queue string) string {
	return fmt.Sprintf("%sstats:%s", b.prefix, queue)
}

func (b *RedisBroker) queuesSetKey() string {
	return b.prefix + "queues"
}

// Enqueue stores task state and puts it in the appropriate queue.
func (b *RedisBroker) Enqueue(ctx context.Context, t *task.Task) error {
	now := time.Now().UTC()
	if t.CreatedAt.IsZero() {
		t.CreatedAt = now
	}
	if t.ScheduledAt.IsZero() {
		t.ScheduledAt = now
	}

	pipe := b.client.Pipeline()

	// Register queue in known queues set
	pipe.SAdd(ctx, b.queuesSetKey(), t.Queue)

	// Store task hash
	taskKey := b.taskKey(t.ID)
	fields := map[string]interface{}{
		"id":                     t.ID,
		"queue":                  t.Queue,
		"type":                   t.Type,
		"payload":                string(t.Payload),
		"priority":               string(t.Priority),
		"state":                  string(t.State),
		"max_retries":            t.MaxRetries,
		"current_retry":          t.CurrentRetry,
		"timeout_ms":             t.Timeout.Milliseconds(),
		"visibility_timeout_sec": int64(t.VisibilityTimeout.Seconds()),
		"created_at":             t.CreatedAt.Unix(),
		"scheduled_at":           t.ScheduledAt.Unix(),
	}
	pipe.HSet(ctx, taskKey, fields)

	// Check if delayed/scheduled or immediate
	if t.ScheduledAt.After(now.Add(100 * time.Millisecond)) {
		t.State = task.StatePending
		pipe.HSet(ctx, taskKey, "state", string(task.StatePending))
		pipe.ZAdd(ctx, b.queueScheduledKey(t.Queue), redis.Z{
			Score:  float64(t.ScheduledAt.Unix()),
			Member: t.ID,
		})
	} else {
		readyKey := b.queueReadyKey(t.Queue, t.Priority)
		pipe.LPush(ctx, readyKey, t.ID)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to enqueue task %s: %w", t.ID, err)
	}

	return nil
}

// EnqueueBatch enqueues multiple tasks in an optimized pipeline.
func (b *RedisBroker) EnqueueBatch(ctx context.Context, tasks []*task.Task) error {
	if len(tasks) == 0 {
		return nil
	}

	now := time.Now().UTC()
	pipe := b.client.Pipeline()

	for _, t := range tasks {
		if t.CreatedAt.IsZero() {
			t.CreatedAt = now
		}
		if t.ScheduledAt.IsZero() {
			t.ScheduledAt = now
		}

		pipe.SAdd(ctx, b.queuesSetKey(), t.Queue)
		taskKey := b.taskKey(t.ID)
		fields := map[string]interface{}{
			"id":                     t.ID,
			"queue":                  t.Queue,
			"type":                   t.Type,
			"payload":                string(t.Payload),
			"priority":               string(t.Priority),
			"state":                  string(t.State),
			"max_retries":            t.MaxRetries,
			"current_retry":          t.CurrentRetry,
			"timeout_ms":             t.Timeout.Milliseconds(),
			"visibility_timeout_sec": int64(t.VisibilityTimeout.Seconds()),
			"created_at":             t.CreatedAt.Unix(),
			"scheduled_at":           t.ScheduledAt.Unix(),
		}
		pipe.HSet(ctx, taskKey, fields)

		if t.ScheduledAt.After(now.Add(100 * time.Millisecond)) {
			pipe.ZAdd(ctx, b.queueScheduledKey(t.Queue), redis.Z{
				Score:  float64(t.ScheduledAt.Unix()),
				Member: t.ID,
			})
		} else {
			readyKey := b.queueReadyKey(t.Queue, t.Priority)
			pipe.LPush(ctx, readyKey, t.ID)
		}
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to enqueue batch: %w", err)
	}
	return nil
}

// Claim atomically claims the highest priority ready task from the queue.
func (b *RedisBroker) Claim(ctx context.Context, queue string, workerID string, visibilityTimeout time.Duration) (*task.Task, error) {
	keys := []string{
		b.queuePausedKey(queue),
		b.queueReadyKey(queue, task.PriorityCritical),
		b.queueReadyKey(queue, task.PriorityHigh),
		b.queueReadyKey(queue, task.PriorityDefault),
		b.queueReadyKey(queue, task.PriorityLow),
		b.queueActiveKey(queue),
	}

	now := time.Now().UTC().Unix()
	visSec := int64(visibilityTimeout.Seconds())
	if visSec <= 0 {
		visSec = 60
	}

	args := []interface{}{
		now,
		visSec,
		workerID,
		b.taskKeyPrefix(),
	}

	res, err := b.scripts.ClaimTask.Run(ctx, b.client, keys, args...).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrNoTaskAvailable
		}
		return nil, fmt.Errorf("claim script execution failed: %w", err)
	}

	if res == nil {
		return nil, ErrNoTaskAvailable
	}

	taskID, ok := res.(string)
	if !ok || taskID == "" {
		return nil, ErrNoTaskAvailable
	}

	return b.GetTask(ctx, taskID)
}

// Ack acknowledges completion of task.
func (b *RedisBroker) Ack(ctx context.Context, taskID string, queue string) error {
	keys := []string{
		b.queueActiveKey(queue),
		b.taskKey(taskID),
		b.queueStatsKey(queue),
	}
	now := time.Now().UTC().Unix()
	retentionTTL := int64(86400) // retain completed task for 24h for audit/history

	err := b.scripts.AckTask.Run(ctx, b.client, keys, taskID, now, retentionTTL).Err()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("failed to ack task %s: %w", taskID, err)
	}
	return nil
}

// Nack handles failure and schedules retry or DLQ.
func (b *RedisBroker) Nack(ctx context.Context, taskID string, queue string, errReason string, stackTrace string, backoffDelay time.Duration) error {
	keys := []string{
		b.queueActiveKey(queue),
		b.queueScheduledKey(queue),
		b.queueDLQKey(queue),
		b.taskKey(taskID),
		b.queueStatsKey(queue),
	}
	now := time.Now().UTC().Unix()
	delaySec := int64(backoffDelay.Seconds())
	if delaySec <= 0 {
		delaySec = 1
	}

	err := b.scripts.NackTask.Run(ctx, b.client, keys, taskID, now, delaySec, errReason, stackTrace).Err()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("failed to nack task %s: %w", taskID, err)
	}
	return nil
}

// Heartbeat extends the active visibility timeout lease.
func (b *RedisBroker) Heartbeat(ctx context.Context, taskID string, queue string, extendBy time.Duration) error {
	keys := []string{
		b.queueActiveKey(queue),
		b.taskKey(taskID),
	}
	now := time.Now().UTC().Unix()
	extSec := int64(extendBy.Seconds())
	if extSec <= 0 {
		extSec = 30
	}

	res, err := b.scripts.Heartbeat.Run(ctx, b.client, keys, taskID, now, extSec).Int64()
	if err != nil {
		return fmt.Errorf("failed to execute heartbeat for %s: %w", taskID, err)
	}
	if res == 0 {
		return ErrLeaseLost
	}
	return nil
}

// ScheduleDueTasks moves scheduled tasks whose execution time has arrived to the ready lists.
func (b *RedisBroker) ScheduleDueTasks(ctx context.Context, queue string, limit int64) (int64, error) {
	if limit <= 0 {
		limit = 100
	}
	keys := []string{
		b.queueScheduledKey(queue),
		b.queueReadyKey(queue, task.PriorityCritical),
		b.queueReadyKey(queue, task.PriorityHigh),
		b.queueReadyKey(queue, task.PriorityDefault),
		b.queueReadyKey(queue, task.PriorityLow),
	}
	now := time.Now().UTC().Unix()

	res, err := b.scripts.ScheduleDue.Run(ctx, b.client, keys, now, limit, b.taskKeyPrefix()).Int64()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		return 0, fmt.Errorf("schedule due tasks failed: %w", err)
	}
	return res, nil
}

// ReclaimExpiredLeases reclaims abandoned tasks in active queue.
func (b *RedisBroker) ReclaimExpiredLeases(ctx context.Context, queue string, limit int64) (int64, int64, error) {
	if limit <= 0 {
		limit = 100
	}
	keys := []string{
		b.queueActiveKey(queue),
		b.queueScheduledKey(queue),
		b.queueDLQKey(queue),
		b.queueStatsKey(queue),
	}
	now := time.Now().UTC().Unix()
	baseBackoffSec := int64(5)

	res, err := b.scripts.ReclaimExpired.Run(ctx, b.client, keys, now, limit, baseBackoffSec, b.taskKeyPrefix()).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("reclaim expired leases failed: %w", err)
	}

	slice, ok := res.([]interface{})
	if !ok || len(slice) < 2 {
		return 0, 0, nil
	}

	var reclaimed, deadlettered int64
	if v, ok := slice[0].(int64); ok {
		reclaimed = v
	}
	if v, ok := slice[1].(int64); ok {
		deadlettered = v
	}

	return reclaimed, deadlettered, nil
}

// GetTask retrieves task details by ID.
func (b *RedisBroker) GetTask(ctx context.Context, taskID string) (*task.Task, error) {
	fields, err := b.client.HGetAll(ctx, b.taskKey(taskID)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get task %s: %w", taskID, err)
	}
	if len(fields) == 0 {
		return nil, ErrTaskNotFound
	}

	t := &task.Task{
		ID:           fields["id"],
		Queue:        fields["queue"],
		Type:         fields["type"],
		Payload:      []byte(fields["payload"]),
		Priority:     task.Priority(fields["priority"]),
		State:        task.State(fields["state"]),
		LastError:    fields["last_error"],
		StackTrace:   fields["stack_trace"],
		WorkerID:     fields["worker_id"],
	}

	if val, ok := fields["max_retries"]; ok {
		t.MaxRetries, _ = strconv.Atoi(val)
	}
	if val, ok := fields["current_retry"]; ok {
		t.CurrentRetry, _ = strconv.Atoi(val)
	}
	if val, ok := fields["timeout_ms"]; ok {
		ms, _ := strconv.ParseInt(val, 10, 64)
		t.Timeout = time.Duration(ms) * time.Millisecond
	}
	if val, ok := fields["visibility_timeout_sec"]; ok {
		sec, _ := strconv.ParseInt(val, 10, 64)
		t.VisibilityTimeout = time.Duration(sec) * time.Second
	}
	if val, ok := fields["created_at"]; ok {
		sec, _ := strconv.ParseInt(val, 10, 64)
		t.CreatedAt = time.Unix(sec, 0).UTC()
	}
	if val, ok := fields["scheduled_at"]; ok {
		sec, _ := strconv.ParseInt(val, 10, 64)
		t.ScheduledAt = time.Unix(sec, 0).UTC()
	}
	if val, ok := fields["last_failed_at"]; ok && val != "" {
		sec, _ := strconv.ParseInt(val, 10, 64)
		tm := time.Unix(sec, 0).UTC()
		t.LastFailedAt = &tm
	}
	if val, ok := fields["heartbeat_at"]; ok && val != "" {
		sec, _ := strconv.ParseInt(val, 10, 64)
		tm := time.Unix(sec, 0).UTC()
		t.HeartbeatAt = &tm
	}
	if val, ok := fields["completed_at"]; ok && val != "" {
		sec, _ := strconv.ParseInt(val, 10, 64)
		tm := time.Unix(sec, 0).UTC()
		t.CompletedAt = &tm
	}

	return t, nil
}

// GetQueueStats returns queue depths, state, and stats.
func (b *RedisBroker) GetQueueStats(ctx context.Context, queue string) (*QueueStats, error) {
	pipe := b.client.Pipeline()

	pausedCmd := pipe.Exists(ctx, b.queuePausedKey(queue))
	critCmd := pipe.LLen(ctx, b.queueReadyKey(queue, task.PriorityCritical))
	highCmd := pipe.LLen(ctx, b.queueReadyKey(queue, task.PriorityHigh))
	defCmd := pipe.LLen(ctx, b.queueReadyKey(queue, task.PriorityDefault))
	lowCmd := pipe.LLen(ctx, b.queueReadyKey(queue, task.PriorityLow))
	actCmd := pipe.ZCard(ctx, b.queueActiveKey(queue))
	schedCmd := pipe.ZCard(ctx, b.queueScheduledKey(queue))
	dlqCmd := pipe.ZCard(ctx, b.queueDLQKey(queue))
	statsCmd := pipe.HGetAll(ctx, b.queueStatsKey(queue))

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to fetch stats for %s: %w", queue, err)
	}

	readyMap := map[string]int64{
		string(task.PriorityCritical): critCmd.Val(),
		string(task.PriorityHigh):     highCmd.Val(),
		string(task.PriorityDefault):  defCmd.Val(),
		string(task.PriorityLow):      lowCmd.Val(),
	}

	totalReady := critCmd.Val() + highCmd.Val() + defCmd.Val() + lowCmd.Val()

	var processed, failed int64
	if stMap := statsCmd.Val(); stMap != nil {
		if p, ok := stMap["processed"]; ok {
			processed, _ = strconv.ParseInt(p, 10, 64)
		}
		if f, ok := stMap["failed"]; ok {
			failed, _ = strconv.ParseInt(f, 10, 64)
		}
	}

	return &QueueStats{
		QueueName:       queue,
		IsPaused:        pausedCmd.Val() > 0,
		ReadyByPriority: readyMap,
		TotalReady:      totalReady,
		Active:          actCmd.Val(),
		Scheduled:       schedCmd.Val(),
		DeadLetter:      dlqCmd.Val(),
		TotalProcessed:  processed,
		TotalFailed:     failed,
	}, nil
}

// GetAllQueueStats returns stats for all known queues.
func (b *RedisBroker) GetAllQueueStats(ctx context.Context) (map[string]*QueueStats, error) {
	queues, err := b.client.SMembers(ctx, b.queuesSetKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch queues: %w", err)
	}

	if len(queues) == 0 {
		queues = []string{"default"}
	}

	stats := make(map[string]*QueueStats)
	for _, q := range queues {
		qs, err := b.GetQueueStats(ctx, q)
		if err != nil {
			return nil, err
		}
		stats[q] = qs
	}
	return stats, nil
}

// PauseQueue halts processing on the queue.
func (b *RedisBroker) PauseQueue(ctx context.Context, queue string) error {
	return b.client.Set(ctx, b.queuePausedKey(queue), "1", 0).Err()
}

// ResumeQueue unpauses the queue.
func (b *RedisBroker) ResumeQueue(ctx context.Context, queue string) error {
	return b.client.Del(ctx, b.queuePausedKey(queue)).Err()
}

// IsQueuePaused checks if queue is paused.
func (b *RedisBroker) IsQueuePaused(ctx context.Context, queue string) (bool, error) {
	count, err := b.client.Exists(ctx, b.queuePausedKey(queue)).Result()
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// GetDLQTasks returns paged dead-letter tasks.
func (b *RedisBroker) GetDLQTasks(ctx context.Context, queue string, offset, limit int64) ([]*task.Task, int64, error) {
	dlqKey := b.queueDLQKey(queue)
	total, err := b.client.ZCard(ctx, dlqKey).Result()
	if err != nil {
		return nil, 0, err
	}

	if total == 0 {
		return []*task.Task{}, 0, nil
	}

	taskIDs, err := b.client.ZRevRange(ctx, dlqKey, offset, offset+limit-1).Result()
	if err != nil {
		return nil, 0, err
	}

	tasks := make([]*task.Task, 0, len(taskIDs))
	for _, id := range taskIDs {
		t, err := b.GetTask(ctx, id)
		if err == nil {
			tasks = append(tasks, t)
		}
	}

	return tasks, total, nil
}

// RetryDLQTask re-enqueues a specific task from DLQ.
func (b *RedisBroker) RetryDLQTask(ctx context.Context, taskID string, queue string) error {
	keys := []string{
		b.queueDLQKey(queue),
		b.queueReadyKey(queue, task.PriorityCritical),
		b.queueReadyKey(queue, task.PriorityHigh),
		b.queueReadyKey(queue, task.PriorityDefault),
		b.queueReadyKey(queue, task.PriorityLow),
		b.taskKey(taskID),
	}

	res, err := b.scripts.RetryDLQ.Run(ctx, b.client, keys, taskID).Int64()
	if err != nil {
		return fmt.Errorf("failed to retry DLQ task %s: %w", taskID, err)
	}
	if res == 0 {
		return fmt.Errorf("task %s not found in DLQ", taskID)
	}
	return nil
}

// RetryAllDLQTasks re-enqueues all tasks currently in DLQ.
func (b *RedisBroker) RetryAllDLQTasks(ctx context.Context, queue string) (int64, error) {
	dlqKey := b.queueDLQKey(queue)
	taskIDs, err := b.client.ZRange(ctx, dlqKey, 0, -1).Result()
	if err != nil {
		return 0, err
	}

	var retried int64
	for _, id := range taskIDs {
		if err := b.RetryDLQTask(ctx, id, queue); err == nil {
			retried++
		}
	}
	return retried, nil
}

// PurgeDLQ deletes all tasks in DLQ.
func (b *RedisBroker) PurgeDLQ(ctx context.Context, queue string) (int64, error) {
	keys := []string{b.queueDLQKey(queue)}
	res, err := b.scripts.PurgeDLQ.Run(ctx, b.client, keys, b.taskKeyPrefix()).Int64()
	if err != nil {
		return 0, fmt.Errorf("failed to purge DLQ: %w", err)
	}
	return res, nil
}

// Close closes the Redis connection.
func (b *RedisBroker) Close() error {
	return b.client.Close()
}

// Ping checks if Redis is reachable.
func (b *RedisBroker) Ping(ctx context.Context) error {
	return b.client.Ping(ctx).Err()
}

// MarshalJSON helper for serializing any object.
func toJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
