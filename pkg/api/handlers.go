package api

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/goqueue/goqueue/internal/broker"
	"github.com/goqueue/goqueue/internal/task"
	"github.com/goqueue/goqueue/internal/worker"
)

// ServerAPI encapsulates broker and worker references for HTTP handlers.
type ServerAPI struct {
	broker  broker.Broker
	pool    *worker.Pool
	metrics *Metrics
}

// NewServerAPI creates a new API instance.
func NewServerAPI(b broker.Broker, pool *worker.Pool, m *Metrics) *ServerAPI {
	return &ServerAPI{
		broker:  b,
		pool:    pool,
		metrics: m,
	}
}

// HandleHealthz returns health status.
func (api *ServerAPI) HandleHealthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := contextWithTimeout(r, 2*time.Second)
	defer cancel()

	redisErr := api.broker.Ping(ctx)
	status := "healthy"
	redisStatus := "connected"
	statusCode := http.StatusOK

	if redisErr != nil {
		status = "degraded"
		redisStatus = redisErr.Error()
		statusCode = http.StatusServiceUnavailable
	}

	writeJSON(w, statusCode, map[string]interface{}{
		"status":    status,
		"redis":     redisStatus,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

// HandleStats returns cluster and queue metrics.
func (api *ServerAPI) HandleStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := contextWithTimeout(r, 3*time.Second)
	defer cancel()

	allStats, err := api.broker.GetAllQueueStats(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var activeWorkers int32
	var inFlight int32
	if api.pool != nil {
		activeWorkers = int32(api.pool.Concurrency())
		inFlight = api.pool.InFlightCount()
	}

	// Update Prometheus Gauges
	if api.metrics != nil {
		api.metrics.ActiveWorkers.Set(float64(activeWorkers))
		api.metrics.InFlightTasks.Set(float64(inFlight))

		for qName, qs := range allStats {
			for p, count := range qs.ReadyByPriority {
				api.metrics.QueueDepth.WithLabelValues(qName, p).Set(float64(count))
			}
			api.metrics.ActiveTasks.WithLabelValues(qName).Set(float64(qs.Active))
			api.metrics.ScheduledTasks.WithLabelValues(qName).Set(float64(qs.Scheduled))
			api.metrics.DLQTasks.WithLabelValues(qName).Set(float64(qs.DeadLetter))
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"active_workers":  activeWorkers,
		"in_flight_tasks": inFlight,
		"queues":          allStats,
	})
}

// HandlePauseQueue pauses task consumption for a queue.
func (api *ServerAPI) HandlePauseQueue(w http.ResponseWriter, r *http.Request) {
	queue := extractParam(r.URL.Path, "/api/v1/queues/", "/pause")
	if queue == "" {
		queue = "default"
	}

	ctx, cancel := contextWithTimeout(r, 2*time.Second)
	defer cancel()

	if err := api.broker.PauseQueue(ctx, queue); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "paused", "queue": queue})
}

// HandleResumeQueue resumes task consumption for a queue.
func (api *ServerAPI) HandleResumeQueue(w http.ResponseWriter, r *http.Request) {
	queue := extractParam(r.URL.Path, "/api/v1/queues/", "/resume")
	if queue == "" {
		queue = "default"
	}

	ctx, cancel := contextWithTimeout(r, 2*time.Second)
	defer cancel()

	if err := api.broker.ResumeQueue(ctx, queue); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "resumed", "queue": queue})
}

// EnqueueRequest represents payload to create a new task.
type EnqueueRequest struct {
	Queue             string `json:"queue"`
	Type              string `json:"type"`
	Payload           string `json:"payload"`
	Priority          string `json:"priority"`
	MaxRetries        int    `json:"max_retries"`
	DelaySeconds      int    `json:"delay_seconds"`
	TimeoutSeconds    int    `json:"timeout_seconds"`
	VisibilityTimeout int    `json:"visibility_timeout"`
}

// HandleEnqueue creates and inserts a task into the queue.
func (api *ServerAPI) HandleEnqueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request json: "+err.Error())
		return
	}

	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}

	qName := req.Queue
	if qName == "" {
		qName = "default"
	}

	priority := task.Priority(strings.ToLower(req.Priority))
	if !priority.IsValid() {
		priority = task.PriorityDefault
	}

	retries := req.MaxRetries
	if retries <= 0 {
		retries = 3
	}

	timeout := 30 * time.Second
	if req.TimeoutSeconds > 0 {
		timeout = time.Duration(req.TimeoutSeconds) * time.Second
	}

	visTimeout := 60 * time.Second
	if req.VisibilityTimeout > 0 {
		visTimeout = time.Duration(req.VisibilityTimeout) * time.Second
	}

	t := task.NewTask(
		req.Type,
		[]byte(req.Payload),
		task.WithQueue(qName),
		task.WithPriority(priority),
		task.WithMaxRetries(retries),
		task.WithTimeout(timeout),
		task.WithVisibilityTimeout(visTimeout),
	)

	if req.DelaySeconds > 0 {
		t.ScheduledAt = time.Now().UTC().Add(time.Duration(req.DelaySeconds) * time.Second)
	}

	ctx, cancel := contextWithTimeout(r, 2*time.Second)
	defer cancel()

	if err := api.broker.Enqueue(ctx, t); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if api.metrics != nil {
		api.metrics.TasksEnqueued.WithLabelValues(t.Queue, string(t.Priority), t.Type).Inc()
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"status":  "enqueued",
		"task_id": t.ID,
		"task":    t,
	})
}

// HandleGetDLQ returns paginated DLQ tasks.
func (api *ServerAPI) HandleGetDLQ(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Query().Get("queue")
	if queue == "" {
		queue = "default"
	}

	offset, _ := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	limit, _ := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 64)
	if limit <= 0 {
		limit = 50
	}

	ctx, cancel := contextWithTimeout(r, 3*time.Second)
	defer cancel()

	tasks, total, err := api.broker.GetDLQTasks(ctx, queue, offset, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"queue":  queue,
		"total":  total,
		"offset": offset,
		"limit":  limit,
		"tasks":  tasks,
	})
}

// HandleRetryDLQ retries a single DLQ task.
func (api *ServerAPI) HandleRetryDLQ(w http.ResponseWriter, r *http.Request) {
	taskID := extractParam(r.URL.Path, "/api/v1/dlq/", "/retry")
	queue := r.URL.Query().Get("queue")
	if queue == "" {
		queue = "default"
	}

	ctx, cancel := contextWithTimeout(r, 2*time.Second)
	defer cancel()

	if err := api.broker.RetryDLQTask(ctx, taskID, queue); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "retried", "task_id": taskID})
}

// HandleRetryAllDLQ retries all DLQ tasks for a queue.
func (api *ServerAPI) HandleRetryAllDLQ(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Query().Get("queue")
	if queue == "" {
		queue = "default"
	}

	ctx, cancel := contextWithTimeout(r, 5*time.Second)
	defer cancel()

	retried, err := api.broker.RetryAllDLQTasks(ctx, queue)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "retried_all", "count": retried, "queue": queue})
}

// HandlePurgeDLQ purges all DLQ tasks.
func (api *ServerAPI) HandlePurgeDLQ(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Query().Get("queue")
	if queue == "" {
		queue = "default"
	}

	ctx, cancel := contextWithTimeout(r, 3*time.Second)
	defer cancel()

	purged, err := api.broker.PurgeDLQ(ctx, queue)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "purged", "count": purged, "queue": queue})
}

// LoadGenRequest configures dummy load generator.
type LoadGenRequest struct {
	Count       int     `json:"count"`
	FailureRate float64 `json:"failure_rate"`
	DelayRate   float64 `json:"delay_rate"`
	Queue       string  `json:"queue"`
}

// HandleLoadGen synthesizes a burst of diverse tasks for performance benchmarking and demo.
func (api *ServerAPI) HandleLoadGen(w http.ResponseWriter, r *http.Request) {
	var req LoadGenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Count <= 0 {
		req.Count = 100
	}
	if req.Queue == "" {
		req.Queue = "default"
	}

	taskTypes := []string{"send_email", "process_payment", "generate_pdf", "sync_webhook"}
	priorities := []task.Priority{
		task.PriorityCritical,
		task.PriorityHigh,
		task.PriorityDefault,
		task.PriorityLow,
	}

	batch := make([]*task.Task, 0, req.Count)
	now := time.Now().UTC()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < req.Count; i++ {
		tType := taskTypes[rng.Intn(len(taskTypes))]
		prio := priorities[rng.Intn(len(priorities))]

		// Inject simulated failures
		isFailure := rng.Float64() < req.FailureRate
		if isFailure {
			tType = "failing_task"
		}

		payload := fmt.Sprintf(`{"index":%d,"generated_at":"%s","simulated":true}`, i+1, now.Format(time.RFC3339))
		t := task.NewTask(
			tType,
			[]byte(payload),
			task.WithQueue(req.Queue),
			task.WithPriority(prio),
			task.WithMaxRetries(2),
			task.WithTimeout(10*time.Second),
			task.WithVisibilityTimeout(15*time.Second),
		)

		// Inject delayed execution
		if rng.Float64() < req.DelayRate {
			delaySec := rng.Intn(10) + 2
			t.ScheduledAt = now.Add(time.Duration(delaySec) * time.Second)
		}

		batch = append(batch, t)
	}

	ctx, cancel := contextWithTimeout(r, 10*time.Second)
	defer cancel()

	if err := api.broker.EnqueueBatch(ctx, batch); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if api.metrics != nil {
		for _, t := range batch {
			api.metrics.TasksEnqueued.WithLabelValues(t.Queue, string(t.Priority), t.Type).Inc()
		}
	}

	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"status":  "generated",
		"count":   req.Count,
		"queue":   req.Queue,
		"message": fmt.Sprintf("Successfully enqueued %d synthetic tasks", req.Count),
	})
}

// Helpers
func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func contextWithTimeout(r *http.Request, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		d = 5 * time.Second
	}
	return context.WithTimeout(r.Context(), d)
}

func extractParam(path, prefix, suffix string) string {
	idx := strings.Index(path, prefix)
	if idx == -1 {
		return ""
	}
	rest := path[idx+len(prefix):]
	if suffix != "" {
		sIdx := strings.Index(rest, suffix)
		if sIdx != -1 {
			return rest[:sIdx]
		}
	}
	return rest
}
