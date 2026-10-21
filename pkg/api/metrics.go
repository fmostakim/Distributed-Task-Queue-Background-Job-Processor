package api

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus collector references for GoQueue.
type Metrics struct {
	TasksEnqueued  *prometheus.CounterVec
	TasksProcessed *prometheus.CounterVec
	TaskDuration   *prometheus.HistogramVec
	ActiveWorkers  prometheus.Gauge
	InFlightTasks  prometheus.Gauge
	QueueDepth     *prometheus.GaugeVec
	ActiveTasks    *prometheus.GaugeVec
	ScheduledTasks *prometheus.GaugeVec
	DLQTasks       *prometheus.GaugeVec
}

// NewMetrics initializes and registers Prometheus metrics collectors.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	factory := promauto.With(reg)

	return &Metrics{
		TasksEnqueued: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "goqueue",
				Name:      "tasks_enqueued_total",
				Help:      "Total number of tasks enqueued into GoQueue.",
			},
			[]string{"queue", "priority", "type"},
		),
		TasksProcessed: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "goqueue",
				Name:      "tasks_processed_total",
				Help:      "Total number of tasks processed, partitioned by status.",
			},
			[]string{"queue", "type", "status"},
		),
		TaskDuration: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "goqueue",
				Name:      "task_duration_seconds",
				Help:      "Execution duration of tasks in seconds.",
				Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
			},
			[]string{"queue", "type"},
		),
		ActiveWorkers: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "goqueue",
				Name:      "active_workers",
				Help:      "Number of configured concurrent workers.",
			},
		),
		InFlightTasks: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "goqueue",
				Name:      "in_flight_tasks",
				Help:      "Number of tasks currently actively executing.",
			},
		),
		QueueDepth: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "goqueue",
				Name:      "queue_depth",
				Help:      "Current depth of ready queues by priority.",
			},
			[]string{"queue", "priority"},
		),
		ActiveTasks: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "goqueue",
				Name:      "active_tasks",
				Help:      "Current number of tasks actively leased.",
			},
			[]string{"queue"},
		),
		ScheduledTasks: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "goqueue",
				Name:      "scheduled_tasks",
				Help:      "Current number of delayed/scheduled tasks.",
			},
			[]string{"queue"},
		),
		DLQTasks: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "goqueue",
				Name:      "dlq_tasks",
				Help:      "Current number of tasks in the Dead-Letter Queue.",
			},
			[]string{"queue"},
		),
	}
}
