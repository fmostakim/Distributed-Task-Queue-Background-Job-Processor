package api

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server encapsulates the HTTP multiplexer and server lifecycle.
type Server struct {
	httpServer *http.Server
	api        *ServerAPI
}

// NewServer creates a new HTTP server configured with all routes.
func NewServer(addr string, api *ServerAPI) *Server {
	mux := http.NewServeMux()

	// Web Dashboard
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "/dashboard" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(DashboardHTML))
	})

	// Prometheus Exporter
	mux.Handle("/metrics", promhttp.Handler())

	// Health Check
	mux.HandleFunc("/healthz", api.HandleHealthz)

	// REST API Endpoints
	mux.HandleFunc("/api/v1/stats", api.HandleStats)
	mux.HandleFunc("/api/v1/tasks", api.HandleEnqueue)
	mux.HandleFunc("/api/v1/loadgen", api.HandleLoadGen)

	// Dynamic REST Routes Router
	mux.HandleFunc("/api/v1/queues/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/pause") {
			api.HandlePauseQueue(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/resume") {
			api.HandleResumeQueue(w, r)
			return
		}
		http.NotFound(w, r)
	})

	mux.HandleFunc("/api/v1/dlq", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			api.HandleGetDLQ(w, r)
		case http.MethodDelete:
			api.HandlePurgeDLQ(w, r)
		default:
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})

	mux.HandleFunc("/api/v1/dlq/retry-all", api.HandleRetryAllDLQ)

	mux.HandleFunc("/api/v1/dlq/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/retry") {
			api.HandleRetryDLQ(w, r)
			return
		}
		http.NotFound(w, r)
	})

	// Middleware: CORS & Request Logging
	handler := corsMiddleware(mux)

	return &Server{
		httpServer: &http.Server{
			Addr:         addr,
			Handler:      handler,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
		},
		api: api,
	}
}

// Start begins listening on the configured address.
func (s *Server) Start() error {
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Stop cleanly terminates HTTP listeners.
func (s *Server) Stop(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
