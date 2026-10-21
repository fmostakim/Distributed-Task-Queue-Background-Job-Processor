package worker

import (
	"context"
	"math"
	"sync"
	"time"
)

// TokenBucket implements a high-performance token-bucket rate limiter.
type TokenBucket struct {
	mu         sync.Mutex
	rate       float64   // tokens added per second
	capacity   float64   // maximum burst size
	tokens     float64   // current available tokens
	lastRefill time.Time // timestamp of last refill
}

// NewTokenBucket creates a token bucket with rate (tokens/sec) and burst capacity.
func NewTokenBucket(rate float64, capacity int) *TokenBucket {
	capF := float64(capacity)
	if capF <= 0 {
		capF = 1
	}
	return &TokenBucket{
		rate:       rate,
		capacity:   capF,
		tokens:     capF,
		lastRefill: time.Now(),
	}
}

// refill adds newly accumulated tokens based on elapsed time.
func (b *TokenBucket) refill(now time.Time) {
	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	b.tokens = math.Min(b.capacity, b.tokens+(elapsed*b.rate))
	b.lastRefill = now
}

// Allow returns true if a token is immediately available.
func (b *TokenBucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	b.refill(now)

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		return true
	}
	return false
}

// Wait blocks until a token becomes available or the context is cancelled.
func (b *TokenBucket) Wait(ctx context.Context) error {
	for {
		b.mu.Lock()
		now := time.Now()
		b.refill(now)

		if b.tokens >= 1.0 {
			b.tokens -= 1.0
			b.mu.Unlock()
			return nil
		}

		// Calculate sleep time until next token
		needed := 1.0 - b.tokens
		sleepSec := needed / b.rate
		sleepDuration := time.Duration(sleepSec * float64(time.Second))
		b.mu.Unlock()

		if sleepDuration <= 0 {
			sleepDuration = time.Millisecond
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepDuration):
		}
	}
}

// RateLimiterRegistry manages dynamic rate limits per task type.
type RateLimiterRegistry struct {
	mu      sync.RWMutex
	buckets map[string]*TokenBucket
}

// NewRateLimiterRegistry creates an empty registry.
func NewRateLimiterRegistry() *RateLimiterRegistry {
	return &RateLimiterRegistry{
		buckets: make(map[string]*TokenBucket),
	}
}

// SetLimit configures the rate (tokens/sec) and burst size for a given task type.
func (r *RateLimiterRegistry) SetLimit(taskType string, rate float64, burst int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buckets[taskType] = NewTokenBucket(rate, burst)
}

// Wait applies the rate limit for a task type. If no rate limit is set, it returns immediately.
func (r *RateLimiterRegistry) Wait(ctx context.Context, taskType string) error {
	r.mu.RLock()
	bucket, exists := r.buckets[taskType]
	r.mu.RUnlock()

	if !exists {
		return nil // No rate limit configured for this task type
	}

	return bucket.Wait(ctx)
}

// Allow checks if a task type can execute without waiting.
func (r *RateLimiterRegistry) Allow(taskType string) bool {
	r.mu.RLock()
	bucket, exists := r.buckets[taskType]
	r.mu.RUnlock()

	if !exists {
		return true
	}

	return bucket.Allow()
}
