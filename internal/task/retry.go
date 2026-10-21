package task

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

// RetryPolicy defines backoff and jitter calculations for failed tasks.
type RetryPolicy struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Factor    float64
	mu        sync.Mutex
	rng       *rand.Rand
}

// DefaultRetryPolicy returns standard exponential backoff with jitter defaults:
// BaseDelay: 1s, MaxDelay: 5m, Factor: 2.0.
func DefaultRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		BaseDelay: 1 * time.Second,
		MaxDelay:  5 * time.Minute,
		Factor:    2.0,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// NewRetryPolicy creates a custom retry policy.
func NewRetryPolicy(baseDelay, maxDelay time.Duration, factor float64) *RetryPolicy {
	if factor <= 1.0 {
		factor = 2.0
	}
	if baseDelay <= 0 {
		baseDelay = 1 * time.Second
	}
	if maxDelay < baseDelay {
		maxDelay = baseDelay * 10
	}
	return &RetryPolicy{
		BaseDelay: baseDelay,
		MaxDelay:  maxDelay,
		Factor:    factor,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// CalculateBackoff computes the delay for a given retry attempt using Full Jitter algorithm:
// delay = min(MaxDelay, BaseDelay * Factor^attempt)
// sleep = random_between(0, delay)
//
// Full Jitter distributes retry attempts uniformly over the backoff interval,
// preventing the "thundering herd" problem on downstream dependencies and Redis.
func (p *RetryPolicy) CalculateBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	multiplier := math.Pow(p.Factor, float64(attempt-1))
	computed := float64(p.BaseDelay) * multiplier

	if computed > float64(p.MaxDelay) || computed <= 0 {
		computed = float64(p.MaxDelay)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Full Jitter: rand in [0, computed)
	jittered := time.Duration(p.rng.Float64() * computed)
	// Ensure a minimum delay of at least half base delay
	minDelay := p.BaseDelay / 2
	if jittered < minDelay {
		jittered = minDelay
	}

	return jittered
}
