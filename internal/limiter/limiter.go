// internal/ratelimiter/limiter.go
package limiter

import (
	"context"
	"sync"
	"time"
)

type RateLimiter interface {
	Wait(ctx context.Context) error
	TryAcquire() bool
	UpdateLimits(perSecond, burst int)
	GetLimits() (perSecond, burst int)
}

type TokenBucketLimiter struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

func NewTokenBucketLimiter(perSecond, burst int) *TokenBucketLimiter {
	return &TokenBucketLimiter{
		tokens:     float64(burst),
		maxTokens:  float64(burst),
		refillRate: float64(perSecond),
		lastRefill: time.Now(),
	}
}

func (l *TokenBucketLimiter) refill() {
	now := time.Now()
	elapsed := now.Sub(l.lastRefill).Seconds()
	l.tokens += elapsed * l.refillRate

	if l.tokens > l.maxTokens {
		l.tokens = l.maxTokens
	}

	l.lastRefill = now
}

func (l *TokenBucketLimiter) Wait(ctx context.Context) error {
	for {
		l.mu.Lock()
		l.refill()

		if l.tokens >= 1 {
			l.tokens--
			l.mu.Unlock()
			return nil
		}

		// Calculate wait time for next token
		waitTime := time.Duration((1 - l.tokens) / l.refillRate * float64(time.Second))
		l.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
			// Continue and try again
		}
	}
}

func (l *TokenBucketLimiter) TryAcquire() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.refill()

	if l.tokens >= 1 {
		l.tokens--
		return true
	}

	return false
}

func (l *TokenBucketLimiter) UpdateLimits(perSecond, burst int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.refillRate = float64(perSecond)
	l.maxTokens = float64(burst)

	// Adjust current tokens if they exceed new max
	if l.tokens > l.maxTokens {
		l.tokens = l.maxTokens
	}
}

func (l *TokenBucketLimiter) GetLimits() (perSecond, burst int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return int(l.refillRate), int(l.maxTokens)
}

// AdaptiveRateLimiter adjusts limits based on API responses
type AdaptiveRateLimiter struct {
	*TokenBucketLimiter
	mu              sync.Mutex
	originalRate    int
	originalBurst   int
	backoffFactor   float64
	recoveryFactor  float64
	minRate         int
	consecutiveOK   int
	recoveryThreshold int
}

func NewAdaptiveRateLimiter(perSecond, burst int) *AdaptiveRateLimiter {
	return &AdaptiveRateLimiter{
		TokenBucketLimiter: NewTokenBucketLimiter(perSecond, burst),
		originalRate:       perSecond,
		originalBurst:      burst,
		backoffFactor:      0.5,
		recoveryFactor:     1.1,
		minRate:            1,
		recoveryThreshold:  10,
	}
}

func (l *AdaptiveRateLimiter) OnRateLimited() {
	l.mu.Lock()
	defer l.mu.Unlock()

	currentRate, currentBurst := l.GetLimits()
	newRate := int(float64(currentRate) * l.backoffFactor)
	newBurst := int(float64(currentBurst) * l.backoffFactor)

	if newRate < l.minRate {
		newRate = l.minRate
	}
	if newBurst < 1 {
		newBurst = 1
	}

	l.UpdateLimits(newRate, newBurst)
	l.consecutiveOK = 0
}

func (l *AdaptiveRateLimiter) OnSuccess() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.consecutiveOK++

	if l.consecutiveOK >= l.recoveryThreshold {
		currentRate, currentBurst := l.GetLimits()

		if currentRate < l.originalRate {
			newRate := int(float64(currentRate) * l.recoveryFactor)
			if newRate <= currentRate {
				newRate = currentRate + 1
			}
			newBurst := int(float64(currentBurst) * l.recoveryFactor)
			if newBurst <= currentBurst {
				newBurst = currentBurst + 1
			}

			if newRate > l.originalRate {
				newRate = l.originalRate
			}
			if newBurst > l.originalBurst {
				newBurst = l.originalBurst
			}

			l.UpdateLimits(newRate, newBurst)
		}

		l.consecutiveOK = 0
	}
}