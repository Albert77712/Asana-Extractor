package limiter_test

import (
	"context"
	"testing"
	"time"

	"asana-extractor/internal/limiter"
)

func TestTokenBucketLimiter_Wait(t *testing.T) {
	l := limiter.NewTokenBucketLimiter(10, 5)

	ctx := context.Background()

	for i := 0; i < 5; i++ {
		start := time.Now()
		err := l.Wait(ctx)
		if err != nil {
			t.Fatalf("Wait failed: %v", err)
		}
		elapsed := time.Since(start)
		if elapsed > 10*time.Millisecond {
			t.Errorf("Wait took too long for burst: %v", elapsed)
		}
	}

	start := time.Now()
	err := l.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait failed: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < 50*time.Millisecond {
		t.Errorf("Wait should have blocked, but took only: %v", elapsed)
	}
}

func TestTokenBucketLimiter_TryAcquire(t *testing.T) {
	l := limiter.NewTokenBucketLimiter(10, 3)

	for i := 0; i < 3; i++ {
		if !l.TryAcquire() {
			t.Errorf("TryAcquire should succeed for token %d", i+1)
		}
	}

	if l.TryAcquire() {
		t.Error("TryAcquire should fail when no tokens available")
	}

	time.Sleep(150 * time.Millisecond)
	if !l.TryAcquire() {
		t.Error("TryAcquire should succeed after refill")
	}
}

func TestTokenBucketLimiter_UpdateLimits(t *testing.T) {
	l := limiter.NewTokenBucketLimiter(10, 5)

	l.UpdateLimits(20, 10)

	perSecond, burst := l.GetLimits()
	if perSecond != 20 {
		t.Errorf("got perSecond %d, want 20", perSecond)
	}
	if burst != 10 {
		t.Errorf("got burst %d, want 10", burst)
	}
}

func TestTokenBucketLimiter_ContextCancellation(t *testing.T) {
	l := limiter.NewTokenBucketLimiter(1, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := l.Wait(ctx)
	if err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestAdaptiveRateLimiter_BackoffOnRateLimit(t *testing.T) {
	l := limiter.NewAdaptiveRateLimiter(10, 5)

	originalRate, originalBurst := l.GetLimits()

	l.OnRateLimited()

	newRate, newBurst := l.GetLimits()

	if newRate >= originalRate {
		t.Errorf("rate should decrease after rate limit: %d >= %d", newRate, originalRate)
	}
	if newBurst >= originalBurst {
		t.Errorf("burst should decrease after rate limit: %d >= %d", newBurst, originalBurst)
	}
}

func TestAdaptiveRateLimiter_RecoveryOnSuccess(t *testing.T) {
	l := limiter.NewAdaptiveRateLimiter(10, 5)

	l.OnRateLimited()
	afterBackoff, _ := l.GetLimits()

	for i := 0; i < 15; i++ {
		l.OnSuccess()
	}

	afterRecovery, _ := l.GetLimits()

	if afterRecovery <= afterBackoff {
		t.Errorf("rate should recover after success: %d <= %d", afterRecovery, afterBackoff)
	}
}

func TestAdaptiveRateLimiter_MinimumRate(t *testing.T) {
	l := limiter.NewAdaptiveRateLimiter(2, 2)

	for i := 0; i < 10; i++ {
		l.OnRateLimited()
	}

	rate, burst := l.GetLimits()
	if rate < 1 {
		t.Errorf("rate should not go below 1: %d", rate)
	}
	if burst < 1 {
		t.Errorf("burst should not go below 1: %d", burst)
	}
}
