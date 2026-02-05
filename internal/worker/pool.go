// internal/worker/pool.go
package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"asana-extractor/internal/asana"
	"asana-extractor/internal/cache"
	"asana-extractor/internal/storage"
	"asana-extractor/pkg/logger"
	"asana-extractor/internal/config"
)

// EntityFetcher interface for fetching full entity details
type EntityFetcher interface {
	FetchEntity(ctx context.Context, dataType config.DataType, guid string) ([]byte, error)
}

type WorkerPool struct {
	maxWorkers       int
	storage          *storage.FileStorage
	processedTracker *cache.ProcessedItemsTracker
	entityFetcher    EntityFetcher
	stats            *Stats
	retryConfig      RetryConfig
}

type RetryConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

type Stats struct {
	Processed  int64 `json:"processed"`
	Skipped    int64 `json:"skipped"`
	Failed     int64 `json:"failed"`
	InProgress int64 `json:"in_progress"`
}

func NewWorkerPool(
	maxWorkers int,
	storage *storage.FileStorage,
	tracker *cache.ProcessedItemsTracker,
	fetcher EntityFetcher,
) *WorkerPool {
	return &WorkerPool{
		maxWorkers:       maxWorkers,
		storage:          storage,
		processedTracker: tracker,
		entityFetcher:    fetcher,
		stats:            &Stats{},
		retryConfig: RetryConfig{
			MaxRetries: 3,
			BaseDelay:  time.Second,
			MaxDelay:   30 * time.Second,
		},
	}
}

func NewWorkerPoolWithRetry(
	maxWorkers int,
	storage *storage.FileStorage,
	tracker *cache.ProcessedItemsTracker,
	fetcher EntityFetcher,
	retryConfig RetryConfig,
) *WorkerPool {
	wp := NewWorkerPool(maxWorkers, storage, tracker, fetcher)
	wp.retryConfig = retryConfig
	return wp
}

func (wp *WorkerPool) Process(ctx context.Context, results <-chan asana.StreamResult) error {
	var wg sync.WaitGroup
	errChan := make(chan error, wp.maxWorkers)

	// Create semaphore for limiting concurrent workers
	sem := make(chan struct{}, wp.maxWorkers)

	for result := range results {
		if result.Err != nil {
			return result.Err
		}

		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Acquire semaphore
		sem <- struct{}{}
		wg.Add(1)
		atomic.AddInt64(&wp.stats.InProgress, 1)

		go func(res asana.StreamResult) {
			defer wg.Done()
			defer func() { <-sem }()
			defer atomic.AddInt64(&wp.stats.InProgress, -1)

			if err := wp.processItem(ctx, res); err != nil {
				atomic.AddInt64(&wp.stats.Failed, 1)
				logger.Error("Failed to process item", "error", err)
				select {
				case errChan <- err:
				default:
				}
			}
		}(result)
	}

	wg.Wait()
	close(errChan)

	// Return first error if any
	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

func (wp *WorkerPool) processItem(ctx context.Context, result asana.StreamResult) error {
	guid := result.RawData.GUID
	dataType := result.DataType

	// Check if already processed using cache
	if wp.processedTracker.IsProcessed(ctx, string(result.DataType), guid) {
		atomic.AddInt64(&wp.stats.Skipped, 1)
		logger.Debug("Skipping already processed item", "guid", guid, "type", dataType)
		return nil
	}

	// Fetch full entity details with retry
	var fullEntity []byte
	var lastErr error

	for attempt := 0; attempt <= wp.retryConfig.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := wp.calculateBackoff(attempt)
			logger.Debug("Retrying fetch", "guid", guid, "attempt", attempt, "delay", delay)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		var err error
		fullEntity, err = wp.entityFetcher.FetchEntity(ctx, config.DataType(dataType), guid)
		if err == nil {
			lastErr = nil
			break
		}

		lastErr = err

		// Don't retry on non-retryable errors
		if apiErr, ok := err.(*asana.APIError); ok {
			if apiErr.IsNotFound() {
				logger.Warn("Entity not found, skipping", "guid", guid, "type", dataType)
				atomic.AddInt64(&wp.stats.Skipped, 1)
				return nil
			}
			if !apiErr.IsRetryable() {
				return err
			}
		}
	}

	if lastErr != nil {
		return lastErr
	}

	// Save the full entity
	if err := wp.storage.SaveRaw(config.DataType(dataType), guid, fullEntity); err != nil {
		return err
	}

	// Mark as processed in cache
	if err := wp.processedTracker.MarkAsProcessed(ctx, string(dataType), guid); err != nil {
		logger.Warn("Failed to mark item as processed in cache", "guid", guid, "error", err)
	}

	atomic.AddInt64(&wp.stats.Processed, 1)
	logger.Debug("Processed item", "guid", guid, "type", dataType)

	return nil
}

func (wp *WorkerPool) calculateBackoff(attempt int) time.Duration {
	delay := wp.retryConfig.BaseDelay * time.Duration(1<<uint(attempt-1))
	if delay > wp.retryConfig.MaxDelay {
		delay = wp.retryConfig.MaxDelay
	}
	return delay
}

func (wp *WorkerPool) GetStats() Stats {
	return Stats{
		Processed:  atomic.LoadInt64(&wp.stats.Processed),
		Skipped:    atomic.LoadInt64(&wp.stats.Skipped),
		Failed:     atomic.LoadInt64(&wp.stats.Failed),
		InProgress: atomic.LoadInt64(&wp.stats.InProgress),
	}
}

func (wp *WorkerPool) ResetStats() {
	atomic.StoreInt64(&wp.stats.Processed, 0)
	atomic.StoreInt64(&wp.stats.Skipped, 0)
	atomic.StoreInt64(&wp.stats.Failed, 0)
	atomic.StoreInt64(&wp.stats.InProgress, 0)
}
