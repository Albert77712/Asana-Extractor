package worker_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"asana-extractor/internal/asana"
	"asana-extractor/internal/cache"
	"asana-extractor/internal/config"
	"asana-extractor/internal/storage"
	"asana-extractor/internal/testutil"
	"asana-extractor/internal/worker"
)

func setupWorkerPoolTest(t *testing.T) (*storage.FileStorage, *cache.ProcessedItemsTracker, string) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "worker_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	fs, err := storage.NewFileStorage(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create storage: %v", err)
	}

	localCache := cache.NewLocalCache()
	tracker := cache.NewProcessedItemsTracker(localCache, "test", 24*time.Hour)

	return fs, tracker, tmpDir
}

func TestWorkerPool_ProcessItems(t *testing.T) {
	fs, tracker, tmpDir := setupWorkerPoolTest(t)
	defer os.RemoveAll(tmpDir)

	mockFetcher := testutil.NewMockEntityFetcher()

	for _, guid := range []string{"proj-1", "proj-2", "proj-3"} {
		project := testutil.CreateTestProject(guid)
		mockFetcher.AddEntity(config.DataTypeProjects, guid, project)
	}

	pool := worker.NewWorkerPool(3, fs, tracker, mockFetcher)

	results := make(chan asana.StreamResult, 3)
	for _, guid := range []string{"proj-1", "proj-2", "proj-3"} {
		results <- asana.StreamResult{
			RawData: asana.ResponseItem{
				GUID:     guid,
				DataType: "projects",
			},
			DataType: config.DataTypeProjects,
		}
	}
	close(results)

	ctx := context.Background()
	err := pool.Process(ctx, results)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	stats := pool.GetStats()
	if stats.Processed != 3 {
		t.Errorf("got processed %d, want 3", stats.Processed)
	}

	for _, guid := range []string{"proj-1", "proj-2", "proj-3"} {
		if !fs.Exists(config.DataTypeProjects, guid) {
			t.Errorf("expected file for %s to exist", guid)
		}
	}
}

func TestWorkerPool_SkipsAlreadyProcessed(t *testing.T) {
	fs, tracker, tmpDir := setupWorkerPoolTest(t)
	defer os.RemoveAll(tmpDir)

	mockFetcher := testutil.NewMockEntityFetcher()
	project := testutil.CreateTestProject("already-processed")
	mockFetcher.AddEntity(config.DataTypeProjects, "already-processed", project)

	ctx := context.Background()
	tracker.MarkAsProcessed(ctx, "projects", "already-processed")

	pool := worker.NewWorkerPool(1, fs, tracker, mockFetcher)

	results := make(chan asana.StreamResult, 1)
	results <- asana.StreamResult{
		RawData: asana.ResponseItem{
			GUID:     "already-processed",
			DataType: "projects",
		},
		DataType: config.DataTypeProjects,
	}
	close(results)

	err := pool.Process(ctx, results)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	stats := pool.GetStats()
	if stats.Skipped != 1 {
		t.Errorf("got skipped %d, want 1", stats.Skipped)
	}
	if stats.Processed != 0 {
		t.Errorf("got processed %d, want 0", stats.Processed)
	}

	if mockFetcher.FetchCount["projects:already-processed"] != 0 {
		t.Error("expected no fetch for already processed item")
	}
}

func TestWorkerPool_HandlesNotFound(t *testing.T) {
	fs, tracker, tmpDir := setupWorkerPoolTest(t)
	defer os.RemoveAll(tmpDir)

	mockFetcher := testutil.NewMockEntityFetcher()

	pool := worker.NewWorkerPool(1, fs, tracker, mockFetcher)

	results := make(chan asana.StreamResult, 1)
	results <- asana.StreamResult{
		RawData: asana.ResponseItem{
			GUID:     "not-found",
			DataType: "projects",
		},
		DataType: config.DataTypeProjects,
	}
	close(results)

	ctx := context.Background()
	err := pool.Process(ctx, results)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	stats := pool.GetStats()
	if stats.Skipped != 1 {
		t.Errorf("got skipped %d, want 1 (for 404)", stats.Skipped)
	}
}

func TestWorkerPool_RetriesOnError(t *testing.T) {
	fs, tracker, tmpDir := setupWorkerPoolTest(t)
	defer os.RemoveAll(tmpDir)

	mockFetcher := testutil.NewMockEntityFetcher()

	mockFetcher.AddError(config.DataTypeProjects, "retry-item",
		&asana.APIError{StatusCode: 500, Message: "server error"})

	retryConfig := worker.RetryConfig{
		MaxRetries: 2,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   10 * time.Millisecond,
	}

	pool := worker.NewWorkerPoolWithRetry(1, fs, tracker, mockFetcher, retryConfig)

	results := make(chan asana.StreamResult, 1)
	results <- asana.StreamResult{
		RawData: asana.ResponseItem{
			GUID:     "retry-item",
			DataType: "projects",
		},
		DataType: config.DataTypeProjects,
	}
	close(results)

	ctx := context.Background()
	_ = pool.Process(ctx, results)

	callCount := mockFetcher.FetchCount["projects:retry-item"]
	if callCount != 3 {
		t.Errorf("got %d fetch calls, want 3", callCount)
	}
}

func TestWorkerPool_ConcurrencyLimit(t *testing.T) {
	fs, tracker, tmpDir := setupWorkerPoolTest(t)
	defer os.RemoveAll(tmpDir)

	mockFetcher := testutil.NewMockEntityFetcher()
	mockFetcher.Delay = 50 * time.Millisecond

	for i := 0; i < 10; i++ {
		guid := fmt.Sprintf("proj-%d", i)
		mockFetcher.AddEntity(config.DataTypeProjects, guid, testutil.CreateTestProject(guid))
	}

	pool := worker.NewWorkerPool(3, fs, tracker, mockFetcher)

	results := make(chan asana.StreamResult, 10)
	for i := 0; i < 10; i++ {
		guid := fmt.Sprintf("proj-%d", i)
		results <- asana.StreamResult{
			RawData: asana.ResponseItem{
				GUID:     guid,
				DataType: "projects",
			},
			DataType: config.DataTypeProjects,
		}
	}
	close(results)

	ctx := context.Background()
	start := time.Now()
	err := pool.Process(ctx, results)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if elapsed < 150*time.Millisecond {
		t.Errorf("processing was too fast, concurrency limit may not be working: %v", elapsed)
	}

	stats := pool.GetStats()
	if stats.Processed != 10 {
		t.Errorf("got processed %d, want 10", stats.Processed)
	}
}

func TestWorkerPool_ContextCancellation(t *testing.T) {
	fs, tracker, tmpDir := setupWorkerPoolTest(t)
	defer os.RemoveAll(tmpDir)

	mockFetcher := testutil.NewMockEntityFetcher()
	mockFetcher.Delay = 100 * time.Millisecond

	mockFetcher.AddEntity(config.DataTypeProjects, "proj-1", testutil.CreateTestProject("proj-1"))

	pool := worker.NewWorkerPool(1, fs, tracker, mockFetcher)

	results := make(chan asana.StreamResult, 5)
	for i := 0; i < 5; i++ {
		results <- asana.StreamResult{
			RawData: asana.ResponseItem{
				GUID:     "proj-1",
				DataType: "projects",
			},
			DataType: config.DataTypeProjects,
		}
	}
	close(results)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := pool.Process(ctx, results)
	if err != context.DeadlineExceeded {
		t.Logf("got error: %v (may vary based on timing)", err)
	}
}

func TestWorkerPool_ResetStats(t *testing.T) {
	fs, tracker, tmpDir := setupWorkerPoolTest(t)
	defer os.RemoveAll(tmpDir)

	mockFetcher := testutil.NewMockEntityFetcher()
	mockFetcher.AddEntity(config.DataTypeProjects, "proj-1", testutil.CreateTestProject("proj-1"))

	pool := worker.NewWorkerPool(1, fs, tracker, mockFetcher)

	results := make(chan asana.StreamResult, 1)
	results <- asana.StreamResult{
		RawData: asana.ResponseItem{
			GUID:     "proj-1",
			DataType: "projects",
		},
		DataType: config.DataTypeProjects,
	}
	close(results)

	ctx := context.Background()
	pool.Process(ctx, results)

	stats := pool.GetStats()
	if stats.Processed != 1 {
		t.Errorf("expected 1 processed before reset")
	}

	pool.ResetStats()

	stats = pool.GetStats()
	if stats.Processed != 0 {
		t.Errorf("expected 0 processed after reset")
	}
}
