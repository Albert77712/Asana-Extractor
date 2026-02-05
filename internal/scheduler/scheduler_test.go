package scheduler_test

import (
	"context"
	"os"
	"testing"
	"time"

	"asana-extractor/internal/cache"
	"asana-extractor/internal/config"
	"asana-extractor/internal/scheduler"
	"asana-extractor/internal/storage"
	"asana-extractor/internal/testutil"
	"asana-extractor/internal/worker"
)

func setupSchedulerTest(t *testing.T) (*config.Config, *storage.FileStorage, *cache.ProcessedItemsTracker, *testutil.MockEntityFetcher, string) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "scheduler_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	cfg := config.NewDefaultConfig()
	cfg.ExportsBasePath = tmpDir
	cfg.SetCronInterval(config.DataTypeProjects, 100*time.Millisecond)
	cfg.SetCronInterval(config.DataTypeUsers, 100*time.Millisecond)

	fs, err := storage.NewFileStorage(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create storage: %v", err)
	}

	localCache := cache.NewLocalCache()
	tracker := cache.NewProcessedItemsTracker(localCache, "test", 24*time.Hour)
	mockFetcher := testutil.NewMockEntityFetcher()

	return cfg, fs, tracker, mockFetcher, tmpDir
}

func TestScheduler_TriggerJob(t *testing.T) {
	cfg, fs, tracker, mockFetcher, tmpDir := setupSchedulerTest(t)
	defer os.RemoveAll(tmpDir)

	mockStreamer := testutil.NewMockGUIDStreamer()
	mockStreamer.AddGUIDs(config.DataTypeProjects, "proj-1", "proj-2")

	mockFetcher.AddEntity(config.DataTypeProjects, "proj-1", testutil.CreateTestProject("proj-1"))
	mockFetcher.AddEntity(config.DataTypeProjects, "proj-2", testutil.CreateTestProject("proj-2"))

	pool := worker.NewWorkerPool(2, fs, tracker, mockFetcher)
	sched := scheduler.NewScheduler(cfg, mockStreamer, fs, tracker, pool)

	ctx := context.Background()

	err := sched.TriggerJob(ctx, config.DataTypeProjects)
	if err != nil {
		t.Fatalf("TriggerJob failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if !fs.Exists(config.DataTypeProjects, "proj-1") {
		t.Error("expected proj-1 file to exist")
	}
	if !fs.Exists(config.DataTypeProjects, "proj-2") {
		t.Error("expected proj-2 file to exist")
	}
}

func TestScheduler_GetJobStatus(t *testing.T) {
	cfg, fs, tracker, mockFetcher, tmpDir := setupSchedulerTest(t)
	defer os.RemoveAll(tmpDir)

	mockStreamer := testutil.NewMockGUIDStreamer()
	pool := worker.NewWorkerPool(1, fs, tracker, mockFetcher)
	sched := scheduler.NewScheduler(cfg, mockStreamer, fs, tracker, pool)

	status, err := sched.GetJobStatus(config.DataTypeProjects)
	if err != nil {
		t.Fatalf("GetJobStatus failed: %v", err)
	}

	if status.Status != scheduler.JobStatusIdle {
		t.Errorf("expected idle status, got %v", status.Status)
	}

	_, err = sched.GetJobStatus("unknown")
	if err == nil {
		t.Error("expected error for unknown data type")
	}
}

func TestScheduler_PreventsDuplicateExecution(t *testing.T) {
	cfg, fs, tracker, mockFetcher, tmpDir := setupSchedulerTest(t)
	defer os.RemoveAll(tmpDir)

	mockStreamer := testutil.NewMockGUIDStreamer()
	mockStreamer.AddGUIDs(config.DataTypeProjects, "proj-1")

	mockFetcher.AddEntity(config.DataTypeProjects, "proj-1", testutil.CreateTestProject("proj-1"))
	mockFetcher.Delay = 200 * time.Millisecond

	pool := worker.NewWorkerPool(1, fs, tracker, mockFetcher)
	sched := scheduler.NewScheduler(cfg, mockStreamer, fs, tracker, pool)

	ctx := context.Background()

	err := sched.TriggerJob(ctx, config.DataTypeProjects)
	if err != nil {
		t.Fatalf("First TriggerJob failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	err = sched.TriggerJob(ctx, config.DataTypeProjects)
	if err == nil {
		t.Error("expected error when triggering duplicate job")
	}
}

func TestScheduler_StartAndStop(t *testing.T) {
	cfg, fs, tracker, mockFetcher, tmpDir := setupSchedulerTest(t)
	defer os.RemoveAll(tmpDir)

	mockStreamer := testutil.NewMockGUIDStreamer()
	pool := worker.NewWorkerPool(1, fs, tracker, mockFetcher)
	sched := scheduler.NewScheduler(cfg, mockStreamer, fs, tracker, pool)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sched.Start(ctx)

	time.Sleep(50 * time.Millisecond)

	done := make(chan bool)
	go func() {
		sched.Stop()
		done <- true
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() timed out")
	}
}

func TestScheduler_GetAllJobStatuses(t *testing.T) {
	cfg, fs, tracker, mockFetcher, tmpDir := setupSchedulerTest(t)
	defer os.RemoveAll(tmpDir)

	mockStreamer := testutil.NewMockGUIDStreamer()
	pool := worker.NewWorkerPool(1, fs, tracker, mockFetcher)
	sched := scheduler.NewScheduler(cfg, mockStreamer, fs, tracker, pool)

	statuses := sched.GetAllJobStatuses()

	if len(statuses) != 2 {
		t.Errorf("expected 2 job statuses, got %d", len(statuses))
	}

	hasProjects := false
	hasUsers := false
	for _, s := range statuses {
		if s.DataType == config.DataTypeProjects {
			hasProjects = true
		}
		if s.DataType == config.DataTypeUsers {
			hasUsers = true
		}
	}

	if !hasProjects || !hasUsers {
		t.Error("expected both projects and users job statuses")
	}
}

func TestScheduler_JobFailure(t *testing.T) {
	cfg, fs, tracker, mockFetcher, tmpDir := setupSchedulerTest(t)
	defer os.RemoveAll(tmpDir)

	mockStreamer := testutil.NewMockGUIDStreamer()
	mockStreamer.StreamErr = context.DeadlineExceeded

	pool := worker.NewWorkerPool(1, fs, tracker, mockFetcher)
	sched := scheduler.NewScheduler(cfg, mockStreamer, fs, tracker, pool)

	ctx := context.Background()
	sched.TriggerJob(ctx, config.DataTypeProjects)

	time.Sleep(100 * time.Millisecond)

	status, _ := sched.GetJobStatus(config.DataTypeProjects)
	if status.Status != scheduler.JobStatusFailed {
		t.Errorf("expected failed status, got %v", status.Status)
	}

	if status.LastError == "" {
		t.Error("expected error message for failed job")
	}
}
