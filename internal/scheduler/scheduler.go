// internal/scheduler/scheduler.go
package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"asana-extractor/internal/asana"
	"asana-extractor/internal/cache"
	"asana-extractor/internal/config"
	"asana-extractor/internal/storage"
	"asana-extractor/internal/worker"
	"asana-extractor/pkg/logger"
)

type JobStatus string

const (
	JobStatusIdle     JobStatus = "idle"
	JobStatusRunning  JobStatus = "running"
	JobStatusFinished JobStatus = "finished"
	JobStatusFailed   JobStatus = "failed"
)

type Job struct {
	DataType    config.DataType
	Status      JobStatus
	LastRun     time.Time
	LastError   string
	RunDuration time.Duration
	mu          sync.RWMutex
	running     int32
	cancel      context.CancelFunc
}

func (j *Job) isRunning() bool {
	return atomic.LoadInt32(&j.running) == 1
}

// tryStart atomically sets running to true. Returns false if already running.
func (j *Job) tryStart() bool {
	return atomic.CompareAndSwapInt32(&j.running, 0, 1)
}

func (j *Job) setDone() {
	atomic.StoreInt32(&j.running, 0)
}

// GUIDStreamer interface for streaming GUIDs
type GUIDStreamer interface {
	StreamGUIDs(ctx context.Context, dataType config.DataType) (<-chan asana.StreamResult, error)
}

type Scheduler struct {
	config           *config.Config
	guidStreamer     GUIDStreamer
	storage          *storage.FileStorage
	processedTracker *cache.ProcessedItemsTracker
	workerPool       *worker.WorkerPool

	jobs     map[config.DataType]*Job
	jobsMu   sync.RWMutex
	stopChan chan struct{}
	wg       sync.WaitGroup
}

func NewScheduler(
	cfg *config.Config,
	streamer GUIDStreamer,
	storage *storage.FileStorage,
	tracker *cache.ProcessedItemsTracker,
	pool *worker.WorkerPool,
) *Scheduler {
	return &Scheduler{
		config:           cfg,
		guidStreamer:     streamer,
		storage:          storage,
		processedTracker: tracker,
		workerPool:       pool,
		jobs: map[config.DataType]*Job{
			config.DataTypeProjects: {DataType: config.DataTypeProjects, Status: JobStatusIdle},
			config.DataTypeUsers:    {DataType: config.DataTypeUsers, Status: JobStatusIdle},
		},
		stopChan: make(chan struct{}),
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	logger.Info("Starting scheduler")

	for dataType := range s.jobs {
		s.wg.Add(1)
		go s.runJobLoop(ctx, dataType)
	}
}

func (s *Scheduler) Stop() {
	logger.Info("Stopping scheduler")
	close(s.stopChan)

	s.jobsMu.RLock()
	for _, job := range s.jobs {
		if job.cancel != nil {
			job.cancel()
		}
	}
	s.jobsMu.RUnlock()

	s.wg.Wait()
	logger.Info("Scheduler stopped")
}

func (s *Scheduler) runJobLoop(ctx context.Context, dataType config.DataType) {
	defer s.wg.Done()

	s.executeJob(ctx, dataType)

	for {
		interval := s.config.GetCronInterval(dataType)
		timer := time.NewTimer(interval)

		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-s.stopChan:
			timer.Stop()
			return
		case <-timer.C:
			s.executeJob(ctx, dataType)
		}
	}
}

func (s *Scheduler) executeJob(ctx context.Context, dataType config.DataType) {
	s.jobsMu.RLock()
	job := s.jobs[dataType]
	s.jobsMu.RUnlock()

	if !job.tryStart() {
		logger.Warn("Skipping job execution - previous run still in progress",
			"dataType", dataType)
		return
	}

	job.mu.Lock()
	job.Status = JobStatusRunning
	job.LastRun = time.Now()
	job.mu.Unlock()

	jobCtx, cancel := context.WithCancel(ctx)
	job.mu.Lock()
	job.cancel = cancel
	job.mu.Unlock()

	startTime := time.Now()

	err := s.runFetch(jobCtx, dataType)

	job.mu.Lock()
	job.RunDuration = time.Since(startTime)
	if err != nil {
		job.Status = JobStatusFailed
		job.LastError = err.Error()
		logger.Error("Job failed", "dataType", dataType, "error", err, "duration", job.RunDuration)
	} else {
		job.Status = JobStatusFinished
		job.LastError = ""
		logger.Info("Job completed successfully", "dataType", dataType, "duration", job.RunDuration)
	}
	job.mu.Unlock()

	job.setDone()
	cancel()
}

func (s *Scheduler) runFetch(ctx context.Context, dataType config.DataType) error {
	logger.Info("Starting fetch job", "dataType", dataType)

	s.workerPool.ResetStats()

	// Stream GUIDs from paginated API
	results, err := s.guidStreamer.StreamGUIDs(ctx, dataType)
	if err != nil {
		return err
	}

	// Process items using worker pool (which fetches full details)
	if err := s.workerPool.Process(ctx, results); err != nil {
		return err
	}

	stats := s.workerPool.GetStats()
	logger.Info("Fetch job completed",
		"dataType", dataType,
		"processed", stats.Processed,
		"skipped", stats.Skipped,
		"failed", stats.Failed)

	return nil
}

func (s *Scheduler) TriggerJob(ctx context.Context, dataType config.DataType) error {
	s.jobsMu.RLock()
	job, exists := s.jobs[dataType]
	s.jobsMu.RUnlock()

	if !exists {
		return &JobError{Message: "unknown data type"}
	}

	if job.isRunning() {
		return &JobError{Message: "job is already running"}
	}

	go s.executeJob(ctx, dataType)
	return nil
}

func (s *Scheduler) GetJobStatus(dataType config.DataType) (JobStatusInfo, error) {
	s.jobsMu.RLock()
	job, exists := s.jobs[dataType]
	s.jobsMu.RUnlock()

	if !exists {
		return JobStatusInfo{}, &JobError{Message: "unknown data type"}
	}

	job.mu.RLock()
	defer job.mu.RUnlock()

	return JobStatusInfo{
		DataType:    job.DataType,
		Status:      job.Status,
		LastRun:     job.LastRun,
		LastError:   job.LastError,
		RunDuration: job.RunDuration,
		IsRunning:   job.isRunning(),
	}, nil
}

func (s *Scheduler) GetAllJobStatuses() []JobStatusInfo {
	s.jobsMu.RLock()
	defer s.jobsMu.RUnlock()

	statuses := make([]JobStatusInfo, 0, len(s.jobs))
	for _, job := range s.jobs {
		job.mu.RLock()
		statuses = append(statuses, JobStatusInfo{
			DataType:    job.DataType,
			Status:      job.Status,
			LastRun:     job.LastRun,
			LastError:   job.LastError,
			RunDuration: job.RunDuration,
			IsRunning:   job.isRunning(),
		})
		job.mu.RUnlock()
	}

	return statuses
}

type JobStatusInfo struct {
	DataType    config.DataType `json:"data_type"`
	Status      JobStatus       `json:"status"`
	LastRun     time.Time       `json:"last_run"`
	LastError   string          `json:"last_error,omitempty"`
	RunDuration time.Duration   `json:"run_duration"`
	IsRunning   bool            `json:"is_running"`
}

type JobError struct {
	Message string
}

func (e *JobError) Error() string {
	return e.Message
}