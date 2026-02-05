// internal/handlers/handlers.go
package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"asana-extractor/internal/cache"
	"asana-extractor/internal/config"
	"asana-extractor/internal/limiter"
	"asana-extractor/internal/scheduler"
	"asana-extractor/internal/storage"
	"asana-extractor/internal/worker"
)

type Handlers struct {
	config      *config.Config
	scheduler   *scheduler.Scheduler
	rateLimiter limiter.RateLimiter
	storage     *storage.FileStorage
	cache       cache.Cache
	workerPool  *worker.WorkerPool
}

func NewHandlers(
	cfg *config.Config,
	sched *scheduler.Scheduler,
	limiter limiter.RateLimiter,
	storage *storage.FileStorage,
	cache cache.Cache,
	pool *worker.WorkerPool,
) *Handlers {
	return &Handlers{
		config:      cfg,
		scheduler:   sched,
		rateLimiter: limiter,
		storage:     storage,
		cache:       cache,
		workerPool:  pool,
	}
}

// Response helpers
type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeSuccess(w http.ResponseWriter, data interface{}) {
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Data: data})
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, APIResponse{Success: false, Error: message})
}

// Health check
func (h *Handlers) HealthCheck(w http.ResponseWriter, r *http.Request) {
	writeSuccess(w, map[string]string{"status": "healthy"})
}

// Token management
type UpdateTokenRequest struct {
	Token string `json:"token"`
}

func (h *Handlers) UpdateToken(w http.ResponseWriter, r *http.Request) {
	var req UpdateTokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Token == "" {
		writeError(w, http.StatusBadRequest, "token cannot be empty")
		return
	}

	h.config.SetAuthToken(req.Token)
	writeSuccess(w, map[string]string{"message": "token updated successfully"})
}

func (h *Handlers) GetTokenStatus(w http.ResponseWriter, r *http.Request) {
	token := h.config.GetAuthToken()
	hasToken := token != ""

	writeSuccess(w, map[string]interface{}{
		"has_token":  hasToken,
		"token_hint": maskToken(token),
	})
}

func maskToken(token string) string {
	if len(token) <= 8 {
		return "****"
	}
	return token[:4] + "****" + token[len(token)-4:]
}

// Cron interval management
type UpdateIntervalRequest struct {
	DataType string `json:"data_type"`
	Interval string `json:"interval"` // e.g., "5m", "1h"
}

func (h *Handlers) UpdateCronInterval(w http.ResponseWriter, r *http.Request) {
	var req UpdateIntervalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	dataType := config.DataType(req.DataType)
	if dataType != config.DataTypeProjects && dataType != config.DataTypeUsers {
		writeError(w, http.StatusBadRequest, "invalid data type")
		return
	}

	interval, err := time.ParseDuration(req.Interval)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid interval format")
		return
	}

	if interval < time.Minute {
		writeError(w, http.StatusBadRequest, "interval must be at least 1 minute")
		return
	}

	h.config.SetCronInterval(dataType, interval)
	writeSuccess(w, map[string]string{
		"message":   "interval updated successfully",
		"data_type": string(dataType),
		"interval":  interval.String(),
	})
}

func (h *Handlers) GetCronIntervals(w http.ResponseWriter, r *http.Request) {
	writeSuccess(w, map[string]string{
		"projects": h.config.GetCronInterval(config.DataTypeProjects).String(),
		"users":    h.config.GetCronInterval(config.DataTypeUsers).String(),
	})
}

// Rate limit management
type UpdateRateLimitRequest struct {
	PerSecond int `json:"per_second"`
	Burst     int `json:"burst"`
}

func (h *Handlers) UpdateRateLimit(w http.ResponseWriter, r *http.Request) {
	var req UpdateRateLimitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.PerSecond < 1 || req.Burst < 1 {
		writeError(w, http.StatusBadRequest, "rate limits must be positive")
		return
	}

	h.config.SetRateLimits(req.PerSecond, req.Burst)
	h.rateLimiter.UpdateLimits(req.PerSecond, req.Burst)

	writeSuccess(w, map[string]interface{}{
		"message":    "rate limit updated successfully",
		"per_second": req.PerSecond,
		"burst":      req.Burst,
	})
}

func (h *Handlers) GetRateLimits(w http.ResponseWriter, r *http.Request) {
	perSecond, burst := h.rateLimiter.GetLimits()
	writeSuccess(w, map[string]int{
		"per_second": perSecond,
		"burst":      burst,
	})
}

// Job management
type TriggerJobRequest struct {
	DataType string `json:"data_type"`
}

func (h *Handlers) TriggerJob(w http.ResponseWriter, r *http.Request) {
	var req TriggerJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	dataType := config.DataType(req.DataType)
	if dataType != config.DataTypeProjects && dataType != config.DataTypeUsers {
		writeError(w, http.StatusBadRequest, "invalid data type")
		return
	}

	if err := h.scheduler.TriggerJob(r.Context(), dataType); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	writeSuccess(w, map[string]string{
		"message":   "job triggered successfully",
		"data_type": string(dataType),
	})
}

func (h *Handlers) GetJobStatus(w http.ResponseWriter, r *http.Request) {
	dataType := r.URL.Query().Get("data_type")

	if dataType == "" {
		// Return all job statuses
		statuses := h.scheduler.GetAllJobStatuses()
		writeSuccess(w, statuses)
		return
	}

	status, err := h.scheduler.GetJobStatus(config.DataType(dataType))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeSuccess(w, status)
}

// Storage stats
func (h *Handlers) GetStorageStats(w http.ResponseWriter, r *http.Request) {
	projectStats, _ := h.storage.GetStats(config.DataTypeProjects)
	userStats, _ := h.storage.GetStats(config.DataTypeUsers)

	writeSuccess(w, map[string]interface{}{
		"projects": projectStats,
		"users":    userStats,
	})
}

// Worker stats
func (h *Handlers) GetWorkerStats(w http.ResponseWriter, r *http.Request) {
	stats := h.workerPool.GetStats()
	writeSuccess(w, stats)
}

// Full configuration
func (h *Handlers) GetConfig(w http.ResponseWriter, r *http.Request) {
	perSecond, burst := h.config.GetRateLimits()

	writeSuccess(w, map[string]interface{}{
		"base_api_url":           h.config.BaseAPIURL,
		"has_token":              h.config.GetAuthToken() != "",
		"cron_interval_projects": h.config.GetCronInterval(config.DataTypeProjects).String(),
		"cron_interval_users":    h.config.GetCronInterval(config.DataTypeUsers).String(),
		"rate_limit_per_second":  perSecond,
		"rate_limit_burst":       burst,
		"page_size":              h.config.PageSize,
		"max_concurrent_fetch":   h.config.MaxConcurrentFetch,
		"exports_base_path":      h.config.ExportsBasePath,
	})
}