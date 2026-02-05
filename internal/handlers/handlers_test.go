package handlers_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"asana-extractor/internal/cache"
	"asana-extractor/internal/config"
	"asana-extractor/internal/handlers"
	"asana-extractor/internal/scheduler"
	"asana-extractor/internal/storage"
	"asana-extractor/internal/testutil"
	"asana-extractor/internal/worker"
)

func setupHandlersTest(t *testing.T) (*handlers.Handlers, string) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "handlers_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	cfg := config.NewDefaultConfig()
	cfg.ExportsBasePath = tmpDir

	fs, _ := storage.NewFileStorage(tmpDir)
	localCache := cache.NewLocalCache()
	tracker := cache.NewProcessedItemsTracker(localCache, "test", 24*time.Hour)
	mockFetcher := testutil.NewMockEntityFetcher()
	mockStreamer := testutil.NewMockGUIDStreamer()
	mockLimiter := testutil.NewMockRateLimiter()

	pool := worker.NewWorkerPool(2, fs, tracker, mockFetcher)
	sched := scheduler.NewScheduler(cfg, mockStreamer, fs, tracker, pool)

	h := handlers.NewHandlers(cfg, sched, mockLimiter, fs, localCache, pool)

	return h, tmpDir
}

func TestHandlers_HealthCheck(t *testing.T) {
	h, tmpDir := setupHandlersTest(t)
	defer os.RemoveAll(tmpDir)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	h.HealthCheck(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}

	var resp handlers.APIResponse
	json.NewDecoder(w.Body).Decode(&resp)

	if !resp.Success {
		t.Error("expected success response")
	}
}

func TestHandlers_UpdateToken(t *testing.T) {
	h, tmpDir := setupHandlersTest(t)
	defer os.RemoveAll(tmpDir)

	body := bytes.NewBufferString(`{"token": "new-test-token"}`)
	req := httptest.NewRequest(http.MethodPut, "/api/token", body)
	w := httptest.NewRecorder()

	h.UpdateToken(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}

	var resp handlers.APIResponse
	json.NewDecoder(w.Body).Decode(&resp)

	if !resp.Success {
		t.Error("expected success response")
	}
}

func TestHandlers_UpdateToken_Empty(t *testing.T) {
	h, tmpDir := setupHandlersTest(t)
	defer os.RemoveAll(tmpDir)

	body := bytes.NewBufferString(`{"token": ""}`)
	req := httptest.NewRequest(http.MethodPut, "/api/token", body)
	w := httptest.NewRecorder()

	h.UpdateToken(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("got status %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandlers_UpdateCronInterval(t *testing.T) {
	h, tmpDir := setupHandlersTest(t)
	defer os.RemoveAll(tmpDir)

	body := bytes.NewBufferString(`{"data_type": "projects", "interval": "10m"}`)
	req := httptest.NewRequest(http.MethodPut, "/api/cron/interval", body)
	w := httptest.NewRecorder()

	h.UpdateCronInterval(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandlers_UpdateCronInterval_TooShort(t *testing.T) {
	h, tmpDir := setupHandlersTest(t)
	defer os.RemoveAll(tmpDir)

	body := bytes.NewBufferString(`{"data_type": "projects", "interval": "30s"}`)
	req := httptest.NewRequest(http.MethodPut, "/api/cron/interval", body)
	w := httptest.NewRecorder()

	h.UpdateCronInterval(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("got status %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandlers_GetCronIntervals(t *testing.T) {
	h, tmpDir := setupHandlersTest(t)
	defer os.RemoveAll(tmpDir)

	req := httptest.NewRequest(http.MethodGet, "/api/cron/intervals", nil)
	w := httptest.NewRecorder()

	h.GetCronIntervals(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}

	var resp handlers.APIResponse
	json.NewDecoder(w.Body).Decode(&resp)

	if !resp.Success {
		t.Error("expected success response")
	}

	data := resp.Data.(map[string]interface{})
	if _, ok := data["projects"]; !ok {
		t.Error("expected projects interval in response")
	}
	if _, ok := data["users"]; !ok {
		t.Error("expected users interval in response")
	}
}

func TestHandlers_UpdateRateLimit(t *testing.T) {
	h, tmpDir := setupHandlersTest(t)
	defer os.RemoveAll(tmpDir)

	body := bytes.NewBufferString(`{"per_second": 20, "burst": 10}`)
	req := httptest.NewRequest(http.MethodPut, "/api/rate-limit", body)
	w := httptest.NewRecorder()

	h.UpdateRateLimit(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandlers_UpdateRateLimit_Invalid(t *testing.T) {
	h, tmpDir := setupHandlersTest(t)
	defer os.RemoveAll(tmpDir)

	body := bytes.NewBufferString(`{"per_second": 0, "burst": 0}`)
	req := httptest.NewRequest(http.MethodPut, "/api/rate-limit", body)
	w := httptest.NewRecorder()

	h.UpdateRateLimit(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("got status %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandlers_GetRateLimits(t *testing.T) {
	h, tmpDir := setupHandlersTest(t)
	defer os.RemoveAll(tmpDir)

	req := httptest.NewRequest(http.MethodGet, "/api/rate-limit", nil)
	w := httptest.NewRecorder()

	h.GetRateLimits(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandlers_TriggerJob(t *testing.T) {
	h, tmpDir := setupHandlersTest(t)
	defer os.RemoveAll(tmpDir)

	body := bytes.NewBufferString(`{"data_type": "projects"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/trigger", body)
	w := httptest.NewRecorder()

	h.TriggerJob(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandlers_TriggerJob_InvalidType(t *testing.T) {
	h, tmpDir := setupHandlersTest(t)
	defer os.RemoveAll(tmpDir)

	body := bytes.NewBufferString(`{"data_type": "invalid"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/trigger", body)
	w := httptest.NewRecorder()

	h.TriggerJob(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("got status %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandlers_GetJobStatus(t *testing.T) {
	h, tmpDir := setupHandlersTest(t)
	defer os.RemoveAll(tmpDir)

	req := httptest.NewRequest(http.MethodGet, "/api/jobs/status", nil)
	w := httptest.NewRecorder()

	h.GetJobStatus(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/jobs/status?data_type=projects", nil)
	w = httptest.NewRecorder()

	h.GetJobStatus(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandlers_GetStorageStats(t *testing.T) {
	h, tmpDir := setupHandlersTest(t)
	defer os.RemoveAll(tmpDir)

	req := httptest.NewRequest(http.MethodGet, "/api/stats/storage", nil)
	w := httptest.NewRecorder()

	h.GetStorageStats(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandlers_GetWorkerStats(t *testing.T) {
	h, tmpDir := setupHandlersTest(t)
	defer os.RemoveAll(tmpDir)

	req := httptest.NewRequest(http.MethodGet, "/api/stats/workers", nil)
	w := httptest.NewRecorder()

	h.GetWorkerStats(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandlers_GetConfig(t *testing.T) {
	h, tmpDir := setupHandlersTest(t)
	defer os.RemoveAll(tmpDir)

	req := httptest.NewRequest(http.MethodGet, "/config", nil)
	w := httptest.NewRecorder()

	h.GetConfig(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}

	var resp handlers.APIResponse
	json.NewDecoder(w.Body).Decode(&resp)

	if !resp.Success {
		t.Error("expected success response")
	}

	data := resp.Data.(map[string]interface{})
	if _, ok := data["base_api_url"]; !ok {
		t.Error("expected base_api_url in config")
	}
}
