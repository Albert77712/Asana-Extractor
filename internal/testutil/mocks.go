package testutil

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"asana-extractor/internal/asana"
	"asana-extractor/internal/config"
	"asana-extractor/internal/domain"
)

// MockHTTPClient implements asana.HTTPClient for testing
type MockHTTPClient struct {
	mu           sync.Mutex
	Responses    map[string]*http.Response
	Errors       map[string]error
	RequestCount map[string]int
	Delay        time.Duration
}

func NewMockHTTPClient() *MockHTTPClient {
	return &MockHTTPClient{
		Responses:    make(map[string]*http.Response),
		Errors:       make(map[string]error),
		RequestCount: make(map[string]int),
	}
}

func (m *MockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Delay > 0 {
		time.Sleep(m.Delay)
	}

	url := req.URL.String()
	m.RequestCount[url]++

	if err, ok := m.Errors[url]; ok {
		return nil, err
	}

	if resp, ok := m.Responses[url]; ok {
		return resp, nil
	}

	return &http.Response{
		StatusCode: http.StatusNotFound,
		Body:       io.NopCloser(bytes.NewReader([]byte(`{"error": "not found"}`))),
	}, nil
}

func (m *MockHTTPClient) AddResponse(url string, statusCode int, body interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()

	bodyBytes, _ := json.Marshal(body)
	m.Responses[url] = &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(bytes.NewReader(bodyBytes)),
		Header:     make(http.Header),
	}
}

func (m *MockHTTPClient) AddError(url string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Errors[url] = err
}

func (m *MockHTTPClient) GetRequestCount(url string) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.RequestCount[url]
}

// MockRateLimiter implements limiter.RateLimiter
type MockRateLimiter struct {
	WaitErr   error
	WaitDelay time.Duration
	AcquireOK bool
	PerSecond int
	Burst     int
	mu        sync.Mutex
	WaitCount int
}

func NewMockRateLimiter() *MockRateLimiter {
	return &MockRateLimiter{
		AcquireOK: true,
		PerSecond: 10,
		Burst:     5,
	}
}

func (m *MockRateLimiter) Wait(ctx context.Context) error {
	m.mu.Lock()
	m.WaitCount++
	m.mu.Unlock()

	if m.WaitDelay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.WaitDelay):
		}
	}

	return m.WaitErr
}

func (m *MockRateLimiter) TryAcquire() bool {
	return m.AcquireOK
}

func (m *MockRateLimiter) UpdateLimits(perSecond, burst int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.PerSecond = perSecond
	m.Burst = burst
}

func (m *MockRateLimiter) GetLimits() (int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.PerSecond, m.Burst
}

// MockEntityFetcher implements worker.EntityFetcher
type MockEntityFetcher struct {
	mu         sync.Mutex
	Entities   map[string][]byte
	Errors     map[string]error
	FetchCount map[string]int
	Delay      time.Duration
}

func NewMockEntityFetcher() *MockEntityFetcher {
	return &MockEntityFetcher{
		Entities:   make(map[string][]byte),
		Errors:     make(map[string]error),
		FetchCount: make(map[string]int),
	}
}

func (m *MockEntityFetcher) FetchEntity(ctx context.Context, dataType config.DataType, guid string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := string(dataType) + ":" + guid
	m.FetchCount[key]++

	if m.Delay > 0 {
		time.Sleep(m.Delay)
	}

	if err, ok := m.Errors[key]; ok {
		return nil, err
	}

	if data, ok := m.Entities[key]; ok {
		return data, nil
	}

	return nil, &asana.APIError{StatusCode: 404, Message: "not found"}
}

func (m *MockEntityFetcher) AddEntity(dataType config.DataType, guid string, entity interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := string(dataType) + ":" + guid
	data, _ := json.Marshal(entity)
	m.Entities[key] = data
}

func (m *MockEntityFetcher) AddError(dataType config.DataType, guid string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := string(dataType) + ":" + guid
	m.Errors[key] = err
}

// MockGUIDStreamer implements scheduler.GUIDStreamer
type MockGUIDStreamer struct {
	GUIDs     map[config.DataType][]string
	StreamErr error
	ResultErr error
}

func NewMockGUIDStreamer() *MockGUIDStreamer {
	return &MockGUIDStreamer{
		GUIDs: make(map[config.DataType][]string),
	}
}

func (m *MockGUIDStreamer) StreamGUIDs(ctx context.Context, dataType config.DataType) (<-chan asana.StreamResult, error) {
	if m.StreamErr != nil {
		return nil, m.StreamErr
	}

	resultChan := make(chan asana.StreamResult)

	go func() {
		defer close(resultChan)

		guids := m.GUIDs[dataType]
		for _, guid := range guids {
			select {
			case <-ctx.Done():
				resultChan <- asana.StreamResult{Err: ctx.Err()}
				return
			default:
			}

			if m.ResultErr != nil {
				resultChan <- asana.StreamResult{Err: m.ResultErr}
				return
			}

			resultChan <- asana.StreamResult{
				RawData: asana.ResponseItem{
					GUID:     guid,
					DataType: string(dataType),
				},
				DataType: dataType,
			}
		}
	}()

	return resultChan, nil
}

func (m *MockGUIDStreamer) AddGUIDs(dataType config.DataType, guids ...string) {
	m.GUIDs[dataType] = append(m.GUIDs[dataType], guids...)
}

// CreatePaginatedResponse builds a GetItemResponse for testing
func CreatePaginatedResponse(guids []string, hasMore bool, nextToken string) asana.GetItemResponse {
	items := make([]asana.ResponseItem, len(guids))
	for i, guid := range guids {
		items[i] = asana.ResponseItem{
			GUID:     guid,
			DataType: "project",
		}
	}

	resp := asana.GetItemResponse{
		Data: items,
	}

	if hasMore && nextToken != "" {
		resp.NextPage = &asana.NextPage{
			Offset: nextToken,
		}
	}

	return resp
}

// CreateTestProject creates a domain.Project for testing
func CreateTestProject(guid string) domain.Project {
	return domain.Project{
		BaseEntity: domain.BaseEntity{
			GUID: guid,
		},
		Name:        "Test Project " + guid,
		Description: "Description for " + guid,
	}
}

// CreateTestUser creates a domain.User for testing
func CreateTestUser(guid string) domain.User {
	return domain.User{
		BaseEntity: domain.BaseEntity{
			GUID: guid,
		},
		Email:       guid + "@example.com",
		DisplayName: "User " + guid,
		Role:        "member",
	}
}
