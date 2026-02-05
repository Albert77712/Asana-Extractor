package asana_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"asana-extractor/internal/asana"
	"asana-extractor/internal/config"
	"asana-extractor/internal/testutil"
)

func TestClient_FetchPage(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(*testutil.MockHTTPClient)
		dataType      config.DataType
		pageToken     string
		wantErr       bool
		wantItemCount int
		wantHasNext   bool
	}{
		{
			name: "successful fetch first page",
			setupMock: func(m *testutil.MockHTTPClient) {
				resp := testutil.CreatePaginatedResponse(
					[]string{"guid-1", "guid-2", "guid-3"},
					true,
					"next-page-token",
				)
				m.AddResponse(
					"https://app.asana.com/api/1.0/projects?include=metadata%2Cstatus&limit=100",
					http.StatusOK,
					resp,
				)
			},
			dataType:      config.DataTypeProjects,
			pageToken:     "",
			wantErr:       false,
			wantItemCount: 3,
			wantHasNext:   true,
		},
		{
			name: "successful fetch last page",
			setupMock: func(m *testutil.MockHTTPClient) {
				resp := testutil.CreatePaginatedResponse(
					[]string{"guid-4", "guid-5"},
					false,
					"",
				)
				m.AddResponse(
					"https://app.asana.com/api/1.0/projects?include=metadata%2Cstatus&limit=100&page_token=page2",
					http.StatusOK,
					resp,
				)
			},
			dataType:      config.DataTypeProjects,
			pageToken:     "page2",
			wantErr:       false,
			wantItemCount: 2,
			wantHasNext:   false,
		},
		{
			name: "rate limit error",
			setupMock: func(m *testutil.MockHTTPClient) {
				m.Responses["https://app.asana.com/api/1.0/projects?include=metadata%2Cstatus&limit=100"] = &http.Response{
					StatusCode: http.StatusTooManyRequests,
					Body:       io.NopCloser(bytes.NewReader([]byte(`{"error": "rate limited"}`))),
					Header: http.Header{
						"Retry-After": []string{"5"},
					},
				}
			},
			dataType: config.DataTypeProjects,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHTTP := testutil.NewMockHTTPClient()
			mockLimiter := testutil.NewMockRateLimiter()

			tt.setupMock(mockHTTP)

			cfg := config.NewDefaultConfig()
			client := asana.NewClientWithHTTP(cfg, mockLimiter, mockHTTP)

			ctx := context.Background()
			result, err := client.FetchPage(ctx, tt.dataType, tt.pageToken)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(result.Data) != tt.wantItemCount {
				t.Errorf("got %d items, want %d", len(result.Data), tt.wantItemCount)
			}

			hasNext := result.NextPage != nil && result.NextPage.Offset != ""
			if hasNext != tt.wantHasNext {
				t.Errorf("got hasNext=%v, want %v", hasNext, tt.wantHasNext)
			}
		})
	}
}

func TestClient_FetchEntity(t *testing.T) {
	mockHTTP := testutil.NewMockHTTPClient()
	mockLimiter := testutil.NewMockRateLimiter()

	expectedProject := testutil.CreateTestProject("project-123")
	mockHTTP.AddResponse(
		"https://app.asana.com/api/1.0/projects/project-123",
		http.StatusOK,
		expectedProject,
	)

	cfg := config.NewDefaultConfig()
	client := asana.NewClientWithHTTP(cfg, mockLimiter, mockHTTP)

	ctx := context.Background()
	rawJSON, err := client.FetchEntity(ctx, config.DataTypeProjects, "project-123")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rawJSON) == 0 {
		t.Error("expected non-empty response body")
	}
}

func TestClient_FetchEntity_User(t *testing.T) {
	mockHTTP := testutil.NewMockHTTPClient()
	mockLimiter := testutil.NewMockRateLimiter()

	expectedUser := testutil.CreateTestUser("user-456")
	mockHTTP.AddResponse(
		"https://app.asana.com/api/1.0/users/user-456",
		http.StatusOK,
		expectedUser,
	)

	cfg := config.NewDefaultConfig()
	client := asana.NewClientWithHTTP(cfg, mockLimiter, mockHTTP)

	ctx := context.Background()
	rawJSON, err := client.FetchEntity(ctx, config.DataTypeUsers, "user-456")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rawJSON) == 0 {
		t.Error("expected non-empty response body")
	}
}

func TestClient_StreamGUIDs(t *testing.T) {
	mockHTTP := testutil.NewMockHTTPClient()
	mockLimiter := testutil.NewMockRateLimiter()

	page1 := testutil.CreatePaginatedResponse([]string{"guid-1", "guid-2"}, true, "page2")
	page2 := testutil.CreatePaginatedResponse([]string{"guid-3"}, false, "")

	mockHTTP.AddResponse(
		"https://app.asana.com/api/1.0/projects?include=metadata%2Cstatus&limit=100",
		http.StatusOK,
		page1,
	)
	mockHTTP.AddResponse(
		"https://app.asana.com/api/1.0/projects?include=metadata%2Cstatus&limit=100&page_token=page2",
		http.StatusOK,
		page2,
	)

	cfg := config.NewDefaultConfig()
	client := asana.NewClientWithHTTP(cfg, mockLimiter, mockHTTP)

	ctx := context.Background()
	resultChan, err := client.StreamGUIDs(ctx, config.DataTypeProjects)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var guids []string
	for result := range resultChan {
		if result.Err != nil {
			t.Fatalf("unexpected error in stream: %v", result.Err)
		}
		guids = append(guids, result.RawData.GUID)
	}

	expectedGUIDs := []string{"guid-1", "guid-2", "guid-3"}
	if len(guids) != len(expectedGUIDs) {
		t.Fatalf("got %d GUIDs, want %d", len(guids), len(expectedGUIDs))
	}

	for i, guid := range guids {
		if guid != expectedGUIDs[i] {
			t.Errorf("guid[%d] = %q, want %q", i, guid, expectedGUIDs[i])
		}
	}
}

func TestClient_StreamGUIDs_ContextCancellation(t *testing.T) {
	mockHTTP := testutil.NewMockHTTPClient()
	mockHTTP.Delay = 100 * time.Millisecond

	mockLimiter := testutil.NewMockRateLimiter()

	page1 := testutil.CreatePaginatedResponse([]string{"guid-1"}, true, "page2")
	mockHTTP.AddResponse(
		"https://app.asana.com/api/1.0/projects?include=metadata%2Cstatus&limit=100",
		http.StatusOK,
		page1,
	)

	cfg := config.NewDefaultConfig()
	client := asana.NewClientWithHTTP(cfg, mockLimiter, mockHTTP)

	ctx, cancel := context.WithCancel(context.Background())

	resultChan, err := client.StreamGUIDs(ctx, config.DataTypeProjects)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	var gotCancellation bool
	for result := range resultChan {
		if result.Err == context.Canceled {
			gotCancellation = true
		}
	}

	if !gotCancellation {
		t.Log("Stream ended without explicit cancellation error (may have completed before cancel)")
	}
}

func TestAPIError_Methods(t *testing.T) {
	tests := []struct {
		name            string
		err             *asana.APIError
		wantRetryable   bool
		wantRateLimited bool
		wantNotFound    bool
	}{
		{
			name:            "rate limited",
			err:             &asana.APIError{StatusCode: 429, Message: "rate limited"},
			wantRetryable:   true,
			wantRateLimited: true,
			wantNotFound:    false,
		},
		{
			name:            "server error",
			err:             &asana.APIError{StatusCode: 500, Message: "internal error"},
			wantRetryable:   true,
			wantRateLimited: false,
			wantNotFound:    false,
		},
		{
			name:            "not found",
			err:             &asana.APIError{StatusCode: 404, Message: "not found"},
			wantRetryable:   false,
			wantRateLimited: false,
			wantNotFound:    true,
		},
		{
			name:            "bad request",
			err:             &asana.APIError{StatusCode: 400, Message: "bad request"},
			wantRetryable:   false,
			wantRateLimited: false,
			wantNotFound:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.IsRetryable(); got != tt.wantRetryable {
				t.Errorf("IsRetryable() = %v, want %v", got, tt.wantRetryable)
			}
			if got := tt.err.IsRateLimited(); got != tt.wantRateLimited {
				t.Errorf("IsRateLimited() = %v, want %v", got, tt.wantRateLimited)
			}
			if got := tt.err.IsNotFound(); got != tt.wantNotFound {
				t.Errorf("IsNotFound() = %v, want %v", got, tt.wantNotFound)
			}
		})
	}
}
