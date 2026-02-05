package asana

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"asana-extractor/internal/config"
	"asana-extractor/internal/limiter"
	"asana-extractor/pkg/logger"
)

// HTTPClient interface for testing
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type AsanaClient struct {
	httpClient  HTTPClient
	config      *config.Config
	rateLimiter limiter.RateLimiter
}

func NewClient(cfg *config.Config, rl limiter.RateLimiter) *AsanaClient {
	return &AsanaClient{
		httpClient: &http.Client{
			Timeout: 1 * time.Minute,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		config:      cfg,
		rateLimiter: rl,
	}
}

// NewClientWithHTTP creates a client with a custom HTTP client (for testing)
func NewClientWithHTTP(cfg *config.Config, rl limiter.RateLimiter, httpClient HTTPClient) *AsanaClient {
	return &AsanaClient{
		httpClient:  httpClient,
		config:      cfg,
		rateLimiter: rl,
	}
}

func (c *AsanaClient) BuildUrl(dataType config.DataType, pageToken string, pageSize int) string {
	endpointCfg := c.config.GetEndpointConfig(dataType)
	baseURL := c.config.BaseAPIURL + endpointCfg.Suffix

	params := url.Values{}
	for key, value := range endpointCfg.Parameters {
		params.Set(key, value)
	}

	params.Set("limit", fmt.Sprintf("%d", pageSize))

	if pageToken != "" {
		params.Set("page_token", pageToken)
	}

	return baseURL + "?" + params.Encode()
}

func (c *AsanaClient) buildDetailURL(dataType config.DataType, guid string) string {
	endpointCfg := c.config.GetEndpointConfig(dataType)
	return c.config.BaseAPIURL + endpointCfg.Suffix + "/" + guid
}

func (c *AsanaClient) doRequest(ctx context.Context, requestURL string) (*http.Response, error) {
	if err := c.rateLimiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limiter wait: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.config.GetAuthToken())
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}

	return resp, nil
}

// handleResponse checks the HTTP status and returns an error for non-2xx responses.
// It does NOT close the response body - the caller is responsible for that.
func (c *AsanaClient) handleResponse(resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if adaptive, ok := c.rateLimiter.(*limiter.AdaptiveRateLimiter); ok {
			adaptive.OnSuccess()
		}
		return nil
	}

	body, _ := io.ReadAll(resp.Body)

	apiErr := &APIError{
		StatusCode: resp.StatusCode,
		Message:    string(body),
	}

	if resp.StatusCode == 429 {
		if adaptive, ok := c.rateLimiter.(*limiter.AdaptiveRateLimiter); ok {
			adaptive.OnRateLimited()
		}

		if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
			var seconds int
			fmt.Sscanf(retryAfter, "%d", &seconds)
			apiErr.RetryAfter = seconds
		}
	}

	return apiErr
}

func (c *AsanaClient) FetchPage(ctx context.Context, dataType config.DataType, pageToken string) (*GetItemResponse, error) {
	requestURL := c.BuildUrl(dataType, pageToken, c.config.PageSize)

	resp, err := c.doRequest(ctx, requestURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := c.handleResponse(resp); err != nil {
		return nil, err
	}

	var result GetItemResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &result, nil
}

// StreamItems streams all items using a channel pattern for memory efficiency
func (c *AsanaClient) StreamItems(ctx context.Context, dataType config.DataType) (<-chan StreamResult, error) {
	resultChan := make(chan StreamResult, c.config.PageSize)

	go func() {
		defer close(resultChan)

		var pageToken string
		pageNum := 0

		for {
			pageNum++
			logger.Info("Fetching page", "page", pageNum, "dataType", dataType)

			select {
			case <-ctx.Done():
				resultChan <- StreamResult{Err: ctx.Err()}
				return
			default:
			}

			page, err := c.FetchPage(ctx, dataType, pageToken)
			if err != nil {
				if apiErr, ok := err.(*APIError); ok && apiErr.IsRetryable() {
					retryDelay := time.Duration(apiErr.RetryAfter) * time.Second
					if retryDelay == 0 {
						retryDelay = 5 * time.Second
					}

					logger.Warn("Retryable error, waiting", "delay", retryDelay, "error", err)

					select {
					case <-ctx.Done():
						resultChan <- StreamResult{Err: ctx.Err()}
						return
					case <-time.After(retryDelay):
						continue
					}
				}

				resultChan <- StreamResult{Err: err}
				return
			}

			for _, rawItem := range page.Data {
				resultChan <- StreamResult{
					RawData:  rawItem,
					DataType: dataType,
				}
			}

			if page.NextPage == nil || page.NextPage.Offset == "" {
				logger.Info("Finished fetching all pages", "totalPages", pageNum, "dataType", dataType)
				return
			}

			pageToken = page.NextPage.Offset
		}
	}()

	return resultChan, nil
}

type StreamResult struct {
	RawData  ResponseItem
	DataType config.DataType
	Err      error
}

// StreamGUIDs streams all item GUIDs using a channel pattern
func (c *AsanaClient) StreamGUIDs(ctx context.Context, dataType config.DataType) (<-chan StreamResult, error) {
	resultChan := make(chan StreamResult, c.config.PageSize)

	go func() {
		defer close(resultChan)

		var pageToken string
		pageNum := 0

		for {
			pageNum++
			logger.Info("Fetching page", "page", pageNum, "dataType", dataType)

			select {
			case <-ctx.Done():
				resultChan <- StreamResult{Err: ctx.Err()}
				return
			default:
			}

			page, err := c.FetchPage(ctx, dataType, pageToken)
			if err != nil {
				if apiErr, ok := err.(*APIError); ok && apiErr.IsRetryable() {
					retryDelay := time.Duration(apiErr.RetryAfter) * time.Second
					if retryDelay == 0 {
						retryDelay = 5 * time.Second
					}

					logger.Warn("Retryable error, waiting", "delay", retryDelay, "error", err)

					select {
					case <-ctx.Done():
						resultChan <- StreamResult{Err: ctx.Err()}
						return
					case <-time.After(retryDelay):
						continue
					}
				}

				resultChan <- StreamResult{Err: err}
				return
			}

			for _, item := range page.Data {
				resultChan <- StreamResult{
					RawData: ResponseItem{
						GUID:     item.GUID,
						DataType: string(dataType),
					},
					DataType: dataType,
				}
			}

			if page.NextPage == nil || page.NextPage.Offset == "" {
				logger.Info("Finished fetching all pages", "totalPages", pageNum, "dataType", dataType)
				return
			}

			pageToken = page.NextPage.Offset
		}
	}()

	return resultChan, nil
}

// FetchEntity fetches full entity details by type and GUID, returns raw JSON
func (c *AsanaClient) FetchEntity(ctx context.Context, dataType config.DataType, guid string) ([]byte, error) {
	requestURL := c.buildDetailURL(dataType, guid)

	resp, err := c.doRequest(ctx, requestURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := c.handleResponse(resp); err != nil {
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	return body, nil
}
