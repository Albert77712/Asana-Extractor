package asana

type ResponseItem struct {
	GUID string `json:"gid"`
	DataType string `json:"resource_type"`
	Name string `json:"name"`
}

type NextPage struct {
	Offset string `json:"offset"`
	Path string `json:"path"`
	Uri string `json:"uri"`
}

type GetItemResponse struct {
	Data []ResponseItem `json:"data"`
	NextPage *NextPage `json:"next_page,omitempty"`
}

type APIError struct {
	StatusCode int
	Message    string
	RetryAfter int // seconds
}

func (e *APIError) Error() string {
	return e.Message
}

func (e *APIError) IsRateLimited() bool {
	return e.StatusCode == 429
}

func (e *APIError) IsRetryable() bool {
	return e.StatusCode >= 500 || e.StatusCode == 429
}

func (e *APIError) IsNotFound() bool {
	return e.StatusCode == 404
}