package config

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

type DataType string

const (
	DataTypeProjects DataType = "projects"
	DataTypeUsers    DataType = "users"
)

type Config struct {
	mu sync.RWMutex

	// API Config
	BaseAPIURL string `json:"base_api_url"`
	AuthToken  string `json:"auth_token"`

	// Scheduler Configuration
	CronIntervalProjects time.Duration `json:"cron_interval_projects"`
	CronIntervalUsers    time.Duration `json:"cron_interval_users"`

	// Rate Limit
	RateLimitPerSecond int `json:"rate_limit_per_second"`
	RateLimitBurst     int `json:"rate_limit_burst"`

	// Pagination
	PageSize           int `json:"page_size"`
	MaxConcurrentFetch int `json:"max_concurrent_fetch"`

	// Storage
	ExportsBasePath string `json:"exports_base_path"`
}

type EndpointConfig struct {
	Suffix     string            `json:"suffix"`
	Parameters map[string]string `json:"parameters"`
}

var defaultEndpoints = map[DataType]EndpointConfig{
	DataTypeProjects: {
		Suffix: "api/1.0/projects",
		Parameters: map[string]string{
			"include": "metadata,status",
		},
	},
	DataTypeUsers: {
		Suffix: "api/1.0/users",
		Parameters: map[string]string{
			"include": "profile,permissions",
		},
	},
}

func NewDefaultConfig() *Config {
	token := os.Getenv("ASANA_API")

	return &Config{
		BaseAPIURL:           "https://app.asana.com/",
		AuthToken:            token,
		CronIntervalProjects: 5 * time.Minute,
		CronIntervalUsers:    5 * time.Minute,
		RateLimitPerSecond:   10,
		RateLimitBurst:       5,
		PageSize:             100,
		MaxConcurrentFetch:   5,
		ExportsBasePath:      "./exports",
	}
}

func (c *Config) GetCronInterval(dataType DataType) time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()

	switch dataType {
	case DataTypeProjects:
		return c.CronIntervalProjects
	case DataTypeUsers:
		return c.CronIntervalUsers
	default:
		return 5 * time.Minute
	}
}

func (c *Config) SetCronInterval(dataType DataType, interval time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch dataType {
	case DataTypeProjects:
		c.CronIntervalProjects = interval
	case DataTypeUsers:
		c.CronIntervalUsers = interval
	}
}

func (c *Config) GetAuthToken() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.AuthToken
}

func (c *Config) SetAuthToken(token string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.AuthToken = token
}

func (c *Config) GetRateLimits() (perSecond, burst int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.RateLimitPerSecond, c.RateLimitBurst
}

func (c *Config) SetRateLimits(perSecond, burst int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.RateLimitPerSecond = perSecond
	c.RateLimitBurst = burst
}

func (c *Config) GetEndpointConfig(dataType DataType) EndpointConfig {
	return defaultEndpoints[dataType]
}

func (c *Config) GetExportPath(dataType DataType) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	switch dataType {
	case DataTypeProjects:
		return c.ExportsBasePath + "/projects"
	case DataTypeUsers:
		return c.ExportsBasePath + "/users"
	default:
		return c.ExportsBasePath
	}
}

func (c *Config) LoadFromFile(path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, c)
}

func (c *Config) SaveToFile(path string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}