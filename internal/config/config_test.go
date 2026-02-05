package config_test

import (
	"os"
	"testing"
	"time"

	"asana-extractor/internal/config"
)

func TestConfig_Defaults(t *testing.T) {
	cfg := config.NewDefaultConfig()

	if cfg.BaseAPIURL != "https://app.asana.com/" {
		t.Errorf("unexpected default base URL: %s", cfg.BaseAPIURL)
	}

	if cfg.GetCronInterval(config.DataTypeProjects) != 5*time.Minute {
		t.Error("unexpected default cron interval")
	}

	if cfg.PageSize != 100 {
		t.Errorf("unexpected page size: %d", cfg.PageSize)
	}
}

func TestConfig_SetAndGetCronInterval(t *testing.T) {
	cfg := config.NewDefaultConfig()

	cfg.SetCronInterval(config.DataTypeProjects, 10*time.Minute)
	cfg.SetCronInterval(config.DataTypeUsers, 15*time.Minute)

	if got := cfg.GetCronInterval(config.DataTypeProjects); got != 10*time.Minute {
		t.Errorf("got %v, want 10m", got)
	}

	if got := cfg.GetCronInterval(config.DataTypeUsers); got != 15*time.Minute {
		t.Errorf("got %v, want 15m", got)
	}
}

func TestConfig_SetAndGetAuthToken(t *testing.T) {
	cfg := config.NewDefaultConfig()

	cfg.SetAuthToken("test-token-123")

	if got := cfg.GetAuthToken(); got != "test-token-123" {
		t.Errorf("got %q, want %q", got, "test-token-123")
	}
}

func TestConfig_SetAndGetRateLimits(t *testing.T) {
	cfg := config.NewDefaultConfig()

	cfg.SetRateLimits(20, 10)

	perSecond, burst := cfg.GetRateLimits()
	if perSecond != 20 || burst != 10 {
		t.Errorf("got (%d, %d), want (20, 10)", perSecond, burst)
	}
}

func TestConfig_GetExportPath(t *testing.T) {
	cfg := config.NewDefaultConfig()

	projectPath := cfg.GetExportPath(config.DataTypeProjects)
	if projectPath != "./exports/projects" {
		t.Errorf("got %q, want ./exports/projects", projectPath)
	}

	userPath := cfg.GetExportPath(config.DataTypeUsers)
	if userPath != "./exports/users" {
		t.Errorf("got %q, want ./exports/users", userPath)
	}
}

func TestConfig_GetEndpointConfig(t *testing.T) {
	cfg := config.NewDefaultConfig()

	projectEndpoint := cfg.GetEndpointConfig(config.DataTypeProjects)
	if projectEndpoint.Suffix != "api/1.0/projects" {
		t.Errorf("got suffix %q, want api/1.0/projects", projectEndpoint.Suffix)
	}

	userEndpoint := cfg.GetEndpointConfig(config.DataTypeUsers)
	if userEndpoint.Suffix != "api/1.0/users" {
		t.Errorf("got suffix %q, want api/1.0/users", userEndpoint.Suffix)
	}
}

func TestConfig_SaveAndLoad(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "config_test_*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	cfg := config.NewDefaultConfig()
	cfg.SetAuthToken("saved-token")
	cfg.SetCronInterval(config.DataTypeProjects, 30*time.Minute)
	cfg.SetRateLimits(50, 25)

	err = cfg.SaveToFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("SaveToFile failed: %v", err)
	}

	cfg2 := config.NewDefaultConfig()
	err = cfg2.LoadFromFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}

	if cfg2.GetAuthToken() != "saved-token" {
		t.Errorf("token not loaded correctly")
	}

	if cfg2.GetCronInterval(config.DataTypeProjects) != 30*time.Minute {
		t.Error("cron interval not loaded correctly")
	}

	perSecond, burst := cfg2.GetRateLimits()
	if perSecond != 50 || burst != 25 {
		t.Errorf("rate limits not loaded correctly: got (%d, %d)", perSecond, burst)
	}
}

func TestConfig_Concurrency(t *testing.T) {
	cfg := config.NewDefaultConfig()

	done := make(chan bool)

	for i := 0; i < 100; i++ {
		go func(i int) {
			cfg.SetAuthToken("token-" + string(rune(i)))
			cfg.GetAuthToken()
			cfg.SetCronInterval(config.DataTypeProjects, time.Duration(i)*time.Minute)
			cfg.GetCronInterval(config.DataTypeProjects)
			cfg.SetRateLimits(i, i)
			cfg.GetRateLimits()
			done <- true
		}(i)
	}

	for i := 0; i < 100; i++ {
		<-done
	}
}
