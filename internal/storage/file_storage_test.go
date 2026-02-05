package storage_test

import (
	"os"
	"path/filepath"
	"testing"

	"asana-extractor/internal/config"
	"asana-extractor/internal/storage"
)

func setupTestStorage(t *testing.T) (*storage.FileStorage, string) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "storage_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	fs, err := storage.NewFileStorage(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create storage: %v", err)
	}

	return fs, tmpDir
}

func TestFileStorage_SaveAndLoad(t *testing.T) {
	fs, tmpDir := setupTestStorage(t)
	defer os.RemoveAll(tmpDir)

	testData := map[string]interface{}{
		"guid": "test-123",
		"name": "Test Project",
		"tags": []string{"test", "example"},
	}

	err := fs.SaveItem(config.DataTypeProjects, "test-123", testData)
	if err != nil {
		t.Fatalf("SaveItem failed: %v", err)
	}

	if !fs.Exists(config.DataTypeProjects, "test-123") {
		t.Error("expected file to exist after save")
	}

	var loaded map[string]interface{}
	err = fs.LoadItem(config.DataTypeProjects, "test-123", &loaded)
	if err != nil {
		t.Fatalf("LoadItem failed: %v", err)
	}

	if loaded["guid"] != "test-123" {
		t.Errorf("got guid %v, want test-123", loaded["guid"])
	}

	if loaded["name"] != "Test Project" {
		t.Errorf("got name %v, want Test Project", loaded["name"])
	}
}

func TestFileStorage_SaveRaw(t *testing.T) {
	fs, tmpDir := setupTestStorage(t)
	defer os.RemoveAll(tmpDir)

	rawJSON := []byte(`{"guid":"raw-456","name":"Raw Test","nested":{"key":"value"}}`)

	err := fs.SaveRaw(config.DataTypeUsers, "raw-456", rawJSON)
	if err != nil {
		t.Fatalf("SaveRaw failed: %v", err)
	}

	content, err := os.ReadFile(filepath.Join(tmpDir, "users", "raw-456.json"))
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	if len(content) <= len(rawJSON) {
		t.Error("expected pretty-printed JSON to be longer")
	}
}

func TestFileStorage_Delete(t *testing.T) {
	fs, tmpDir := setupTestStorage(t)
	defer os.RemoveAll(tmpDir)

	testData := map[string]string{"guid": "to-delete"}
	fs.SaveItem(config.DataTypeProjects, "to-delete", testData)

	err := fs.DeleteItem(config.DataTypeProjects, "to-delete")
	if err != nil {
		t.Fatalf("DeleteItem failed: %v", err)
	}

	if fs.Exists(config.DataTypeProjects, "to-delete") {
		t.Error("expected file to be deleted")
	}
}

func TestFileStorage_ListGUIDs(t *testing.T) {
	fs, tmpDir := setupTestStorage(t)
	defer os.RemoveAll(tmpDir)

	for _, guid := range []string{"guid-1", "guid-2", "guid-3"} {
		fs.SaveItem(config.DataTypeProjects, guid, map[string]string{"guid": guid})
	}

	guids, err := fs.ListGUIDs(config.DataTypeProjects)
	if err != nil {
		t.Fatalf("ListGUIDs failed: %v", err)
	}

	if len(guids) != 3 {
		t.Errorf("got %d guids, want 3", len(guids))
	}

	guidMap := make(map[string]bool)
	for _, g := range guids {
		guidMap[g] = true
	}

	for _, expected := range []string{"guid-1", "guid-2", "guid-3"} {
		if !guidMap[expected] {
			t.Errorf("missing expected guid: %s", expected)
		}
	}
}

func TestFileStorage_GetStats(t *testing.T) {
	fs, tmpDir := setupTestStorage(t)
	defer os.RemoveAll(tmpDir)

	fs.SaveItem(config.DataTypeProjects, "p1", map[string]string{"data": "test1"})
	fs.SaveItem(config.DataTypeProjects, "p2", map[string]string{"data": "test2"})
	fs.SaveItem(config.DataTypeUsers, "u1", map[string]string{"data": "test3"})

	projectStats, err := fs.GetStats(config.DataTypeProjects)
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}

	if projectStats.FileCount != 2 {
		t.Errorf("got file count %d, want 2", projectStats.FileCount)
	}

	if projectStats.TotalSize == 0 {
		t.Error("expected non-zero total size")
	}

	userStats, _ := fs.GetStats(config.DataTypeUsers)
	if userStats.FileCount != 1 {
		t.Errorf("got user file count %d, want 1", userStats.FileCount)
	}
}

func TestFileStorage_Concurrency(t *testing.T) {
	fs, tmpDir := setupTestStorage(t)
	defer os.RemoveAll(tmpDir)

	done := make(chan bool)

	for i := 0; i < 50; i++ {
		go func(i int) {
			guid := "concurrent-" + string(rune('a'+i%26))
			data := map[string]int{"index": i}

			fs.SaveItem(config.DataTypeProjects, guid, data)
			fs.Exists(config.DataTypeProjects, guid)

			var loaded map[string]int
			fs.LoadItem(config.DataTypeProjects, guid, &loaded)

			done <- true
		}(i)
	}

	for i := 0; i < 50; i++ {
		<-done
	}
}
