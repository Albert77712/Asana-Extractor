// internal/storage/file_storage.go
package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"asana-extractor/internal/config"
)

type FileStorage struct {
	basePath string
	mu       sync.RWMutex
	// Per-directory locks for finer granularity
	dirLocks map[string]*sync.RWMutex
	dirMu    sync.Mutex
}

func NewFileStorage(basePath string) (*FileStorage, error) {
	fs := &FileStorage{
		basePath: basePath,
		dirLocks: make(map[string]*sync.RWMutex),
	}

	// Create base directories
	for _, dir := range []string{
		filepath.Join(basePath, "projects"),
		filepath.Join(basePath, "users"),
	} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("create directory %s: %w", dir, err)
		}
	}

	return fs, nil
}

func (fs *FileStorage) getDirLock(dir string) *sync.RWMutex {
	fs.dirMu.Lock()
	defer fs.dirMu.Unlock()

	if lock, exists := fs.dirLocks[dir]; exists {
		return lock
	}

	lock := &sync.RWMutex{}
	fs.dirLocks[dir] = lock
	return lock
}

func (fs *FileStorage) getFilePath(dataType config.DataType, guid string) string {
	return filepath.Join(fs.basePath, string(dataType), guid+".json")
}

func (fs *FileStorage) SaveItem(dataType config.DataType, guid string, data interface{}) error {
	filePath := fs.getFilePath(dataType, guid)
	dir := filepath.Dir(filePath)

	lock := fs.getDirLock(dir)
	lock.Lock()
	defer lock.Unlock()

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal data: %w", err)
	}

	if err := os.WriteFile(filePath, jsonData, 0644); err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	return nil
}

func (fs *FileStorage) SaveRaw(dataType config.DataType, guid string, rawJSON []byte) error {
	filePath := fs.getFilePath(dataType, guid)
	dir := filepath.Dir(filePath)

	lock := fs.getDirLock(dir)
	lock.Lock()
	defer lock.Unlock()

	// Pretty print the JSON
	var prettyJSON interface{}
	if err := json.Unmarshal(rawJSON, &prettyJSON); err != nil {
		return fmt.Errorf("unmarshal raw json: %w", err)
	}

	formatted, err := json.MarshalIndent(prettyJSON, "", "  ")
	if err != nil {
		return fmt.Errorf("format json: %w", err)
	}

	if err := os.WriteFile(filePath, formatted, 0644); err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	return nil
}

func (fs *FileStorage) Exists(dataType config.DataType, guid string) bool {
	filePath := fs.getFilePath(dataType, guid)
	dir := filepath.Dir(filePath)

	lock := fs.getDirLock(dir)
	lock.RLock()
	defer lock.RUnlock()

	_, err := os.Stat(filePath)
	return err == nil
}

func (fs *FileStorage) LoadItem(dataType config.DataType, guid string, dest interface{}) error {
	filePath := fs.getFilePath(dataType, guid)
	dir := filepath.Dir(filePath)

	lock := fs.getDirLock(dir)
	lock.RLock()
	defer lock.RUnlock()

	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("unmarshal data: %w", err)
	}

	return nil
}

func (fs *FileStorage) DeleteItem(dataType config.DataType, guid string) error {
	filePath := fs.getFilePath(dataType, guid)
	dir := filepath.Dir(filePath)

	lock := fs.getDirLock(dir)
	lock.Lock()
	defer lock.Unlock()

	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete file: %w", err)
	}

	return nil
}

func (fs *FileStorage) ListGUIDs(dataType config.DataType) ([]string, error) {
	dir := filepath.Join(fs.basePath, string(dataType))

	lock := fs.getDirLock(dir)
	lock.RLock()
	defer lock.RUnlock()

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read directory: %w", err)
	}

	guids := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if filepath.Ext(name) == ".json" {
			guid := name[:len(name)-5] // Remove .json extension
			guids = append(guids, guid)
		}
	}

	return guids, nil
}

func (fs *FileStorage) GetStats(dataType config.DataType) (StorageStats, error) {
	dir := filepath.Join(fs.basePath, string(dataType))

	lock := fs.getDirLock(dir)
	lock.RLock()
	defer lock.RUnlock()

	entries, err := os.ReadDir(dir)
	if err != nil {
		return StorageStats{}, fmt.Errorf("read directory: %w", err)
	}

	var stats StorageStats
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if filepath.Ext(entry.Name()) == ".json" {
			stats.FileCount++

			info, err := entry.Info()
			if err == nil {
				stats.TotalSize += info.Size()
			}
		}
	}

	return stats, nil
}

type StorageStats struct {
	FileCount int   `json:"file_count"`
	TotalSize int64 `json:"total_size"`
}