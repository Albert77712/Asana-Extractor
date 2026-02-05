// internal/cache/local_cache.go
package cache

import (
	"context"
	"encoding/gob"
	"os"
	"sync"
	"time"
)

type cacheItem struct {
	Value      interface{}
	Expiration int64
}

func (i cacheItem) isExpired() bool {
	if i.Expiration == 0 {
		return false
	}
	return time.Now().UnixNano() > i.Expiration
}

type LocalCache struct {
	mu              sync.RWMutex
	items           map[string]cacheItem
	sets            map[string]map[string]struct{}
	cleanupInterval time.Duration
	stopCleanup     chan struct{}
	persistPath     string
}

type LocalCacheOption func(*LocalCache)

func WithCleanupInterval(d time.Duration) LocalCacheOption {
	return func(c *LocalCache) {
		c.cleanupInterval = d
	}
}

func WithPersistence(path string) LocalCacheOption {
	return func(c *LocalCache) {
		c.persistPath = path
	}
}

func NewLocalCache(opts ...LocalCacheOption) *LocalCache {
	c := &LocalCache{
		items:           make(map[string]cacheItem),
		sets:            make(map[string]map[string]struct{}),
		cleanupInterval: 5 * time.Minute,
		stopCleanup:     make(chan struct{}),
	}

	for _, opt := range opts {
		opt(c)
	}

	// Load from persistence if available
	if c.persistPath != "" {
		_ = c.loadFromDisk()
	}

	// Start cleanup goroutine
	go c.cleanupLoop()

	return c
}

func (c *LocalCache) cleanupLoop() {
	ticker := time.NewTicker(c.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.deleteExpired()
		case <-c.stopCleanup:
			return
		}
	}
}

func (c *LocalCache) deleteExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, item := range c.items {
		if item.isExpired() {
			delete(c.items, key)
		}
	}
}

func (c *LocalCache) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var expiration int64
	if ttl > 0 {
		expiration = time.Now().Add(ttl).UnixNano()
	}

	c.items[key] = cacheItem{
		Value:      value,
		Expiration: expiration,
	}

	return nil
}

func (c *LocalCache) Get(ctx context.Context, key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, found := c.items[key]
	if !found || item.isExpired() {
		return nil, false
	}

	return item.Value, true
}

func (c *LocalCache) Delete(ctx context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.items, key)
	return nil
}

func (c *LocalCache) Exists(ctx context.Context, key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, found := c.items[key]
	return found && !item.isExpired()
}

// SetAdd adds members to a set (like Redis SADD)
func (c *LocalCache) SetAdd(ctx context.Context, key string, members ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sets[key] == nil {
		c.sets[key] = make(map[string]struct{})
	}

	for _, member := range members {
		c.sets[key][member] = struct{}{}
	}

	return nil
}

// SetMembers returns all members of a set (like Redis SMEMBERS)
func (c *LocalCache) SetMembers(ctx context.Context, key string) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	set, found := c.sets[key]
	if !found {
		return []string{}, nil
	}

	members := make([]string, 0, len(set))
	for member := range set {
		members = append(members, member)
	}

	return members, nil
}

// SetIsMember checks if member exists in set (like Redis SISMEMBER)
func (c *LocalCache) SetIsMember(ctx context.Context, key, member string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	set, found := c.sets[key]
	if !found {
		return false
	}

	_, exists := set[member]
	return exists
}

func (c *LocalCache) Clear(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]cacheItem)
	c.sets = make(map[string]map[string]struct{})

	return nil
}

func (c *LocalCache) Close() error {
	close(c.stopCleanup)

	if c.persistPath != "" {
		return c.saveToDisk()
	}

	return nil
}

type persistedData struct {
	Items map[string]cacheItem
	Sets  map[string]map[string]struct{}
}

func (c *LocalCache) saveToDisk() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	file, err := os.Create(c.persistPath)
	if err != nil {
		return err
	}
	defer file.Close()

	data := persistedData{
		Items: c.items,
		Sets:  c.sets,
	}

	return gob.NewEncoder(file).Encode(data)
}

func (c *LocalCache) loadFromDisk() error {
	file, err := os.Open(c.persistPath)
	if err != nil {
		return err
	}
	defer file.Close()

	var data persistedData
	if err := gob.NewDecoder(file).Decode(&data); err != nil {
		return err
	}

	c.items = data.Items
	c.sets = data.Sets

	return nil
}

// ProcessedItemsTracker provides a high-level API for tracking processed items
type ProcessedItemsTracker struct {
	cache    Cache
	prefix   string
	lifetime time.Duration
}

func NewProcessedItemsTracker(cache Cache, prefix string, lifetime time.Duration) *ProcessedItemsTracker {
	return &ProcessedItemsTracker{
		cache:    cache,
		prefix:   prefix,
		lifetime: lifetime,
	}
}

func (t *ProcessedItemsTracker) MarkAsProcessed(ctx context.Context, dataType, guid string) error {
	key := t.buildSetKey(dataType)
	return t.cache.SetAdd(ctx, key, guid)
}

func (t *ProcessedItemsTracker) IsProcessed(ctx context.Context, dataType, guid string) bool {
	key := t.buildSetKey(dataType)
	return t.cache.SetIsMember(ctx, key, guid)
}

func (t *ProcessedItemsTracker) GetProcessedGUIDs(ctx context.Context, dataType string) ([]string, error) {
	key := t.buildSetKey(dataType)
	return t.cache.SetMembers(ctx, key)
}

func (t *ProcessedItemsTracker) buildSetKey(dataType string) string {
	return t.prefix + ":processed:" + dataType
}