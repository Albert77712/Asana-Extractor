package cache_test

import (
	"context"
	"testing"
	"time"

	"asana-extractor/internal/cache"
)

func TestLocalCache_SetAndGet(t *testing.T) {
	c := cache.NewLocalCache()
	defer c.Close()

	ctx := context.Background()

	err := c.Set(ctx, "key1", "value1", 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	val, found := c.Get(ctx, "key1")
	if !found {
		t.Fatal("expected to find key1")
	}

	if val != "value1" {
		t.Errorf("got %v, want value1", val)
	}
}

func TestLocalCache_Expiration(t *testing.T) {
	c := cache.NewLocalCache(cache.WithCleanupInterval(10 * time.Millisecond))
	defer c.Close()

	ctx := context.Background()

	err := c.Set(ctx, "expiring", "value", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	_, found := c.Get(ctx, "expiring")
	if !found {
		t.Fatal("expected to find key immediately after set")
	}

	time.Sleep(100 * time.Millisecond)

	_, found = c.Get(ctx, "expiring")
	if found {
		t.Error("expected key to be expired")
	}
}

func TestLocalCache_Delete(t *testing.T) {
	c := cache.NewLocalCache()
	defer c.Close()

	ctx := context.Background()

	c.Set(ctx, "to-delete", "value", 0)
	c.Delete(ctx, "to-delete")

	_, found := c.Get(ctx, "to-delete")
	if found {
		t.Error("expected key to be deleted")
	}
}

func TestLocalCache_Exists(t *testing.T) {
	c := cache.NewLocalCache()
	defer c.Close()

	ctx := context.Background()

	if c.Exists(ctx, "nonexistent") {
		t.Error("expected nonexistent key to not exist")
	}

	c.Set(ctx, "exists", "value", 0)

	if !c.Exists(ctx, "exists") {
		t.Error("expected key to exist")
	}
}

func TestLocalCache_SetOperations(t *testing.T) {
	c := cache.NewLocalCache()
	defer c.Close()

	ctx := context.Background()

	err := c.SetAdd(ctx, "myset", "member1", "member2", "member3")
	if err != nil {
		t.Fatalf("SetAdd failed: %v", err)
	}

	if !c.SetIsMember(ctx, "myset", "member1") {
		t.Error("expected member1 to be in set")
	}

	if c.SetIsMember(ctx, "myset", "nonmember") {
		t.Error("expected nonmember to not be in set")
	}

	members, err := c.SetMembers(ctx, "myset")
	if err != nil {
		t.Fatalf("SetMembers failed: %v", err)
	}

	if len(members) != 3 {
		t.Errorf("got %d members, want 3", len(members))
	}
}

func TestLocalCache_Clear(t *testing.T) {
	c := cache.NewLocalCache()
	defer c.Close()

	ctx := context.Background()

	c.Set(ctx, "key1", "value1", 0)
	c.Set(ctx, "key2", "value2", 0)
	c.SetAdd(ctx, "set1", "member1")

	err := c.Clear(ctx)
	if err != nil {
		t.Fatalf("Clear failed: %v", err)
	}

	if c.Exists(ctx, "key1") || c.Exists(ctx, "key2") {
		t.Error("expected all keys to be cleared")
	}

	members, _ := c.SetMembers(ctx, "set1")
	if len(members) != 0 {
		t.Error("expected sets to be cleared")
	}
}

func TestProcessedItemsTracker(t *testing.T) {
	c := cache.NewLocalCache()
	defer c.Close()

	tracker := cache.NewProcessedItemsTracker(c, "test", 24*time.Hour)
	ctx := context.Background()

	if tracker.IsProcessed(ctx, "projects", "guid-1") {
		t.Error("expected item to not be processed initially")
	}

	err := tracker.MarkAsProcessed(ctx, "projects", "guid-1")
	if err != nil {
		t.Fatalf("MarkAsProcessed failed: %v", err)
	}

	if !tracker.IsProcessed(ctx, "projects", "guid-1") {
		t.Error("expected item to be processed after marking")
	}

	if tracker.IsProcessed(ctx, "users", "guid-1") {
		t.Error("expected different type to not be processed")
	}

	guids, err := tracker.GetProcessedGUIDs(ctx, "projects")
	if err != nil {
		t.Fatalf("GetProcessedGUIDs failed: %v", err)
	}

	if len(guids) != 1 || guids[0] != "guid-1" {
		t.Errorf("got guids %v, want [guid-1]", guids)
	}
}

func TestLocalCache_Concurrency(t *testing.T) {
	c := cache.NewLocalCache()
	defer c.Close()

	ctx := context.Background()

	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func(i int) {
			key := "key"
			c.Set(ctx, key, i, 0)
			c.Get(ctx, key)
			done <- true
		}(i)
	}

	for i := 0; i < 100; i++ {
		<-done
	}

	for i := 0; i < 100; i++ {
		go func(i int) {
			c.SetAdd(ctx, "concurrentSet", "member")
			c.SetIsMember(ctx, "concurrentSet", "member")
			done <- true
		}(i)
	}

	for i := 0; i < 100; i++ {
		<-done
	}
}
