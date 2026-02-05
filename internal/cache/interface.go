package cache

import (
	"context"
	"time"
)

type Cache interface {
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error
	Get(ctx context.Context, key string) (interface{}, bool)
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) bool
	SetAdd(ctx context.Context, key string, members ...string) error
	SetMembers(ctx context.Context, key string) ([]string, error)
	SetIsMember(ctx context.Context, key, member string) bool
	Clear(ctx context.Context) error
	Close() error
}
