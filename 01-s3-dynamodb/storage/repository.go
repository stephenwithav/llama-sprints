package storage

import "context"

type Repository interface {
	Get(ctx context.Context, key string) ([]byte, error)
	PutObject(ctx context.Context, key string, data []byte) error
	Delete(ctx context.Context, key string) error
	NewContainer(ctx context.Context, name string) error
	RemoveContainer(ctx context.Context, name string) error
}
