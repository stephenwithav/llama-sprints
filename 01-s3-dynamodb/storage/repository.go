package storage

import (
	"context"
)

type Client interface {
	NewContainer(ctx context.Context, name string) (Storer, error)
	RemoveContainer(ctx context.Context, name string) error
	ListContainers(ctx context.Context) ([]string, error)
	ChooseContainer(ctx context.Context, name string) (Storer, error)
}

type Storer interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, data []byte) error
	Delete(ctx context.Context, key string) error
}

// Containers in S3 are Buckets.
// Containers in Memory are map values.
// Containers in DynamoDB are tables.
//
// Get, Put, and Delete should all have a reference to their container.
//
// Maybe implement a Storage interface for each?  Storer?
//
// NewContainer would have to return a Storer.
