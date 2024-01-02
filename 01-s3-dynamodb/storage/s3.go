package storage

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3 struct {
	svc        *s3.Client
	bucketName string
}

type S3Options struct {
	BucketName string
}

func NewS3(cfg aws.Config, opts S3Options) Repository {
	return &S3{
		svc:        s3.NewFromConfig(cfg),
		bucketName: opts.BucketName,
	}
}

func (db *S3) PutObject(ctx context.Context, key string, data []byte) error {
	return nil
}

func (db *S3) Get(ctx context.Context, key string) ([]byte, error) {
	return nil, nil
}

func (db *S3) Delete(ctx context.Context, key string) error {
	return nil
}
