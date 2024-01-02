package storage

import (
	"bytes"
	"context"
	"io"

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

func (db *S3) NewContainer(ctx context.Context, name string) error {
	_, err := db.svc.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	db.bucketName = name

	return err
}

func (db *S3) RemoveContainer(ctx context.Context, name string) error {
	_, err := db.svc.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})

	return err
}

func (db *S3) PutObject(ctx context.Context, key string, data []byte) error {
	_, err := db.svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &db.bucketName,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})

	return err
}

func (db *S3) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := db.svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &db.bucketName,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}

	return io.ReadAll(resp.Body)
}

func (db *S3) Delete(ctx context.Context, key string) error {
	_, err := db.svc.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &db.bucketName,
		Key:    &key,
	})

	return err
}
