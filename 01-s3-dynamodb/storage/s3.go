package storage

import (
	"bytes"
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3 struct {
	svc *s3.Client
}

type s3Storer struct {
	svc        *s3.Client
	bucketName *string
}

func NewS3Client(cfg aws.Config) Client {
	return &S3{
		svc: s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.UsePathStyle = true
		}),
	}
}

func (db *S3) NewContainer(ctx context.Context, name string) (Storer, error) {
	createBucketOutput, err := db.svc.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	if err != nil {
		return nil, err
	}

	return &s3Storer{
		svc:        db.svc,
		bucketName: createBucketOutput.Location,
	}, err
}

func (db *S3) ListContainers(ctx context.Context) ([]string, error) {
	buckets, err := db.svc.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	bucketNames := make([]string, len(buckets.Buckets))
	for i, bucket := range buckets.Buckets {
		bucketNames[i] = *bucket.Name
	}

	return bucketNames, nil
}

func (db *S3) RemoveContainer(ctx context.Context, name string) error {
	_, err := db.svc.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})

	return err
}

func (db *S3) ChooseContainer(ctx context.Context, name string) (Storer, error) {
	return &s3Storer{
		svc:        db.svc,
		bucketName: &name,
	}, nil
}

func (db *s3Storer) Put(ctx context.Context, key string, data []byte) error {
	_, err := db.svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket: db.bucketName,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})

	return err
}

func (db *s3Storer) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := db.svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: db.bucketName,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}

	return io.ReadAll(resp.Body)
}

func (db *s3Storer) Delete(ctx context.Context, key string) error {
	_, err := db.svc.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: db.bucketName,
		Key:    &key,
	})

	return err
}
