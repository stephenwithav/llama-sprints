package storage

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type DynamoDB struct {
	svc       *dynamodb.Client
	tableName string
}

type DynamoDBOptions struct {
	TableName string
}

func NewDynamoDB(cfg aws.Config, opts DynamoDBOptions) Repository {
	return &DynamoDB{
		svc:       dynamodb.NewFromConfig(cfg),
		tableName: opts.TableName,
	}
}

func (db *DynamoDB) PutObject(ctx context.Context, key string, data []byte) error {
	return nil
}

func (db *DynamoDB) Get(ctx context.Context, key string) ([]byte, error) {
	return nil, nil
}

func (db *DynamoDB) Delete(ctx context.Context, key string) error {
	return nil
}
