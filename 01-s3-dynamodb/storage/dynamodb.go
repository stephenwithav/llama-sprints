package storage

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
	_, err := db.svc.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: &db.tableName,
		Item: map[string]types.AttributeValue{
			"id":   &types.AttributeValueMemberS{Value: key},
			"data": &types.AttributeValueMemberB{Value: data},
		},
	})
	return err
}

func (db *DynamoDB) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := db.svc.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: &db.tableName,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: key},
		},
	})
	if err != nil {
		return nil, err
	}

	return resp.Item["data"].(*types.AttributeValueMemberB).Value, nil
}

func (db *DynamoDB) Delete(ctx context.Context, key string) error {
	_, err := db.svc.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName: &db.tableName,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: key},
		},
	})

	return err
}
