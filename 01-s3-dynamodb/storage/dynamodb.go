package storage

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DynamoDB struct {
	svc  *dynamodb.Client
	opts *DynamoDBOptions
}

// Refactored as follows:
type DynamoDBOptions struct {
	TableName             string
	AttributeDefinitions  []types.AttributeDefinition
	KeySchema             []types.KeySchemaElement
	ProvisionedThroughput *types.ProvisionedThroughput
}

type dynamoDbStorer struct {
	svc       *dynamodb.Client
	tableName *string
}

func NewDynamoDB(cfg aws.Config, opts *DynamoDBOptions) Client {
	return &DynamoDB{
		svc:  dynamodb.NewFromConfig(cfg),
		opts: opts,
	}
}

func (db *DynamoDB) NewContainer(ctx context.Context, tableName string) (Storer, error) {
	params := &dynamodb.CreateTableInput{
		AttributeDefinitions:  db.opts.AttributeDefinitions,
		KeySchema:             db.opts.KeySchema,
		ProvisionedThroughput: db.opts.ProvisionedThroughput,
		TableName:             &tableName,
	}
	createTableOutput, err := db.svc.CreateTable(context.Background(), params)
	if err != nil {
		return nil, err
	}
	return &dynamoDbStorer{
		svc:       db.svc,
		tableName: createTableOutput.TableDescription.TableName,
	}, nil
}

func (db *DynamoDB) ListContainers(ctx context.Context) ([]string, error) {
	tables, err := db.svc.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		return nil, err
	}

	return tables.TableNames, nil
}

func (db *DynamoDB) RemoveContainer(ctx context.Context, name string) error {
	_, err := db.svc.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
		TableName: aws.String(name),
	})

	return err
}

func (db *DynamoDB) ChooseContainer(ctx context.Context, tableName string) (Storer, error) {
	return &dynamoDbStorer{
		svc:       db.svc,
		tableName: &tableName,
	}, nil
}

func (db *dynamoDbStorer) Put(ctx context.Context, key string, data []byte) error {
	_, err := db.svc.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: db.tableName,
		Item: map[string]types.AttributeValue{
			"id":   &types.AttributeValueMemberS{Value: key},
			"data": &types.AttributeValueMemberB{Value: data},
		},
	})
	return err
}

func (db *dynamoDbStorer) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := db.svc.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: db.tableName,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: key},
		},
	})
	if err != nil {
		return nil, err
	}

	return resp.Item["data"].(*types.AttributeValueMemberB).Value, nil
}

func (db *dynamoDbStorer) Delete(ctx context.Context, key string) error {
	_, err := db.svc.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName: db.tableName,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: key},
		},
	})

	return err
}
