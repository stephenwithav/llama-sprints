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

// NewContainer creates a new table in your AWS account using the provided configuration parameters from DynamoDBOptions.
func (db *DynamoDB) NewContainer(ctx context.Context, tableName string) (Storer, error) {
	params := &dynamodb.CreateTableInput{
		AttributeDefinitions:  db.opts.AttributeDefinitions,
		KeySchema:             db.opts.KeySchema,
		ProvisionedThroughput: db.opts.ProvisionedThroughput,
		TableName:             &tableName,
	}
	createTableOutput, err := db.svc.CreateTable(ctx, params)
	if err != nil {
		return nil, err
	}
	return &dynamoDbStorer{
		svc:       db.svc,
		tableName: createTableOutput.TableDescription.TableName,
	}, nil
}

// ListContainersreturns a list of all available tables (containers).
func (db *DynamoDB) ListContainers(ctx context.Context) ([]string, error) {
	tables, err := db.svc.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		return nil, err
	}

	return tables.TableNames, nil
}

// RemoveContainer deletes an existing table (container) by its name.
func (db *DynamoDB) RemoveContainer(ctx context.Context, name string) error {
	_, err := db.svc.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(name),
	})

	return err
}

// ChooseContainer creates and returns a new Storer instance with the provided tableName.
func (db *DynamoDB) ChooseContainer(ctx context.Context, tableName string) (Storer, error) {
	return &dynamoDbStorer{
		svc:       db.svc,
		tableName: &tableName,
	}, nil
}

// Put is a method to insert (or update if existing) an item into DynamoDB table.
// It takes in a context object, a key string and data byte array as arguments.
// The key-value pair is stored in 'id' attribute and the raw bytes in 'data'.
func (db *dynamoDbStorer) Put(ctx context.Context, key string, data []byte) error {
	_, err := db.svc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: db.tableName,
		Item: map[string]types.AttributeValue{
			"id":   &types.AttributeValueMemberS{Value: key},
			"data": &types.AttributeValueMemberB{Value: data},
		},
	})
	return err
}

// Get is a method to retrieve an item from DynamoDB table by key.
// It takes in a context object and a key string as arguments.
func (db *dynamoDbStorer) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := db.svc.GetItem(ctx, &dynamodb.GetItemInput{
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

// Delete is a method that deletes an item from DynamoDB table by key.
func (db *dynamoDbStorer) Delete(ctx context.Context, key string) error {
	_, err := db.svc.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: db.tableName,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: key},
		},
	})

	return err
}
