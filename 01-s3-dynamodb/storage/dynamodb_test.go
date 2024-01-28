package storage

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestDynamoDBRepository(t *testing.T) {
	cfg, err := getLocalConfig()
	if err != nil {
		t.Fatalf("is LocalStack running?\nerr: %s", err)
	}
	client := NewDynamoDB(cfg, &DynamoDBOptions{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("data"),
				AttributeType: types.ScalarAttributeTypeB,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("data"),
				KeyType:       types.KeyTypeRange,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	var repo Storer

	t.Run("Create container", func(t *testing.T) {
		cntr, err := client.NewContainer(context.Background(), "abc")
		if err != nil {
			t.Fatalf("container creation failed: %s", err.Error())
		}
		repo = cntr
	})

	t.Run("List containers", func(t *testing.T) {
		containers, err := client.ListContainers(context.Background())
		if err != nil {
			t.Fatalf("container listing failed: %s", err.Error())
		}

		for _, c := range containers {
			t.Logf("- %s", c)
		}
	})

	t.Run("Get object from empty container", func(t *testing.T) {
		_, err := repo.Get(context.Background(), "abc")
		if err != nil {
			t.Logf("Correctly retrieved an error")
		}
	})

	t.Run("Put object in container", func(t *testing.T) {
		err := repo.Put(context.Background(), "abc", []byte("xyz"))
		if err != nil {
			t.Logf("unable to store object: %s", err.Error())
		}
	})

	t.Run("Get object from non-empty container", func(t *testing.T) {
		_, err := repo.Get(context.Background(), "abc")
		if err != nil {
			t.Logf("Failed to retrieve: %s", err.Error())
		}
	})

	t.Run("Delete object from container", func(t *testing.T) {
		err := repo.Delete(context.Background(), "abc")
		if err != nil {
			t.Logf("Failed to delete object: %s", err.Error())
		}
	})
}

func getLocalConfig() (aws.Config, error) {
	// awsRegion = os.Getenv("AWS_REGION")
	// awsEndpoint = os.Getenv("AWS_ENDPOINT")
	// bucketName = os.Getenv("S3_BUCKET")

	awsRegion := "us-east-1"
	awsEndpoint := "http://localhost:4566"

	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if awsEndpoint != "" {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           awsEndpoint,
				SigningRegion: awsRegion,
			}, nil
		}

		// returning EndpointNotFoundError will allow the service to fallback to it's default resolution
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	return config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(awsRegion),
		config.WithEndpointResolverWithOptions(customResolver),
	)
}
