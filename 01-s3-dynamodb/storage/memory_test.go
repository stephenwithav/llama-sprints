package storage

import (
	"context"
	"testing"
)

func TestMemoryStorer(t *testing.T) {
	client := NewMemory()
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
