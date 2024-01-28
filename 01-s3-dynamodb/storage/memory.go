package storage

import (
	"context"
	"fmt"
)

type Memory struct {
	data map[string]map[string][]byte
}

type memoryStorer struct {
	data map[string][]byte
}

func NewMemory() Client {
	return &Memory{
		data: map[string]map[string][]byte{},
	}
}

func (m *Memory) NewContainer(ctx context.Context, name string) (Storer, error) {
	m.data[name] = make(map[string][]byte)
	return &memoryStorer{
		data: m.data[name],
	}, nil
}

// RemoveContainer deletes the container with the given name from Memory.
func (m *Memory) RemoveContainer(ctx context.Context, name string) error {
	delete(m.data, name)
	return nil
}

// ListContainers lists all containers present in Memory. Returns a slice of
// strings representing container names.
func (m *Memory) ListContainers(ctx context.Context) ([]string, error) {
	containers := []string{}
	for containerName := range m.data {
		containers = append(containers, containerName)
	}
	return containers, nil
}

// ChooseContainer chooses a specific container from Memory and return it as
// memoryStorer interface along with any error if occurs during selection.
func (m *Memory) ChooseContainer(ctx context.Context, key string) (Storer, error) {
	data, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("%s not found", key)
	}

	return &memoryStorer{
		data: data,
	}, nil
}

// Get fetches object from the storage using provided key (object name). Returns
// byte array and error if any occurs during fetching process.
func (m *memoryStorer) Get(ctx context.Context, key string) ([]byte, error) {
	data, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("%s not found", key)
	}

	return data, nil
}

// Put puts new object into the storage using provided key (object name) and
// data (byte array).
func (m *memoryStorer) Put(ctx context.Context, key string, data []byte) error {
	// assignment to entry in nil map
	m.data[key] = data
	return nil
}

// Delete deletes an existing object from the storage using provided key (object
// name).
func (m *memoryStorer) Delete(ctx context.Context, key string) error {
	delete(m.data, key)
	return nil
}
