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

func (m *Memory) RemoveContainer(ctx context.Context, name string) error {
	delete(m.data, name)
	return nil
}

func (m *Memory) ListContainers(ctx context.Context) ([]string, error) {
	containers := []string{}
	for containerName := range m.data {
		containers = append(containers, containerName)
	}
	return containers, nil
}

func (m *Memory) ChooseContainer(ctx context.Context, key string) (Storer, error) {
	data, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("%s not found", key)
	}

	return &memoryStorer{
		data: data,
	}, nil
}

func (m *memoryStorer) Get(ctx context.Context, key string) ([]byte, error) {
	data, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("%s not found", key)
	}

	return data, nil
}

func (m *memoryStorer) Put(ctx context.Context, key string, data []byte) error {
	// assignment to entry in nil map
	m.data[key] = data
	return nil
}

func (m *memoryStorer) Delete(ctx context.Context, key string) error {
	delete(m.data, key)
	return nil
}
