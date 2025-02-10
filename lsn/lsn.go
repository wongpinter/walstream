package lsn

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Storage defines the interface for LSN persistence
type Storage interface {
	// Get returns the last processed LSN for a given publication
	Get(publication string) (uint64, error)
	// Set updates the last processed LSN for a given publication
	Set(publication string, lsn uint64) error
	// Close releases any resources held by the storage
	Close() error
}

type LSNState struct {
	LSN       uint64    `json:"lsn"`
	UpdatedAt time.Time `json:"updated_at"`
}

type FileStorage struct {
	mu       sync.RWMutex
	filePath string
	state    map[string]LSNState
}

// NewFileStorage creates a new file-based LSN storage
func NewFileStorage(filePath string) (*FileStorage, error) {
	fs := &FileStorage{
		filePath: filePath,
		state:    make(map[string]LSNState),
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Load existing state if file exists
	if _, err := os.Stat(filePath); err == nil {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read state file: %w", err)
		}

		if err := json.Unmarshal(data, &fs.state); err != nil {
			return nil, fmt.Errorf("failed to unmarshal state: %w", err)
		}
	}

	return fs, nil
}

func (fs *FileStorage) Get(publication string) (uint64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if state, ok := fs.state[publication]; ok {
		return state.LSN, nil
	}
	return 0, nil
}

func (fs *FileStorage) Set(publication string, lsn uint64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.state[publication] = LSNState{
		LSN:       lsn,
		UpdatedAt: time.Now(),
	}

	// Persist to file
	data, err := json.MarshalIndent(fs.state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	if err := os.WriteFile(fs.filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}

	return nil
}

func (fs *FileStorage) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Ensure final state is written
	data, err := json.MarshalIndent(fs.state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	if err := os.WriteFile(fs.filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}

	return nil
}
