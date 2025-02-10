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

// LSNState represents the state of LSN processing
type LSNState struct {
	LSN       uint64    `json:"lsn"`
	UpdatedAt time.Time `json:"updated_at"`
}

// FileStorage implements Storage interface using file-based persistence
type FileStorage struct {
	dir      string
	states   map[string]LSNState
	mu       sync.RWMutex
	interval time.Duration // Interval for persisting to disk
	done     chan struct{}
}

// NewFileStorage creates a new file-based LSN storage
func NewFileStorage(dir string, persistInterval time.Duration) (*FileStorage, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	fs := &FileStorage{
		dir:      dir,
		states:   make(map[string]LSNState),
		interval: persistInterval,
		done:     make(chan struct{}),
	}

	// Load existing states
	if err := fs.loadStates(); err != nil {
		return nil, err
	}

	// Start periodic persistence
	go fs.persistPeriodically()

	return fs, nil
}

// Get returns the last processed LSN for a given publication
func (fs *FileStorage) Get(publication string) (uint64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if state, ok := fs.states[publication]; ok {
		return state.LSN, nil
	}
	return 0, nil
}

// Set updates the last processed LSN for a given publication
func (fs *FileStorage) Set(publication string, lsn uint64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.states[publication] = LSNState{
		LSN:       lsn,
		UpdatedAt: time.Now(),
	}

	return nil
}

// Close releases any resources and ensures final state is persisted
func (fs *FileStorage) Close() error {
	close(fs.done)
	return fs.persist()
}

// loadStates loads LSN states from disk
func (fs *FileStorage) loadStates() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	files, err := filepath.Glob(filepath.Join(fs.dir, "*.lsn"))
	if err != nil {
		return fmt.Errorf("failed to list LSN files: %w", err)
	}

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("failed to read LSN file %s: %w", file, err)
		}

		var state LSNState
		if err := json.Unmarshal(data, &state); err != nil {
			return fmt.Errorf("failed to unmarshal LSN state from %s: %w", file, err)
		}

		publication := filepath.Base(file[:len(file)-4]) // remove .lsn extension
		fs.states[publication] = state
	}

	return nil
}

// persist writes current states to disk
func (fs *FileStorage) persist() error {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	for pub, state := range fs.states {
		data, err := json.Marshal(state)
		if err != nil {
			return fmt.Errorf("failed to marshal LSN state for %s: %w", pub, err)
		}

		filename := filepath.Join(fs.dir, pub+".lsn")
		if err := os.WriteFile(filename, data, 0644); err != nil {
			return fmt.Errorf("failed to write LSN file %s: %w", filename, err)
		}
	}

	return nil
}

// persistPeriodically periodically persists states to disk
func (fs *FileStorage) persistPeriodically() {
	ticker := time.NewTicker(fs.interval)
	defer ticker.Stop()

	for {
		select {
		case <-fs.done:
			return
		case <-ticker.C:
			if err := fs.persist(); err != nil {
				// Log error but continue
				fmt.Printf("Error persisting LSN states: %v\n", err)
			}
		}
	}
}
