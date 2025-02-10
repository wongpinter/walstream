package lsn

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileStorage(t *testing.T) {
	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "lsn_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	filePath := filepath.Join(tmpDir, "lsn.json")

	// Test creating new storage
	storage, err := NewFileStorage(filePath)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Test initial state
	lsn, err := storage.Get("test_pub")
	if err != nil {
		t.Fatalf("Failed to get initial LSN: %v", err)
	}
	if lsn != 0 {
		t.Errorf("Expected initial LSN to be 0, got %d", lsn)
	}

	// Test setting LSN
	testLSN := uint64(12345)
	if err := storage.Set("test_pub", testLSN); err != nil {
		t.Fatalf("Failed to set LSN: %v", err)
	}

	// Test getting LSN
	lsn, err = storage.Get("test_pub")
	if err != nil {
		t.Fatalf("Failed to get LSN: %v", err)
	}
	if lsn != testLSN {
		t.Errorf("Expected LSN %d, got %d", testLSN, lsn)
	}

	// Test persistence
	if err := storage.Close(); err != nil {
		t.Fatalf("Failed to close storage: %v", err)
	}

	// Create new storage instance and verify state is loaded
	storage2, err := NewFileStorage(filePath)
	if err != nil {
		t.Fatalf("Failed to create second storage: %v", err)
	}

	lsn, err = storage2.Get("test_pub")
	if err != nil {
		t.Fatalf("Failed to get LSN from second storage: %v", err)
	}
	if lsn != testLSN {
		t.Errorf("Expected LSN %d from second storage, got %d", testLSN, lsn)
	}
}
