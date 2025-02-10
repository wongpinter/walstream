package lsn

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileStorage(t *testing.T) {
	// Create temp directory for tests
	tmpDir, err := os.MkdirTemp("", "lsn_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	tests := []struct {
		name        string
		publication string
		lsn         uint64
		wantErr     bool
	}{
		{
			name:        "basic set and get",
			publication: "test_pub",
			lsn:         12345,
			wantErr:     false,
		},
		{
			name:        "zero LSN",
			publication: "test_pub_zero",
			lsn:         0,
			wantErr:     false,
		},
		{
			name:        "large LSN",
			publication: "test_pub_large",
			lsn:         18446744073709551615, // max uint64
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create storage with short persist interval for testing
			storage, err := NewFileStorage(tmpDir, 100*time.Millisecond)
			if err != nil {
				t.Fatalf("NewFileStorage() error = %v", err)
			}
			defer storage.Close()

			// Set LSN
			if err := storage.Set(tt.publication, tt.lsn); err != nil {
				if !tt.wantErr {
					t.Errorf("Set() error = %v", err)
				}
				return
			}

			// Wait for persistence
			time.Sleep(200 * time.Millisecond)

			// Verify file exists
			if _, err := os.Stat(filepath.Join(tmpDir, tt.publication+".lsn")); err != nil {
				t.Errorf("LSN file not created: %v", err)
			}

			// Get LSN
			got, err := storage.Get(tt.publication)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.lsn {
				t.Errorf("Get() = %v, want %v", got, tt.lsn)
			}

			// Create new storage instance to test persistence
			storage2, err := NewFileStorage(tmpDir, 100*time.Millisecond)
			if err != nil {
				t.Fatalf("NewFileStorage() error = %v", err)
			}
			defer storage2.Close()

			// Get LSN from new instance
			got, err = storage2.Get(tt.publication)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.lsn {
				t.Errorf("Get() = %v, want %v", got, tt.lsn)
			}
		})
	}
}

func TestFileStorage_Concurrent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lsn_test_concurrent_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := NewFileStorage(tmpDir, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("NewFileStorage() error = %v", err)
	}
	defer storage.Close()

	const numGoroutines = 10
	const numOperations = 100

	done := make(chan bool)
	for i := 0; i < numGoroutines; i++ {
		go func(n int) {
			pub := fmt.Sprintf("pub_%d", n)
			for j := 0; j < numOperations; j++ {
				if err := storage.Set(pub, uint64(j)); err != nil {
					t.Errorf("Set() error = %v", err)
				}
				if _, err := storage.Get(pub); err != nil {
					t.Errorf("Get() error = %v", err)
				}
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to finish
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}
