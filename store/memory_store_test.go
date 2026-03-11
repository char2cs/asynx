package store_test

import (
	"context"
	"sync"
	"testing"

	"github.com/char2cs/asynx/store"
)

func TestNewMemoryStore(t *testing.T) {
	s := store.NewMemoryStore()
	if s == nil {
		t.Fatal("expected non-nil MemoryStore")
	}
}

func TestAppend_BasicWrite(t *testing.T) {
	s := store.NewMemoryStore()

	err := s.Append(context.Background(), "agg1", 1, []byte("data1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAppend_DuplicateVersion(t *testing.T) {
	s := store.NewMemoryStore()

	_ = s.Append(context.Background(), "agg1", 1, []byte("data1"))
	err := s.Append(context.Background(), "agg1", 1, []byte("data2"))

	if err == nil {
		t.Fatal("expected error for duplicate version, got nil")
	}
}

func TestAppend_MultipleVersions(t *testing.T) {
	s := store.NewMemoryStore()

	for i := int64(1); i <= 5; i++ {
		err := s.Append(context.Background(), "agg1", i, []byte("data"))
		if err != nil {
			t.Fatalf("version %d: unexpected error: %v", i, err)
		}
	}
}

func TestAppend_MultipleAggregates(t *testing.T) {
	s := store.NewMemoryStore()

	_ = s.Append(context.Background(), "agg1", 1, []byte("data1"))
	err := s.Append(context.Background(), "agg2", 1, []byte("data2"))

	if err != nil {
		t.Fatalf("different aggregates must not conflict: %v", err)
	}
}

func TestAppend_ContextCancelled(t *testing.T) {
	s := store.NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.Append(ctx, "agg1", 1, []byte("data"))
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

func TestReadFrom_Empty(t *testing.T) {
	s := store.NewMemoryStore()

	result, err := s.ReadFrom(context.Background(), "nonexistent", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(result))
	}
}

func TestReadFrom_AllEntries(t *testing.T) {
	s := store.NewMemoryStore()

	_ = s.Append(context.Background(), "agg1", 1, []byte("v1"))
	_ = s.Append(context.Background(), "agg1", 2, []byte("v2"))
	_ = s.Append(context.Background(), "agg1", 3, []byte("v3"))

	result, err := s.ReadFrom(context.Background(), "agg1", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(result))
	}
	if string(result[0]) != "v1" || string(result[1]) != "v2" || string(result[2]) != "v3" {
		t.Fatal("entries returned in wrong order or with wrong data")
	}
}

func TestReadFrom_FromZero(t *testing.T) {
	s := store.NewMemoryStore()

	_ = s.Append(context.Background(), "agg1", 1, []byte("v1"))
	_ = s.Append(context.Background(), "agg1", 2, []byte("v2"))

	result, err := s.ReadFrom(context.Background(), "agg1", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 entries from version 0, got %d", len(result))
	}
}

func TestReadFrom_FromMiddle(t *testing.T) {
	s := store.NewMemoryStore()

	_ = s.Append(context.Background(), "agg1", 1, []byte("v1"))
	_ = s.Append(context.Background(), "agg1", 2, []byte("v2"))
	_ = s.Append(context.Background(), "agg1", 3, []byte("v3"))

	result, err := s.ReadFrom(context.Background(), "agg1", 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 entries starting at version 2, got %d", len(result))
	}
	if string(result[0]) != "v2" || string(result[1]) != "v3" {
		t.Fatal("wrong entries returned")
	}
}

func TestReadFrom_BeyondEnd(t *testing.T) {
	s := store.NewMemoryStore()

	_ = s.Append(context.Background(), "agg1", 1, []byte("v1"))

	result, err := s.ReadFrom(context.Background(), "agg1", 99)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty slice beyond last version, got %d entries", len(result))
	}
}

func TestReadFrom_VersionOrder(t *testing.T) {
	s := store.NewMemoryStore()

	_ = s.Append(context.Background(), "agg1", 3, []byte("v3"))
	_ = s.Append(context.Background(), "agg1", 1, []byte("v1"))
	_ = s.Append(context.Background(), "agg1", 2, []byte("v2"))

	result, err := s.ReadFrom(context.Background(), "agg1", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(result))
	}
	if string(result[0]) != "v1" || string(result[1]) != "v2" || string(result[2]) != "v3" {
		t.Fatal("entries not returned in strict version order")
	}
}

func TestReadFrom_ContextCancelled(t *testing.T) {
	s := store.NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := s.ReadFrom(ctx, "agg1", 1)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

func TestReadRange_Basic(t *testing.T) {
	s := store.NewMemoryStore()

	_ = s.Append(context.Background(), "agg1", 1, []byte("v1"))
	_ = s.Append(context.Background(), "agg1", 2, []byte("v2"))
	_ = s.Append(context.Background(), "agg1", 3, []byte("v3"))

	result, err := s.ReadRange(context.Background(), "agg1", 1, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(result))
	}
	if string(result[0]) != "v1" || string(result[1]) != "v2" {
		t.Fatal("wrong entries returned")
	}
}

func TestReadRange_CountZero(t *testing.T) {
	s := store.NewMemoryStore()

	_ = s.Append(context.Background(), "agg1", 1, []byte("v1"))

	result, err := s.ReadRange(context.Background(), "agg1", 1, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty slice for count=0, got %d entries", len(result))
	}
}

func TestReadRange_CountExceedsStream(t *testing.T) {
	s := store.NewMemoryStore()

	_ = s.Append(context.Background(), "agg1", 1, []byte("v1"))
	_ = s.Append(context.Background(), "agg1", 2, []byte("v2"))

	result, err := s.ReadRange(context.Background(), "agg1", 1, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 entries (stream size), got %d", len(result))
	}
}

func TestReadRange_Empty(t *testing.T) {
	s := store.NewMemoryStore()

	result, err := s.ReadRange(context.Background(), "nonexistent", 1, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(result) != 0 {
		t.Fatalf("expected empty slice, got %d entries", len(result))
	}
}

func TestReadRange_FromMiddle(t *testing.T) {
	s := store.NewMemoryStore()

	for i := int64(1); i <= 10; i++ {
		_ = s.Append(context.Background(), "agg1", i, []byte("data"))
	}

	result, err := s.ReadRange(context.Background(), "agg1", 5, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 entries starting at version 5, got %d", len(result))
	}
}

func TestReadRange_ContextCancelled(t *testing.T) {
	s := store.NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := s.ReadRange(ctx, "agg1", 1, 10)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

func TestConcurrentAppend_UniqueVersions(t *testing.T) {
	s := store.NewMemoryStore()
	const n = 50

	var wg sync.WaitGroup
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			_ = s.Append(context.Background(), "agg1", int64(v), []byte("data"))
		}(i)
	}
	wg.Wait()

	result, err := s.ReadFrom(context.Background(), "agg1", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != n {
		t.Fatalf("expected %d entries after concurrent appends, got %d", n, len(result))
	}
}

func TestConcurrentAppend_UniqueVersionEnforced(t *testing.T) {
	s := store.NewMemoryStore()
	const n = 20

	var (
		wg           sync.WaitGroup
		mu           sync.Mutex
		successCount int
	)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.Append(context.Background(), "agg1", 1, []byte("data"))
			if err == nil {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if successCount != 1 {
		t.Fatalf("expected exactly 1 success for concurrent appends to same version, got %d", successCount)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	s := store.NewMemoryStore()

	for i := int64(1); i <= 10; i++ {
		_ = s.Append(context.Background(), "agg1", i, []byte("data"))
	}

	var wg sync.WaitGroup
	for i := 1; i <= 20; i++ {
		wg.Add(2)
		go func(v int) {
			defer wg.Done()
			_ = s.Append(context.Background(), "agg2", int64(v), []byte("data"))
		}(i)
		go func() {
			defer wg.Done()
			_, _ = s.ReadFrom(context.Background(), "agg1", 1)
		}()
	}
	wg.Wait()
}
