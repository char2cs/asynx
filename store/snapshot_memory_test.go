package store

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestSnapshotMemory_PutReplacesRatherThanAccumulates(t *testing.T) {
	ctx := context.Background()
	s := NewSnapshots()

	for i := range 100 {
		if err := s.Put(ctx, "agg-1", int64(i+1), []byte{byte(i)}); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	data, found, err := s.Get(ctx, "agg-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !found {
		t.Fatal("found = false, want true")
	}
	if len(data) != 1 || data[0] != 99 {
		t.Errorf("data = %v, want [99] — Put must replace, not accumulate", data)
	}
}

func TestSnapshotMemory_GetMissingIsNotAnError(t *testing.T) {
	ctx := context.Background()
	s := NewSnapshots()

	data, found, err := s.Get(ctx, "nope")
	if err != nil {
		t.Fatalf("get: %v — a missing snapshot is normal, not an error", err)
	}
	if found {
		t.Error("found = true, want false")
	}
	if data != nil {
		t.Errorf("data = %v, want nil", data)
	}
}

func TestSnapshotMemory_DeleteIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s := NewSnapshots()

	if err := s.Put(ctx, "agg-1", 1, []byte("x")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := s.Delete(ctx, "agg-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := s.Delete(ctx, "agg-1"); err != nil {
		t.Fatalf("second delete: %v — Delete must be idempotent", err)
	}
	if _, found, _ := s.Get(ctx, "agg-1"); found {
		t.Error("found = true after delete, want false")
	}
}

func TestSnapshotMemory_DeleteIsScopedToOneAggregate(t *testing.T) {
	ctx := context.Background()
	s := NewSnapshots()

	if err := s.Put(ctx, "agg-1", 1, []byte("a")); err != nil {
		t.Fatalf("put agg-1: %v", err)
	}
	if err := s.Put(ctx, "agg-2", 1, []byte("b")); err != nil {
		t.Fatalf("put agg-2: %v", err)
	}
	if err := s.Delete(ctx, "agg-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if _, found, _ := s.Get(ctx, "agg-2"); !found {
		t.Error("agg-2 snapshot was removed by deleting agg-1")
	}
}

func TestSnapshotMemory_SetErrorIsOneShotOnPut(t *testing.T) {
	ctx := context.Background()
	s := NewSnapshots()
	sentinel := errors.New("boom")

	s.SetError("agg-1", sentinel)

	if err := s.Put(ctx, "agg-1", 1, []byte("x")); !errors.Is(err, sentinel) {
		t.Fatalf("first put err = %v, want %v", err, sentinel)
	}
	if err := s.Put(ctx, "agg-1", 1, []byte("x")); err != nil {
		t.Fatalf("second put err = %v, want nil — SetError is one-shot", err)
	}
}

func TestSnapshotMemory_ContextCancellationIsRespected(t *testing.T) {
	s := NewSnapshots()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := s.Put(ctx, "agg-1", 1, []byte("x")); !errors.Is(err, context.Canceled) {
		t.Errorf("put err = %v, want context.Canceled", err)
	}
	if _, _, err := s.Get(ctx, "agg-1"); !errors.Is(err, context.Canceled) {
		t.Errorf("get err = %v, want context.Canceled", err)
	}
	if err := s.Delete(ctx, "agg-1"); !errors.Is(err, context.Canceled) {
		t.Errorf("delete err = %v, want context.Canceled", err)
	}
}

func TestSnapshotMemory_ConcurrentPutGetIsRaceFree(t *testing.T) {
	ctx := context.Background()
	s := NewSnapshots()

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = s.Put(ctx, "agg-1", int64(i+1), []byte{byte(i)})
		}()
		go func() {
			defer wg.Done()
			_, _, _ = s.Get(ctx, "agg-1")
		}()
	}
	wg.Wait()

	if _, found, err := s.Get(ctx, "agg-1"); err != nil || !found {
		t.Errorf("get after concurrent writes: found=%v err=%v", found, err)
	}
}
