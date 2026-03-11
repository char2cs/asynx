package store

import (
	"context"
	"errors"
	"sync"
	"testing"

	asynxmd "github.com/char2cs/asynx/models"
)

// --- SetError ---

func TestSetError_CausesNextAppendToFail(t *testing.T) {
	s := New()
	ctx := context.Background()
	sentinel := errors.New("injected")

	s.SetError("agg1", sentinel)

	err := s.Append(ctx, "agg1", 1, []byte("data"))
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
}

func TestSetError_IsOneShot_SubsequentAppendSucceeds(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.SetError("agg1", errors.New("once"))

	s.Append(ctx, "agg1", 1, []byte("x")) //nolint:errcheck — expected to fail

	if err := s.Append(ctx, "agg1", 2, []byte("ok")); err != nil {
		t.Errorf("second Append after SetError: %v (want nil)", err)
	}
}

func TestSetError_OnlyAffectsTargetAggregate(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.SetError("agg1", errors.New("fail agg1"))

	if err := s.Append(ctx, "agg2", 1, []byte("data")); err != nil {
		t.Errorf("Append to agg2 failed unexpectedly: %v", err)
	}
}

// --- New ---

func TestNew_ReturnsEmptyStore(t *testing.T) {
	s := New()
	blobs, err := s.ReadFrom(context.Background(), "agg1", 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(blobs) != 0 {
		t.Errorf("expected empty store, got %d blobs", len(blobs))
	}
}

// --- Append ---

func TestAppend_StoresBlob(t *testing.T) {
	s := New()
	ctx := context.Background()

	if err := s.Append(ctx, "agg1", 1, []byte("data")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	blobs, _ := s.ReadFrom(ctx, "agg1", 1)
	if len(blobs) != 1 || string(blobs[0]) != "data" {
		t.Errorf("got %v, want [data]", blobs)
	}
}

func TestAppend_DuplicateVersion_ReturnsErrPipelineFailed(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("first")) //nolint:errcheck

	err := s.Append(ctx, "agg1", 1, []byte("second"))
	if !errors.Is(err, asynxmd.ErrPipelineFailed) {
		t.Errorf("err = %v, want ErrPipelineFailed", err)
	}
}

func TestAppend_DifferentAggregates_DoNotConflict(t *testing.T) {
	s := New()
	ctx := context.Background()

	if err := s.Append(ctx, "agg1", 1, []byte("a")); err != nil {
		t.Fatalf("Append agg1: %v", err)
	}
	if err := s.Append(ctx, "agg2", 1, []byte("b")); err != nil {
		t.Fatalf("Append agg2: %v", err)
	}
}

func TestAppend_VersionsAreIndependentPerAggregate(t *testing.T) {
	s := New()
	ctx := context.Background()

	for i := int64(1); i <= 3; i++ {
		if err := s.Append(ctx, "agg1", i, []byte("x")); err != nil {
			t.Fatalf("Append agg1 v%d: %v", i, err)
		}
	}

	blobs, _ := s.ReadFrom(ctx, "agg1", 1)
	if len(blobs) != 3 {
		t.Errorf("expected 3 blobs, got %d", len(blobs))
	}
}

// --- ReadFrom ---

func TestReadFrom_ReturnsAllBlobsFromVersion(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck
	s.Append(ctx, "agg1", 2, []byte("v2")) //nolint:errcheck
	s.Append(ctx, "agg1", 3, []byte("v3")) //nolint:errcheck

	blobs, err := s.ReadFrom(ctx, "agg1", 2)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(blobs) != 2 {
		t.Fatalf("expected 2 blobs, got %d", len(blobs))
	}
	if string(blobs[0]) != "v2" || string(blobs[1]) != "v3" {
		t.Errorf("got %v, want [v2 v3]", blobs)
	}
}

func TestReadFrom_FromZero_ReturnsAll(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck
	s.Append(ctx, "agg1", 2, []byte("v2")) //nolint:errcheck

	blobs, _ := s.ReadFrom(ctx, "agg1", 0)
	if len(blobs) != 2 {
		t.Errorf("expected 2 blobs, got %d", len(blobs))
	}
}

func TestReadFrom_UnknownAggregate_ReturnsEmpty(t *testing.T) {
	s := New()
	blobs, err := s.ReadFrom(context.Background(), "ghost", 1)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(blobs) != 0 {
		t.Errorf("expected empty, got %d blobs", len(blobs))
	}
}

func TestReadFrom_VersionBeyondAll_ReturnsEmpty(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck

	blobs, _ := s.ReadFrom(ctx, "agg1", 99)
	if len(blobs) != 0 {
		t.Errorf("expected empty, got %d blobs", len(blobs))
	}
}

// --- ReadRange ---

func TestReadRange_ReturnsUpToCount(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck
	s.Append(ctx, "agg1", 2, []byte("v2")) //nolint:errcheck
	s.Append(ctx, "agg1", 3, []byte("v3")) //nolint:errcheck

	blobs, err := s.ReadRange(ctx, "agg1", 1, 2)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if len(blobs) != 2 {
		t.Fatalf("expected 2 blobs, got %d", len(blobs))
	}
	if string(blobs[0]) != "v1" || string(blobs[1]) != "v2" {
		t.Errorf("got %v, want [v1 v2]", blobs)
	}
}

func TestReadRange_CountExceedsAvailable_ReturnsAll(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck
	s.Append(ctx, "agg1", 2, []byte("v2")) //nolint:errcheck

	blobs, err := s.ReadRange(ctx, "agg1", 1, 100)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if len(blobs) != 2 {
		t.Errorf("expected 2 blobs, got %d", len(blobs))
	}
}

func TestReadRange_FromMidRange(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck
	s.Append(ctx, "agg1", 2, []byte("v2")) //nolint:errcheck
	s.Append(ctx, "agg1", 3, []byte("v3")) //nolint:errcheck

	blobs, _ := s.ReadRange(ctx, "agg1", 2, 10)
	if len(blobs) != 2 {
		t.Fatalf("expected 2 blobs from v2, got %d", len(blobs))
	}
	if string(blobs[0]) != "v2" {
		t.Errorf("first blob = %q, want v2", blobs[0])
	}
}

func TestReadRange_UnknownAggregate_ReturnsEmpty(t *testing.T) {
	s := New()
	blobs, err := s.ReadRange(context.Background(), "ghost", 1, 10)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if len(blobs) != 0 {
		t.Errorf("expected empty, got %d blobs", len(blobs))
	}
}

func TestReadRange_CountZero_ReturnsEmpty(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck

	blobs, err := s.ReadRange(ctx, "agg1", 1, 0)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if len(blobs) != 0 {
		t.Errorf("expected empty for count=0, got %d blobs", len(blobs))
	}
}

// --- Out-of-order appends ---

func TestAppend_OutOfOrder_ReadsInVersionOrder(t *testing.T) {
	s := New()
	ctx := context.Background()

	// Append out of order.
	s.Append(ctx, "agg1", 3, []byte("v3")) //nolint:errcheck
	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck
	s.Append(ctx, "agg1", 2, []byte("v2")) //nolint:errcheck

	blobs, err := s.ReadFrom(ctx, "agg1", 1)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(blobs) != 3 {
		t.Fatalf("expected 3 blobs, got %d", len(blobs))
	}
	if string(blobs[0]) != "v1" || string(blobs[1]) != "v2" || string(blobs[2]) != "v3" {
		t.Errorf("got %v, want [v1 v2 v3]", blobs)
	}
}

// --- Context cancellation ---

func TestAppend_CancelledContext_ReturnsError(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.Append(ctx, "agg1", 1, []byte("data"))
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

func TestReadFrom_CancelledContext_ReturnsError(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := s.ReadFrom(ctx, "agg1", 1)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

func TestReadRange_CancelledContext_ReturnsError(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := s.ReadRange(ctx, "agg1", 1, 10)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

// --- Concurrent access ---

func TestConcurrentAppend_UniqueVersions_AllSucceed(t *testing.T) {
	s := New()
	const n = 50

	var wg sync.WaitGroup
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			s.Append(context.Background(), "agg1", int64(v), []byte("data")) //nolint:errcheck
		}(i)
	}
	wg.Wait()

	blobs, err := s.ReadFrom(context.Background(), "agg1", 1)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(blobs) != n {
		t.Errorf("expected %d blobs after concurrent appends, got %d", n, len(blobs))
	}
}

func TestConcurrentAppend_SameVersion_ExactlyOneSucceeds(t *testing.T) {
	s := New()
	const n = 20

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		success int
	)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.Append(context.Background(), "agg1", 1, []byte("data"))
			if err == nil {
				mu.Lock()
				success++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if success != 1 {
		t.Errorf("expected exactly 1 success, got %d", success)
	}
}
