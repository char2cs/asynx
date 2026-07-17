package mocks

import (
	"context"
	"errors"
	"testing"

	asynxmd "github.com/char2cs/asynx/models"
)

// --- Store (no-op) ---

func TestStore_Append_ReturnsNil(t *testing.T) {
	s := &Store{}
	err := s.Append(context.Background(), "agg1", 1, []byte("data"))
	if err != nil {
		t.Errorf("Append = %v, want nil", err)
	}
}

func TestStore_ReadFrom_ReturnsNilNil(t *testing.T) {
	s := &Store{}
	blobs, err := s.ReadFrom(context.Background(), "agg1", 1)
	if err != nil || blobs != nil {
		t.Errorf("ReadFrom = (%v, %v), want (nil, nil)", blobs, err)
	}
}

func TestStore_ReadRange_ReturnsNilNil(t *testing.T) {
	s := &Store{}
	blobs, err := s.ReadRange(context.Background(), "agg1", 1, 10)
	if err != nil || blobs != nil {
		t.Errorf("ReadRange = (%v, %v), want (nil, nil)", blobs, err)
	}
}

// --- ErrStore ---

func TestErrStore_Append_ReturnsErr(t *testing.T) {
	sentinel := errors.New("fail")
	es := &ErrStore{Err: sentinel}
	if err := es.Append(context.Background(), "agg1", 1, nil); !errors.Is(err, sentinel) {
		t.Errorf("Append = %v, want sentinel", err)
	}
}

func TestErrStore_ReadFrom_ReturnsErr(t *testing.T) {
	sentinel := errors.New("fail")
	es := &ErrStore{Err: sentinel}
	_, err := es.ReadFrom(context.Background(), "agg1", 1)
	if !errors.Is(err, sentinel) {
		t.Errorf("ReadFrom = %v, want sentinel", err)
	}
}

func TestErrStore_ReadRange_ReturnsErr(t *testing.T) {
	sentinel := errors.New("fail")
	es := &ErrStore{Err: sentinel}
	_, err := es.ReadRange(context.Background(), "agg1", 1, 10)
	if !errors.Is(err, sentinel) {
		t.Errorf("ReadRange = %v, want sentinel", err)
	}
}

func TestErrStore_ImplementsStore(t *testing.T) {
	var _ asynxmd.Store = &ErrStore{}
}

// --- CorruptBlobStore ---

func TestCorruptBlobStore_Append_ReturnsNil(t *testing.T) {
	c := &CorruptBlobStore{}
	if err := c.Append(context.Background(), "agg1", 1, nil); err != nil {
		t.Errorf("Append = %v, want nil", err)
	}
}

func TestCorruptBlobStore_ReadFrom_ReturnsCorruptBlob(t *testing.T) {
	c := &CorruptBlobStore{}
	blobs, err := c.ReadFrom(context.Background(), "agg1", 1)
	if err != nil {
		t.Fatalf("ReadFrom error: %v", err)
	}
	if len(blobs) != 1 {
		t.Fatalf("expected 1 blob, got %d", len(blobs))
	}
	if string(blobs[0]) != "!!!invalid" {
		t.Errorf("blob = %q, want !!!invalid", blobs[0])
	}
}

func TestCorruptBlobStore_ReadRange_ReturnsCorruptBlob(t *testing.T) {
	c := &CorruptBlobStore{}
	blobs, err := c.ReadRange(context.Background(), "agg1", 1, 10)
	if err != nil {
		t.Fatalf("ReadRange error: %v", err)
	}
	if len(blobs) != 1 || string(blobs[0]) != "!!!invalid" {
		t.Errorf("blobs = %v, want [!!!invalid]", blobs)
	}
}

func TestCorruptBlobStore_ImplementsStore(t *testing.T) {
	var _ asynxmd.Store = &CorruptBlobStore{}
}

// --- SnapshotStore (no-op) ---

func TestSnapshotStore_Put_ReturnsNil(t *testing.T) {
	s := &SnapshotStore{}
	if err := s.Put(context.Background(), "agg1", 1, []byte("data")); err != nil {
		t.Errorf("Put = %v, want nil", err)
	}
}

func TestSnapshotStore_Get_ReturnsNotFound(t *testing.T) {
	s := &SnapshotStore{}
	data, found, err := s.Get(context.Background(), "agg1")
	if data != nil || found || err != nil {
		t.Errorf("Get = (%v, %v, %v), want (nil, false, nil)", data, found, err)
	}
}

func TestSnapshotStore_Delete_ReturnsNil(t *testing.T) {
	s := &SnapshotStore{}
	if err := s.Delete(context.Background(), "agg1"); err != nil {
		t.Errorf("Delete = %v, want nil", err)
	}
}

func TestSnapshotStore_ImplementsSnapshotStore(t *testing.T) {
	var _ asynxmd.SnapshotStore = &SnapshotStore{}
}

// --- CountingSnapshotStore ---

func TestCountingSnapshotStore_CountsGetCallsAndBytes(t *testing.T) {
	ctx := context.Background()
	c := &CountingSnapshotStore{Inner: &SnapshotStore{}}

	if err := c.Put(ctx, "agg1", 1, []byte("data")); err != nil {
		t.Fatalf("Put = %v, want nil", err)
	}
	if _, _, err := c.Get(ctx, "agg1"); err != nil {
		t.Fatalf("Get = %v, want nil", err)
	}
	if err := c.Delete(ctx, "agg1"); err != nil {
		t.Fatalf("Delete = %v, want nil", err)
	}

	// The no-op inner returns no data, so bytes stay 0 but the call is counted.
	if c.GetCalls != 1 {
		t.Errorf("GetCalls = %d, want 1", c.GetCalls)
	}
	if c.GetBytes != 0 {
		t.Errorf("GetBytes = %d, want 0 (no-op inner returns no data)", c.GetBytes)
	}
}

func TestCountingSnapshotStore_GetBytesTracksReturnedData(t *testing.T) {
	ctx := context.Background()
	inner := &recordingSnapshotStore{blob: []byte("hello")}
	c := &CountingSnapshotStore{Inner: inner}

	if _, _, err := c.Get(ctx, "agg1"); err != nil {
		t.Fatalf("Get = %v", err)
	}
	if c.GetBytes != 5 {
		t.Errorf("GetBytes = %d, want 5", c.GetBytes)
	}
}

func TestCountingSnapshotStore_Reset(t *testing.T) {
	ctx := context.Background()
	c := &CountingSnapshotStore{Inner: &recordingSnapshotStore{blob: []byte("x")}}

	_, _, _ = c.Get(ctx, "agg1")
	c.Reset()

	if c.GetCalls != 0 || c.GetBytes != 0 {
		t.Errorf("after Reset: GetCalls=%d GetBytes=%d, want 0 0", c.GetCalls, c.GetBytes)
	}
}

func TestCountingSnapshotStore_ImplementsSnapshotStore(t *testing.T) {
	var _ asynxmd.SnapshotStore = &CountingSnapshotStore{}
}

// recordingSnapshotStore is a minimal SnapshotStore whose Get returns a fixed
// blob, so CountingSnapshotStore's byte tallying can be exercised.
type recordingSnapshotStore struct{ blob []byte }

func (r *recordingSnapshotStore) Put(context.Context, string, int64, []byte) error { return nil }
func (r *recordingSnapshotStore) Get(context.Context, string) ([]byte, bool, error) {
	return r.blob, true, nil
}
func (r *recordingSnapshotStore) Delete(context.Context, string) error { return nil }
