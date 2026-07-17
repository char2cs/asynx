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
