package mockstores

import (
	"context"
	asynxmd "github.com/char2cs/asynx/models"
	"testing"
)

func TestCorruptBlobStore_Append_ReturnsNil(
	t *testing.T,
) {
	c := &CorruptBlobStore{}
	if err := c.Append(
		context.Background(),
		"agg1",
		1,
		nil,
	); err != nil {
		t.Errorf(
			"Append = %v, want nil",
			err,
		)
	}
}

func TestCorruptBlobStore_ReadFrom_ReturnsCorruptBlob(
	t *testing.T,
) {
	c := &CorruptBlobStore{}
	blobs, err := c.ReadFrom(
		context.Background(),
		"agg1",
		1,
	)
	if err != nil {
		t.Fatalf(
			"ReadFrom error: %v",
			err,
		)
	}

	if len(blobs) != 1 {
		t.Fatalf(
			"expected 1 blob, got %d",
			len(blobs),
		)
	}

	if string(blobs[0]) != "!!!invalid" {
		t.Errorf(
			"blob = %q, want !!!invalid",
			blobs[0],
		)
	}
}

func TestCorruptBlobStore_ReadRange_ReturnsCorruptBlob(
	t *testing.T,
) {
	c := &CorruptBlobStore{}
	blobs, err := c.ReadRange(
		context.Background(),
		"agg1",
		1,
		10,
	)
	if err != nil {
		t.Fatalf(
			"ReadRange error: %v",
			err,
		)
	}

	if len(blobs) != 1 ||
		string(blobs[0]) != "!!!invalid" {
		t.Errorf(
			"blobs = %v, want [!!!invalid]",
			blobs,
		)
	}
}

func TestCorruptBlobStore_ImplementsStore(
	t *testing.T,
) {
	var _ asynxmd.Store = &CorruptBlobStore{}
}
