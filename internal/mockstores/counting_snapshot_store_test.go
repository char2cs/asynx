package mockstores

import (
	"context"
	asynxmd "github.com/char2cs/asynx/models"
	"testing"
)

func TestCountingSnapshotStore_CountsGetCallsAndBytes(
	t *testing.T,
) {
	ctx := context.Background()
	c := &CountingSnapshotStore{Inner: &SnapshotStore{}}

	if err := c.Put(
		ctx,
		"agg1",
		1,
		[]byte("data"),
	); err != nil {
		t.Fatalf(
			"Put = %v, want nil",
			err,
		)
	}

	if _, _, err := c.Get(
		ctx,
		"agg1",
	); err != nil {
		t.Fatalf(
			"Get = %v, want nil",
			err,
		)
	}

	if err := c.Delete(
		ctx,
		"agg1",
	); err != nil {
		t.Fatalf(
			"Delete = %v, want nil",
			err,
		)
	}

	if c.GetCalls != 1 {
		t.Errorf(
			"GetCalls = %d, want 1",
			c.GetCalls,
		)
	}

	if c.GetBytes != 0 {
		t.Errorf(
			"GetBytes = %d, want 0 (no-op inner returns no data)",
			c.GetBytes,
		)
	}
}

func TestCountingSnapshotStore_GetBytesTracksReturnedData(
	t *testing.T,
) {
	ctx := context.Background()
	inner := &recordingSnapshotStore{blob: []byte("hello")}
	c := &CountingSnapshotStore{Inner: inner}

	if _, _, err := c.Get(
		ctx,
		"agg1",
	); err != nil {
		t.Fatalf(
			"Get = %v",
			err,
		)
	}

	if c.GetBytes != 5 {
		t.Errorf(
			"GetBytes = %d, want 5",
			c.GetBytes,
		)
	}
}

func TestCountingSnapshotStore_Reset(
	t *testing.T,
) {
	ctx := context.Background()
	c := &CountingSnapshotStore{Inner: &recordingSnapshotStore{blob: []byte("x")}}

	_, _, _ = c.Get(
		ctx,
		"agg1",
	)
	c.Reset()

	if c.GetCalls != 0 ||
		c.GetBytes != 0 {
		t.Errorf(
			"after Reset: GetCalls=%d GetBytes=%d, want 0 0",
			c.GetCalls,
			c.GetBytes,
		)
	}
}

func TestCountingSnapshotStore_ImplementsSnapshotStore(
	t *testing.T,
) {
	var _ asynxmd.SnapshotStore = &CountingSnapshotStore{}
}
