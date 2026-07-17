package eventstore

import (
	"context"
	"testing"

	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/store"
)

// TestSnapshotReadDoesNotScaleWithHistory is the regression guard for the
// original defect: Get() read every snapshot ever written to use only the last.
func TestSnapshotReadDoesNotScaleWithHistory(t *testing.T) {
	ctx := context.Background()
	snaps := &mocks.CountingSnapshotStore{Inner: store.NewSnapshots()}
	es := New[order](store.New(), snaps, nil, 1, nil)

	const n = 500
	for i := range n {
		if _, err := es.Write(ctx, mocks.UpdateOrderCmd{
			ID:       "agg-1",
			NewState: order{ID: "agg-1", Total: float64(i), Status: "Pending"},
			Snapshot: true,
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	snaps.Reset()
	if _, err := es.Get(ctx, "agg-1"); err != nil {
		t.Fatalf("get: %v", err)
	}

	if snaps.GetCalls != 1 {
		t.Errorf("Get calls = %d, want 1", snaps.GetCalls)
	}
	if snaps.GetBytes > 512 {
		t.Errorf("bytes read = %d after %d snapshots, want one blob — read must not scale with history", snaps.GetBytes, n)
	}
}

// TestWritePaysConstantSnapshotCost guards the half of the defect the original
// report missed: EventStore.Write read all snapshots twice, via Get and
// nextVersion.
func TestWritePaysConstantSnapshotCost(t *testing.T) {
	ctx := context.Background()
	snaps := &mocks.CountingSnapshotStore{Inner: store.NewSnapshots()}
	es := New[order](store.New(), snaps, nil, 1, nil)

	const n = 500
	for i := range n {
		if _, err := es.Write(ctx, mocks.UpdateOrderCmd{
			ID:       "agg-2",
			NewState: order{ID: "agg-2", Total: float64(i), Status: "Pending"},
			Snapshot: true,
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	snaps.Reset()
	if _, err := es.Write(ctx, mocks.UpdateOrderCmd{
		ID:       "agg-2",
		NewState: order{ID: "agg-2", Total: 999, Status: "Pending"},
		Snapshot: true,
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	if snaps.GetBytes > 1024 {
		t.Errorf("bytes read = %d on one Write after %d snapshots, want ~2 blobs", snaps.GetBytes, n)
	}
}
