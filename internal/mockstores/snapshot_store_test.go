package mockstores

import (
	"context"
	asynxmd "github.com/char2cs/asynx/models"
	"testing"
)

func TestSnapshotStore_Put_ReturnsNil(
	t *testing.T,
) {
	s := &SnapshotStore{}
	if err := s.Put(
		context.Background(),
		"agg1",
		1,
		[]byte("data"),
	); err != nil {
		t.Errorf(
			"Put = %v, want nil",
			err,
		)
	}
}

func TestSnapshotStore_Get_ReturnsNotFound(
	t *testing.T,
) {
	s := &SnapshotStore{}
	data, found, err := s.Get(
		context.Background(),
		"agg1",
	)
	if data != nil ||
		found ||
		err != nil {
		t.Errorf(
			"Get = (%v, %v, %v), want (nil, false, nil)",
			data,
			found,
			err,
		)
	}
}

func TestSnapshotStore_Delete_ReturnsNil(
	t *testing.T,
) {
	s := &SnapshotStore{}
	if err := s.Delete(
		context.Background(),
		"agg1",
	); err != nil {
		t.Errorf(
			"Delete = %v, want nil",
			err,
		)
	}
}

func TestSnapshotStore_ImplementsSnapshotStore(
	t *testing.T,
) {
	var _ asynxmd.SnapshotStore = &SnapshotStore{}
}
