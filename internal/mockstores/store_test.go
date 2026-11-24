package mockstores

import (
	"context"
	"testing"
)

func TestStore_Append_ReturnsNil(
	t *testing.T,
) {
	s := &Store{}
	err := s.Append(
		context.Background(),
		"agg1",
		1,
		[]byte("data"),
	)
	if err != nil {
		t.Errorf(
			"Append = %v, want nil",
			err,
		)
	}
}

func TestStore_ReadFrom_ReturnsNilNil(
	t *testing.T,
) {
	s := &Store{}
	blobs, err := s.ReadFrom(
		context.Background(),
		"agg1",
		1,
	)
	if err != nil ||
		blobs != nil {
		t.Errorf(
			"ReadFrom = (%v, %v), want (nil, nil)",
			blobs,
			err,
		)
	}
}

func TestStore_ReadRange_ReturnsNilNil(
	t *testing.T,
) {
	s := &Store{}
	blobs, err := s.ReadRange(
		context.Background(),
		"agg1",
		1,
		10,
	)
	if err != nil ||
		blobs != nil {
		t.Errorf(
			"ReadRange = (%v, %v), want (nil, nil)",
			blobs,
			err,
		)
	}
}

type recordingSnapshotStore struct{ blob []byte }

func (r *recordingSnapshotStore) Put(
	context.Context,
	string,
	int64,
	[]byte,
) error {
	return nil
}

func (r *recordingSnapshotStore) Get(
	context.Context,
	string,
) ([]byte, bool, error) {
	return r.blob,
		true,
		nil
}

func (r *recordingSnapshotStore) Delete(
	context.Context,
	string,
) error {
	return nil
}
