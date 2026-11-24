package reader

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/char2cs/asynx/internal/eventstore/internal/replayer"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

type failingSnapshots struct{ err error }

// Put returns the configured error.
func (f *failingSnapshots) Put(
	context.Context,
	string,
	int64,
	[]byte,
) error {
	return f.err
}

// Get returns the configured error.
func (f *failingSnapshots) Get(
	context.Context,
	string,
) ([]byte, bool, error) {
	return nil,
		false,
		f.err
}

// Delete returns the configured error.
func (f *failingSnapshots) Delete(
	context.Context,
	string,
) error {
	return f.err
}

type afterReadStore struct {
	asynxmd.Store
	hook func()
	once bool
}

// ReadFrom reads from the wrapped store, then runs the hook once.
func (s *afterReadStore) ReadFrom(
	ctx context.Context,
	streamID string,
	fromVersion int64,
) ([][]byte, error) {
	blobs, err := s.Store.ReadFrom(
		ctx,
		streamID,
		fromVersion,
	)
	if !s.once {
		s.once = true
		s.hook()
	}

	return blobs,
		err
}

func upcastingReader(
	es asynxmd.Store,
	ss asynxmd.SnapshotStore,
) *Reader[order] {
	fn := func(
		_ context.Context,
		_ string,
		p []byte,
	) ([]byte, error) {
		return p,
			nil
	}
	upcasters := map[int]asynxmd.Upcaster{
		1: fn,
	}
	return New[order](
		es,
		ss,
		replayer.New[order](
			es,
			upcasters,
			2,
			order{},
		),
		2,
		order{},
		nil,
	)
}

func TestGet_AutoSnapshot_NotWrittenOverAStreamErasedMeanwhile(
	t *testing.T,
) {
	mem := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	mem.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[]`),
		),
	)
	hook2 := func() {
		mem.Delete(
			ctx,
			"events:agg1",
		)
	}
	es := &afterReadStore{Store: mem, hook: hook2}
	if _, err := upcastingReader(
		es,
		ss,
	).Get(
		ctx,
		"agg1",
	); err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if _, found, _ := ss.Get(
		ctx,
		"agg1",
	); found {
		t.Fatal("a snapshot of an erased aggregate was written")
	}
}

func TestGet_AutoSnapshot_NotWrittenOverAStreamRecreatedMeanwhile(
	t *testing.T,
) {
	mem := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	mem.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[]`),
		),
	)

	recreate := func() {
		mem.Delete(
			ctx,
			"events:agg1",
		)
		mem.Append(
			ctx,
			"events:agg1",
			1,
			makeEventBlob(
				t,
				"e9",
				"Created",
				1,
				2,
				json.RawMessage(`[]`),
			),
		)
	}
	es := &afterReadStore{Store: mem, hook: recreate}
	if _, err := upcastingReader(
		es,
		ss,
	).Get(
		ctx,
		"agg1",
	); err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if _, found, _ := ss.Get(
		ctx,
		"agg1",
	); found {
		t.Fatal("a snapshot of the old aggregate was written over the re-created one")
	}
}

func TestGet_AutoSnapshot_NotWrittenWhenTheStreamCannotBeReChecked(
	t *testing.T,
) {
	mem := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	mem.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[]`),
		),
	)

	es := &readRangeFailsStore{Store: mem}
	if _, err := upcastingReader(
		es,
		ss,
	).Get(
		ctx,
		"agg1",
	); err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if _, found, _ := ss.Get(
		ctx,
		"agg1",
	); found {
		t.Fatal("a snapshot was written without confirming the stream")
	}

	mem.Append(
		ctx,
		"events:agg2",
		1,
		[]byte("not json"),
	)
	r := upcastingReader(
		mem,
		ss,
	)
	if r.streamHolds(
		ctx,
		"agg2",
		1,
		"e1",
	) {
		t.Fatal("an unreadable event was taken for the expected one")
	}
}

type readRangeFailsStore struct{ asynxmd.Store }

// ReadRange always fails with the shared storage error.
func (readRangeFailsStore) ReadRange(
	context.Context,
	string,
	int64,
	int64,
) ([][]byte, error) {
	return nil,
		storageErr
}

func assertNoSnapshot(
	t *testing.T,
	ss asynxmd.SnapshotStore,
) {
	t.Helper()

	_, found, _ := ss.Get(
		context.Background(),
		"agg1",
	)
	if found {
		t.Error("the snapshot was written before the guard ran")
	}
}

func TestGet_AutoSnapshot_IsWrittenUnderTheGuard(
	t *testing.T,
) {
	mem := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	mem.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[]`),
		),
	)

	r := upcastingReader(
		mem,
		ss,
	)
	var guarded []string
	setSnapshotGuardFn := func(
		id string,
		fn func(),
	) bool {
		guarded = append(
			guarded,
			id,
		)
		assertNoSnapshot(
			t,
			ss,
		)
		fn()
		return true
	}
	r.SetSnapshotGuard(setSnapshotGuardFn)

	if _, err := r.Get(
		ctx,
		"agg1",
	); err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if len(guarded) != 1 ||
		guarded[0] != "agg1" {
		t.Fatalf(
			"guard calls = %v, want [agg1]",
			guarded,
		)
	}

	if _, found, _ := ss.Get(
		ctx,
		"agg1",
	); !found {
		t.Fatal("no snapshot written under the guard")
	}
}

func TestGet_AutoSnapshot_SkippedWhenTheGuardRefuses(
	t *testing.T,
) {
	mem := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	mem.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[]`),
		),
	)

	r := upcastingReader(
		mem,
		ss,
	)
	setSnapshotGuardFn := func(
		string,
		func(),
	) bool {
		return false
	}
	r.SetSnapshotGuard(setSnapshotGuardFn)

	if _, err := r.Get(
		ctx,
		"agg1",
	); err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if _, found, _ := ss.Get(
		ctx,
		"agg1",
	); found {
		t.Fatal("a snapshot was written although the guard refused")
	}
}

func TestLoad_AutoSnapshot_DoesNotUseTheGuard(
	t *testing.T,
) {
	mem := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	mem.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[]`),
		),
	)

	r := upcastingReader(
		mem,
		ss,
	)
	setSnapshotGuardFn := func(
		string,
		func(),
	) bool {
		t.Error("Load asked for the guard")
		return false
	}
	r.SetSnapshotGuard(setSnapshotGuardFn)

	if _, _, err := r.Load(
		ctx,
		"agg1",
	); err != nil {
		t.Fatalf(
			"Load: %v",
			err,
		)
	}

	if _, found, _ := ss.Get(
		ctx,
		"agg1",
	); !found {
		t.Fatal("Load did not write the auto-snapshot")
	}
}
