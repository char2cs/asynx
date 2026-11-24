package reader

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/char2cs/asynx/internal/eventstore/internal/replayer"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/storage"
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
	storage.Store
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

func keepPayloadUpcaster(
	_ context.Context,
	_ string,
	payload []byte,
) ([]byte, error) {
	return payload,
		nil
}

func upcastingReader(
	es storage.Store,
	ss storage.SnapshotStore,
) *Reader[order] {
	upcasters := map[int]event.Upcaster{
		1: keepPayloadUpcaster,
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
	holds, err := r.streamHolds(
		ctx,
		"agg2",
		1,
		"e1",
	)
	if holds ||
		err == nil {
		t.Fatalf(
			"holds = %v, err = %v; want an unreadable event reported, not taken for the expected one",
			holds,
			err,
		)
	}
}

type readRangeFailsStore struct{ storage.Store }

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
	ss storage.SnapshotStore,
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

func seedUpcastableOrder(
	t *testing.T,
	mem store.Memory,
) {
	t.Helper()

	err := mem.Append(
		context.Background(),
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`),
		),
	)
	if err != nil {
		t.Fatalf(
			"seed: %v",
			err,
		)
	}
}

type snapshotFailure struct {
	event event.Event[order]
	err   error
}

func recordSnapshotFailures(
	failures *[]snapshotFailure,
) func(context.Context, event.Event[order], error) {
	return func(
		_ context.Context,
		evt event.Event[order],
		err error,
	) {
		*failures = append(
			*failures,
			snapshotFailure{
				event: evt,
				err:   err,
			},
		)
	}
}

func assertSingleSnapshotFailure(
	t *testing.T,
	failures []snapshotFailure,
) {
	t.Helper()

	if len(failures) != 1 {
		t.Fatalf(
			"handler calls = %d, want 1",
			len(failures),
		)
	}

	got := failures[0]
	if !errors.Is(
		got.err,
		storageErr,
	) {
		t.Errorf(
			"err = %v, want %v",
			got.err,
			storageErr,
		)
	}

	if got.event.AggregateID != "agg1" ||
		got.event.Version != 1 ||
		got.event.ID != "e1" ||
		got.event.SchemaVersion != 2 ||
		got.event.Aggregate.Status != "Shipped" {
		t.Errorf(
			"event = %+v, want agg1 v1 e1 schema 2 Shipped",
			got.event,
		)
	}
}

func TestGet_AutoSnapshotFailure_ReportsToTheHandlerOnce(
	t *testing.T,
) {
	mem := store.New()
	ss := store.NewSnapshots()
	seedUpcastableOrder(
		t,
		mem,
	)
	ss.SetError(
		"agg1",
		storageErr,
	)
	var failures []snapshotFailure
	r := upcastingReader(
		mem,
		ss,
	)
	r.SetSnapshotErrorHandler(recordSnapshotFailures(&failures))

	state, err := r.Get(
		context.Background(),
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if state.Status != "Shipped" {
		t.Errorf(
			"Status = %q, want Shipped",
			state.Status,
		)
	}

	assertSingleSnapshotFailure(
		t,
		failures,
	)
}

func TestLoad_AutoSnapshotFailure_ReportsToTheHandlerOnce(
	t *testing.T,
) {
	mem := store.New()
	ss := store.NewSnapshots()
	seedUpcastableOrder(
		t,
		mem,
	)
	ss.SetError(
		"agg1",
		storageErr,
	)
	var failures []snapshotFailure
	r := upcastingReader(
		mem,
		ss,
	)
	r.SetSnapshotErrorHandler(recordSnapshotFailures(&failures))

	state, version, err := r.Load(
		context.Background(),
		"agg1",
	)
	if err != nil ||
		version != 1 ||
		state.Status != "Shipped" {
		t.Fatalf(
			"Load = %+v, %d, %v; want Shipped, 1, nil",
			state,
			version,
			err,
		)
	}

	assertSingleSnapshotFailure(
		t,
		failures,
	)
}

func TestGet_AutoSnapshotSuccess_DoesNotCallTheHandler(
	t *testing.T,
) {
	mem := store.New()
	ss := store.NewSnapshots()
	seedUpcastableOrder(
		t,
		mem,
	)
	var failures []snapshotFailure
	r := upcastingReader(
		mem,
		ss,
	)
	r.SetSnapshotErrorHandler(recordSnapshotFailures(&failures))

	_, err := r.Get(
		context.Background(),
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if len(failures) != 0 {
		t.Errorf(
			"handler calls = %d, want 0",
			len(failures),
		)
	}
}

func TestGet_AutoSnapshotFailure_NilHandlerDropsTheError(
	t *testing.T,
) {
	mem := store.New()
	ss := store.NewSnapshots()
	seedUpcastableOrder(
		t,
		mem,
	)
	ss.SetError(
		"agg1",
		storageErr,
	)
	r := upcastingReader(
		mem,
		ss,
	)
	r.SetSnapshotErrorHandler(nil)

	state, err := r.Get(
		context.Background(),
		"agg1",
	)
	if err != nil ||
		state.Status != "Shipped" {
		t.Fatalf(
			"Get = %+v, %v; want Shipped, nil",
			state,
			err,
		)
	}
}

func TestGet_AutoSnapshotFailure_HandlerPanicDoesNotBreakTheRead(
	t *testing.T,
) {
	mem := store.New()
	ss := store.NewSnapshots()
	seedUpcastableOrder(
		t,
		mem,
	)
	ss.SetError(
		"agg1",
		storageErr,
	)
	r := upcastingReader(
		mem,
		ss,
	)
	r.SetSnapshotErrorHandler(panicOnSnapshotFailure)

	state, err := r.Get(
		context.Background(),
		"agg1",
	)
	if err != nil ||
		state.Status != "Shipped" {
		t.Fatalf(
			"Get = %+v, %v; want Shipped, nil",
			state,
			err,
		)
	}
}

func panicOnSnapshotFailure(
	context.Context,
	event.Event[order],
	error,
) {
	panic("handler blew up")
}

func TestGet_AutoSnapshotFailure_HandlerPanicIsWrittenToStderr(
	t *testing.T,
) {
	mem := store.New()
	ss := store.NewSnapshots()
	seedUpcastableOrder(
		t,
		mem,
	)
	ss.SetError(
		"agg1",
		storageErr,
	)
	var corruption corruptionCapture
	r := New[order](
		mem,
		ss,
		replayer.New[order](
			mem,
			map[int]event.Upcaster{1: keepPayloadUpcaster},
			2,
			order{},
		),
		2,
		order{},
		corruption.record,
	)
	r.SetSnapshotErrorHandler(panicOnSnapshotFailure)
	var stderr stderrCapture
	stderr.begin(t)

	_, err := r.Get(
		context.Background(),
		"agg1",
	)
	written := stderr.end(t)
	if err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if !strings.Contains(
		written,
		"handler blew up",
	) {
		t.Errorf(
			"stderr = %q, want the contained panic written to it",
			written,
		)
	}

	if corruption.err != nil {
		t.Errorf(
			"a handler panic reached the corruption callback: %v",
			corruption.err,
		)
	}
}

type stderrCapture struct {
	original *os.File
	reader   *os.File
}

func (c *stderrCapture) begin(
	t *testing.T,
) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	c.original = os.Stderr
	c.reader = reader
	os.Stderr = writer
}

func (c *stderrCapture) end(
	t *testing.T,
) string {
	t.Helper()
	os.Stderr.Close()
	os.Stderr = c.original
	captured, err := io.ReadAll(c.reader)
	if err != nil {
		t.Fatal(err)
	}

	return string(captured)
}

func TestGet_AutoSnapshot_UnreadableStreamCheckIsReportedToTheHandler(
	t *testing.T,
) {
	mem := store.New()
	seedUpcastableOrder(
		t,
		mem,
	)
	r := upcastingReader(
		&readRangeFailsStore{Store: mem},
		store.NewSnapshots(),
	)
	var failures []snapshotFailure
	r.SetSnapshotErrorHandler(recordSnapshotFailures(&failures))

	_, err := r.Get(
		context.Background(),
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if len(failures) != 1 ||
		!errors.Is(
			failures[0].err,
			storageErr,
		) {
		t.Fatalf(
			"handler saw %v, want one error wrapping the stream-check failure",
			failures,
		)
	}
}
