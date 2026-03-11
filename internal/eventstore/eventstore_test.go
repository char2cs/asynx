package eventstore

import (
	"context"
	"testing"

	asynxmd "github.com/char2cs/asynx/models"
)

// newTestEventStore builds a full EventStore backed by memStores.
func newTestEventStore(upcasters map[int]asynxmd.Upcaster, schemaVersion int) (*EventStore[order], *memStore, *memStore) {
	es := newMemStore()
	ss := newMemStore()
	store := New[order](es, ss, upcasters, schemaVersion)
	return store, es, ss
}

// testCommand is a simple Command[order] for testing.
type testCommand struct {
	id             string
	eventName      string
	shouldSnapshot bool
	emitFn         func(current *order) order
	validateFn     func(current *order) error
}

func (c *testCommand) AggregateID() string { return c.id }
func (c *testCommand) EventName() string   { return c.eventName }
func (c *testCommand) ShouldSnapshot() bool { return c.shouldSnapshot }
func (c *testCommand) Validate(current *order) error {
	if c.validateFn != nil {
		return c.validateFn(current)
	}
	return nil
}
func (c *testCommand) EmitEvent(current *order) order {
	return c.emitFn(current)
}

// --- EventStore.Get ---

func TestEventStoreGet_NotFound(t *testing.T) {
	store, _, _ := newTestEventStore(nil, 1)

	_, err := store.Get(context.Background(), "no-such")
	if err != asynxmd.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestEventStoreGet_AfterWrite(t *testing.T) {
	store, _, _ := newTestEventStore(nil, 1)

	cmd := &testCommand{
		id:        "agg1",
		eventName: "OrderCreated",
		emitFn: func(_ *order) order {
			return order{ID: "agg1", Status: "Pending", Total: 99}
		},
	}

	newState := cmd.EmitEvent(nil)
	_, err := store.Write(context.Background(), "agg1", nil, newState, cmd)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := store.Get(context.Background(), "agg1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != "Pending" {
		t.Errorf("Status = %q, want Pending", got.Status)
	}
	if got.Total != 99 {
		t.Errorf("Total = %v, want 99", got.Total)
	}
}

// --- EventStore.Write ---

func TestEventStoreWrite_NewAggregate(t *testing.T) {
	store, es, _ := newTestEventStore(nil, 1)

	cmd := &testCommand{
		id:        "agg1",
		eventName: "OrderCreated",
		emitFn:    func(_ *order) order { return order{ID: "agg1", Status: "Pending"} },
	}
	newState := cmd.EmitEvent(nil)

	evt, err := store.Write(context.Background(), "agg1", nil, newState, cmd)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if evt.Version != 1 {
		t.Errorf("Version = %d, want 1", evt.Version)
	}
	if evt.EventName != "OrderCreated" {
		t.Errorf("EventName = %q, want OrderCreated", evt.EventName)
	}

	blobs, _ := es.ReadFrom(context.Background(), "events:agg1", 1)
	if len(blobs) != 1 {
		t.Fatalf("expected 1 event in store, got %d", len(blobs))
	}
}

func TestEventStoreWrite_VersionIncrements(t *testing.T) {
	store, _, _ := newTestEventStore(nil, 1)

	write := func(status string, version int64) {
		t.Helper()
		cmd := &testCommand{
			id:        "agg1",
			eventName: "Updated",
			emitFn:    func(_ *order) order { return order{ID: "agg1", Status: status} },
		}
		newState := cmd.EmitEvent(nil)
		evt, err := store.Write(context.Background(), "agg1", nil, newState, cmd)
		if err != nil {
			t.Fatalf("Write %q: %v", status, err)
		}
		if evt.Version != version {
			t.Errorf("Version = %d, want %d", evt.Version, version)
		}
	}

	write("Pending", 1)
	write("Shipped", 2)
	write("Delivered", 3)
}

func TestEventStoreWrite_WithSnapshot(t *testing.T) {
	store, _, ss := newTestEventStore(nil, 1)

	cmd := &testCommand{
		id:             "agg1",
		eventName:      "Checkout",
		shouldSnapshot: true,
		emitFn:         func(_ *order) order { return order{ID: "agg1", Status: "Checked Out"} },
	}
	newState := cmd.EmitEvent(nil)

	_, err := store.Write(context.Background(), "agg1", nil, newState, cmd)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	snapBlobs, _ := ss.ReadFrom(context.Background(), "snapshots:agg1", 0)
	if len(snapBlobs) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snapBlobs))
	}
}

func TestEventStoreWrite_StorageError(t *testing.T) {
	store, es, _ := newTestEventStore(nil, 1)

	es.setError("events:agg1", storageErr)

	cmd := &testCommand{
		id:        "agg1",
		eventName: "OrderCreated",
		emitFn:    func(_ *order) order { return order{} },
	}
	_, err := store.Write(context.Background(), "agg1", nil, cmd.EmitEvent(nil), cmd)
	if err == nil {
		t.Fatal("expected error on Append failure")
	}
}

// --- EventStore.Exists ---

func TestEventStoreExists_True(t *testing.T) {
	store, _, _ := newTestEventStore(nil, 1)

	cmd := &testCommand{
		id:        "agg1",
		eventName: "OrderCreated",
		emitFn:    func(_ *order) order { return order{ID: "agg1"} },
	}
	newState := cmd.EmitEvent(nil)
	store.Write(context.Background(), "agg1", nil, newState, cmd) //nolint:errcheck

	exists, err := store.Exists(context.Background(), "agg1")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Error("expected Exists = true")
	}
}

func TestEventStoreExists_False(t *testing.T) {
	store, _, _ := newTestEventStore(nil, 1)

	exists, err := store.Exists(context.Background(), "unknown")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Error("expected Exists = false for unknown aggregate")
	}
}

// --- EventStore.Preload ---

func TestEventStorePreload(t *testing.T) {
	store, _, _ := newTestEventStore(nil, 1)

	// Preload on non-existent aggregate must not error.
	if err := store.Preload(context.Background(), "unknown"); err != nil {
		t.Errorf("Preload: %v", err)
	}
}

// --- EventStore.Replay ---

func TestEventStoreReplay_FullHistory(t *testing.T) {
	store, _, _ := newTestEventStore(nil, 1)

	// Write two events.
	write := func(status string) {
		t.Helper()
		cmd := &testCommand{
			id:        "agg1",
			eventName: "Updated",
			emitFn:    func(_ *order) order { return order{ID: "agg1", Status: status} },
		}
		newState := cmd.EmitEvent(nil)
		if _, err := store.Write(context.Background(), "agg1", nil, newState, cmd); err != nil {
			t.Fatalf("Write %q: %v", status, err)
		}
	}

	write("Pending")
	write("Shipped")

	var statuses []string
	err := store.Replay(context.Background(), "agg1", 1, 0, func(e asynxmd.Event[order]) {
		statuses = append(statuses, e.Aggregate.Status)
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(statuses) != 2 {
		t.Fatalf("expected 2 events, got %d", len(statuses))
	}
	if statuses[0] != "Pending" || statuses[1] != "Shipped" {
		t.Errorf("statuses = %v, want [Pending Shipped]", statuses)
	}
}

func TestEventStoreReplay_Empty(t *testing.T) {
	store, _, _ := newTestEventStore(nil, 1)

	var count int
	err := store.Replay(context.Background(), "no-such", 1, 0, func(asynxmd.Event[order]) {
		count++
	})
	if err != nil {
		t.Fatalf("Replay on empty aggregate: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 events, got %d", count)
	}
}

// --- EventStore.nextVersion ---

func TestEventStoreNextVersion_NewAggregate(t *testing.T) {
	store, _, _ := newTestEventStore(nil, 1)

	v, err := store.nextVersion(context.Background(), "new")
	if err != nil {
		t.Fatalf("nextVersion: %v", err)
	}
	if v != 1 {
		t.Errorf("nextVersion = %d, want 1", v)
	}
}

func TestEventStoreNextVersion_WithExistingEvents(t *testing.T) {
	store, _, _ := newTestEventStore(nil, 1)

	cmd := &testCommand{
		id:        "agg1",
		eventName: "Created",
		emitFn:    func(_ *order) order { return order{ID: "agg1"} },
	}
	store.Write(context.Background(), "agg1", nil, cmd.EmitEvent(nil), cmd) //nolint:errcheck
	store.Write(context.Background(), "agg1", nil, cmd.EmitEvent(nil), cmd) //nolint:errcheck

	v, err := store.nextVersion(context.Background(), "agg1")
	if err != nil {
		t.Fatalf("nextVersion: %v", err)
	}
	if v != 3 {
		t.Errorf("nextVersion = %d, want 3", v)
	}
}

// --- Integration: Get/Write/Replay cycle ---

func TestEventStoreGetWriteReplayCycle(t *testing.T) {
	store, _, _ := newTestEventStore(nil, 1)

	// Verify not found initially.
	_, err := store.Get(context.Background(), "agg1")
	if err != asynxmd.ErrNotFound {
		t.Fatalf("expected ErrNotFound before any writes, got %v", err)
	}

	// Write event 1: create order.
	cmd1 := &testCommand{
		id:        "agg1",
		eventName: "OrderCreated",
		emitFn:    func(_ *order) order { return order{ID: "agg1", Status: "Pending", Total: 50} },
	}
	evt1, err := store.Write(context.Background(), "agg1", nil, cmd1.EmitEvent(nil), cmd1)
	if err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	if evt1.Version != 1 {
		t.Errorf("evt1.Version = %d, want 1", evt1.Version)
	}

	// Get current state.
	s1, err := store.Get(context.Background(), "agg1")
	if err != nil {
		t.Fatalf("Get after write 1: %v", err)
	}
	if s1.Status != "Pending" {
		t.Errorf("s1.Status = %q, want Pending", s1.Status)
	}

	// Write event 2: ship order.
	cmd2 := &testCommand{
		id:             "agg1",
		eventName:      "OrderShipped",
		shouldSnapshot: true,
		emitFn: func(current *order) order {
			o := *current
			o.Status = "Shipped"
			return o
		},
	}
	evt2, err := store.Write(context.Background(), "agg1", &s1, cmd2.EmitEvent(&s1), cmd2)
	if err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	if evt2.Version != 2 {
		t.Errorf("evt2.Version = %d, want 2", evt2.Version)
	}

	// Get final state.
	s2, err := store.Get(context.Background(), "agg1")
	if err != nil {
		t.Fatalf("Get after write 2: %v", err)
	}
	if s2.Status != "Shipped" {
		t.Errorf("s2.Status = %q, want Shipped", s2.Status)
	}

	// Replay all events.
	var versions []int64
	err = store.Replay(context.Background(), "agg1", 1, 0, func(e asynxmd.Event[order]) {
		versions = append(versions, e.Version)
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(versions) != 2 || versions[0] != 1 || versions[1] != 2 {
		t.Errorf("versions = %v, want [1 2]", versions)
	}
}

// --- EventStore.nextVersion: snapshot store error ---

func TestEventStoreNextVersion_SnapshotStoreError(t *testing.T) {
	es := newMemStore()
	ss := &errStore{err: storageErr}
	store := New[order](es, ss, nil, 1)

	_, err := store.nextVersion(context.Background(), "agg1")
	if err == nil {
		t.Fatal("expected error when snapshot store fails")
	}
}

func TestEventStoreNextVersion_EventStoreError(t *testing.T) {
	es := &errStore{err: storageErr}
	ss := newMemStore()
	store := New[order](es, ss, nil, 1)

	_, err := store.nextVersion(context.Background(), "agg1")
	if err == nil {
		t.Fatal("expected error when event store fails")
	}
}

func TestEventStoreNextVersion_WithSnapshot(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()
	store := New[order](es, ss, nil, 1)

	// Snapshot at version 5, no delta events.
	snapBlob := makeSnapshotBlob(t, 5, 1, order{ID: "agg1"})
	ss.Append(context.Background(), "snapshots:agg1", 5, snapBlob) //nolint:errcheck

	v, err := store.nextVersion(context.Background(), "agg1")
	if err != nil {
		t.Fatalf("nextVersion: %v", err)
	}
	if v != 6 {
		t.Errorf("nextVersion = %d, want 6", v)
	}
}

func TestEventStoreWrite_NextVersionError(t *testing.T) {
	es := newMemStore()
	ss := &errStore{err: storageErr}
	store := New[order](es, ss, nil, 1)

	cmd := &testCommand{
		id:        "agg1",
		eventName: "Created",
		emitFn:    func(_ *order) order { return order{} },
	}
	_, err := store.Write(context.Background(), "agg1", nil, order{}, cmd)
	if err == nil {
		t.Fatal("expected error when nextVersion fails")
	}
}

// --- Integration: schema migration ---

func TestEventStoreWrite_SameStore_EventAndSnapshot(t *testing.T) {
	// When eventStore == snapshotStore (default), both streams must coexist.
	es := newMemStore()
	store := New[order](es, es, nil, 1)

	cmd := &testCommand{
		id:             "agg1",
		eventName:      "OrderCreated",
		shouldSnapshot: true,
		emitFn:         func(_ *order) order { return order{ID: "agg1", Status: "Pending"} },
	}
	newState := cmd.EmitEvent(nil)

	_, err := store.Write(context.Background(), "agg1", nil, newState, cmd)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := store.Get(context.Background(), "agg1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != "Pending" {
		t.Errorf("Status = %q, want Pending", got.Status)
	}
}
