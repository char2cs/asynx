package core

import (
	"context"
	"errors"
	"fmt"
	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/models"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func seedPending(
	t *testing.T,
	s store.Memory,
	es eventstore.EventStore[order],
	ids ...string,
) {
	t.Helper()
	for _, id := range ids {
		seedOne(
			t,
			s,
			es,
			id,
		)
	}
}

func seedOne(
	t *testing.T,
	s store.Memory,
	es eventstore.EventStore[order],
	id string,
) {
	t.Helper()
	ctx := context.Background()
	mustWrite(
		t,
		es,
		mocks.CreateOrderCmd{ID: id, Total: 1},
	)
	enqueue := func(
		tx asynxmd.Tx,
	) error {
		return tx.EnqueueDelivery(
			ctx,
			id,
			1,
		)
	}
	s.Atomically(
		ctx,
		enqueue,
	)
}

func seedUnrebuildable(
	t *testing.T,
	s store.Memory,
	aggregateID string,
) {
	t.Helper()
	ctx := context.Background()
	if err := s.Append(
		ctx,
		"events:"+aggregateID,
		1,
		[]byte("!!!"),
	); err != nil {
		t.Fatal(err)
	}

	enqueue := func(
		tx asynxmd.Tx,
	) error {
		return tx.EnqueueDelivery(
			ctx,
			aggregateID,
			1,
		)
	}
	if err := s.Atomically(
		ctx,
		enqueue,
	); err != nil {
		t.Fatal(err)
	}
}

func assertRecovered(
	t *testing.T,
	delivered *sync.Map,
	ids ...string,
) {
	t.Helper()
	for _, id := range ids {
		assertDeliveredTo(
			t,
			delivered,
			id,
		)
	}
}

func assertDeliveredTo(
	t *testing.T,
	delivered *sync.Map,
	id string,
) {
	t.Helper()
	_, ok := delivered.Load(id)
	if !ok {
		t.Errorf(
			"aggregate %s was not recovered",
			id,
		)
	}
}

type ackErrStore struct{ store.Memory }

// AckDelivery is the test double implementation for the code under test.
func (ackErrStore) AckDelivery(
	context.Context,
	string,
	int64,
) error {
	return errors.New("ack failed")
}

func TestProcessor_Resume_RedeliversParkedAggregatesInOrder(
	t *testing.T,
) {
	var healthy atomic.Bool
	parked := make(
		chan struct{},
		4,
	)
	f := newFixture(
		t,
		store.New(),
		parkingOpts(
			&healthy,
			parkedSignal(parked),
		)...,
	)
	ctx := context.Background()

	for i := range 2 {
		mustSendUpdate(
			t,
			f.p,
			"o1",
			float64(i+1),
		)
	}

	<-parked
	if pending, _ := f.s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 2 {
		t.Fatalf(
			"pending = %v, want both events",
			pending,
		)
	}

	healthy.Store(true)
	if err := f.p.Resume(
		ctx,
		1,
	); err != nil {
		t.Fatalf(
			"Resume: %v",
			err,
		)
	}

	f.p.WaitPublish()
	if pending, _ := f.s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 0 {
		t.Fatalf(
			"pending after Resume = %v",
			pending,
		)
	}

	if err := f.p.Resume(
		ctx,
		0,
	); err != nil {
		t.Fatalf(
			"second Resume: %v",
			err,
		)
	}
}

func TestProcessor_Resume_WithoutDispatcherIsNoOp(
	t *testing.T,
) {
	es := eventstore.New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	p := mustNew[order](
		t,
		es,
		nil,
	)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	if err := p.Resume(
		context.Background(),
		10,
	); err != nil {
		t.Fatalf(
			"Resume: %v",
			err,
		)
	}
}

type flakyPendingStore struct {
	store.Memory
	fail atomic.Bool
}

// PendingDeliveries is the test double implementation for the code under test.
func (s *flakyPendingStore) PendingDeliveries(
	ctx context.Context,
	limit int,
) ([]asynxmd.Delivery, error) {
	if s.fail.Load() {
		return nil,
			errors.New("log unreadable")
	}

	return s.Memory.PendingDeliveries(
		ctx,
		limit,
	)
}

func TestProcessor_Resume_ReportsAFailureReadingTheLogAndStaysParked(
	t *testing.T,
) {
	var healthy atomic.Bool
	parked := make(
		chan struct{},
		4,
	)
	s := &flakyPendingStore{Memory: store.New()}
	f := newFixture(
		t,
		s,
		parkingOpts(
			&healthy,
			parkedSignal(parked),
		)...,
	)
	ctx := context.Background()
	mustSendUpdate(
		t,
		f.p,
		"o1",
		1,
	)

	<-parked

	s.fail.Store(true)
	if err := f.p.Resume(
		ctx,
		10,
	); err == nil {
		t.Fatal("Resume succeeded although the delivery log is unreadable")
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	s.fail.Store(false)
	if err := f.p.Resume(
		cancelled,
		10,
	); err == nil {
		t.Fatal("Resume with a cancelled context should fail taking the aggregate lock")
	}
}

func TestProcessor_Paused_ListsParkedAggregatesAndSendWaitSaysSo(
	t *testing.T,
) {
	var healthy atomic.Bool
	parked := make(
		chan struct{},
		4,
	)
	f := newFixture(
		t,
		store.New(),
		parkingOpts(
			&healthy,
			parkedSignal(parked),
		)...,
	)
	ctx := context.Background()
	if got := f.p.Paused(); len(got) != 0 {
		t.Fatalf(
			"Paused = %v before anything failed",
			got,
		)
	}

	mustSendUpdate(
		t,
		f.p,
		"o1",
		1,
	)

	<-parked
	if got := f.p.Paused(); len(got) != 1 ||
		got[0] != "o1" {
		t.Fatalf(
			"Paused = %v, want [o1]",
			got,
		)
	}

	evt, err := f.p.SendWait(
		ctx,
		updateCmd(
			"o1",
			2,
		),
	)
	if !errors.Is(
		err,
		asynxmd.ErrDeliveryParked,
	) ||
		evt.Version != 2 {
		t.Fatalf(
			"SendWait = v%d, %v; want the committed v2 with ErrDeliveryParked",
			evt.Version,
			err,
		)
	}
}

func TestProcessor_Paused_WithoutDispatcherIsEmpty(
	t *testing.T,
) {
	es := eventstore.New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	p := mustNew[order](
		t,
		es,
		nil,
	)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	if got := p.Paused(); got != nil {
		t.Fatalf(
			"Paused = %v",
			got,
		)
	}
}

func TestProcessor_Resume_StopsWhenItsContextEnds(
	t *testing.T,
) {
	var healthy atomic.Bool
	parked := make(
		chan struct{},
		4,
	)
	f := newFixture(
		t,
		store.New(),
		parkingOpts(
			&healthy,
			parkedSignal(parked),
		)...,
	)
	for _, id := range []string{"o1", "o2"} {
		mustSendUpdate(
			t,
			f.p,
			id,
			1,
		)

		<-parked
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	err := f.p.Resume(
		cancelled,
		10,
	)
	if err == nil {
		t.Fatal("Resume ignored its cancelled context")
	}

	if strings.Contains(
		err.Error(),
		"\n",
	) {
		t.Fatalf(
			"Resume kept going after its context ended, reporting every aggregate: %v",
			err,
		)
	}

	if got := f.p.Paused(); len(got) != 2 {
		t.Fatalf(
			"Paused = %v, want both still parked",
			got,
		)
	}
}

func TestProcessor_Resume_ConcurrentCallsRedeliverOnce(
	t *testing.T,
) {
	var healthy atomic.Bool
	var mu sync.Mutex
	var delivered []int64
	parked := make(
		chan struct{},
		4,
	)
	release := make(chan struct{})
	f := newFixture(
		t,
		store.New(),
		models.WithDurableDelivery[order](isOrderUpdated),
		gatedVersionSubs(
			&healthy,
			release,
			&mu,
			&delivered,
		),
		models.WithDeliveryRetry[order](
			time.Millisecond,
			2*time.Millisecond,
		),
		models.WithDeliveryMaxAttempts[order](2),
		parkedSignal(parked),
	)
	ctx := context.Background()
	for i := range 3 {
		mustSendUpdate(
			t,
			f.p,
			"o1",
			float64(i+1),
		)
	}

	<-parked
	healthy.Store(true)

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go resumeAndDone(
			ctx,
			f.p,
			&wg,
		)
	}

	wg.Wait()
	close(release)
	f.p.WaitPublish()
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(
		delivered,
		[]int64{1, 2, 3},
	) {
		t.Fatalf(
			"delivered %v, want [1 2 3] exactly once",
			delivered,
		)
	}
}

type rowCountingStore struct {
	store.Memory
	rows atomic.Int64
}

// ReadFrom is the test double implementation for the code under test.
func (s *rowCountingStore) ReadFrom(
	ctx context.Context,
	streamID string,
	fromVersion int64,
) ([][]byte, error) {
	blobs, err := s.Memory.ReadFrom(
		ctx,
		streamID,
		fromVersion,
	)
	s.rows.Add(int64(len(blobs)))
	return blobs,
		err
}

func TestProcessor_Resume_ReplaysAnAggregatesStreamOnce(
	t *testing.T,
) {
	const events = 60
	var healthy atomic.Bool
	parked := make(
		chan struct{},
		4,
	)
	s := &rowCountingStore{Memory: store.New()}
	f := newFixture(
		t,
		s,
		parkingOpts(
			&healthy,
			parkedSignal(parked),
		)...,
	)
	ctx := context.Background()
	for i := range events {
		mustSendUpdate(
			t,
			f.p,
			"o1",
			float64(i+1),
		)
	}

	<-parked

	healthy.Store(true)
	s.rows.Store(0)
	if err := f.p.Resume(
		ctx,
		10,
	); err != nil {
		t.Fatalf(
			"Resume: %v",
			err,
		)
	}

	f.p.WaitPublish()

	if pending, _ := s.PendingDeliveries(
		ctx,
		1000,
	); len(pending) != 0 {
		t.Fatalf(
			"pending after Resume = %d entries",
			len(pending),
		)
	}

	if rows := s.rows.Load(); rows > 2*events {
		t.Fatalf(
			"resume read %d stream rows for %d pending entries, want a single pass",
			rows,
			events,
		)
	}
}

type logCountingStore struct {
	store.Memory
	rows  atomic.Int64
	reads atomic.Int64
}

// PendingDeliveries is the test double implementation for the code under test.
func (s *logCountingStore) PendingDeliveries(
	ctx context.Context,
	limit int,
) ([]asynxmd.Delivery, error) {
	pending, err := s.Memory.PendingDeliveries(
		ctx,
		limit,
	)
	s.reads.Add(1)
	s.rows.Add(int64(len(pending)))
	return pending,
		err
}

func TestProcessor_Resume_ReadsTheDeliveryLogOnceForAllAggregates(
	t *testing.T,
) {
	const aggregates = 600
	var healthy atomic.Bool
	parked := make(
		chan struct{},
		aggregates,
	)
	s := &logCountingStore{Memory: store.New()}
	f := newFixture(
		t,
		s,
		parkingOpts(
			&healthy,
			parkedSignal(parked),
		)...,
	)
	ctx := context.Background()
	for i := range aggregates {
		mustSendUpdate(
			t,
			f.p,
			fmt.Sprintf(
				"o%03d",
				i,
			),
			1,
		)
	}

	for range aggregates {
		awaitParked(
			t,
			f.p,
			parked,
			aggregates,
		)
	}

	healthy.Store(true)
	s.rows.Store(0)
	if err := f.p.Resume(
		ctx,
		10,
	); err != nil {
		t.Fatalf(
			"Resume: %v",
			err,
		)
	}

	f.p.WaitPublish()

	if len(f.p.Paused()) != 0 {
		t.Fatalf(
			"still parked: %d aggregates",
			len(f.p.Paused()),
		)
	}

	rows, reads := s.rows.Load(), s.reads.Load()
	if rows > 3*aggregates {
		t.Fatalf(
			"resume read %d delivery-log rows for %d pending entries, want a single pass over the log",
			rows,
			aggregates,
		)
	}

	if err := f.p.Resume(
		ctx,
		10,
	); err != nil {
		t.Fatalf(
			"second Resume: %v",
			err,
		)
	}

	if again := s.reads.Load(); again != reads {
		t.Fatalf(
			"a Resume with nothing parked read the delivery log %d more times",
			again-reads,
		)
	}
}

func TestProcessor_Resume_DoesNotDeadlockWithACommandSendingToAnotherAggregate(
	t *testing.T,
) {
	var healthy atomic.Bool
	parked := make(
		chan struct{},
		2,
	)
	var readA sync.Once
	redelivering := make(chan struct{})
	onRead := func(
		streamID string,
	) {
		signalOnStream(
			streamID,
			&readA,
			redelivering,
		)
	}
	s := &readHookStore{Memory: store.New(), onRead: onRead}
	f := newFixture(
		t,
		s,
		parkingOpts(
			&healthy,
			parkedSignal(parked),
		)...,
	)
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		mustSendUpdate(
			t,
			f.p,
			id,
			1,
		)

		<-parked
	}

	healthy.Store(true)

	validating := make(chan struct{})
	validate := func() error {
		close(validating)
		return sendToAAfter(
			ctx,
			f.p,
			redelivering,
		)
	}
	cmd := validatingUpdate{UpdateOrderCmd: updateCmd(
		"b",
		2,
	), validate: validate}
	sent := make(
		chan error,
		1,
	)
	go func() {
		_, err := f.p.Send(
			ctx,
			cmd,
		)
		sent <- err
	}()
	<-validating

	resumed := make(
		chan error,
		1,
	)
	go func() {
		resumed <- f.p.Resume(
			ctx,
			10,
		)
	}()
	requireFinishes(
		t,
		"command",
		sent,
	)
	requireFinishes(
		t,
		"Resume",
		resumed,
	)
}

func requireFinishes(
	t *testing.T,
	name string,
	done <-chan error,
) {
	t.Helper()
	select {
	case err := <-done:
		requireNoErrorNamed(
			t,
			name,
			err,
		)
	case <-time.After(10 * time.Second):
		t.Fatalf(
			"%s is deadlocked",
			name,
		)
	}
}

func requireNoErrorNamed(
	t *testing.T,
	name string,
	err error,
) {
	t.Helper()
	if err != nil {
		t.Fatalf(
			"%s: %v",
			name,
			err,
		)
	}
}

func signalOnStream(
	streamID string,
	once *sync.Once,
	redelivering chan struct{},
) {
	if streamID != "events:a" {
		return
	}

	once.Do(func() { close(redelivering) })
}

func sendToAAfter(
	ctx context.Context,
	p *Processor[order],
	redelivering <-chan struct{},
) error {
	select {
	case <-redelivering:
	case <-time.After(3 * time.Second):
	}

	sendCtx, cancel := context.WithTimeout(
		ctx,
		3*time.Second,
	)
	defer cancel()
	_, err := p.Send(
		sendCtx,
		updateCmd(
			"a",
			3,
		),
	)
	return err
}

type validatingUpdate struct {
	mocks.UpdateOrderCmd
	validate func() error
}

// Validate is the test double implementation for the code under test.
func (c validatingUpdate) Validate(
	*order,
) error {
	return c.validate()
}

type readHookStore struct {
	store.Memory
	onRead func(
		streamID string,
	)
}

// ReadFrom is the test double implementation for the code under test.
func (s *readHookStore) ReadFrom(
	ctx context.Context,
	streamID string,
	fromVersion int64,
) ([][]byte, error) {
	s.onRead(streamID)
	return s.Memory.ReadFrom(
		ctx,
		streamID,
		fromVersion,
	)
}

type staleLogStore struct {
	store.Memory
	afterRead func()
	ran       atomic.Bool
	sender    *Processor[order]
}

// PendingDeliveries is the test double implementation for the code under test.
func (s *staleLogStore) PendingDeliveries(
	ctx context.Context,
	limit int,
) ([]asynxmd.Delivery, error) {
	pending, err := s.Memory.PendingDeliveries(
		ctx,
		limit,
	)
	if s.ran.CompareAndSwap(
		false,
		true,
	) {
		s.afterRead()
	}

	return pending,
		err
}

func TestProcessor_Resume_DeliversAnEventCommittedAfterItReadTheLog(
	t *testing.T,
) {
	var healthy atomic.Bool
	parked := make(
		chan struct{},
		1,
	)
	var mu sync.Mutex
	var delivered []int64
	var s *staleLogStore
	afterRead := func() {
		sendDuringResume(
			t,
			s,
		)
	}
	s = &staleLogStore{Memory: store.New(), afterRead: afterRead}
	f := newFixture(
		t,
		s,
		parkingOpts(
			&healthy,
			parkedSignal(parked),
			gatedSubs(
				&healthy,
				&mu,
				&delivered,
			),
		)...,
	)
	s.sender = f.p
	ctx := context.Background()
	mustSendUpdate(
		t,
		f.p,
		"o1",
		1,
	)

	<-parked

	healthy.Store(true)
	if err := f.p.Resume(
		ctx,
		10,
	); err != nil {
		t.Fatalf(
			"Resume: %v",
			err,
		)
	}

	f.p.WaitPublish()
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(
		delivered,
		[]int64{1, 2},
	) {
		t.Fatalf(
			"delivered %v, want [1 2]",
			delivered,
		)
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 0 {
		t.Fatalf(
			"pending after Resume = %v",
			pending,
		)
	}
}

func TestProcessor_Resume_SkipsAnAggregateAnotherResumeLiftedMeanwhile(
	t *testing.T,
) {
	var healthy atomic.Bool
	parked := make(
		chan struct{},
		1,
	)
	var mu sync.Mutex
	var delivered []int64
	var s *staleLogStore
	afterRead := func() {
		resumeDuringResume(
			t,
			s,
		)
	}
	s = &staleLogStore{Memory: store.New(), afterRead: afterRead}
	f := newFixture(
		t,
		s,
		parkingOpts(
			&healthy,
			parkedSignal(parked),
			gatedSubs(
				&healthy,
				&mu,
				&delivered,
			),
		)...,
	)
	s.sender = f.p
	mustSendUpdate(
		t,
		f.p,
		"o1",
		1,
	)

	<-parked

	healthy.Store(true)
	if err := f.p.Resume(
		context.Background(),
		10,
	); err != nil {
		t.Fatalf(
			"Resume: %v",
			err,
		)
	}

	f.p.WaitPublish()
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(
		delivered,
		[]int64{1},
	) {
		t.Fatalf(
			"delivered %v, want [1] once",
			delivered,
		)
	}
}

func TestProcessor_Resume_LeavesAParkImposedAgainSinceItReadTheLog(
	t *testing.T,
) {
	var healthy atomic.Bool
	parked := make(
		chan struct{},
		2,
	)
	var mu sync.Mutex
	var delivered []int64
	var s *staleLogStore
	afterRead := func() {
		parkAgainDuringResume(
			t,
			s,
			&healthy,
			parked,
		)
	}
	s = &staleLogStore{Memory: store.New(), afterRead: afterRead}
	f := newFixture(
		t,
		s,
		parkingOpts(
			&healthy,
			parkedSignal(parked),
			gatedSubs(
				&healthy,
				&mu,
				&delivered,
			),
		)...,
	)
	s.sender = f.p
	ctx := context.Background()
	mustSendUpdate(
		t,
		f.p,
		"o1",
		1,
	)

	<-parked

	healthy.Store(true)
	if err := f.p.Resume(
		ctx,
		10,
	); err != nil {
		t.Fatalf(
			"Resume: %v",
			err,
		)
	}

	f.p.WaitPublish()
	if paused := f.p.Paused(); !slices.Equal(
		paused,
		[]string{"o1"},
	) {
		t.Fatalf(
			"Paused = %v, want o1 left parked for the next Resume",
			paused,
		)
	}

	if err := f.p.Resume(
		ctx,
		10,
	); err != nil {
		t.Fatalf(
			"second Resume: %v",
			err,
		)
	}

	f.p.WaitPublish()
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(
		delivered,
		[]int64{1, 2},
	) {
		t.Fatalf(
			"delivered %v, want [1 2], each once and in order",
			delivered,
		)
	}
}

func TestProcessor_Resume_StopsAtTheAggregateWhereItsContextEnds(
	t *testing.T,
) {
	var healthy atomic.Bool
	parked := make(
		chan struct{},
		3,
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &readHookStore{Memory: store.New(), onRead: func(
		string,
	) {
		cancel()
	}}
	f := newFixture(
		t,
		s,
		parkingOpts(
			&healthy,
			parkedSignal(parked),
		)...,
	)
	for _, id := range []string{"o1", "o2", "o3"} {
		mustSendUpdate(
			t,
			f.p,
			id,
			1,
		)

		<-parked
	}

	healthy.Store(true)
	f.p.Resume(
		ctx,
		10,
	)
	paused := f.p.Paused()
	if !slices.Contains(
		paused,
		"o2",
	) ||
		!slices.Contains(
			paused,
			"o3",
		) {
		t.Fatalf(
			"Paused = %v, want o2 and o3 left alone once the context ended during o1",
			paused,
		)
	}
}

type idemOrderUpdate struct {
	mocks.UpdateOrderCmd
	Cmd string
}

// CommandID is the test double implementation for the code under test.
func (c idemOrderUpdate) CommandID() string { return c.Cmd }

type failingReadsStore struct {
	store.Memory
	fail   atomic.Bool
	stream string
}

// ReadFrom is the test double implementation for the code under test.
func (s *failingReadsStore) ReadFrom(
	ctx context.Context,
	streamID string,
	fromVersion int64,
) ([][]byte, error) {
	isFailingStream := s.stream == "" || s.stream == streamID
	if s.fail.Load() &&
		isFailingStream {
		return nil,
			errors.New("stream unreadable")
	}

	return s.Memory.ReadFrom(
		ctx,
		streamID,
		fromVersion,
	)
}

func parkWithTwoCommands(
	t *testing.T,
	f fixture,
	parked <-chan struct{},
) {
	t.Helper()
	ctx := context.Background()
	if _, err := f.p.Send(
		ctx,
		idemOrderUpdate{updateCmd(
			"o1",
			1,
		), "c-1"},
	); err != nil {
		t.Fatal(err)
	}

	<-parked
	_, err := f.p.SendWait(
		ctx,
		idemOrderUpdate{updateCmd(
			"o1",
			2,
		), "c-2"},
	)
	if !errors.Is(
		err,
		asynxmd.ErrDeliveryParked,
	) {
		t.Fatalf(
			"c-2 behind the park = %v, want ErrDeliveryParked",
			err,
		)
	}
}

func parkedSignal(
	parked chan<- struct{},
) models.Opt[order] {
	handle := func(
		_ context.Context,
		_ asynxmd.Event[order],
		err error,
		_ int,
	) {
		signalIfParked(
			parked,
			err,
		)
	}
	return models.WithDeliveryErrorHandler(handle)
}

func signalIfParked(
	parked chan<- struct{},
	err error,
) {
	if !errors.Is(
		err,
		asynxmd.ErrDeliveryParked,
	) {
		return
	}

	select {
	case parked <- struct{}{}:
	default:
	}
}

func mustSendUpdate(
	t *testing.T,
	p *Processor[order],
	id string,
	total float64,
) {
	t.Helper()
	if _, err := p.Send(
		context.Background(),
		updateCmd(
			id,
			total,
		),
	); err != nil {
		t.Fatal(err)
	}
}

func retryOutcomes(
	t *testing.T,
	f fixture,
) (ranAndFailed, neverTaken error) {
	t.Helper()
	ctx := context.Background()
	_, ranAndFailed = f.p.SendWait(
		ctx,
		idemOrderUpdate{updateCmd(
			"o1",
			1,
		), "c-1"},
	)
	_, neverTaken = f.p.SendWait(
		ctx,
		idemOrderUpdate{updateCmd(
			"o1",
			2,
		), "c-2"},
	)
	return ranAndFailed,
		neverTaken
}

func TestProcessor_Resume_AfterALogReadFailureRetriesStillReportTheParkedEvent(
	t *testing.T,
) {
	var healthy atomic.Bool
	parked := make(
		chan struct{},
		1,
	)
	s := &flakyPendingStore{Memory: store.New()}
	f := newFixture(
		t,
		s,
		parkingOpts(
			&healthy,
			parkedSignal(parked),
		)...,
	)
	parkWithTwoCommands(
		t,
		f,
		parked,
	)

	s.fail.Store(true)
	if err := f.p.Resume(
		context.Background(),
		10,
	); err == nil {
		t.Fatal("Resume hid the unreadable log")
	}

	ranAndFailed, neverTaken := retryOutcomes(
		t,
		f,
	)
	if ranAndFailed != nil ||
		!errors.Is(
			neverTaken,
			asynxmd.ErrDeliveryParked,
		) {
		t.Fatalf(
			"retries = %v, %v; want nil for c-1 (its handler ran) and ErrDeliveryParked for c-2",
			ranAndFailed,
			neverTaken,
		)
	}
}

func TestProcessor_Resume_AfterAReplayFailureRetriesKeepTheirAnswers(
	t *testing.T,
) {
	var healthy atomic.Bool
	parked := make(
		chan struct{},
		1,
	)
	s := &failingReadsStore{Memory: store.New()}
	f := newFixture(
		t,
		s,
		parkingOpts(
			&healthy,
			parkedSignal(parked),
		)...,
	)
	parkWithTwoCommands(
		t,
		f,
		parked,
	)

	s.fail.Store(true)
	if err := f.p.Resume(
		context.Background(),
		10,
	); err == nil {
		t.Fatal("Resume hid the unreadable stream")
	}

	s.fail.Store(false)
	if paused := f.p.Paused(); len(paused) != 1 {
		t.Fatalf(
			"Paused = %v, want o1 parked again",
			paused,
		)
	}

	ranAndFailed, neverTaken := retryOutcomes(
		t,
		f,
	)
	if ranAndFailed != nil ||
		!errors.Is(
			neverTaken,
			asynxmd.ErrDeliveryParked,
		) {
		t.Fatalf(
			"retries = %v, %v; want nil for c-1 (its handler ran) and ErrDeliveryParked for c-2",
			ranAndFailed,
			neverTaken,
		)
	}
}

type ackSignalStore struct {
	store.Memory
	acked chan struct{}
}

// AckDelivery is the test double implementation for the code under test.
func (s *ackSignalStore) AckDelivery(
	ctx context.Context,
	aggregateID string,
	version int64,
) error {
	err := s.Memory.AckDelivery(
		ctx,
		aggregateID,
		version,
	)
	select {
	case s.acked <- struct{}{}:
	default:
	}

	return err
}

func TestProcessor_Resume_AnAggregateThatFailsDoesNotStopTheOthers(
	t *testing.T,
) {
	var healthy atomic.Bool
	parked := make(
		chan struct{},
		2,
	)
	s := &failingReadsStore{Memory: store.New(), stream: "events:o1"}
	f := newFixture(
		t,
		s,
		parkingOpts(
			&healthy,
			parkedSignal(parked),
		)...,
	)
	ctx := context.Background()
	for _, id := range []string{"o1", "o2"} {
		mustSendUpdate(
			t,
			f.p,
			id,
			1,
		)

		<-parked
	}

	healthy.Store(true)
	s.fail.Store(true)
	err := f.p.Resume(
		ctx,
		10,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"o1",
		) {
		t.Fatalf(
			"Resume = %v, want an error naming o1",
			err,
		)
	}

	f.p.WaitPublish()
	if paused := f.p.Paused(); !slices.Equal(
		paused,
		[]string{"o1"},
	) {
		t.Fatalf(
			"Paused = %v, want only o1",
			paused,
		)
	}

	pending, _ := s.PendingDeliveries(
		ctx,
		10,
	)
	if len(pending) != 1 ||
		pending[0].AggregateID != "o1" {
		t.Fatalf(
			"pending = %v, want only o1's entry: o2 was redelivered",
			pending,
		)
	}
}

func enqueueUpTo(
	ctx context.Context,
	tx asynxmd.Tx,
	aggregateID string,
	last int64,
) error {
	versions := make(
		[]int64,
		0,
		last,
	)
	for v := int64(1); v <= last; v++ {
		versions = append(
			versions,
			v,
		)
	}

	return enqueueVersions(
		ctx,
		tx,
		aggregateID,
		versions...,
	)
}

func TestProcessor_Inspect_FailedResumeKeepsTheCauseAndUndelivered(
	t *testing.T,
) {
	var healthy atomic.Bool
	parked := make(
		chan struct{},
		1,
	)
	s := &failingReadsStore{Memory: store.New()}
	f := newFixture(
		t,
		s,
		parkingOpts(
			&healthy,
			parkedSignal(parked),
		)...,
	)
	parkWithTwoCommands(
		t,
		f,
		parked,
	)
	before := f.p.Inspect("o1")
	if !before.Parked ||
		!errors.Is(
			before.Cause,
			asynxmd.ErrDeliveryParked,
		) {
		t.Fatalf(
			"Inspect before Resume = %+v, want parked by the give-up",
			before,
		)
	}

	s.fail.Store(true)
	if err := f.p.Resume(
		context.Background(),
		10,
	); err == nil {
		t.Fatal("Resume hid the unreadable stream")
	}

	s.fail.Store(false)

	got := f.p.Inspect("o1")
	if !got.Parked ||
		got.Generation == before.Generation {
		t.Fatalf(
			"Inspect = %+v, want parked again with a new generation (was %d)",
			got,
			before.Generation,
		)
	}

	if got.Cause == nil ||
		!strings.Contains(
			got.Cause.Error(),
			"stream unreadable",
		) {
		t.Fatalf(
			"Cause = %v, want the redelivery error",
			got.Cause,
		)
	}

	if errors.Is(
		got.Cause,
		asynxmd.ErrDeliveryParked,
	) {
		t.Fatalf(
			"Cause = %v, still the lifted park's give-up error",
			got.Cause,
		)
	}

	if !slices.Contains(
		got.Undelivered,
		2,
	) ||
		slices.Contains(
			got.Undelivered,
			1,
		) {
		t.Fatalf(
			"Undelivered = %v, want v2 (no handler took it) and not v1 (its handler ran)",
			got.Undelivered,
		)
	}
}

func TestProcessor_Inspect_FailedRecoverParksWithTheRedeliveryError(
	t *testing.T,
) {
	ctx := context.Background()
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	seedUnrebuildable(
		t,
		s,
		"bad",
	)
	p := mustNew(
		t,
		es,
		bus.New[order](),
		ignoringReporter(),
		okSubs(),
	)
	t.Cleanup(func() { p.Shutdown(ctx) })

	if err := p.Recover(
		ctx,
		10,
	); err != nil {
		t.Fatalf(
			"Recover: %v",
			err,
		)
	}

	got := p.Inspect("bad")
	wrongPark := !got.Parked || got.Cause == nil || errors.Is(
		got.Cause,
		asynxmd.ErrDeliveryParked,
	)
	if wrongPark {
		t.Fatalf(
			"Inspect = %+v, want parked with the redelivery error",
			got,
		)
	}
}

func TestProcessor_Inspect_WithoutDispatcherIsTheZeroStatus(
	t *testing.T,
) {
	p := mustNew[order](
		t,
		newES(),
		nil,
	)
	t.Cleanup(func() { p.Shutdown(context.Background()) })

	if got := p.Inspect("a"); !reflect.DeepEqual(
		got,
		asynxmd.DeliveryStatus{},
	) {
		t.Fatalf(
			"Inspect = %+v, want the zero status",
			got,
		)
	}
}

func gatedVersionSubs(
	healthy *atomic.Bool,
	release chan struct{},
	mu *sync.Mutex,
	delivered *[]int64,
) models.Opt[order] {
	handle := func(
		_ context.Context,
		e asynxmd.Event[order],
	) error {
		return deliverWhenHealthy(
			healthy,
			release,
			mu,
			delivered,
			e,
		)
	}
	return durableSubs(handle)
}

func deliverWhenHealthy(
	healthy *atomic.Bool,
	release chan struct{},
	mu *sync.Mutex,
	delivered *[]int64,
	e asynxmd.Event[order],
) error {
	if !healthy.Load() {
		return errors.New("down")
	}

	<-release
	mu.Lock()
	*delivered = append(
		*delivered,
		e.Version,
	)
	mu.Unlock()
	return nil
}

func ignoringReporter() models.Opt[order] {
	handle := func(
		context.Context,
		asynxmd.Event[order],
		error,
	) {
	}
	return models.WithPublishErrorHandler(handle)
}

func resumeAndDone(
	ctx context.Context,
	p *Processor[order],
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	p.Resume(
		ctx,
		10,
	)
}

func awaitParked(
	t *testing.T,
	p *Processor[order],
	parked <-chan struct{},
	aggregates int,
) {
	t.Helper()
	select {
	case <-parked:
	case <-time.After(10 * time.Second):
		t.Fatalf(
			"%d of %d aggregates parked",
			len(p.Paused()),
			aggregates,
		)
	}
}

func gatedSubs(
	healthy *atomic.Bool,
	mu *sync.Mutex,
	delivered *[]int64,
) models.Opt[order] {
	released := make(chan struct{})
	close(released)
	return gatedVersionSubs(
		healthy,
		released,
		mu,
		delivered,
	)
}

func sendDuringResume(
	t *testing.T,
	s *staleLogStore,
) {
	t.Helper()
	sendCtx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()
	if _, err := s.sender.Send(
		sendCtx,
		updateCmd(
			"o1",
			2,
		),
	); err != nil {
		t.Errorf(
			"a command during the resume: %v",
			err,
		)
	}
}

func resumeDuringResume(
	t *testing.T,
	s *staleLogStore,
) {
	t.Helper()
	if err := s.sender.Resume(
		context.Background(),
		10,
	); err != nil {
		t.Errorf(
			"the second Resume: %v",
			err,
		)
	}
}

func parkAgainDuringResume(
	t *testing.T,
	s *staleLogStore,
	healthy *atomic.Bool,
	parked <-chan struct{},
) {
	t.Helper()
	ctx := context.Background()
	if err := s.sender.Resume(
		ctx,
		10,
	); err != nil {
		t.Errorf(
			"the inner Resume: %v",
			err,
		)
	}

	s.sender.WaitPublish()
	healthy.Store(false)
	if _, err := s.sender.Send(
		ctx,
		updateCmd(
			"o1",
			2,
		),
	); err != nil {
		t.Errorf(
			"the command that parks again: %v",
			err,
		)
	}

	<-parked
	healthy.Store(true)
}
