package pool_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher"
	"github.com/char2cs/asynx/internal/processor/internal/exec"
	"github.com/char2cs/asynx/internal/processor/internal/pool"
	"github.com/char2cs/asynx/internal/processor/models"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

func TestShard_SingleCommandSuccess(
	t *testing.T,
) {
	s := store.New()
	b := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: b})
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		1,
		0,
		1,
	)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope := newEnvelope(
		executor,
		ctx,
		mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
	)

	shard.CommandChan() <- envelope

	result := <-envelope.ResultChan
	if result.Err != nil {
		t.Fatalf(
			"expected nil, got %v",
			result.Err,
		)
	}
}

func TestShard_ValidationErrorDecrementVersionSingleWorker(
	t *testing.T,
) {
	s := store.New()
	b := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: b})
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		1,
		0,
		1,
	)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope1 := newEnvelope(
		executor,
		ctx,
		mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
	)
	shard.CommandChan() <- envelope1
	<-envelope1.ResultChan

	envelope2 := newEnvelope(
		executor,
		ctx,
		mocks.CancelOrderCmd{ID: "order1"},
	)
	shard.CommandChan() <- envelope2
	<-envelope2.ResultChan

	envelope3 := newEnvelope(
		executor,
		ctx,
		mocks.CancelOrderCmd{ID: "order2"},
	)
	shard.CommandChan() <- envelope3
	result3 := <-envelope3.ResultChan
	if result3.Err != asynxmd.ErrValidation {
		t.Fatalf(
			"expected ErrValidation, got %v",
			result3.Err,
		)
	}

	envelope4 := newEnvelope(
		executor,
		ctx,
		mocks.CreateOrderCmd{ID: "order3", Total: 200.0},
	)
	shard.CommandChan() <- envelope4
	result4 := <-envelope4.ResultChan
	if result4.Err != nil {
		t.Fatalf(
			"create after validation error failed: %v",
			result4.Err,
		)
	}
}

func TestShard_ContextCancelledDuringExecution(
	t *testing.T,
) {
	s := store.New()
	b := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: b})
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		1,
		0,
		1,
	)
	defer p.Drain(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	shard := p.Shards()[0]

	envelope := newEnvelope(
		executor,
		ctx,
		mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
	)

	shard.CommandChan() <- envelope
	cancel()

	select {
	case <-envelope.ResultChan:
	case <-time.After(2 * time.Second):
		t.Fatalf("result not received within timeout")
	}
}

func TestShard_MultipleWorkersNoCorrection(
	t *testing.T,
) {
	s := store.New()
	b := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: b})
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		1,
		0,
		2,
	)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope1 := newEnvelope(
		executor,
		ctx,
		mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
	)
	shard.CommandChan() <- envelope1
	<-envelope1.ResultChan

	envelope2 := newEnvelope(
		executor,
		ctx,
		mocks.CancelOrderCmd{ID: "order2"},
	)
	shard.CommandChan() <- envelope2
	result2 := <-envelope2.ResultChan
	if result2.Err != asynxmd.ErrValidation {
		t.Fatalf(
			"expected ErrValidation, got %v",
			result2.Err,
		)
	}

	envelope3 := newEnvelope(
		executor,
		ctx,
		mocks.CreateOrderCmd{ID: "order3", Total: 100.0},
	)
	shard.CommandChan() <- envelope3
	result3 := <-envelope3.ResultChan
	if result3.Err != nil {
		t.Fatalf(
			"create after failed validation failed: %v",
			result3.Err,
		)
	}
}

func TestShard_ResultCarriesEvent(
	t *testing.T,
) {
	s := store.New()
	b := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: b})
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		1,
		0,
		1,
	)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope := newEnvelope(
		executor,
		ctx,
		mocks.CreateOrderCmd{ID: "order-evt", Total: 50.0},
	)
	shard.CommandChan() <- envelope

	result := <-envelope.ResultChan
	if result.Err != nil {
		t.Fatalf(
			"expected nil error, got %v",
			result.Err,
		)
	}

	if result.Event.AggregateID != "order-evt" {
		t.Errorf(
			"expected AggregateID order-evt, got %q",
			result.Event.AggregateID,
		)
	}

	if result.Event.EventName == "" {
		t.Error("expected non-empty EventName in result")
	}
}

func TestShard_ResultUsesCommandResult(
	t *testing.T,
) {
	s := store.New()
	b := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: b})
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		1,
		0,
		1,
	)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope := newEnvelope(
		executor,
		ctx,
		mocks.CreateOrderCmd{ID: "order-result", Total: 50.0},
	)
	shard.CommandChan() <- envelope

	result := <-envelope.ResultChan
	if result.Err != nil {
		t.Fatalf(
			"expected nil error, got %v",
			result.Err,
		)
	}
}

func TestShard_NilEnvelopeClosesJobQueue(
	t *testing.T,
) {
	p := pool.New[order](
		1,
		0,
		1,
	)

	shard := p.Shards()[0]

	shard.CommandChan() <- nil

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	if err := p.Drain(ctx); err != nil {
		t.Fatalf(
			"Drain after nil envelope failed: %v",
			err,
		)
	}
}

func TestShard_WaitingForHandlersDoesNotHoldTheWorker(
	t *testing.T,
) {
	s := store.New()
	b := bus.New[order]()
	blocked := &releaseWaiter{release: make(chan struct{})}
	if _, err := b.Subscribe(
		"OrderCreated",
		blocked.wait,
	); err != nil {
		t.Fatal(err)
	}

	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: b})
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		1,
		0,
		1,
	)
	defer p.Drain(context.Background())

	sender := &envelopeSender{
		executor: executor,
		shard:    p.Shards()[0],
	}

	first := sender.sendWaitingCreate("order1")
	select {
	case res := <-first.ResultChan:
		checkFirstResult(
			t,
			res,
		)
	case <-time.After(5 * time.Second):
		t.Fatal("worker is held by a blocked handler: first result never arrived")
	}

	second := sender.sendWaitingCreate("order2")
	select {
	case res := <-second.ResultChan:
		failIf(
			t,
			res.Err,
		)
	case <-time.After(5 * time.Second):
		t.Fatal("second aggregate stalled behind the first one's handler")
	}

	close(blocked.release)
}

type releaseWaiter struct {
	release chan struct{}
}

func (w *releaseWaiter) wait(
	context.Context,
	asynxmd.Event[order],
) {
	<-w.release
}

type envelopeSender struct {
	executor models.Executor[order]
	shard    *pool.Shard[order]
}

func (s *envelopeSender) send(
	cmd asynxmd.Command[order],
) *models.CommandEnvelope[order] {
	env := newEnvelope(
		s.executor,
		context.Background(),
		cmd,
	)
	s.shard.CommandChan() <- env
	return env
}

func (s *envelopeSender) sendWaitingCreate(
	id string,
) *models.CommandEnvelope[order] {
	env := newWaitingEnvelope(
		s.executor,
		context.Background(),
		mocks.CreateOrderCmd{ID: id, Total: 1},
	)
	s.shard.CommandChan() <- env
	return env
}

func checkFirstResult(
	t *testing.T,
	res models.CommandResult[order],
) {
	t.Helper()
	if res.Err != nil ||
		res.Wait == nil {
		t.Fatalf(
			"result = %+v, want committed with a Wait",
			res,
		)
	}

	short, cancel := context.WithTimeout(
		context.Background(),
		20*time.Millisecond,
	)
	defer cancel()
	if err := res.Wait(short); !errors.Is(
		err,
		asynxmd.ErrContextCancelled,
	) {
		t.Fatalf(
			"Wait returned %v while the handler is still blocked",
			err,
		)
	}
}

func failIf(
	t *testing.T,
	err error,
) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func requireNil(
	t *testing.T,
	what string,
	err error,
) {
	t.Helper()
	if err != nil {
		t.Fatalf(
			"%s: %v",
			what,
			err,
		)
	}
}

func TestPool_Drain_AnswersEnvelopesLeftInTheQueue(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := exec.New[order](
		es,
		nil,
	)
	p := pool.New[order](
		1,
		8,
		1,
	)

	gate, release := gateReleasedOnCleanup(t)
	entered := make(
		chan struct{},
		1,
	)
	sender := &envelopeSender{
		executor: executor,
		shard:    p.Shards()[0],
	}
	var envs []*models.CommandEnvelope[order]
	envs = append(
		envs,
		sender.send(gatedUpdate{UpdateOrderCmd: mocks.UpdateOrderCmd{ID: "a", NewState: order{ID: "a"}}, entered: entered, gate: gate}),
	)
	<-entered
	for i := range 5 {
		id := string(rune('b' + i))
		envs = append(
			envs,
			sender.send(mocks.UpdateOrderCmd{ID: id, NewState: order{ID: id}}),
		)
	}

	drained := make(
		chan error,
		1,
	)
	go func() { drained <- p.Drain(context.Background()) }()
	<-p.Shards()[0].Stopping()
	release()

	for i, env := range envs {
		awaitEnvelope(
			t,
			i,
			env,
		)
	}

	if err := <-drained; err != nil {
		t.Fatalf(
			"Drain: %v",
			err,
		)
	}
}

func awaitEnvelope(
	t *testing.T,
	i int,
	env *models.CommandEnvelope[order],
) {
	t.Helper()
	select {
	case <-env.ResultChan:
	case <-time.After(5 * time.Second):
		t.Fatalf(
			"envelope %d never got a result",
			i,
		)
	}
}

type gatedUpdate struct {
	mocks.UpdateOrderCmd
	entered chan<- struct{}
	gate    <-chan struct{}
}

// Validate announces on entered, then blocks until the gate is released, so a
// test can hold an update mid-flight on its worker.
func (c gatedUpdate) Validate(
	*order,
) error {
	c.entered <- struct{}{}
	<-c.gate
	return nil
}

func TestShard_ABusyAggregateLeavesTheOtherWorkersFree(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := exec.New[order](
		es,
		nil,
	)
	p := pool.New[order](
		1,
		8,
		2,
	)
	t.Cleanup(func() { p.Drain(context.Background()) })

	gate, release := gateReleasedOnCleanup(t)
	entered := make(
		chan struct{},
		1,
	)
	sender := &envelopeSender{
		executor: executor,
		shard:    p.Shards()[0],
	}

	hot := sender.send(gatedUpdate{UpdateOrderCmd: mocks.UpdateOrderCmd{ID: "hot", NewState: order{ID: "hot"}}, entered: entered, gate: gate})
	<-entered
	queued := sender.send(mocks.UpdateOrderCmd{ID: "hot", NewState: order{ID: "hot", Total: 2}})
	cold := sender.send(mocks.CreateOrderCmd{ID: "cold", Total: 1})

	select {
	case res := <-cold.ResultChan:
		requireNil(
			t,
			"cold aggregate",
			res.Err,
		)
	case <-time.After(5 * time.Second):
		t.Fatal("an unrelated aggregate stalled behind the busy one")
	}

	release()
	for name, env := range map[string]*models.CommandEnvelope[order]{"running": hot, "queued": queued} {
		res := resultWithin(
			t,
			env,
			name+" hot command",
		)
		requireNil(
			t,
			name+" hot command",
			res.Err,
		)
	}
}

func gateReleasedOnCleanup(
	t *testing.T,
) (gate chan struct{}, release func()) {
	t.Helper()
	gate = make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	return gate,
		release
}

func resultWithin(
	t *testing.T,
	env *models.CommandEnvelope[order],
	what string,
) models.CommandResult[order] {
	t.Helper()
	select {
	case res := <-env.ResultChan:
		return res
	case <-time.After(5 * time.Second):
		t.Fatalf(
			"%s never finished",
			what,
		)
		panic("unreachable")
	}
}

func TestShard_AMailboxKeepsTheOrderOfItsCommands(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := exec.New[order](
		es,
		nil,
	)
	p := pool.New[order](
		1,
		8,
		4,
	)
	t.Cleanup(func() { p.Drain(context.Background()) })

	gate, release := gateReleasedOnCleanup(t)
	entered := make(
		chan struct{},
		1,
	)
	sender := &envelopeSender{
		executor: executor,
		shard:    p.Shards()[0],
	}

	envs := []*models.CommandEnvelope[order]{
		sender.send(gatedCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "a", Total: 1}, entered: entered, gate: gate}),
	}
	<-entered
	for i := 2; i <= 4; i++ {
		envs = append(
			envs,
			sender.send(mocks.UpdateOrderCmd{ID: "a", NewState: order{ID: "a", Total: float64(i)}}),
		)
		waitForBacklog(
			t,
			p.Shards()[0],
			len(envs)-1,
		)
	}

	release()

	for i, env := range envs {
		res := resultWithin(
			t,
			env,
			fmt.Sprintf(
				"command %d",
				i,
			),
		)
		requireVersion(
			t,
			i,
			res,
		)
	}
}

func requireVersion(
	t *testing.T,
	i int,
	res models.CommandResult[order],
) {
	t.Helper()
	if res.Err != nil ||
		res.Event.Version != int64(i+1) {
		t.Fatalf(
			"command %d = v%d, %v; want v%d",
			i,
			res.Event.Version,
			res.Err,
			i+1,
		)
	}
}

func TestShard_AMailboxRefusesWhatExceedsTheQueueDepth(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := exec.New[order](
		es,
		nil,
	)
	p := pool.New[order](
		1,
		2,
		2,
	)
	t.Cleanup(func() { p.Drain(context.Background()) })

	gate, release := gateReleasedOnCleanup(t)
	entered := make(
		chan struct{},
		1,
	)
	sender := &envelopeSender{
		executor: executor,
		shard:    p.Shards()[0],
	}

	running := sender.send(gatedUpdate{UpdateOrderCmd: mocks.UpdateOrderCmd{ID: "hot", NewState: order{ID: "hot"}}, entered: entered, gate: gate})
	<-entered
	var waiting []*models.CommandEnvelope[order]
	for i := range 2 {
		waiting = append(
			waiting,
			sender.send(mocks.UpdateOrderCmd{ID: "hot", NewState: order{ID: "hot", Total: float64(i)}}),
		)
		waitForBacklog(
			t,
			p.Shards()[0],
			i+1,
		)
	}

	overflow := sender.send(mocks.UpdateOrderCmd{ID: "hot", NewState: order{ID: "hot", Total: 9}})
	select {
	case res := <-overflow.ResultChan:
		requireQueueFull(
			t,
			res.Err,
		)
	case <-time.After(5 * time.Second):
		t.Fatal("overflow was neither queued nor refused")
	}

	release()
	for _, env := range append(
		waiting,
		running,
	) {
		res := <-env.ResultChan
		requireNil(
			t,
			"queued command",
			res.Err,
		)
	}
}

func requireQueueFull(
	t *testing.T,
	err error,
) {
	t.Helper()
	if !errors.Is(
		err,
		asynxmd.ErrQueueFull,
	) {
		t.Fatalf(
			"overflow = %v, want ErrQueueFull",
			err,
		)
	}
}

func TestShard_SkipsACommandWhoseContextEnded(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := exec.New[order](
		es,
		nil,
	)
	p := pool.New[order](
		1,
		8,
		1,
	)
	t.Cleanup(func() { p.Drain(context.Background()) })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	env := newEnvelope(
		executor,
		ctx,
		mocks.CreateOrderCmd{ID: "gone", Total: 1},
	)
	p.Shards()[0].CommandChan() <- env

	if res := <-env.ResultChan; !errors.Is(
		res.Err,
		asynxmd.ErrContextCancelled,
	) {
		t.Fatalf(
			"result = %v, want ErrContextCancelled",
			res.Err,
		)
	}

	exists, err := es.Exists(
		context.Background(),
		"gone",
	)
	failIf(
		t,
		err,
	)
	if exists {
		t.Fatal("a cancelled command was executed")
	}
}

type gatedCreate struct {
	mocks.CreateOrderCmd
	entered chan<- struct{}
	gate    <-chan struct{}
}

// Validate announces on entered, blocks until the gate is released and then
// validates as the plain create command, so a test can hold a create
// mid-flight on its worker.
func (c gatedCreate) Validate(
	current *order,
) error {
	c.entered <- struct{}{}
	<-c.gate
	return c.CreateOrderCmd.Validate(current)
}

func waitForBacklog[T any](
	t *testing.T,
	s *pool.Shard[T],
	n int,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for s.Backlog() < n {
		requireBefore(
			t,
			deadline,
			n,
		)
		runtime.Gosched()
	}
}

func requireBefore(
	t *testing.T,
	deadline time.Time,
	n int,
) {
	t.Helper()
	if time.Now().After(deadline) {
		t.Fatalf(
			"backlog stayed below %d",
			n,
		)
	}
}

func BenchmarkShard_SingleWorker(
	b *testing.B,
) {
	s := store.New()
	bu := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: bu})
	defer d.Close(context.Background())
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		1,
		0,
		1,
	)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		envelope := newEnvelope(
			executor,
			ctx,
			mocks.CreateOrderCmd{ID: "order", Total: 100.0},
		)

		shard.CommandChan() <- envelope
		<-envelope.ResultChan
	}
}

func BenchmarkShard_MultipleWorkers(
	b *testing.B,
) {
	for _, workers := range []int{1, 4, 8} {
		b.Run(
			"workers="+string(rune('0'+workers)),
			workersBench(workers),
		)
	}
}

func workersBench(
	workers int,
) func(*testing.B) {
	return func(
		b *testing.B,
	) {
		benchWorkers(
			b,
			workers,
		)
	}
}

func benchWorkers(
	b *testing.B,
	workers int,
) {
	s := store.New()
	bu := bus.New[order]()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: bu})
	defer d.Close(context.Background())
	executor := exec.New(
		es,
		d,
	)

	p := pool.New[order](
		1,
		0,
		workers,
	)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		envelope := newEnvelope(
			executor,
			ctx,
			mocks.CreateOrderCmd{ID: "order", Total: 100.0},
		)

		shard.CommandChan() <- envelope
		<-envelope.ResultChan
	}
}

func newEnvelope(
	executor models.Executor[order],
	ctx context.Context,
	cmd asynxmd.Command[order],
) *models.CommandEnvelope[order] {
	envelope := models.NewCommandEnvelope(cmd)
	go takeTurn(
		executor,
		ctx,
		envelope,
	)
	return envelope
}

func newWaitingEnvelope(
	executor models.Executor[order],
	ctx context.Context,
	cmd asynxmd.Command[order],
) *models.CommandEnvelope[order] {
	envelope := models.NewCommandEnvelope(cmd)
	envelope.WaitHandlers = true
	go takeTurn(
		executor,
		ctx,
		envelope,
	)
	return envelope
}

func takeTurn(
	executor models.Executor[order],
	ctx context.Context,
	envelope *models.CommandEnvelope[order],
) {
	<-envelope.Turn
	envelope.Execute(
		ctx,
		executor,
	)
}

type recordingExecutor struct {
	mu       sync.Mutex
	order    []float64
	inFlight atomic.Int32
	overlap  atomic.Bool
	started  atomic.Bool
	entered  chan struct{}
	gate     chan struct{}
}

// ExecuteAsync notes whether two calls ever ran at once (overlap), holds the
// first call on entered and gate so a test can queue commands behind it, and
// appends each update's total to order, so a test can assert that commands of
// one aggregate ran serially and in the order they were sent.
func (r *recordingExecutor) ExecuteAsync(
	_ context.Context,
	cmd asynxmd.Command[order],
) (asynxmd.Event[order], func(context.Context) error, error) {
	if r.inFlight.Add(1) > 1 {
		r.overlap.Store(true)
	}

	defer r.inFlight.Add(-1)
	if r.started.CompareAndSwap(
		false,
		true,
	) {
		r.entered <- struct{}{}
		<-r.gate
	}

	update, _ := cmd.(mocks.UpdateOrderCmd)
	runtime.Gosched()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(
		r.order,
		update.NewState.Total,
	)
	return asynxmd.Event[order]{},
		nil,
		nil
}

func TestShard_RunsEachCommandOnceInOrderPerAggregate(
	t *testing.T,
) {
	executor := &recordingExecutor{
		entered: make(
			chan struct{},
			1,
		),
		gate: make(chan struct{}),
	}
	p := pool.New[order](
		1,
		64,
		4,
	)
	t.Cleanup(func() { p.Drain(context.Background()) })

	const total = 40
	envs := make(
		[]*models.CommandEnvelope[order],
		0,
		total,
	)
	for i := range total {
		env := newEnvelope(
			executor,
			context.Background(),
			mocks.UpdateOrderCmd{
				ID:       "agg",
				NewState: order{ID: "agg", Total: float64(i)},
			},
		)
		p.Shards()[0].CommandChan() <- env
		envs = append(
			envs,
			env,
		)
		awaitQueued(
			t,
			executor,
			p.Shards()[0],
			i,
		)
	}

	close(executor.gate)
	for _, env := range envs {
		requireNil(
			t,
			"command",
			resultWithin(
				t,
				env,
				"command",
			).Err,
		)
	}

	requireRanInOrder(
		t,
		executor,
		total,
	)
}

func requireRanInOrder(
	t *testing.T,
	executor *recordingExecutor,
	total int,
) {
	t.Helper()
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if len(executor.order) != total {
		t.Fatalf(
			"ran %d commands, want %d, each once",
			len(executor.order),
			total,
		)
	}

	for i, got := range executor.order {
		requireNumber(
			t,
			i,
			got,
		)
	}

	if executor.overlap.Load() {
		t.Fatal("two commands of one aggregate ran at the same time")
	}
}

func TestShard_SkipsACommandItsCallerAbandonedBeforeTheTurn(
	t *testing.T,
) {
	es := eventstore.New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := exec.New[order](
		es,
		nil,
	)
	p := pool.New[order](
		1,
		8,
		1,
	)
	t.Cleanup(func() { p.Drain(context.Background()) })
	gate, release := gateReleasedOnCleanup(t)
	entered := make(
		chan struct{},
		1,
	)

	holder := newEnvelope(
		executor,
		context.Background(),
		gatedCreate{
			CreateOrderCmd: mocks.CreateOrderCmd{ID: "holder", Total: 1},
			entered:        entered,
			gate:           gate,
		},
	)
	p.Shards()[0].CommandChan() <- holder
	<-entered

	abandoned := models.NewCommandEnvelope[order](mocks.CreateOrderCmd{ID: "abandoned", Total: 1})
	p.Shards()[0].CommandChan() <- abandoned
	abandoned.Abandon()
	last := newEnvelope(
		executor,
		context.Background(),
		mocks.CreateOrderCmd{ID: "last", Total: 1},
	)
	p.Shards()[0].CommandChan() <- last
	release()

	requireNil(
		t,
		"last command",
		resultWithin(
			t,
			last,
			"last command",
		).Err,
	)
	requireMissing(
		t,
		es,
		"abandoned",
	)
}

func requireMissing(
	t *testing.T,
	es eventstore.EventStore[order],
	id string,
) {
	t.Helper()
	exists, err := es.Exists(
		context.Background(),
		id,
	)
	failIf(
		t,
		err,
	)
	if exists {
		t.Fatalf(
			"%s was executed although its caller gave up",
			id,
		)
	}
}

func requireNumber(
	t *testing.T,
	i int,
	got float64,
) {
	t.Helper()
	if got != float64(i) {
		t.Fatalf(
			"command %d ran as number %v: order lost",
			i,
			got,
		)
	}
}

func awaitQueued(
	t *testing.T,
	executor *recordingExecutor,
	shard *pool.Shard[order],
	i int,
) {
	t.Helper()
	if i == 0 {
		<-executor.entered
		return
	}

	waitForBacklog(
		t,
		shard,
		i,
	)
}
