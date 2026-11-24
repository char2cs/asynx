package exec_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/v2/internal/bus/internal/exec"
	"github.com/char2cs/asynx/v2/models/event"
)

func captureStderr(
	t *testing.T,
	f func(),
) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	orig := os.Stderr
	os.Stderr = w

	f()

	w.Close()
	os.Stderr = orig

	var buf bytes.Buffer
	if _, err := io.Copy(
		&buf,
		r,
	); err != nil {
		t.Fatal(err)
	}

	r.Close()
	return buf.String()
}

func receiveNow[V any](
	ch <-chan V,
) (v V, ok bool) {
	select {
	case v = <-ch:
		return v,
			true
	default:
		return v,
			false
	}
}

func assertPanicValue(
	t *testing.T,
	got <-chan any,
	want string,
) {
	t.Helper()

	v, ok := receiveNow(got)
	if !ok {
		t.Error("panic callback was not called")
		return
	}

	if v != want {
		t.Errorf(
			"panic callback got %v, want %q",
			v,
			want,
		)
	}
}

func ev(
	name string,
) event.Event[string] {
	return event.Event[string]{EventName: name}
}

func newJob(
	handler,
	fallback func(context.Context, event.Event[string]),
	timeout time.Duration,
) *exec.HandlerJob[string] {
	return &exec.HandlerJob[string]{
		Handler:         handler,
		FallbackHandler: fallback,
		Timeout:         timeout,
		Event:           ev("test"),
	}
}

type eventHandler = func(
	context.Context,
	event.Event[string],
)

type panicObserver = func(
	context.Context,
	event.Event[string],
	any,
)

type timeoutObserver = func(
	context.Context,
	event.Event[string],
	time.Duration,
)

func panicsWith(
	value any,
) eventHandler {
	return func(
		_ context.Context,
		_ event.Event[string],
	) {
		panic(value)
	}
}

func signals(
	done chan<- struct{},
) eventHandler {
	return func(
		_ context.Context,
		_ event.Event[string],
	) {
		done <- struct{}{}
	}
}

func sleepsFor(
	d time.Duration,
) eventHandler {
	return func(
		_ context.Context,
		_ event.Event[string],
	) {
		time.Sleep(d)
	}
}

func blocksUntilReleased(
	release <-chan struct{},
) eventHandler {
	return func(
		_ context.Context,
		_ event.Event[string],
	) {
		<-release
	}
}

func announcesStartThenBlocks(
	started chan<- struct{},
	release <-chan struct{},
) eventHandler {
	return func(
		_ context.Context,
		_ event.Event[string],
	) {
		close(started)
		<-release
	}
}

func blocksUntilReleasedThenPanics(
	release <-chan struct{},
	value any,
) eventHandler {
	return func(
		_ context.Context,
		_ event.Event[string],
	) {
		<-release
		panic(value)
	}
}

func countsCallsThenPanics(
	calls *atomic.Int32,
	value any,
) eventHandler {
	return func(
		_ context.Context,
		_ event.Event[string],
	) {
		calls.Add(1)
		panic(value)
	}
}

func sendsContext(
	received chan<- context.Context,
) eventHandler {
	return func(
		got context.Context,
		_ event.Event[string],
	) {
		received <- got
	}
}

func sendsContextThenPanics(
	received chan<- context.Context,
	value any,
) eventHandler {
	return func(
		got context.Context,
		_ event.Event[string],
	) {
		received <- got
		panic(value)
	}
}

func sendsContextOnPanic(
	received chan<- context.Context,
) panicObserver {
	return func(
		got context.Context,
		_ event.Event[string],
		_ any,
	) {
		received <- got
	}
}

func forwardsPanicValue(
	got chan<- any,
) panicObserver {
	return func(
		_ context.Context,
		_ event.Event[string],
		value any,
	) {
		got <- value
	}
}

func countsPanics(
	panics *atomic.Int32,
) panicObserver {
	return func(
		_ context.Context,
		_ event.Event[string],
		_ any,
	) {
		panics.Add(1)
	}
}

func panicsWhenObservingPanic(
	value any,
) panicObserver {
	return func(
		context.Context,
		event.Event[string],
		any,
	) {
		panic(value)
	}
}

func signalsTimeout(
	fired chan<- struct{},
) timeoutObserver {
	return func(
		_ context.Context,
		_ event.Event[string],
		_ time.Duration,
	) {
		fired <- struct{}{}
	}
}

func countsTimeouts(
	timeouts *atomic.Int32,
) timeoutObserver {
	return func(
		_ context.Context,
		_ event.Event[string],
		_ time.Duration,
	) {
		timeouts.Add(1)
	}
}

func sendsContextOnTimeout(
	observed chan<- context.Context,
) timeoutObserver {
	return func(
		got context.Context,
		_ event.Event[string],
		_ time.Duration,
	) {
		observed <- got
	}
}

func releasesThenPanicsWhenObservingTimeout(
	release chan struct{},
	value any,
) timeoutObserver {
	return func(
		context.Context,
		event.Event[string],
		time.Duration,
	) {
		close(release)
		panic(value)
	}
}

func forwardsRecoveredValue(
	got chan<- any,
) func(any) {
	return func(
		value any,
	) {
		got <- value
	}
}

func panicsWhenRecoveredValueArrives(
	value any,
) func(any) {
	return func(
		any,
	) {
		panic(value)
	}
}

func signalsOnTimeout(
	fired chan<- struct{},
) func() {
	return func() {
		fired <- struct{}{}
	}
}

func reportingPanic(
	value any,
	job *exec.HandlerJob[string],
) func() {
	return func() {
		exec.OnHandlerPanic[string](
			value,
			job,
		)
	}
}

func reportingTimeout(
	evt event.Event[string],
	timeout time.Duration,
) func() {
	return func() {
		exec.OnHandlerTimeout[string](
			evt,
			timeout,
		)
	}
}

type jobRun struct {
	job     *exec.HandlerJob[string]
	started <-chan struct{}
}

func (r jobRun) untilDone() {
	var wg sync.WaitGroup
	wg.Add(1)
	go exec.Handler(
		context.Background(),
		&wg,
		r.job,
	)
	if r.started != nil {
		<-r.started
	}

	wg.Wait()
}

func TestHandler_Normal(
	t *testing.T,
) {
	called := make(
		chan struct{},
		1,
	)
	job := newJob(
		signals(called),
		nil,
		0,
	)

	jobRun{job: job}.untilDone()

	select {
	case <-called:
	default:
		t.Error("handler was not called")
	}
}

func TestHandler_PanicNoFallbackNoTimeout(
	t *testing.T,
) {
	job := newJob(
		panicsWith("boom"),
		nil,
		0,
	)

	output := captureStderr(
		t,
		jobRun{job: job}.untilDone,
	)
	if !strings.Contains(
		output,
		"panicked",
	) {
		t.Errorf(
			"expected panic message in stderr, got: %q",
			output,
		)
	}

	if !strings.Contains(
		output,
		"boom",
	) {
		t.Errorf(
			"expected panic value in stderr, got: %q",
			output,
		)
	}
}

func TestHandler_PanicWithFallback(
	t *testing.T,
) {
	fallbackCalled := make(
		chan struct{},
		1,
	)
	job := newJob(
		panicsWith("oops"),
		signals(fallbackCalled),
		0,
	)

	captureStderr(
		t,
		jobRun{job: job}.untilDone,
	)

	select {
	case <-fallbackCalled:
	default:
		t.Error("fallback handler was not called")
	}
}

func TestHandler_PanicWithTimeout(
	t *testing.T,
) {
	job := newJob(
		panicsWith("hidden"),
		nil,
		5*time.Second,
	)

	output := captureStderr(
		t,
		jobRun{job: job}.untilDone,
	)
	if !strings.Contains(
		output,
		"panicked",
	) {
		t.Errorf(
			"OnHandlerPanic should have been called, but stderr had: %q",
			output,
		)
	}
}

func TestHandler_DoublePanic(
	t *testing.T,
) {
	job := &exec.HandlerJob[string]{
		Handler: panicsWith("primary"),

		FallbackHandler: panicsWith("fallback"),

		Timeout: 0,
		Event:   ev("test"),
	}

	captureStderr(
		t,
		jobRun{job: job}.untilDone,
	)
}

func TestHandler_DoublePanic_BothLogged(
	t *testing.T,
) {
	var panics atomic.Int32
	job := &exec.HandlerJob[string]{
		Handler: panicsWith("primary"),

		FallbackHandler: panicsWith("fallback"),

		Timeout: 0,
		Event:   ev("test"),
		OnPanic: countsPanics(&panics),
	}

	jobRun{job: job}.untilDone()

	if count := panics.Load(); count != 2 {
		t.Errorf(
			"expected OnPanic called 2 times, got %d",
			count,
		)
	}
}

func TestWithTimeout_ZeroTimeout(
	t *testing.T,
) {
	called := make(
		chan struct{},
		1,
	)
	exec.WithTimeout(
		context.Background(),
		signals(called),
		ev("x"),
		0,
		nil,
		nil,
	)

	if _, ok := receiveNow(called); !ok {
		t.Error("handler not called with zero timeout")
	}
}

func TestWithTimeout_FinishesBeforeTimeout(
	t *testing.T,
) {
	called := make(
		chan struct{},
		1,
	)
	exec.WithTimeout(
		context.Background(),
		signals(called),
		ev("x"),
		5*time.Second,
		nil,
		nil,
	)

	select {
	case <-called:
	default:
		t.Error("handler not called")
	}
}

func TestWithTimeout_HandlerTimesOut(
	t *testing.T,
) {
	started := make(chan struct{})
	timedOut := make(
		chan struct{},
		1,
	)
	block := make(chan struct{})
	defer close(block)
	exec.WithTimeout(
		context.Background(),
		announcesStartThenBlocks(
			started,
			block,
		),
		ev("slow-event"),
		20*time.Millisecond,
		nil,
		signalsOnTimeout(timedOut),
	)
	<-started

	select {
	case <-timedOut:
	default:
		t.Error("expected onTimeout to be called")
	}
}

func TestWithTimeout_PanicInsideGoroutine(
	t *testing.T,
) {
	exec.WithTimeout(
		context.Background(),
		panicsWith("inner"),
		ev("x"),
		5*time.Second,
		nil,
		nil,
	)
}

func TestWithTimeout_PanicCallsOnPanic(
	t *testing.T,
) {
	got := make(
		chan any,
		1,
	)
	exec.WithTimeout(
		context.Background(),
		panicsWith("boom"),
		ev("x"),
		5*time.Second,
		forwardsRecoveredValue(got),
		nil,
	)

	assertPanicValue(
		t,
		got,
		"boom",
	)
}

func TestHandler_CustomPanicCallback(
	t *testing.T,
) {
	got := make(
		chan any,
		1,
	)
	job := &exec.HandlerJob[string]{
		Handler: panicsWith("custom-panic"),

		Timeout: 0,
		Event:   ev("test"),
		OnPanic: forwardsPanicValue(got),
	}

	output := captureStderr(
		t,
		jobRun{job: job}.untilDone,
	)
	if strings.Contains(
		output,
		"panicked",
	) {
		t.Errorf(
			"default stderr callback fired; got: %q",
			output,
		)
	}

	assertPanicValue(
		t,
		got,
		"custom-panic",
	)
}

func TestHandler_DefaultTimeoutCallback(
	t *testing.T,
) {
	started := make(chan struct{})
	block := make(chan struct{})
	defer close(block)
	job := &exec.HandlerJob[string]{
		Handler: announcesStartThenBlocks(
			started,
			block,
		),

		Timeout: 20 * time.Millisecond,
		Event:   ev("SlowEvent"),
	}

	output := captureStderr(
		t,
		jobRun{
			job:     job,
			started: started,
		}.untilDone,
	)
	if !strings.Contains(
		output,
		"timed out",
	) {
		t.Errorf(
			"expected default timeout message in stderr, got: %q",
			output,
		)
	}
}

func TestHandler_CustomTimeoutCallback(
	t *testing.T,
) {
	fired := make(
		chan struct{},
		1,
	)
	started := make(chan struct{})
	block := make(chan struct{})
	defer close(block)
	job := &exec.HandlerJob[string]{
		Handler: announcesStartThenBlocks(
			started,
			block,
		),

		Timeout:   20 * time.Millisecond,
		Event:     ev("test"),
		OnTimeout: signalsTimeout(fired),
	}

	output := captureStderr(
		t,
		jobRun{
			job:     job,
			started: started,
		}.untilDone,
	)
	if strings.Contains(
		output,
		"timed out",
	) {
		t.Errorf(
			"default stderr callback fired; got: %q",
			output,
		)
	}

	select {
	case <-fired:
	default:
		t.Error("OnTimeout not called")
	}
}

func TestOnHandlerPanic_Smoke(
	t *testing.T,
) {
	job := newJob(
		nil,
		nil,
		0,
	)
	job.Event = ev("MyEvent")
	output := captureStderr(
		t,
		reportingPanic(
			"something bad",
			job,
		),
	)
	if !strings.Contains(
		output,
		"MyEvent",
	) {
		t.Errorf(
			"expected event name in output, got: %q",
			output,
		)
	}

	if !strings.Contains(
		output,
		"something bad",
	) {
		t.Errorf(
			"expected panic value in output, got: %q",
			output,
		)
	}

	if !strings.Contains(
		output,
		"panicked",
	) {
		t.Errorf(
			"expected 'panicked' in output, got: %q",
			output,
		)
	}
}

func TestOnHandlerTimeout_Smoke(
	t *testing.T,
) {
	output := captureStderr(
		t,
		reportingTimeout(
			ev("SlowEvent"),
			42*time.Millisecond,
		),
	)
	if !strings.Contains(
		output,
		"SlowEvent",
	) {
		t.Errorf(
			"expected event name in output, got: %q",
			output,
		)
	}

	if !strings.Contains(
		output,
		"timed out",
	) {
		t.Errorf(
			"expected 'timed out' in output, got: %q",
			output,
		)
	}

	if !strings.Contains(
		output,
		"42ms",
	) {
		t.Errorf(
			"expected timeout duration in output, got: %q",
			output,
		)
	}
}

func TestWithTimeout_PanicCallbackPanics(
	t *testing.T,
) {
	exec.WithTimeout(
		context.Background(),
		panicsWith("handler panics"),
		ev("x"),
		5*time.Second,
		panicsWhenRecoveredValueArrives("callback panics"),
		nil,
	)
}

func TestHandler_PanicInAnObserverLeavesATraceOnStderr(
	t *testing.T,
) {
	job := newJob(
		panicsWith("handler"),
		nil,
		0,
	)
	job.OnPanic = panicsWhenObservingPanic("observer failure")

	out := captureStderr(
		t,
		jobRun{job: job}.untilDone,
	)
	if !strings.Contains(
		out,
		"observer failure",
	) {
		t.Errorf(
			"contained observer panic left no trace on stderr: %q",
			out,
		)
	}
}

func TestHandler_PanicInTheObserversIsSwallowed(
	t *testing.T,
) {
	var wg sync.WaitGroup

	wg.Add(1)
	panicking := &exec.HandlerJob[string]{
		Handler: panicsWith("handler"),

		Event:   ev("test"),
		OnPanic: panicsWhenObservingPanic("observer"),
	}
	go exec.Handler(
		context.Background(),
		&wg,
		panicking,
	)

	wg.Add(1)
	release := make(chan struct{})
	slow := &exec.HandlerJob[string]{
		Handler: blocksUntilReleased(release),

		Timeout: time.Millisecond,
		Event:   ev("test"),
		OnTimeout: releasesThenPanicsWhenObservingTimeout(
			release,
			"observer",
		),
	}
	go exec.Handler(
		context.Background(),
		&wg,
		slow,
	)

	wg.Wait()
}

func TestHandler_PanicInTheObserverOfTheFallbackIsSwallowed(
	t *testing.T,
) {
	var wg sync.WaitGroup
	wg.Add(1)
	job := &exec.HandlerJob[string]{
		Handler: panicsWith("handler"),

		FallbackHandler: panicsWith("fallback"),

		Event:   ev("test"),
		OnPanic: panicsWhenObservingPanic("observer"),
	}
	go exec.Handler(
		context.Background(),
		&wg,
		job,
	)
	wg.Wait()
}

type ctxKey struct{}

func TestHandler_PassesTheContextToHandlerFallbackAndObservers(
	t *testing.T,
) {
	ctx := context.WithValue(
		context.Background(),
		ctxKey{},
		"caller",
	)
	received := make(
		chan context.Context,
		4,
	)
	job := newJob(
		sendsContextThenPanics(
			received,
			"boom",
		),
		sendsContext(received),
		0,
	)
	job.OnPanic = sendsContextOnPanic(received)

	var wg sync.WaitGroup
	wg.Add(1)
	exec.Handler(
		ctx,
		&wg,
		job,
	)

	close(received)
	var contexts []context.Context
	for got := range received {
		contexts = append(
			contexts,
			got,
		)
	}

	if len(contexts) != 3 {
		t.Errorf(
			"want handler, observer and fallback to run once each, got %d calls",
			len(contexts),
		)
	}

	if !slices.Equal(
		contexts,
		slices.Repeat(
			[]context.Context{ctx},
			len(contexts),
		),
	) {
		t.Error("a callback got a different context")
	}
}

func TestHandler_PassesTheContextToTheTimeoutObserver(
	t *testing.T,
) {
	ctx := context.WithValue(
		context.Background(),
		ctxKey{},
		"caller",
	)
	got := make(
		chan context.Context,
		1,
	)
	job := newJob(
		sleepsFor(50*time.Millisecond),
		nil,
		time.Millisecond,
	)
	job.OnTimeout = sendsContextOnTimeout(got)

	var wg sync.WaitGroup
	wg.Add(1)
	exec.Handler(
		ctx,
		&wg,
		job,
	)

	observed, ok := receiveNow(got)
	if !ok ||
		observed != ctx {
		t.Error("timeout observer did not receive the caller's context")
	}
}

func noopJob() *exec.HandlerJob[string] {
	return &exec.HandlerJob[string]{
		Handler: func(
			_ context.Context,
			_ event.Event[string],
		) {
		},
		Event: event.Event[string]{EventName: "test"},
	}
}

func BenchmarkHandler_NoTimeout(
	b *testing.B,
) {
	job := noopJob()
	b.ReportAllocs()
	for range b.N {
		var wg sync.WaitGroup
		wg.Add(1)
		go exec.Handler(
			context.Background(),
			&wg,
			job,
		)
		wg.Wait()
	}
}

func BenchmarkHandler_WithTimeout(
	b *testing.B,
) {
	job := noopJob()
	job.Timeout = 5 * time.Second
	b.ReportAllocs()
	for range b.N {
		var wg sync.WaitGroup
		wg.Add(1)
		go exec.Handler(
			context.Background(),
			&wg,
			job,
		)
		wg.Wait()
	}
}

var stderrMu sync.Mutex

func BenchmarkHandler_PanicRecovery(
	b *testing.B,
) {
	job := &exec.HandlerJob[string]{
		Handler: panicsWith("bench"),

		Event: event.Event[string]{EventName: "test"},
	}

	devNull, err := os.OpenFile(
		os.DevNull,
		os.O_WRONLY,
		0,
	)
	if err != nil {
		b.Fatal(err)
	}

	defer func() { _ = devNull.Close() }()

	stderrMu.Lock()
	orig := os.Stderr
	os.Stderr = devNull
	stderrMu.Unlock()

	defer func() {
		stderrMu.Lock()
		os.Stderr = orig
		stderrMu.Unlock()
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var wg sync.WaitGroup
		wg.Add(1)
		go exec.Handler(
			context.Background(),
			&wg,
			job,
		)
		wg.Wait()
	}
}

func BenchmarkWithTimeout_ZeroTimeout(
	b *testing.B,
) {
	handler := func(
		_ context.Context,
		_ event.Event[string],
	) {
	}
	evt := event.Event[string]{EventName: "test"}
	b.ReportAllocs()
	for range b.N {
		exec.WithTimeout(
			context.Background(),
			handler,
			evt,
			0,
			nil,
			nil,
		)
	}
}

func BenchmarkWithTimeout_WithTimer(
	b *testing.B,
) {
	handler := func(
		_ context.Context,
		_ event.Event[string],
	) {
	}
	evt := event.Event[string]{EventName: "test"}
	timeout := 5 * time.Second
	b.ReportAllocs()
	for range b.N {
		exec.WithTimeout(
			context.Background(),
			handler,
			evt,
			timeout,
			nil,
			nil,
		)
	}
}

type fallbackProbe struct {
	calls  atomic.Int32
	panics atomic.Int32
	events chan string
	values chan any
}

func newFallbackProbe() *fallbackProbe {
	return &fallbackProbe{
		events: make(
			chan string,
			4,
		),
		values: make(
			chan any,
			4,
		),
	}
}

func (p *fallbackProbe) fallback(
	ctx context.Context,
	evt event.Event[string],
) {
	p.calls.Add(1)
	p.events <- evt.EventName
	p.values <- ctx.Value(ctxKey{})
}

func (p *fallbackProbe) onPanic(
	_ context.Context,
	_ event.Event[string],
	_ any,
) {
	p.panics.Add(1)
}

func startHandler(
	ctx context.Context,
	job *exec.HandlerJob[string],
) <-chan struct{} {
	var wg sync.WaitGroup
	wg.Add(1)
	go exec.Handler(
		ctx,
		&wg,
		job,
	)

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()

	return finished
}

func awaitClosed(
	t *testing.T,
	ch <-chan struct{},
	what string,
) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf(
			"timed out waiting for %s",
			what,
		)
	}
}

func assertStillOpen(
	t *testing.T,
	ch <-chan struct{},
	what string,
) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf(
			"%s finished too early",
			what,
		)
	case <-time.After(50 * time.Millisecond):
	}
}

func assertNoGoroutineLeak(
	t *testing.T,
	baseline int,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > baseline &&
		time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if got := runtime.NumGoroutine(); got > baseline {
		t.Fatalf(
			"goroutines leaked: %d > baseline %d",
			got,
			baseline,
		)
	}
}

func assertFallbackSaw(
	t *testing.T,
	probe *fallbackProbe,
	want string,
) {
	t.Helper()
	if got := probe.calls.Load(); got != 1 {
		t.Fatalf(
			"fallback ran %d times, want 1",
			got,
		)
	}

	if got := <-probe.events; got != "test" {
		t.Errorf(
			"fallback got event %q, want test",
			got,
		)
	}

	if got := <-probe.values; got != want {
		t.Errorf(
			"fallback got context value %v, want %q",
			got,
			want,
		)
	}
}

func TestHandler_PanicUnderTimeoutRunsTheFallback(
	t *testing.T,
) {
	baseline := runtime.NumGoroutine()
	ctx := context.WithValue(
		context.Background(),
		ctxKey{},
		"caller",
	)
	probe := newFallbackProbe()
	job := newJob(
		panicsWith("boom"),
		probe.fallback,
		5*time.Second,
	)
	job.OnPanic = probe.onPanic

	awaitClosed(
		t,
		startHandler(
			ctx,
			job,
		),
		"the job",
	)

	assertFallbackSaw(
		t,
		probe,
		"caller",
	)
	if got := probe.panics.Load(); got != 1 {
		t.Errorf(
			"panic reported %d times, want 1",
			got,
		)
	}

	assertNoGoroutineLeak(
		t,
		baseline,
	)
}

func TestHandler_TimeoutRunsTheFallbackAndAbandonsThePrimary(
	t *testing.T,
) {
	baseline := runtime.NumGoroutine()
	ctx := context.WithValue(
		context.Background(),
		ctxKey{},
		"caller",
	)
	probe := newFallbackProbe()
	release := make(chan struct{})
	var timeouts atomic.Int32
	job := newJob(
		blocksUntilReleased(release),
		probe.fallback,
		10*time.Millisecond,
	)
	job.OnTimeout = countsTimeouts(&timeouts)

	awaitClosed(
		t,
		startHandler(
			ctx,
			job,
		),
		"the job",
	)

	assertFallbackSaw(
		t,
		probe,
		"caller",
	)
	if got := timeouts.Load(); got != 1 {
		t.Errorf(
			"timeout reported %d times, want 1",
			got,
		)
	}

	close(release)
	assertNoGoroutineLeak(
		t,
		baseline,
	)
}

func TestHandler_TimeoutWithWaitAfterTimeoutRunsFallbackOnlyAfterPrimaryReturns(
	t *testing.T,
) {
	baseline := runtime.NumGoroutine()
	probe := newFallbackProbe()
	release := make(chan struct{})
	job := newJob(
		blocksUntilReleased(release),
		probe.fallback,
		10*time.Millisecond,
	)
	job.WaitAfterTimeout = true

	finished := startHandler(
		context.Background(),
		job,
	)

	time.Sleep(100 * time.Millisecond)
	if got := probe.calls.Load(); got != 0 {
		t.Errorf(
			"fallback ran %d times while the primary still runs, want 0",
			got,
		)
	}

	assertStillOpen(
		t,
		finished,
		"the job while the primary still runs",
	)

	close(release)
	<-probe.events
	awaitClosed(
		t,
		finished,
		"the job after the primary returned",
	)

	if got := probe.calls.Load(); got != 1 {
		t.Errorf(
			"fallback ran %d times, want 1",
			got,
		)
	}

	assertNoGoroutineLeak(
		t,
		baseline,
	)
}

func TestHandler_PrimaryPanicAfterTimeoutIsReportedWithoutASecondFallback(
	t *testing.T,
) {
	baseline := runtime.NumGoroutine()
	probe := newFallbackProbe()
	release := make(chan struct{})
	job := newJob(
		blocksUntilReleasedThenPanics(
			release,
			"late",
		),
		probe.fallback,
		10*time.Millisecond,
	)
	job.WaitAfterTimeout = true
	job.OnPanic = probe.onPanic

	finished := startHandler(
		context.Background(),
		job,
	)

	close(release)
	<-probe.events
	awaitClosed(
		t,
		finished,
		"the job",
	)

	if got := probe.calls.Load(); got != 1 {
		t.Errorf(
			"fallback ran %d times, want 1",
			got,
		)
	}

	if got := probe.panics.Load(); got != 1 {
		t.Errorf(
			"late panic reported %d times, want 1",
			got,
		)
	}

	assertNoGoroutineLeak(
		t,
		baseline,
	)
}

func TestHandler_FallbackPanicUnderTimeoutIsReportedOnceWithoutASecondFallback(
	t *testing.T,
) {
	baseline := runtime.NumGoroutine()
	var fallbackCalls atomic.Int32
	probe := newFallbackProbe()
	job := newJob(
		panicsWith("primary"),
		countsCallsThenPanics(
			&fallbackCalls,
			"fallback",
		),
		5*time.Second,
	)
	job.OnPanic = probe.onPanic

	awaitClosed(
		t,
		startHandler(
			context.Background(),
			job,
		),
		"the job",
	)

	if got := fallbackCalls.Load(); got != 1 {
		t.Errorf(
			"fallback ran %d times, want 1",
			got,
		)
	}

	if got := probe.panics.Load(); got != 2 {
		t.Errorf(
			"panics reported %d times, want 2 (primary and fallback)",
			got,
		)
	}

	assertNoGoroutineLeak(
		t,
		baseline,
	)
}

func TestHandler_NoFallbackWhenThePrimarySucceeds(
	t *testing.T,
) {
	baseline := runtime.NumGoroutine()
	probe := newFallbackProbe()
	handle := func(
		_ context.Context,
		_ event.Event[string],
	) {
	}
	for _, timeout := range []time.Duration{
		0,
		5 * time.Second,
	} {
		job := newJob(
			handle,
			probe.fallback,
			timeout,
		)
		job.OnPanic = probe.onPanic

		awaitClosed(
			t,
			startHandler(
				context.Background(),
				job,
			),
			"the job",
		)
	}

	if got := probe.calls.Load(); got != 0 {
		t.Errorf(
			"fallback ran %d times, want 0",
			got,
		)
	}

	assertNoGoroutineLeak(
		t,
		baseline,
	)
}
