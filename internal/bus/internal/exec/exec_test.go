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

	"github.com/char2cs/asynx/internal/bus/internal/exec"
	"github.com/char2cs/asynx/models"
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
) models.Event[string] {
	return models.Event[string]{EventName: name}
}

func newJob(
	handler,
	fallback func(context.Context, models.Event[string]),
	timeout time.Duration,
) *exec.HandlerJob[string] {
	return &exec.HandlerJob[string]{
		Handler:         handler,
		FallbackHandler: fallback,
		Timeout:         timeout,
		Event:           ev("test"),
	}
}

func TestHandler_Normal(
	t *testing.T,
) {
	called := make(
		chan struct{},
		1,
	)
	newJobFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		called <- struct{}{}
	}
	job := newJob(
		newJobFn,
		nil,
		0,
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go exec.Handler(
		context.Background(),
		&wg,
		job,
	)
	wg.Wait()

	select {
	case <-called:
	default:
		t.Error("handler was not called")
	}
}

func TestHandler_PanicNoFallbackNoTimeout(
	t *testing.T,
) {
	newJobFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("boom")
	}
	job := newJob(
		newJobFn,
		nil,
		0,
	)

	var wg sync.WaitGroup
	wg.Add(1)

	var output string
	captureStderrFn := func() {
		go exec.Handler(
			context.Background(),
			&wg,
			job,
		)
		wg.Wait()
	}
	output = captureStderr(
		t,
		captureStderrFn,
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
	newJobFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("oops")
	}
	newJobFn2 := func(
		_ context.Context,
		_ models.Event[string],
	) {
		fallbackCalled <- struct{}{}
	}
	job := newJob(
		newJobFn,
		newJobFn2,
		0,
	)

	var wg sync.WaitGroup
	wg.Add(1)
	captureStderrFn := func() {
		go exec.Handler(
			context.Background(),
			&wg,
			job,
		)
		wg.Wait()
	}
	captureStderr(
		t,
		captureStderrFn,
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
	newJobFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("hidden")
	}
	job := newJob(
		newJobFn,
		nil,
		5*time.Second,
	)

	var wg sync.WaitGroup
	wg.Add(1)

	var output string
	captureStderrFn := func() {
		go exec.Handler(
			context.Background(),
			&wg,
			job,
		)
		wg.Wait()
	}
	output = captureStderr(
		t,
		captureStderrFn,
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
	handler := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("primary")
	}
	fallbackHandler := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("fallback")
	}
	job := &exec.HandlerJob[string]{
		Handler: handler,

		FallbackHandler: fallbackHandler,

		Timeout: 0,
		Event:   ev("test"),
	}

	var wg sync.WaitGroup
	wg.Add(1)
	captureStderrFn := func() {
		go exec.Handler(
			context.Background(),
			&wg,
			job,
		)
		wg.Wait()
	}
	captureStderr(
		t,
		captureStderrFn,
	)
}

func TestHandler_DoublePanic_BothLogged(
	t *testing.T,
) {
	var panicCount int32
	handler := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("primary")
	}
	fallbackHandler := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("fallback")
	}
	onPanic := func(
		_ context.Context,
		_ models.Event[string],
		_ any,
	) {
		atomic.AddInt32(
			&panicCount,
			1,
		)
	}
	job := &exec.HandlerJob[string]{
		Handler: handler,

		FallbackHandler: fallbackHandler,

		Timeout: 0,
		Event:   ev("test"),
		OnPanic: onPanic,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go exec.Handler(
		context.Background(),
		&wg,
		job,
	)
	wg.Wait()

	if count := atomic.LoadInt32(&panicCount); count != 2 {
		t.Errorf(
			"expected OnPanic called 2 times, got %d",
			count,
		)
	}
}

func TestWithTimeout_ZeroTimeout(
	t *testing.T,
) {
	called := false
	withTimeoutFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		called = true
	}
	exec.WithTimeout(
		context.Background(),
		withTimeoutFn,
		ev("x"),
		0,
		nil,
		nil,
	)
	if !called {
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
	withTimeoutFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		called <- struct{}{}
	}
	exec.WithTimeout(
		context.Background(),
		withTimeoutFn,
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
	withTimeoutFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		close(started)
		<-block
	}
	withTimeoutFn2 := func() { timedOut <- struct{}{} }
	exec.WithTimeout(
		context.Background(),
		withTimeoutFn,
		ev("slow-event"),
		20*time.Millisecond,
		nil,
		withTimeoutFn2,
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
	withTimeoutFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("inner")
	}
	exec.WithTimeout(
		context.Background(),
		withTimeoutFn,
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
	withTimeoutFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("boom")
	}
	withTimeoutFn2 := func(
		v any,
	) {
		got <- v
	}
	exec.WithTimeout(
		context.Background(),
		withTimeoutFn,
		ev("x"),
		5*time.Second,
		withTimeoutFn2,
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
	handler := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("custom-panic")
	}
	onPanic := func(
		_ context.Context,
		_ models.Event[string],
		v any,
	) {
		got <- v
	}
	job := &exec.HandlerJob[string]{
		Handler: handler,

		Timeout: 0,
		Event:   ev("test"),
		OnPanic: onPanic,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	captureStderrFn := func() {
		go exec.Handler(
			context.Background(),
			&wg,
			job,
		)
		wg.Wait()
	}
	output := captureStderr(
		t,
		captureStderrFn,
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
	handler := func(
		_ context.Context,
		_ models.Event[string],
	) {
		close(started)
		<-block
	}
	job := &exec.HandlerJob[string]{
		Handler: handler,

		Timeout: 20 * time.Millisecond,
		Event:   ev("SlowEvent"),
	}

	var wg sync.WaitGroup
	wg.Add(1)
	captureStderrFn := func() {
		go exec.Handler(
			context.Background(),
			&wg,
			job,
		)
		<-started
		wg.Wait()
	}
	output := captureStderr(
		t,
		captureStderrFn,
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
	handler := func(
		_ context.Context,
		_ models.Event[string],
	) {
		close(started)
		<-block
	}
	onTimeout := func(
		_ context.Context,
		_ models.Event[string],
		_ time.Duration,
	) {
		fired <- struct{}{}
	}
	job := &exec.HandlerJob[string]{
		Handler: handler,

		Timeout:   20 * time.Millisecond,
		Event:     ev("test"),
		OnTimeout: onTimeout,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	captureStderrFn := func() {
		go exec.Handler(
			context.Background(),
			&wg,
			job,
		)
		<-started
		wg.Wait()
	}
	output := captureStderr(
		t,
		captureStderrFn,
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
	captureStderrFn := func() {
		exec.OnHandlerPanic[string](
			"something bad",
			job,
		)
	}
	output := captureStderr(
		t,
		captureStderrFn,
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
	timeout := 42 * time.Millisecond
	captureStderrFn := func() {
		exec.OnHandlerTimeout[string](
			ev("SlowEvent"),
			timeout,
		)
	}
	output := captureStderr(
		t,
		captureStderrFn,
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
	done := make(chan struct{})
	withTimeoutFn := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("handler panics")
	}
	withTimeoutFn2 := func(
		_ any,
	) {
		panic("callback panics")
	}
	exec.WithTimeout(
		context.Background(),
		withTimeoutFn,
		ev("x"),
		5*time.Second,
		withTimeoutFn2,
		nil,
	)
	close(done)
}

func TestHandler_PanicInAnObserverLeavesATraceOnStderr(
	t *testing.T,
) {
	handler := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("handler")
	}
	onPanic := func(
		context.Context,
		models.Event[string],
		any,
	) {
		panic("observer failure")
	}
	job := newJob(
		handler,
		nil,
		0,
	)
	job.OnPanic = onPanic

	runJob := func() {
		var wg sync.WaitGroup
		wg.Add(1)
		exec.Handler(
			context.Background(),
			&wg,
			job,
		)
	}
	out := captureStderr(
		t,
		runJob,
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
	handler := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("handler")
	}
	onPanic := func(
		context.Context,
		models.Event[string],
		any,
	) {
		panic("observer")
	}
	panicking := &exec.HandlerJob[string]{
		Handler: handler,

		Event:   ev("test"),
		OnPanic: onPanic,
	}
	go exec.Handler(
		context.Background(),
		&wg,
		panicking,
	)

	wg.Add(1)
	release := make(chan struct{})
	handler2 := func(
		_ context.Context,
		_ models.Event[string],
	) {
		<-release
	}
	onTimeout := func(
		context.Context,
		models.Event[string],
		time.Duration,
	) {
		close(release)
		panic("observer")
	}
	slow := &exec.HandlerJob[string]{
		Handler: handler2,

		Timeout:   time.Millisecond,
		Event:     ev("test"),
		OnTimeout: onTimeout,
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
	handler := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("handler")
	}
	fallbackHandler := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("fallback")
	}
	onPanic := func(
		context.Context,
		models.Event[string],
		any,
	) {
		panic("observer")
	}
	job := &exec.HandlerJob[string]{
		Handler: handler,

		FallbackHandler: fallbackHandler,

		Event:   ev("test"),
		OnPanic: onPanic,
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
	handle := func(
		got context.Context,
		_ models.Event[string],
	) {
		received <- got
		panic("boom")
	}
	fallback := func(
		got context.Context,
		_ models.Event[string],
	) {
		received <- got
	}
	onPanic := func(
		got context.Context,
		_ models.Event[string],
		_ any,
	) {
		received <- got
	}
	job := newJob(
		handle,
		fallback,
		0,
	)
	job.OnPanic = onPanic

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

	isForeign := func(
		got context.Context,
	) bool {
		return got != ctx
	}
	if slices.ContainsFunc(
		contexts,
		isForeign,
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
	slow := func(
		_ context.Context,
		_ models.Event[string],
	) {
		time.Sleep(50 * time.Millisecond)
	}
	onTimeout := func(
		observed context.Context,
		_ models.Event[string],
		_ time.Duration,
	) {
		got <- observed
	}
	job := newJob(
		slow,
		nil,
		time.Millisecond,
	)
	job.OnTimeout = onTimeout

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
			_ models.Event[string],
		) {
		},
		Event: models.Event[string]{EventName: "test"},
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
	handler := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("bench")
	}
	job := &exec.HandlerJob[string]{
		Handler: handler,

		Event: models.Event[string]{EventName: "test"},
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
		_ models.Event[string],
	) {
	}
	event := models.Event[string]{EventName: "test"}
	b.ReportAllocs()
	for range b.N {
		exec.WithTimeout(
			context.Background(),
			handler,
			event,
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
		_ models.Event[string],
	) {
	}
	event := models.Event[string]{EventName: "test"}
	timeout := 5 * time.Second
	b.ReportAllocs()
	for range b.N {
		exec.WithTimeout(
			context.Background(),
			handler,
			event,
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
	event models.Event[string],
) {
	p.calls.Add(1)
	p.events <- event.EventName
	p.values <- ctx.Value(ctxKey{})
}

func (p *fallbackProbe) onPanic(
	_ context.Context,
	_ models.Event[string],
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
	handle := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("boom")
	}
	job := newJob(
		handle,
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
	handle := func(
		_ context.Context,
		_ models.Event[string],
	) {
		<-release
	}
	var timeouts atomic.Int32
	job := newJob(
		handle,
		probe.fallback,
		10*time.Millisecond,
	)
	job.OnTimeout = func(
		_ context.Context,
		_ models.Event[string],
		_ time.Duration,
	) {
		timeouts.Add(1)
	}

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
	handle := func(
		_ context.Context,
		_ models.Event[string],
	) {
		<-release
	}
	job := newJob(
		handle,
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
	handle := func(
		_ context.Context,
		_ models.Event[string],
	) {
		<-release
		panic("late")
	}
	job := newJob(
		handle,
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
	handle := func(
		_ context.Context,
		_ models.Event[string],
	) {
		panic("primary")
	}
	fallback := func(
		_ context.Context,
		_ models.Event[string],
	) {
		fallbackCalls.Add(1)
		panic("fallback")
	}
	probe := newFallbackProbe()
	job := newJob(
		handle,
		fallback,
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
		_ models.Event[string],
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
