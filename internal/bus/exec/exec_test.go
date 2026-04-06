package exec_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/internal/bus/exec"
)

// captureStderr redirects os.Stderr to a pipe, runs f, then returns what was written.
// f must be fully synchronous (or the caller must synchronize externally before calling).
func captureStderr(t *testing.T, f func()) string {
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
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatal(err)
	}
	r.Close()
	return buf.String()
}

func ev(name string) models.Event[string] {
	return models.Event[string]{EventName: name}
}

func newJob(handler, fallback func(context.Context, models.Event[string]), timeout time.Duration) *exec.HandlerJob[string] {
	return &exec.HandlerJob[string]{
		Handler:         handler,
		FallbackHandler: fallback,
		Timeout:         timeout,
		Event:           ev("test"),
		Ctx:             context.Background(),
	}
}

// --- ExecuteHandler ---

// 1. Normal handler — called, wg.Done fires.
func TestExecuteHandler_Normal(t *testing.T) {
	called := make(chan struct{}, 1)
	job := newJob(func(_ context.Context, _ models.Event[string]) { called <- struct{}{} }, nil, 0)

	var wg sync.WaitGroup
	wg.Add(1)
	go exec.ExecuteHandler(&wg, job)
	wg.Wait()

	select {
	case <-called:
	default:
		t.Error("handler was not called")
	}
}

// 2. Panic, no fallback, timeout=0 — OnHandlerPanic fires, wg.Done fires.
func TestExecuteHandler_PanicNoFallbackNoTimeout(t *testing.T) {
	job := newJob(func(_ context.Context, _ models.Event[string]) { panic("boom") }, nil, 0)

	var wg sync.WaitGroup
	wg.Add(1)

	var output string
	output = captureStderr(t, func() {
		go exec.ExecuteHandler(&wg, job)
		wg.Wait()
	})

	if !strings.Contains(output, "panicked") {
		t.Errorf("expected panic message in stderr, got: %q", output)
	}
	if !strings.Contains(output, "boom") {
		t.Errorf("expected panic value in stderr, got: %q", output)
	}
}

// 3. Panic, fallback set, timeout=0 — fallback called.
func TestExecuteHandler_PanicWithFallback(t *testing.T) {
	fallbackCalled := make(chan struct{}, 1)
	job := newJob(
		func(_ context.Context, _ models.Event[string]) { panic("oops") },
		func(_ context.Context, _ models.Event[string]) { fallbackCalled <- struct{}{} },
		0,
	)

	var wg sync.WaitGroup
	wg.Add(1)
	captureStderr(t, func() {
		go exec.ExecuteHandler(&wg, job)
		wg.Wait()
	})

	select {
	case <-fallbackCalled:
	default:
		t.Error("fallback handler was not called")
	}
}

// 4. Panic with timeout set — caught in inner goroutine, OnHandlerPanic IS called, wg.Done fires.
func TestExecuteHandler_PanicWithTimeout(t *testing.T) {
	job := newJob(
		func(_ context.Context, _ models.Event[string]) { panic("hidden") },
		nil,
		5*time.Second, // long timeout so we don't time out
	)

	var wg sync.WaitGroup
	wg.Add(1)

	var output string
	output = captureStderr(t, func() {
		go exec.ExecuteHandler(&wg, job)
		wg.Wait()
	})

	if !strings.Contains(output, "panicked") {
		t.Errorf("OnHandlerPanic should have been called, but stderr had: %q", output)
	}
}

// 4a. Both primary and fallback panic — process must not crash, wg.Done fires.
func TestExecuteHandler_DoublePanic(t *testing.T) {
	job := &exec.HandlerJob[string]{
		Handler:         func(_ context.Context, _ models.Event[string]) { panic("primary") },
		FallbackHandler: func(_ context.Context, _ models.Event[string]) { panic("fallback") },
		Timeout:         0,
		Event:           ev("test"),
		Ctx:             context.Background(),
	}

	var wg sync.WaitGroup
	wg.Add(1)
	captureStderr(t, func() {
		go exec.ExecuteHandler(&wg, job)
		wg.Wait()
	})
	// If wg.Wait() returns, wg.Done fired — test passes.
}

// 4b. Both primary and fallback panic — OnPanic callback fires twice.
func TestExecuteHandler_DoublePanic_BothLogged(t *testing.T) {
	var panicCount int32
	job := &exec.HandlerJob[string]{
		Handler:         func(_ context.Context, _ models.Event[string]) { panic("primary") },
		FallbackHandler: func(_ context.Context, _ models.Event[string]) { panic("fallback") },
		Timeout:         0,
		Event:           ev("test"),
		Ctx:             context.Background(),
		OnPanic:         func(_ context.Context, _ models.Event[string], _ any) { atomic.AddInt32(&panicCount, 1) },
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go exec.ExecuteHandler(&wg, job)
	wg.Wait()

	if count := atomic.LoadInt32(&panicCount); count != 2 {
		t.Errorf("expected OnPanic called 2 times, got %d", count)
	}
}

// --- ExecuteWithTimeout ---

// 5. timeout=0 — inline execution.
func TestExecuteWithTimeout_ZeroTimeout(t *testing.T) {
	called := false
	exec.ExecuteWithTimeout(context.Background(), func(_ context.Context, _ models.Event[string]) { called = true }, ev("x"), 0, nil, nil)
	if !called {
		t.Error("handler not called with zero timeout")
	}
}

// 6. Handler finishes before timeout.
func TestExecuteWithTimeout_FinishesBeforeTimeout(t *testing.T) {
	called := make(chan struct{}, 1)
	exec.ExecuteWithTimeout(
		context.Background(),
		func(_ context.Context, _ models.Event[string]) { called <- struct{}{} },
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

// 7. Handler times out — onTimeout fires.
func TestExecuteWithTimeout_HandlerTimesOut(t *testing.T) {
	started := make(chan struct{})
	timedOut := make(chan struct{}, 1)
	block := make(chan struct{})
	defer close(block)

	exec.ExecuteWithTimeout(
		context.Background(),
		func(_ context.Context, _ models.Event[string]) {
			close(started)
			<-block
		},
		ev("slow-event"),
		20*time.Millisecond,
		nil,
		func() { timedOut <- struct{}{} },
	)
	<-started

	select {
	case <-timedOut:
	default:
		t.Error("expected onTimeout to be called")
	}
}

// 8. Handler panics inside goroutine — inner recover catches it, onPanic=nil → returns cleanly.
func TestExecuteWithTimeout_PanicInsideGoroutine(t *testing.T) {
	// onPanic=nil: panic is recovered, done channel is closed, returns cleanly.
	exec.ExecuteWithTimeout(
		context.Background(),
		func(_ context.Context, _ models.Event[string]) { panic("inner") },
		ev("x"),
		5*time.Second,
		nil,
		nil,
	)
	// If we reach here without a crash, the test passes.
}

// 8a. Handler panics inside goroutine with onPanic set — callback is called.
func TestExecuteWithTimeout_PanicCallsOnPanic(t *testing.T) {
	got := make(chan any, 1)
	exec.ExecuteWithTimeout(
		context.Background(),
		func(_ context.Context, _ models.Event[string]) { panic("boom") },
		ev("x"),
		5*time.Second,
		func(v any) { got <- v },
		nil,
	)

	select {
	case v := <-got:
		if v != "boom" {
			t.Errorf("onPanic got %v, want %q", v, "boom")
		}
	default:
		t.Error("onPanic was not called")
	}
}

// 9a. Custom OnPanic callback used instead of stderr.
func TestExecuteHandler_CustomPanicCallback(t *testing.T) {
	got := make(chan any, 1)
	job := &exec.HandlerJob[string]{
		Handler: func(_ context.Context, _ models.Event[string]) { panic("custom-panic") },
		Timeout: 0,
		Event:   ev("test"),
		Ctx:     context.Background(),
		OnPanic: func(_ context.Context, _ models.Event[string], v any) { got <- v },
	}

	var wg sync.WaitGroup
	wg.Add(1)
	output := captureStderr(t, func() {
		go exec.ExecuteHandler(&wg, job)
		wg.Wait()
	})

	if strings.Contains(output, "panicked") {
		t.Errorf("default stderr callback fired; got: %q", output)
	}
	select {
	case v := <-got:
		if v != "custom-panic" {
			t.Errorf("OnPanic got %v, want %q", v, "custom-panic")
		}
	default:
		t.Error("OnPanic not called")
	}
}

// 9a. No OnTimeout set — default OnHandlerTimeout writes to stderr.
func TestExecuteHandler_DefaultTimeoutCallback(t *testing.T) {
	started := make(chan struct{})
	block := make(chan struct{})
	defer close(block)

	job := &exec.HandlerJob[string]{
		Handler: func(_ context.Context, _ models.Event[string]) {
			close(started)
			<-block
		},
		Timeout: 20 * time.Millisecond,
		Event:   ev("SlowEvent"),
		Ctx:     context.Background(),
		// OnTimeout intentionally nil — default OnHandlerTimeout should fire.
	}

	var wg sync.WaitGroup
	wg.Add(1)
	output := captureStderr(t, func() {
		go exec.ExecuteHandler(&wg, job)
		<-started
		wg.Wait()
	})

	if !strings.Contains(output, "timed out") {
		t.Errorf("expected default timeout message in stderr, got: %q", output)
	}
}

// 9b. Custom OnTimeout callback used instead of stderr.
func TestExecuteHandler_CustomTimeoutCallback(t *testing.T) {
	fired := make(chan struct{}, 1)
	started := make(chan struct{})
	block := make(chan struct{})
	defer close(block)

	job := &exec.HandlerJob[string]{
		Handler: func(_ context.Context, _ models.Event[string]) {
			close(started)
			<-block
		},
		Timeout:   20 * time.Millisecond,
		Event:     ev("test"),
		Ctx:       context.Background(),
		OnTimeout: func(_ context.Context, _ models.Event[string], _ time.Duration) { fired <- struct{}{} },
	}

	var wg sync.WaitGroup
	wg.Add(1)
	output := captureStderr(t, func() {
		go exec.ExecuteHandler(&wg, job)
		<-started
		wg.Wait()
	})

	if strings.Contains(output, "timed out") {
		t.Errorf("default stderr callback fired; got: %q", output)
	}
	select {
	case <-fired:
	default:
		t.Error("OnTimeout not called")
	}
}

// --- OnHandlerPanic / OnHandlerTimeout (smoke) ---

// 9. OnHandlerPanic — verifies stderr message format.
func TestOnHandlerPanic_Smoke(t *testing.T) {
	job := newJob(nil, nil, 0)
	job.Event = ev("MyEvent")

	output := captureStderr(t, func() {
		exec.OnHandlerPanic[string]("something bad", job)
	})

	if !strings.Contains(output, "MyEvent") {
		t.Errorf("expected event name in output, got: %q", output)
	}
	if !strings.Contains(output, "something bad") {
		t.Errorf("expected panic value in output, got: %q", output)
	}
	if !strings.Contains(output, "panicked") {
		t.Errorf("expected 'panicked' in output, got: %q", output)
	}
}

// 10. OnHandlerTimeout — verifies stderr message format.
func TestOnHandlerTimeout_Smoke(t *testing.T) {
	timeout := 42 * time.Millisecond
	output := captureStderr(t, func() {
		exec.OnHandlerTimeout[string](ev("SlowEvent"), timeout)
	})

	if !strings.Contains(output, "SlowEvent") {
		t.Errorf("expected event name in output, got: %q", output)
	}
	if !strings.Contains(output, "timed out") {
		t.Errorf("expected 'timed out' in output, got: %q", output)
	}
	if !strings.Contains(output, "42ms") {
		t.Errorf("expected timeout duration in output, got: %q", output)
	}
}

// 11. Handler panics and onPanic callback itself panics — process returns cleanly, no goroutine hang.
func TestExecuteWithTimeout_PanicCallbackPanics(t *testing.T) {
	done := make(chan struct{})
	exec.ExecuteWithTimeout(
		context.Background(),
		func(_ context.Context, _ models.Event[string]) { panic("handler panics") },
		ev("x"),
		5*time.Second,
		func(_ any) { panic("callback panics") },
		nil,
	)
	close(done)
	// If we reach here without hanging or crashing, the test passes.
}
