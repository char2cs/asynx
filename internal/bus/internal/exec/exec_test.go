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
		Ctx:             context.Background(),
	}
}

func TestExecuteHandler_Normal(
	t *testing.T,
) {
	called := make(
		chan struct{},
		1,
	)
	job := newJob(
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			called <- struct{}{}
		},
		nil,
		0,
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go exec.Handler(
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

func TestExecuteHandler_PanicNoFallbackNoTimeout(
	t *testing.T,
) {
	job := newJob(
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("boom")
		},
		nil,
		0,
	)

	var wg sync.WaitGroup
	wg.Add(1)

	var output string
	output = captureStderr(
		t,
		func() {
			go exec.Handler(
				&wg,
				job,
			)
			wg.Wait()
		},
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

func TestExecuteHandler_PanicWithFallback(
	t *testing.T,
) {
	fallbackCalled := make(
		chan struct{},
		1,
	)
	job := newJob(
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("oops")
		},
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			fallbackCalled <- struct{}{}
		},
		0,
	)

	var wg sync.WaitGroup
	wg.Add(1)
	captureStderr(
		t,
		func() {
			go exec.Handler(
				&wg,
				job,
			)
			wg.Wait()
		},
	)

	select {
	case <-fallbackCalled:
	default:
		t.Error("fallback handler was not called")
	}
}

func TestExecuteHandler_PanicWithTimeout(
	t *testing.T,
) {
	job := newJob(
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("hidden")
		},
		nil,
		5*time.Second,
	)

	var wg sync.WaitGroup
	wg.Add(1)

	var output string
	output = captureStderr(
		t,
		func() {
			go exec.Handler(
				&wg,
				job,
			)
			wg.Wait()
		},
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

func TestExecuteHandler_DoublePanic(
	t *testing.T,
) {
	job := &exec.HandlerJob[string]{
		Handler: func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("primary")
		},
		FallbackHandler: func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("fallback")
		},
		Timeout: 0,
		Event:   ev("test"),
		Ctx:     context.Background(),
	}

	var wg sync.WaitGroup
	wg.Add(1)
	captureStderr(
		t,
		func() {
			go exec.Handler(
				&wg,
				job,
			)
			wg.Wait()
		},
	)
}

func TestExecuteHandler_DoublePanic_BothLogged(
	t *testing.T,
) {
	var panicCount int32
	job := &exec.HandlerJob[string]{
		Handler: func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("primary")
		},
		FallbackHandler: func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("fallback")
		},
		Timeout: 0,
		Event:   ev("test"),
		Ctx:     context.Background(),
		OnPanic: func(
			_ context.Context,
			_ models.Event[string],
			_ any,
		) {
			atomic.AddInt32(
				&panicCount,
				1,
			)
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go exec.Handler(
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

func TestExecuteWithTimeout_ZeroTimeout(
	t *testing.T,
) {
	called := false
	exec.WithTimeout(
		context.Background(),
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			called = true
		},
		ev("x"),
		0,
		nil,
		nil,
	)
	if !called {
		t.Error("handler not called with zero timeout")
	}
}

func TestExecuteWithTimeout_FinishesBeforeTimeout(
	t *testing.T,
) {
	called := make(
		chan struct{},
		1,
	)
	exec.WithTimeout(
		context.Background(),
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			called <- struct{}{}
		},
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

func TestExecuteWithTimeout_HandlerTimesOut(
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
		func(
			_ context.Context,
			_ models.Event[string],
		) {
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

func TestExecuteWithTimeout_PanicInsideGoroutine(
	t *testing.T,
) {
	exec.WithTimeout(
		context.Background(),
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("inner")
		},
		ev("x"),
		5*time.Second,
		nil,
		nil,
	)
}

func TestExecuteWithTimeout_PanicCallsOnPanic(
	t *testing.T,
) {
	got := make(
		chan any,
		1,
	)
	exec.WithTimeout(
		context.Background(),
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("boom")
		},
		ev("x"),
		5*time.Second,
		func(
			v any,
		) {
			got <- v
		},
		nil,
	)

	select {
	case v := <-got:
		if v != "boom" {
			t.Errorf(
				"onPanic got %v, want %q",
				v,
				"boom",
			)
		}
	default:
		t.Error("onPanic was not called")
	}
}

func TestExecuteHandler_CustomPanicCallback(
	t *testing.T,
) {
	got := make(
		chan any,
		1,
	)
	job := &exec.HandlerJob[string]{
		Handler: func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("custom-panic")
		},
		Timeout: 0,
		Event:   ev("test"),
		Ctx:     context.Background(),
		OnPanic: func(
			_ context.Context,
			_ models.Event[string],
			v any,
		) {
			got <- v
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	output := captureStderr(
		t,
		func() {
			go exec.Handler(
				&wg,
				job,
			)
			wg.Wait()
		},
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

	select {
	case v := <-got:
		if v != "custom-panic" {
			t.Errorf(
				"OnPanic got %v, want %q",
				v,
				"custom-panic",
			)
		}
	default:
		t.Error("OnPanic not called")
	}
}

func TestExecuteHandler_DefaultTimeoutCallback(
	t *testing.T,
) {
	started := make(chan struct{})
	block := make(chan struct{})
	defer close(block)

	job := &exec.HandlerJob[string]{
		Handler: func(
			_ context.Context,
			_ models.Event[string],
		) {
			close(started)
			<-block
		},
		Timeout: 20 * time.Millisecond,
		Event:   ev("SlowEvent"),
		Ctx:     context.Background(),
	}

	var wg sync.WaitGroup
	wg.Add(1)
	output := captureStderr(
		t,
		func() {
			go exec.Handler(
				&wg,
				job,
			)
			<-started
			wg.Wait()
		},
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

func TestExecuteHandler_CustomTimeoutCallback(
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
		Handler: func(
			_ context.Context,
			_ models.Event[string],
		) {
			close(started)
			<-block
		},
		Timeout: 20 * time.Millisecond,
		Event:   ev("test"),
		Ctx:     context.Background(),
		OnTimeout: func(
			_ context.Context,
			_ models.Event[string],
			_ time.Duration,
		) {
			fired <- struct{}{}
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	output := captureStderr(
		t,
		func() {
			go exec.Handler(
				&wg,
				job,
			)
			<-started
			wg.Wait()
		},
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
		func() {
			exec.OnHandlerPanic[string](
				"something bad",
				job,
			)
		},
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
	output := captureStderr(
		t,
		func() {
			exec.OnHandlerTimeout[string](
				ev("SlowEvent"),
				timeout,
			)
		},
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

func TestExecuteWithTimeout_PanicCallbackPanics(
	t *testing.T,
) {
	done := make(chan struct{})
	exec.WithTimeout(
		context.Background(),
		func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("handler panics")
		},
		ev("x"),
		5*time.Second,
		func(
			_ any,
		) {
			panic("callback panics")
		},
		nil,
	)
	close(done)
}

func TestExecuteHandler_PanicInTheObserversIsSwallowed(
	t *testing.T,
) {
	var wg sync.WaitGroup

	wg.Add(1)
	panicking := &exec.HandlerJob[string]{
		Handler: func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("handler")
		},
		Event: ev("test"),
		Ctx:   context.Background(),
		OnPanic: func(
			context.Context,
			models.Event[string],
			any,
		) {
			panic("observer")
		},
	}
	go exec.Handler(
		&wg,
		panicking,
	)

	wg.Add(1)
	release := make(chan struct{})
	slow := &exec.HandlerJob[string]{
		Handler: func(
			_ context.Context,
			_ models.Event[string],
		) {
			<-release
		},
		Timeout: time.Millisecond,
		Event:   ev("test"),
		Ctx:     context.Background(),
		OnTimeout: func(
			context.Context,
			models.Event[string],
			time.Duration,
		) {
			close(release)
			panic("observer")
		},
	}
	go exec.Handler(
		&wg,
		slow,
	)

	wg.Wait()
}

func TestExecuteHandler_PanicInTheObserverOfTheFallbackIsSwallowed(
	t *testing.T,
) {
	var wg sync.WaitGroup
	wg.Add(1)
	job := &exec.HandlerJob[string]{
		Handler: func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("handler")
		},
		FallbackHandler: func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("fallback")
		},
		Event: ev("test"),
		Ctx:   context.Background(),
		OnPanic: func(
			context.Context,
			models.Event[string],
			any,
		) {
			panic("observer")
		},
	}
	go exec.Handler(
		&wg,
		job,
	)
	wg.Wait()
}

func noopJob() *exec.HandlerJob[string] {
	return &exec.HandlerJob[string]{
		Handler: func(
			_ context.Context,
			_ models.Event[string],
		) {
		},
		Event: models.Event[string]{EventName: "test"},
		Ctx:   context.Background(),
	}
}

func BenchmarkExecuteHandler_NoTimeout(
	b *testing.B,
) {
	job := noopJob()
	b.ReportAllocs()
	for range b.N {
		var wg sync.WaitGroup
		wg.Add(1)
		go exec.Handler(
			&wg,
			job,
		)
		wg.Wait()
	}
}

func BenchmarkExecuteHandler_WithTimeout(
	b *testing.B,
) {
	job := noopJob()
	job.Timeout = 5 * time.Second
	b.ReportAllocs()
	for range b.N {
		var wg sync.WaitGroup
		wg.Add(1)
		go exec.Handler(
			&wg,
			job,
		)
		wg.Wait()
	}
}

var stderrMu sync.Mutex

func BenchmarkExecuteHandler_PanicRecovery(
	b *testing.B,
) {
	job := &exec.HandlerJob[string]{
		Handler: func(
			_ context.Context,
			_ models.Event[string],
		) {
			panic("bench")
		},
		Event: models.Event[string]{EventName: "test"},
		Ctx:   context.Background(),
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
			&wg,
			job,
		)
		wg.Wait()
	}
}

func BenchmarkExecuteWithTimeout_ZeroTimeout(
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

func BenchmarkExecuteWithTimeout_WithTimer(
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
