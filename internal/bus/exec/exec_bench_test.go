package exec_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/internal/bus/exec"
)

func noopJob() *exec.HandlerJob[string] {
	return &exec.HandlerJob[string]{
		Handler: func(_ context.Context, _ models.Event[string]) {},
		Event:   models.Event[string]{EventName: "test"},
		Ctx:     context.Background(),
	}
}

// BenchmarkExecuteHandler_NoTimeout measures the cost of spawning a goroutine,
// running a no-op handler inline (timeout=0), and calling wg.Done.
func BenchmarkExecuteHandler_NoTimeout(b *testing.B) {
	job := noopJob()
	b.ReportAllocs()
	for range b.N {
		var wg sync.WaitGroup
		wg.Add(1)
		go exec.ExecuteHandler(&wg, job)
		wg.Wait()
	}
}

// BenchmarkExecuteHandler_WithTimeout measures the full timeout path:
// inner goroutine spawn + timer + done channel + select.
func BenchmarkExecuteHandler_WithTimeout(b *testing.B) {
	job := noopJob()
	job.Timeout = 5 * time.Second // long enough that it never fires
	b.ReportAllocs()
	for range b.N {
		var wg sync.WaitGroup
		wg.Add(1)
		go exec.ExecuteHandler(&wg, job)
		wg.Wait()
	}
}

// BenchmarkExecuteHandler_PanicRecovery measures the cost of the deferred
// recover path: panic + stack unwind + OnHandlerPanic. Stderr is redirected
// once for the whole run to avoid polluting benchmark output.
func BenchmarkExecuteHandler_PanicRecovery(b *testing.B) {
	job := &exec.HandlerJob[string]{
		Handler: func(_ context.Context, _ models.Event[string]) { panic("bench") },
		Event:   models.Event[string]{EventName: "test"},
		Ctx:     context.Background(),
	}

	// Suppress OnHandlerPanic output for the duration of this benchmark.
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer devNull.Close()
	orig := os.Stderr
	os.Stderr = devNull
	defer func() { os.Stderr = orig }()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var wg sync.WaitGroup
		wg.Add(1)
		go exec.ExecuteHandler(&wg, job)
		wg.Wait()
	}
}

// BenchmarkExecuteWithTimeout_ZeroTimeout measures inline (no goroutine) execution.
func BenchmarkExecuteWithTimeout_ZeroTimeout(b *testing.B) {
	handler := func(_ context.Context, _ models.Event[string]) {}
	event := models.Event[string]{EventName: "test"}
	b.ReportAllocs()
	for range b.N {
		exec.ExecuteWithTimeout(context.Background(), handler, event, 0, nil, nil)
	}
}

// BenchmarkExecuteWithTimeout_WithTimer measures the goroutine + timer + channel
// machinery when the handler completes well before the deadline.
func BenchmarkExecuteWithTimeout_WithTimer(b *testing.B) {
	handler := func(_ context.Context, _ models.Event[string]) {}
	event := models.Event[string]{EventName: "test"}
	timeout := 5 * time.Second
	b.ReportAllocs()
	for range b.N {
		exec.ExecuteWithTimeout(context.Background(), handler, event, timeout, nil, nil)
	}
}
