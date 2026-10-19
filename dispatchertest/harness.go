package dispatchertest

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/char2cs/asynx/models"
)

type harness struct {
	factory  models.DispatcherFactory[Aggregate]
	patience time.Duration
}

func (h harness) subtest(
	check scenario,
) func(
	*testing.T,
) {
	return func(
		t *testing.T,
	) {
		check(t, h)
	}
}

func (h harness) build(
	t testing.TB,
	cfg models.DispatcherConfig[Aggregate],
) models.Dispatcher[Aggregate] {
	t.Helper()
	d, err := h.factory(cfg)
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	t.Cleanup(func() { h.shut(t, d) })
	return d
}

func (h harness) shut(
	t testing.TB,
	d models.Dispatcher[Aggregate],
) {
	if err := h.close(d); err != nil {
		t.Errorf("Close at cleanup: %v", err)
	}
}

func (h harness) close(
	d models.Dispatcher[Aggregate],
) error {
	ctx, cancel := context.WithTimeout(context.Background(), h.patience)
	defer cancel()
	return d.Close(ctx)
}

func (h harness) wait(
	t testing.TB,
	ch <-chan struct{},
	what string,
) {
	t.Helper()
	receive(t, h.patience, ch, what)
}

func (h harness) final(
	t testing.TB,
	tk models.Ticket,
	what string,
) error {
	t.Helper()
	h.wait(t, tk.Final, what)
	return tk.Err()
}

func (h harness) idle(
	t testing.TB,
	d models.Dispatcher[Aggregate],
) {
	t.Helper()
	done := make(chan struct{})
	go waitIdle(d, done)
	h.wait(t, done, "WaitIdle to return")
}

func (h harness) waitFor(
	t testing.TB,
	what string,
	cond func() bool,
) {
	t.Helper()
	deadline := time.Now().Add(h.patience)
	for !cond() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !cond() {
		t.Fatalf("timed out waiting for %s", what)
	}
}

func (h harness) enqueueBounded(
	t testing.TB,
	d models.Dispatcher[Aggregate],
	r models.DispatchRequest[Aggregate],
) models.Ticket {
	t.Helper()
	tickets := make(chan models.Ticket, 1)
	go func() {
		tk, _ := d.Enqueue(context.Background(), r)
		tickets <- tk
	}()
	return receive(t, h.patience, tickets, "Enqueue to return")
}
