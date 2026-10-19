package dispatcher

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

var (
	errImposed = errors.New("test: parked by the test")
	errDown    = errors.New("test: down")
	errAck     = errors.New("test: ack failed")
)

func failingSub(
	attempts *atomic.Int32,
) []asynxmd.DurableSub[int] {
	return []asynxmd.DurableSub[int]{{Name: "flaky", Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		attempts.Add(1)
		return errDown
	}}}
}

func TestInspect_GiveUpCauseNamesTheLastHandlerError(t *testing.T) {
	var attempts atomic.Int32
	d := newDurable(t, failingSub(&attempts), withMaxAttempts[int](3))

	tk, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	finalErr := final(t, tk, "the delivery to give up")

	got := d.Inspect("a")
	if !got.Parked || got.Generation != d.Parks()["a"] {
		t.Fatalf("Inspect = %+v, want parked with generation %d", got, d.Parks()["a"])
	}
	if !errors.Is(got.Cause, asynxmd.ErrDeliveryParked) || !errors.Is(got.Cause, errDown) {
		t.Fatalf("Cause = %v, want ErrDeliveryParked wrapping %v", got.Cause, errDown)
	}
	if !strings.Contains(got.Cause.Error(), "3 attempts") {
		t.Fatalf("Cause = %q, want the attempt count (3)", got.Cause)
	}
	if got.Cause != finalErr {
		t.Fatalf("Cause = %v, want the error the ticket's Final returned, %v", got.Cause, finalErr)
	}
}

func TestInspect_AckFailureCauseIsTheAckError(t *testing.T) {
	d := newDurable(t, []asynxmd.DurableSub[int]{{Match: always, Handler: func(
		context.Context,
		asynxmd.Event[int],
	) error {
		return nil
	}}})

	tk, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return errAck })
	if err != nil {
		t.Fatal(err)
	}
	final(t, tk, "the failed ack")

	got := d.Inspect("a")
	if !got.Parked || !errors.Is(got.Cause, errAck) {
		t.Fatalf("Inspect = %+v, want parked with %v", got, errAck)
	}
}

func TestInspect_InterruptCauseIsDispatcherClosed(t *testing.T) {
	var attempts atomic.Int32
	d := newDurable(t, failingSub(&attempts), withRetryBackoff[int](time.Hour, time.Hour))

	tk, err := enqueueTicket(context.Background(), d, makeEvent("a", 1), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the first attempt", func() bool { return attempts.Load() == 1 })
	d.Interrupt()
	if err := final(t, tk, "the interrupted delivery"); !errors.Is(err, asynxmd.ErrDispatcherClosed) {
		t.Fatalf("final = %v, want ErrDispatcherClosed", err)
	}

	got := d.Inspect("a")
	if !got.Parked || !errors.Is(got.Cause, asynxmd.ErrDispatcherClosed) {
		t.Fatalf("Inspect = %+v, want parked with ErrDispatcherClosed", got)
	}
}

func TestInspect_ReportsTheCauseAndUndeliveredGivenToPark(t *testing.T) {
	d := newDurable(t, nil)
	if got := d.Inspect("a"); !reflect.DeepEqual(got, asynxmd.DeliveryStatus{}) {
		t.Fatalf("Inspect of an unknown aggregate = %+v, want the zero status", got)
	}
	d.Park("a", errImposed, 5, 3)
	got := d.Inspect("a")
	if !got.Parked || !errors.Is(got.Cause, errImposed) || !reflect.DeepEqual(got.Undelivered, []int64{3, 5}) {
		t.Fatalf("Inspect = %+v, want parked with %v and [3 5]", got, errImposed)
	}
	resume(d, "a")
	if got := d.Inspect("a"); !reflect.DeepEqual(got, asynxmd.DeliveryStatus{}) {
		t.Fatalf("Inspect after the lift = %+v, want the zero status", got)
	}
}
