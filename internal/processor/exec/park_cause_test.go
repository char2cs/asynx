package exec

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

var errEnqueue = errors.New("enqueue refused")

func fakeEnv(
	t *testing.T,
) (*CommandExecutor[order], *mocks.Dispatcher[order]) {
	t.Helper()
	es := eventstore.New[order](store.New(), store.NewSnapshots(), nil, 1, nil)
	fake := mocks.NewDispatcher[order]()
	return New[order](es, fake, WithDurableDelivery[order](func(string) bool { return true })), fake
}

func TestExecute_EnqueueFailureParksWithTheEnqueueError(t *testing.T) {
	ex, fake := fakeEnv(t)
	fake.FailEnqueue(errEnqueue)

	if _, err := ex.Execute(context.Background(), update("agg", 1), false); !errors.Is(err, errEnqueue) {
		t.Fatalf("Execute = %v, want the enqueue error", err)
	}
	calls := fake.ParkCalls()
	if len(calls) != 1 || calls[0].AggregateID != "agg" || !errors.Is(calls[0].Cause, errEnqueue) {
		t.Fatalf("ParkCalls = %+v, want agg parked with the enqueue error", calls)
	}
	if got := fake.Inspect("agg"); !got.Parked || !errors.Is(got.Cause, errEnqueue) {
		t.Fatalf("Inspect = %+v, want parked with the enqueue error", got)
	}
}

func TestForget_TombstoneEnqueueFailureParksWithTheEnqueueError(t *testing.T) {
	ex, fake := fakeEnv(t)
	ctx := context.Background()
	if _, err := ex.Execute(ctx, update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	fake.FailEnqueue(errEnqueue)

	if err := ex.Forget(ctx, "agg"); !errors.Is(err, errEnqueue) {
		t.Fatalf("Forget = %v, want the enqueue error", err)
	}
	calls := fake.ParkCalls()
	if len(calls) != 1 || !errors.Is(calls[0].Cause, errEnqueue) {
		t.Fatalf("ParkCalls = %+v, want one park with the enqueue error", calls)
	}
}

func TestForget_ErasePhaseFailureParksWithTheForgetError(t *testing.T) {
	ex, _, _, d, _ := newEnvOn(t, deleteFailStore{store.New()})
	ctx := context.Background()
	if _, err := ex.Execute(ctx, update("agg", 1), false); err != nil {
		t.Fatal(err)
	}

	err := ex.Forget(ctx, "agg")
	if !errors.Is(err, asynxmd.ErrForgetFailed) {
		t.Fatalf("Forget = %v, want ErrForgetFailed", err)
	}
	if got := d.Inspect("agg"); !got.Parked || got.Cause != err {
		t.Fatalf("Inspect = %+v, want parked with the error Forget returned, %v", got, err)
	}
}

func TestForget_GivingUpOnTheTombstoneParksWithTheWaitError(t *testing.T) {
	release := make(chan struct{})
	ex := durableEnv(t, func(
		ctx context.Context,
		e asynxmd.Event[order],
	) error {
		if e.EventName == asynxmd.ForgetEventName {
			<-release
		}
		return nil
	})
	defer close(release)
	if _, err := ex.Execute(context.Background(), update("agg", 1), true); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := ex.Forget(ctx, "agg")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Forget = %v, want the context error", err)
	}
	if got := ex.dispatcher.Inspect("agg"); !got.Parked || got.Cause != err {
		t.Fatalf("Inspect = %+v, want parked with the error Forget returned, %v", got, err)
	}
}
