package models_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/models"
	asynxmd "github.com/char2cs/asynx/models"
)

type countingExecutor struct {
	calls atomic.Int32
	panic bool
}

// ExecuteAsync counts the call, panics when panic is set, and otherwise
// succeeds with an empty event and a no-op wait, so a test can assert whether
// and how often an envelope ran its command.
func (c *countingExecutor) ExecuteAsync(
	_ context.Context,
	_ asynxmd.Command[mocks.Order],
) (asynxmd.Event[mocks.Order], func(context.Context) error, error) {
	c.calls.Add(1)
	if c.panic {
		panic("boom")
	}

	return asynxmd.Event[mocks.Order]{},
		noWait,
		nil
}

func newEnvelope() *models.CommandEnvelope[mocks.Order] {
	return models.NewCommandEnvelope[mocks.Order](
		mocks.CreateOrderCmd{ID: "o", Total: 1},
	)
}

func requireClosed(
	t *testing.T,
	ch <-chan struct{},
	what string,
) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf(
			"%s never closed",
			what,
		)
	}
}

func TestCommandEnvelope_ExecuteRunsOnceAndReleasesTheWorker(
	t *testing.T,
) {
	envelope := newEnvelope()
	executor := &countingExecutor{}
	granted := make(chan struct{})
	go func() {
		envelope.GrantTurn()
		close(granted)
	}()

	requireClosed(
		t,
		envelope.Turn,
		"Turn",
	)
	envelope.Execute(
		context.Background(),
		executor,
	)
	requireClosed(
		t,
		granted,
		"GrantTurn return",
	)

	result := <-envelope.ResultChan
	if result.Err != nil ||
		result.Wait != nil {
		t.Fatalf(
			"result = %+v, want no error and no Wait",
			result,
		)
	}

	if got := executor.calls.Load(); got != 1 {
		t.Fatalf(
			"executor ran %d times, want 1",
			got,
		)
	}
}

func TestCommandEnvelope_ExecuteKeepsWaitOnlyForWaitHandlers(
	t *testing.T,
) {
	envelope := newEnvelope()
	envelope.WaitHandlers = true
	close(envelope.Turn)
	envelope.Execute(
		context.Background(),
		&countingExecutor{},
	)

	if result := <-envelope.ResultChan; result.Wait == nil {
		t.Fatal("Wait dropped for a WaitHandlers envelope")
	}
}

func TestCommandEnvelope_ExecuteSkipsACommandWhoseContextEnded(
	t *testing.T,
) {
	envelope := newEnvelope()
	executor := &countingExecutor{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	close(envelope.Turn)

	envelope.Execute(
		ctx,
		executor,
	)

	result := <-envelope.ResultChan
	if !errors.Is(
		result.Err,
		asynxmd.ErrContextCancelled,
	) {
		t.Fatalf(
			"result = %v, want ErrContextCancelled",
			result.Err,
		)
	}

	if got := executor.calls.Load(); got != 0 {
		t.Fatalf(
			"executor ran %d times for a cancelled command",
			got,
		)
	}

	requireClosed(
		t,
		envelope.Done,
		"Done",
	)
}

func TestCommandEnvelope_AbandonLetsALaterGrantMoveOn(
	t *testing.T,
) {
	envelope := newEnvelope()
	envelope.Abandon()

	granted := make(chan struct{})
	go func() {
		envelope.GrantTurn()
		close(granted)
	}()

	requireClosed(
		t,
		granted,
		"GrantTurn return",
	)
}

func TestCommandEnvelope_APanickingExecutorStillReleasesTheWorker(
	t *testing.T,
) {
	envelope := newEnvelope()
	close(envelope.Turn)
	recovered := executeRecovering(
		envelope,
		&countingExecutor{panic: true},
	)
	if recovered == nil {
		t.Fatal("the executor's panic did not reach the sender")
	}

	requireClosed(
		t,
		envelope.Done,
		"Done",
	)
}

func executeRecovering(
	envelope *models.CommandEnvelope[mocks.Order],
	executor models.Executor[mocks.Order],
) (recovered any) {
	defer func() { recovered = recover() }()
	envelope.Execute(
		context.Background(),
		executor,
	)
	return nil
}

func noWait(
	context.Context,
) error {
	return nil
}

func TestCommandEnvelope_RejectAnswersTheSenderWithoutRunningTheCommand(
	t *testing.T,
) {
	envelope := newEnvelope()
	executor := &countingExecutor{}

	envelope.Reject(asynxmd.ErrQueueFull)

	result := awaitResult(
		t,
		envelope,
	)
	if !errors.Is(
		result.Err,
		asynxmd.ErrQueueFull,
	) {
		t.Fatalf(
			"result err = %v, want ErrQueueFull",
			result.Err,
		)
	}

	if n := executor.calls.Load(); n != 0 {
		t.Fatalf(
			"executor calls = %d, want 0",
			n,
		)
	}
}

func awaitResult(
	t *testing.T,
	envelope *models.CommandEnvelope[mocks.Order],
) models.CommandResult[mocks.Order] {
	t.Helper()
	select {
	case result := <-envelope.ResultChan:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("the envelope was never answered")
	}

	return models.CommandResult[mocks.Order]{}
}
