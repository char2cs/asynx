package models

import (
	"context"

	asynxmd "github.com/char2cs/asynx/models"
)

// CommandEnvelope carries a command through a shard queue. It never carries the
// caller's context: the command runs on the caller's own goroutine, so the
// context stays on the caller's stack.
//
// The hand-off is turn-taking. The sender enqueues the envelope and blocks. When
// the shard worker reaches the command in its aggregate's order it calls
// GrantTurn, which closes Turn and then waits for Done. The sender, seeing Turn
// closed, starts Execute on a goroutine; Execute runs the command with the
// sender's own context (kept on the sender's stack and passed as an argument,
// never stored) and closes Done when it returns (also on panic), releasing the worker. A
// sender that gives up before its turn calls Abandon, which closes Done, so a
// later GrantTurn returns at once and the command never runs.
//
// Guarantees: only the sender runs the command, at most once; it never runs
// after the sender reported ErrContextCancelled before its turn; the worker's
// per-aggregate serialization holds because it waits for Done before moving on.
// Done is closed exactly once by the sender, either by Execute or by Abandon.
// ResultChan, which holds one value, receives the outcome: from Execute when the
// command ran, or from the worker when it could not run it (queue full,
// shutting down); the two never both happen.
type CommandEnvelope[T any] struct {
	Cmd          asynxmd.Command[T]
	ResultChan   chan CommandResult[T]
	WaitHandlers bool
	Turn         chan struct{}
	Done         chan struct{}
}

// NewCommandEnvelope returns an envelope for cmd with its channels ready.
func NewCommandEnvelope[T any](
	cmd asynxmd.Command[T],
) *CommandEnvelope[T] {
	return &CommandEnvelope[T]{
		Cmd: cmd,
		ResultChan: make(
			chan CommandResult[T],
			1,
		),
		Turn: make(chan struct{}),
		Done: make(chan struct{}),
	}
}

// GrantTurn tells the sender it may run its command and blocks until the sender
// has finished or abandoned it. Only the shard worker calls it, once.
func (e *CommandEnvelope[T]) GrantTurn() {
	close(e.Turn)
	<-e.Done
}

// Abandon releases the worker without running the command. Only the sender
// calls it, and only when it did not call Execute.
func (e *CommandEnvelope[T]) Abandon() {
	close(e.Done)
}

// Execute runs the command with ctx, puts its outcome on ResultChan, then
// releases the worker. A ctx already done at the turn yields ErrContextCancelled
// without running the command. Only the sender calls it, once, after Turn
// closed, normally on a goroutine of its own so it can stop waiting when its ctx
// ends while the command keeps running.
func (e *CommandEnvelope[T]) Execute(
	ctx context.Context,
	executor Executor[T],
) {
	defer close(e.Done)

	e.ResultChan <- e.run(
		ctx,
		executor,
	)
}

func (e *CommandEnvelope[T]) run(
	ctx context.Context,
	executor Executor[T],
) CommandResult[T] {
	if err := ctx.Err(); err != nil {
		return CommandResult[T]{Err: asynxmd.ErrContextCancelled}
	}

	event, wait, err := executor.ExecuteAsync(
		ctx,
		e.Cmd,
	)

	result := CommandResult[T]{Event: event, Err: err}
	if e.WaitHandlers {
		result.Wait = wait
	}

	return result
}
