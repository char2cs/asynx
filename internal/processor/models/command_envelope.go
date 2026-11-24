package models

import (
	"context"

	"github.com/char2cs/asynx/models/command"
	"github.com/char2cs/asynx/models/failure"
)

// CommandEnvelope carries one command through a shard queue and coordinates the
// sender with the shard worker. It never carries the caller's context: the
// command runs on the sender's own goroutine, so the context stays on the
// sender's stack and is passed to Execute as an argument.
//
// The hand-off is turn-taking between exactly two parties, the sender
// (Processor.Send/SendWait) and the shard worker that serves the command's
// aggregate:
//
//  1. The sender enqueues the envelope and blocks on Turn, ResultChan and its
//     own context.
//  2. When the worker reaches the command in its aggregate's order it calls
//     GrantTurn, which closes Turn and then blocks on Done. This blocking is
//     what keeps one aggregate's commands strictly serial.
//  3. The sender, seeing Turn closed, runs Execute on a goroutine of its own
//     with its context. Execute runs the command, publishes the outcome on
//     ResultChan and closes Done (also if the executor panics), releasing the
//     worker. Running Execute on a separate goroutine lets the sender stop
//     waiting when its context ends while the command still finishes and
//     commits; the worker keeps waiting until it has.
//  4. A sender that gives up before Turn closes calls Abandon, which closes
//     Done so that a later GrantTurn returns at once and the command never
//     runs.
//  5. A worker that cannot run the command at all (mailbox full, shard
//     stopping) calls Reject instead of GrantTurn; the sender then receives
//     the error from ResultChan.
//
// Guarantees: the command runs at most once and only on the sender's
// goroutine; it never runs after the sender returned ErrContextCancelled
// before its turn; per-aggregate serialisation holds because the worker waits
// for Done. Done is closed exactly once, by Execute or by Abandon, never both,
// because the sender chooses one of them. ResultChan has room for one value
// and receives exactly one of: Execute's outcome, or Reject's error. Neither
// side can leak: ResultChan is buffered, so a result nobody reads (a sender
// that already left) never blocks the writer, and Done is always closed by
// the sender before the sender leaves. No goroutine outlives the command.
type CommandEnvelope[T any] struct {
	// Cmd is the command to run.
	Cmd command.Command[T]
	// ResultChan carries the single outcome of the envelope; see the type
	// documentation for who writes it.
	ResultChan chan CommandResult[T]
	// WaitHandlers asks Execute to return a Wait the sender uses to block
	// until the event's handlers ran. Set before enqueueing, read-only after.
	WaitHandlers bool
	// Turn is closed by the worker (GrantTurn) when the command may run.
	Turn chan struct{}
	// Done is closed by the sender (Execute or Abandon) when the worker may
	// move on to the aggregate's next command.
	Done chan struct{}
}

// NewCommandEnvelope returns an envelope for cmd with a one-slot ResultChan and
// open Turn and Done channels, ready to be enqueued on a shard. Set
// WaitHandlers on the result before enqueueing it if the sender will wait for
// the event's handlers.
func NewCommandEnvelope[T any](
	cmd command.Command[T],
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
// has finished (Execute) or given up (Abandon). Only the shard worker calls it,
// at most once per envelope and never together with Reject. It returns only
// after Done is closed, so a worker is held for as long as the command runs;
// shutdown relies on this to wait for commands in flight.
func (e *CommandEnvelope[T]) GrantTurn() {
	close(e.Turn)
	<-e.Done
}

// Reject answers the sender with err, without running the command, for a
// command the worker will not grant a turn: a full mailbox (ErrQueueFull) or a
// shard that is stopping (ErrShuttingDown). Only the shard worker calls it, at
// most once per envelope and never together with GrantTurn. It never blocks,
// because ResultChan has a free slot until the first and only result is
// written.
func (e *CommandEnvelope[T]) Reject(
	err error,
) {
	e.ResultChan <- CommandResult[T]{Err: err}
}

// Abandon releases the worker without running the command, for a sender whose
// context ended before its turn. Only the sender calls it, and only when it did
// not call Execute. After it, GrantTurn returns immediately and the command is
// never run.
func (e *CommandEnvelope[T]) Abandon() {
	close(e.Done)
}

// Execute runs the command with ctx through executor, puts its outcome on
// ResultChan, then releases the worker by closing Done. A ctx already done at
// the turn yields ErrContextCancelled without running the command. Only the
// sender calls it, once, after Turn closed, normally on a goroutine of its own
// so the sender can stop waiting when its ctx ends while the command keeps
// running. The Wait it may return (see WaitHandlers) is for the sender to call
// on its own goroutine, not the worker's.
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
	if ctx.Err() != nil {
		return CommandResult[T]{Err: failure.ErrContextCancelled}
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
