// Package pool implements shard-based concurrent command execution.
//
// Shard[T] encapsulates dispatcher and worker coordination for a subset of aggregates.
// The dispatcher (dispatchCommands) reads envelopes off commandChan and hands
// them to the worker pool via jobQueue. Workers (workerLoop) execute commands.
//   - executeJob   — Called by workers; runs the command and sends the result
//   - sendResult   — Non-blocking result send; drops if receiver gone
//
// Consistent hashing at the router routes each aggregate to a fixed shard and
// dispatcher. With more than one worker per shard, commands run in parallel
// across the shard's worker pool. A worker that picks a command for an
// aggregate another worker is already running does not wait for it: the command
// joins that aggregate's mailbox and the running worker takes it next, so one
// aggregate occupies at most one worker however many commands it has queued
// and the others keep serving the rest of the shard. The executor's
// per-aggregate lock still guards against the callers outside the pool (Forget,
// ResumeDeliveries). The event version is assigned by the event store on write
// (optimistic concurrency), not by the shard.
package pool

import (
	"sync"

	"github.com/char2cs/asynx/internal/processor/exec"
	"github.com/char2cs/asynx/internal/processor/models"
	asynxmd "github.com/char2cs/asynx/models"
)

type Shard[T any] struct {
	id           int
	commandChan  chan *models.CommandEnvelope[T]
	jobQueue     chan *models.CommandJob[T]
	stopChan     chan struct{}
	stopMu       sync.Mutex
	stopClosed   bool
	onDispatched func()

	mailboxLimit int
	mailboxMu    sync.Mutex
	mailboxes    map[string][]*models.CommandJob[T] // aggregates a worker is running
}

func newShard[T any](
	id int,
	queueDepth int,
	workersPerShard int,
) *Shard[T] {
	return &Shard[T]{
		id:           id,
		commandChan:  make(chan *models.CommandEnvelope[T], queueDepth),
		jobQueue:     make(chan *models.CommandJob[T], max(workersPerShard, queueDepth)),
		stopChan:     make(chan struct{}),
		mailboxLimit: max(queueDepth, workersPerShard),
		mailboxes:    make(map[string][]*models.CommandJob[T]),
	}
}

func (s *Shard[T]) CommandChan() chan *models.CommandEnvelope[T] {
	return s.commandChan
}

func (s *Shard[T]) dispatchCommands(
	dispatcherDoneWg *sync.WaitGroup,
) {
	defer dispatcherDoneWg.Done()

	for {
		if s.handleDispatch() {
			return
		}
	}
}

func (s *Shard[T]) handleDispatch() bool {
	select {
	case <-s.stopChan:
		s.failQueued()
		close(s.jobQueue)
		return true

	case envelope := <-s.commandChan:
		if envelope == nil {
			close(s.jobQueue)
			return true
		}
		s.dispatchJob(envelope)
		if s.onDispatched != nil {
			s.onDispatched()
		}
	}

	return false
}

// failQueued answers the envelopes still waiting when the shard stops, so their
// senders are released instead of waiting for a result nobody will produce.
func (s *Shard[T]) failQueued() {
	for {
		select {
		case envelope := <-s.commandChan:
			s.failEnvelope(envelope)
		default:
			return
		}
	}
}

func (s *Shard[T]) failEnvelope(envelope *models.CommandEnvelope[T]) {
	if envelope == nil {
		return
	}
	s.sendResult(envelope.ResultChan, models.CommandResult[T]{Err: asynxmd.ErrShuttingDown})
}

func (s *Shard[T]) dispatchJob(
	envelope *models.CommandEnvelope[T],
) {
	s.jobQueue <- &models.CommandJob[T]{
		Envelope: envelope,
	}
}

func (s *Shard[T]) workerLoop(
	executor *exec.CommandExecutor[T],
	doneWg *sync.WaitGroup,
) {
	defer doneWg.Done()

	for job := range s.jobQueue {
		s.serve(executor, job)
	}
}

// serve runs job, then every command that queued behind it for the same
// aggregate, unless another worker is already running that aggregate: then job
// joins its mailbox and this worker is free for the next one.
func (s *Shard[T]) serve(
	executor *exec.CommandExecutor[T],
	job *models.CommandJob[T],
) {
	aggregateID := job.Envelope.Cmd.AggregateID()
	if !s.claim(aggregateID, job) {
		return
	}
	for job != nil {
		s.executeJob(executor, job)
		job = s.next(aggregateID)
	}
}

// claim reports whether the caller now runs the aggregate. When another worker
// does, job is left in its mailbox, or answered with ErrQueueFull if the
// mailbox already holds as many commands as the queue depth allows.
func (s *Shard[T]) claim(
	aggregateID string,
	job *models.CommandJob[T],
) bool {
	s.mailboxMu.Lock()
	mailbox, running := s.mailboxes[aggregateID]
	if !running {
		s.mailboxes[aggregateID] = nil
		s.mailboxMu.Unlock()
		return true
	}
	isFull := len(mailbox) >= s.mailboxLimit
	if !isFull {
		s.mailboxes[aggregateID] = append(mailbox, job)
	}
	s.mailboxMu.Unlock()

	if isFull {
		s.sendResult(job.Envelope.ResultChan, models.CommandResult[T]{Err: asynxmd.ErrQueueFull})
	}
	return false
}

// next pops the aggregate's next queued command, or releases the aggregate when
// its mailbox is empty.
func (s *Shard[T]) next(aggregateID string) *models.CommandJob[T] {
	s.mailboxMu.Lock()
	defer s.mailboxMu.Unlock()
	mailbox := s.mailboxes[aggregateID]
	if len(mailbox) == 0 {
		delete(s.mailboxes, aggregateID)
		return nil
	}
	job := mailbox[0]
	mailbox[0] = nil
	s.mailboxes[aggregateID] = mailbox[1:]
	return job
}

func (s *Shard[T]) executeJob(
	executor *exec.CommandExecutor[T],
	job *models.CommandJob[T],
) {
	if err := job.Envelope.Ctx.Err(); err != nil {
		s.sendResult(job.Envelope.ResultChan, models.CommandResult[T]{Err: asynxmd.ErrContextCancelled})
		return
	}

	event, wait, err := executor.ExecuteAsync(
		job.Envelope.Ctx,
		job.Envelope.Cmd,
	)

	result := models.CommandResult[T]{Event: event, Err: err}
	if job.Envelope.WaitHandlers {
		result.Wait = wait
	}
	s.sendResult(job.Envelope.ResultChan, result)
}

func (s *Shard[T]) sendResult(
	resultChan chan models.CommandResult[T],
	result models.CommandResult[T],
) {
	select {
	case resultChan <- result:
	default:
	}
}

func (s *Shard[T]) signalStop() {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()

	if !s.stopClosed {
		close(s.stopChan)
		s.stopClosed = true
	}
}

// ForTesting: Stopping returns a channel that closes once the shard has been
// told to stop. Do not call in production code.
func (s *Shard[T]) Stopping() <-chan struct{} { return s.stopChan }

// ForTesting: Backlog reports how many commands wait in aggregate mailboxes
// behind a running one. Do not call in production code.
func (s *Shard[T]) Backlog() int {
	s.mailboxMu.Lock()
	defer s.mailboxMu.Unlock()
	n := 0
	for _, mailbox := range s.mailboxes {
		n += len(mailbox)
	}
	return n
}

// ForTesting: SetOnDispatched sets a callback invoked each time the dispatcher
// reads a command from commandChan (slot is now free for new senders).
// Do not call in production code.
func (s *Shard[T]) SetOnDispatched(fn func()) {
	s.onDispatched = fn
}
