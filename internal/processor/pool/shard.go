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
// across the shard's worker pool; the executor's per-aggregate lock keeps
// commands for one aggregate strictly serial and ordered regardless. The event
// version is assigned by the event store on write (optimistic concurrency), not
// by the shard.
package pool

import (
	"sync"

	"github.com/char2cs/asynx/internal/processor/exec"
	"github.com/char2cs/asynx/internal/processor/models"
)

type Shard[T any] struct {
	id           int
	commandChan  chan *models.CommandEnvelope[T]
	jobQueue     chan *models.CommandJob[T]
	stopChan     chan struct{}
	stopMu       sync.Mutex
	stopClosed   bool
	onDispatched func()
}

func newShard[T any](
	id int,
	queueDepth int,
	workersPerShard int,
) *Shard[T] {
	return &Shard[T]{
		id:          id,
		commandChan: make(chan *models.CommandEnvelope[T], queueDepth),
		jobQueue:    make(chan *models.CommandJob[T], max(workersPerShard, queueDepth)),
		stopChan:    make(chan struct{}),
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
		s.executeJob(
			executor,
			job,
		)
	}
}

func (s *Shard[T]) executeJob(
	executor *exec.CommandExecutor[T],
	job *models.CommandJob[T],
) {
	// The worker is released once the event is committed; waiting for handlers
	// is the sender's job, so a slow or failing handler never holds a worker.
	event, done, err := executor.ExecuteAsync(
		job.Envelope.Ctx,
		job.Envelope.Cmd,
	)

	result := models.CommandResult[T]{Event: event, Err: err}
	if job.Envelope.WaitHandlers {
		result.Done = done
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

// ForTesting: SetOnDispatched sets a callback invoked each time the dispatcher
// reads a command from commandChan (slot is now free for new senders).
// Do not call in production code.
func (s *Shard[T]) SetOnDispatched(fn func()) {
	s.onDispatched = fn
}
