package pool

import (
	"sync"

	"github.com/char2cs/asynx/v2/internal/processor/models"
	"github.com/char2cs/asynx/v2/models/failure"
)

// Shard is one command queue of the pool. A single dispatcher goroutine reads
// envelopes from the command channel and hands them to the job queue; the
// shard's workers take jobs from it. All commands of one aggregate land on the
// same shard, and the shard keeps them serial: the first command of an
// aggregate is served by a worker, the ones that arrive meanwhile wait in the
// aggregate's mailbox, bounded by the queue depth (at least the worker count), and the same worker runs
// them in arrival order. Commands of different aggregates run in parallel on
// different workers. A worker grants a command its turn and waits for the
// sender to finish it (see models.CommandEnvelope), so it never runs a command
// itself. When the shard is told to stop, envelopes still queued are rejected
// with ErrShuttingDown and workers finish what they hold. Shards are created by
// the pool.
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
	mailboxes    map[string][]*models.CommandJob[T]
}

func newShard[T any](
	id int,
	queueDepth int,
	workersPerShard int,
) *Shard[T] {
	return &Shard[T]{
		id: id,
		commandChan: make(
			chan *models.CommandEnvelope[T],
			queueDepth,
		),
		jobQueue: make(
			chan *models.CommandJob[T],
			max(
				workersPerShard,
				queueDepth,
			),
		),
		stopChan: make(chan struct{}),
		mailboxLimit: max(
			queueDepth,
			workersPerShard,
		),
		mailboxes: make(map[string][]*models.CommandJob[T]),
	}
}

// CommandChan returns the buffered channel senders enqueue envelopes on. Its
// capacity is the shard's queue depth: a send that finds it full should fail
// with ErrQueueFull rather than wait, which is what the processor does. The
// channel is never closed; after the shard stops, nothing reads it, so a
// sender must not enqueue after the processor began shutting down.
func (s *Shard[T]) CommandChan() chan *models.CommandEnvelope[T] {
	return s.commandChan
}

func (s *Shard[T]) dispatchCommands(
	dispatcherDoneWg *sync.WaitGroup,
) {
	defer dispatcherDoneWg.Done()

	stopped := false
	for !stopped {
		stopped = s.dispatchNext()
	}
}

func (s *Shard[T]) dispatchNext() bool {
	select {
	case <-s.stopChan:
		s.failQueued()
		close(s.jobQueue)
		return true

	case envelope := <-s.commandChan:
		return s.accept(envelope)
	}
}

func (s *Shard[T]) accept(
	envelope *models.CommandEnvelope[T],
) bool {
	if envelope == nil {
		close(s.jobQueue)
		return true
	}

	s.jobQueue <- &models.CommandJob[T]{
		Envelope: envelope,
	}

	if s.onDispatched != nil {
		s.onDispatched()
	}

	return false
}

func (s *Shard[T]) failQueued() {
	more := true
	for more {
		more = s.failNext()
	}
}

func (s *Shard[T]) failNext() bool {
	select {
	case envelope := <-s.commandChan:
		s.failEnvelope(envelope)
		return true
	default:
		return false
	}
}

func (s *Shard[T]) failEnvelope(
	envelope *models.CommandEnvelope[T],
) {
	if envelope == nil {
		return
	}

	envelope.Reject(failure.ErrShuttingDown)
}

func (s *Shard[T]) workerLoop(
	doneWg *sync.WaitGroup,
) {
	defer doneWg.Done()

	for job := range s.jobQueue {
		s.serve(job)
	}
}

func (s *Shard[T]) serve(
	job *models.CommandJob[T],
) {
	aggregateID := job.Envelope.Cmd.AggregateID()
	s.mailboxMu.Lock()
	mailbox, running := s.mailboxes[aggregateID]
	if !running {
		s.mailboxes[aggregateID] = nil
		s.mailboxMu.Unlock()
		s.grantInTurn(
			aggregateID,
			job,
		)
		return
	}

	if len(mailbox) >= s.mailboxLimit {
		s.mailboxMu.Unlock()
		job.Envelope.Reject(failure.ErrQueueFull)
		return
	}

	s.mailboxes[aggregateID] = append(
		mailbox,
		job,
	)
	s.mailboxMu.Unlock()
}

func (s *Shard[T]) grantInTurn(
	aggregateID string,
	first *models.CommandJob[T],
) {
	turns := &aggregateTurns[T]{
		shard:       s,
		aggregateID: aggregateID,
		job:         first,
	}
	for turns.job != nil {
		turns.job.Envelope.GrantTurn()
		turns.advance()
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
// told to stop, so a test can wait for Drain to reach the shard. Do not call in
// production code.
func (s *Shard[T]) Stopping() <-chan struct{} { return s.stopChan }

// ForTesting: Backlog reports how many commands wait in aggregate mailboxes
// behind a running one, taken under the mailbox lock, so a test can tell that
// commands have queued. Do not call in production code.
func (s *Shard[T]) Backlog() int {
	s.mailboxMu.Lock()
	defer s.mailboxMu.Unlock()
	n := 0
	for _, mailbox := range s.mailboxes {
		n += len(mailbox)
	}

	return n
}

// ForTesting: SetOnDispatched sets a callback the shard's dispatcher invokes
// each time it moves a command off the command channel, meaning a queue slot is
// free for new senders. It is not synchronised: set it before the first
// command. Do not call in production code.
func (s *Shard[T]) SetOnDispatched(
	fn func(),
) {
	s.onDispatched = fn
}
