package pool

import (
	"sync"

	"github.com/char2cs/asynx/internal/processor/models"
	asynxmd "github.com/char2cs/asynx/models"
)

// Shard is a queue of commands served by a dispatcher and a set of workers.
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

// CommandChan returns the channel commands are submitted on.
func (s *Shard[T]) CommandChan() chan *models.CommandEnvelope[T] {
	return s.commandChan
}

func (s *Shard[T]) dispatchCommands(
	dispatcherDoneWg *sync.WaitGroup,
) {
	defer dispatcherDoneWg.Done()

	stopped := false
	for !stopped {
		stopped = s.handleDispatch()
	}
}

func (s *Shard[T]) handleDispatch() bool {
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

	s.dispatchJob(envelope)
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

	s.sendResult(
		envelope.ResultChan,
		models.CommandResult[T]{Err: asynxmd.ErrShuttingDown},
	)
}

func (s *Shard[T]) dispatchJob(
	envelope *models.CommandEnvelope[T],
) {
	s.jobQueue <- &models.CommandJob[T]{
		Envelope: envelope,
	}
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
	if !s.claim(
		aggregateID,
		job,
	) {
		return
	}

	for job != nil {
		s.executeJob(job)
		job = s.next(aggregateID)
	}
}

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
		s.mailboxes[aggregateID] = append(
			mailbox,
			job,
		)
	}

	s.mailboxMu.Unlock()

	if isFull {
		s.sendResult(
			job.Envelope.ResultChan,
			models.CommandResult[T]{Err: asynxmd.ErrQueueFull},
		)
	}

	return false
}

func (s *Shard[T]) next(
	aggregateID string,
) *models.CommandJob[T] {
	s.mailboxMu.Lock()
	defer s.mailboxMu.Unlock()
	mailbox := s.mailboxes[aggregateID]
	if len(mailbox) == 0 {
		delete(
			s.mailboxes,
			aggregateID,
		)
		return nil
	}

	job := mailbox[0]
	mailbox[0] = nil
	s.mailboxes[aggregateID] = mailbox[1:]
	return job
}

func (s *Shard[T]) executeJob(
	job *models.CommandJob[T],
) {
	job.Envelope.GrantTurn()
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
func (s *Shard[T]) SetOnDispatched(
	fn func(),
) {
	s.onDispatched = fn
}
