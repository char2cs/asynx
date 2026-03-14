// Package pool implements shard-based concurrent command execution.
//
// Shard[T] encapsulates dispatcher and worker coordination for a subset of aggregates.
// The dispatcher (dispatchCommands) is the sole owner of versionMap and decides
// command dispatch order. Workers (workerLoop) execute commands in parallel.
//   - incrementVersion   — Called by dispatcher during dispatch; always increments
//   - decrementVersion   — Called by dispatcher on corrections; only when workersPerShard == 1
//   - executeJob         — Called by workers; sends corrections only when workersPerShard == 1
//   - sendResult         — Non-blocking result send; drops if receiver gone
//
// Serial ordering per aggregate is guaranteed by consistent hashing at router level;
// each aggregate always routes to the same shard and dispatcher sequence.
package pool

import (
	"sync"

	"github.com/char2cs/asynx/internal/processor/exec"
	"github.com/char2cs/asynx/internal/processor/models"
	asynxmd "github.com/char2cs/asynx/models"
)

type Shard[T any] struct {
	id              int
	commandChan     chan *models.CommandEnvelope[T]
	jobQueue        chan *models.CommandJob[T]
	stopChan        chan struct{}
	stopMu          sync.Mutex
	stopClosed      bool
	versionMap      map[string]int64
	versionMutex    sync.Mutex
	correctionChan  chan *versionCorrection
	workersPerShard int
}

type versionCorrection struct {
	aggregateID string
}

func newShard[T any](
	id int,
	queueDepth int,
	workersPerShard int,
) *Shard[T] {
	return &Shard[T]{
		id:              id,
		commandChan:     make(chan *models.CommandEnvelope[T], queueDepth),
		jobQueue:        make(chan *models.CommandJob[T]),
		stopChan:        make(chan struct{}),
		versionMap:      make(map[string]int64),
		correctionChan:  make(chan *versionCorrection),
		workersPerShard: workersPerShard,
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

	case correction := <-s.correctionChan:
		if s.workersPerShard == 1 {
			s.decrementVersion(correction.aggregateID)
		}
	}

	return false
}

func (s *Shard[T]) dispatchJob(
	envelope *models.CommandEnvelope[T],
) {
	aggregateID := envelope.Cmd.AggregateID()
	nextVersion := s.incrementVersion(aggregateID)

	s.jobQueue <- &models.CommandJob[T]{
		Envelope:    envelope,
		NextVersion: nextVersion,
	}
}

func (s *Shard[T]) incrementVersion(
	aggregateID string,
) int64 {
	s.versionMutex.Lock()
	defer s.versionMutex.Unlock()

	nextVersion := s.versionMap[aggregateID] + 1
	s.versionMap[aggregateID] = nextVersion

	return nextVersion
}

func (s *Shard[T]) decrementVersion(
	aggregateID string,
) {
	s.versionMutex.Lock()
	defer s.versionMutex.Unlock()

	s.versionMap[aggregateID]--
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
	err := executor.Execute(
		job.Envelope.Ctx,
		job.Envelope.Cmd,
		job.NextVersion,
	)

	if err == asynxmd.ErrValidation && s.workersPerShard == 1 {
		s.sendCorrection(
			job.Envelope.Cmd.AggregateID(),
		)
	}

	s.sendResult(
		job.Envelope.ResultChan,
		err,
	)
}

func (s *Shard[T]) sendCorrection(
	aggregateID string,
) {
	select {
	case s.correctionChan <- &versionCorrection{aggregateID: aggregateID}:
	default:
	}
}

func (s *Shard[T]) sendResult(
	resultChan chan error,
	err error,
) {
	select {
	case resultChan <- err:
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
