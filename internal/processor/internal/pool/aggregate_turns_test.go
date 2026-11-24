package pool

import (
	"testing"

	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/models"
)

func queuedJob(
	id string,
) *models.CommandJob[mocks.Order] {
	return &models.CommandJob[mocks.Order]{
		Envelope: models.NewCommandEnvelope[mocks.Order](mocks.CreateOrderCmd{ID: id, Total: 1}),
	}
}

func TestAggregateTurns_AdvanceHandsOverTheMailboxInArrivalOrder(
	t *testing.T,
) {
	s := newShard[mocks.Order](
		0,
		4,
		1,
	)
	first, second := queuedJob("a"), queuedJob("a")
	s.mailboxes["a"] = []*models.CommandJob[mocks.Order]{first, second}
	turns := &aggregateTurns[mocks.Order]{
		shard:       s,
		aggregateID: "a",
	}

	turns.advance()
	if turns.job != first {
		t.Fatal("advance did not hand over the oldest queued job")
	}

	turns.advance()
	if turns.job != second {
		t.Fatal("advance did not hand over the next queued job")
	}
}

func TestAggregateTurns_AdvanceOnAnEmptyMailboxEndsTheAggregatesRun(
	t *testing.T,
) {
	s := newShard[mocks.Order](
		0,
		4,
		1,
	)
	s.mailboxes["a"] = nil
	turns := &aggregateTurns[mocks.Order]{
		shard:       s,
		aggregateID: "a",
		job:         queuedJob("a"),
	}

	turns.advance()
	if turns.job != nil {
		t.Fatal("advance kept a job although the mailbox was empty")
	}

	if _, running := s.mailboxes["a"]; running {
		t.Fatal("the aggregate still counts as running after its mailbox emptied")
	}
}
