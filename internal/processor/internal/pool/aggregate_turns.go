package pool

import (
	"github.com/char2cs/asynx/internal/processor/models"
)

type aggregateTurns[T any] struct {
	shard       *Shard[T]
	aggregateID string
	job         *models.CommandJob[T]
}

func (t *aggregateTurns[T]) advance() {
	s := t.shard
	s.mailboxMu.Lock()
	defer s.mailboxMu.Unlock()
	mailbox := s.mailboxes[t.aggregateID]
	if len(mailbox) == 0 {
		delete(
			s.mailboxes,
			t.aggregateID,
		)
		t.job = nil
		return
	}

	t.job = mailbox[0]
	mailbox[0] = nil
	s.mailboxes[t.aggregateID] = mailbox[1:]
}
