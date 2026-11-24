package asynx

import (
	"testing"

	"github.com/char2cs/asynx/models"
)

func TestEventName_MatchedByAsksTheSubscribersMatcher(
	t *testing.T,
) {
	sub := models.DurableSub[int]{
		Match: exactPattern("OrderCreated").matches,
	}
	if !eventName[int]("OrderCreated").matchedBy(sub) {
		t.Error("OrderCreated not matched by its own subscriber")
	}

	if eventName[int]("OrderShipped").matchedBy(sub) {
		t.Error("OrderShipped matched by an OrderCreated subscriber")
	}
}
