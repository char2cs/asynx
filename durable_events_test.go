package asynx

import (
	"testing"
)

func TestDurableEvents_WantsAnEventAnySubscriberMatches(
	t *testing.T,
) {
	subs := durableEvents[int]{
		{Match: exactPattern("OrderCreated").matches},
		{Match: exactPattern("OrderShipped").matches},
	}
	if !subs.wants("OrderShipped") {
		t.Error("wants(OrderShipped) = false, want true")
	}

	if subs.wants("OrderCancelled") {
		t.Error("wants(OrderCancelled) = true, want false")
	}
}

func TestDurableEvents_NoSubscribersWantNothing(
	t *testing.T,
) {
	none := durableEvents[int]{}
	if none.wants("OrderCreated") {
		t.Error("wants on no subscribers = true, want false")
	}
}
