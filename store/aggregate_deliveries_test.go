package store

import (
	"testing"

	"github.com/char2cs/asynx/v2/models/event"
)

func TestAggregateDeliveries_BelongMatchesOnlyItsAggregate(
	t *testing.T,
) {
	own := event.Delivery{AggregateID: "a-1"}
	other := event.Delivery{AggregateID: "a-2"}
	of := aggregateDeliveries("a-1")
	if !of.belong(own) ||
		of.belong(other) {
		t.Fatalf(
			"belong(own) = %v, belong(other) = %v; want true, false",
			of.belong(own),
			of.belong(other),
		)
	}
}
