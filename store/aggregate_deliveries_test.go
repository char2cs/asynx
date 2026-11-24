package store

import (
	"testing"

	"github.com/char2cs/asynx/models"
)

func TestAggregateDeliveries_BelongMatchesOnlyItsAggregate(
	t *testing.T,
) {
	own := models.Delivery{AggregateID: "a-1"}
	other := models.Delivery{AggregateID: "a-2"}
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
