package dispatcher

import (
	"context"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

func TestWithIdleTimeout_IgnoresNonPositiveValues(
	t *testing.T,
) {
	d := newDispatcher(asynxmd.DispatcherConfig[int]{}, WithIdleTimeout[int](0), WithIdleTimeout[int](-time.Second))
	defer d.Close(context.Background())
	if d.idleTimeout != defaultIdleTimeout {
		t.Fatalf("idleTimeout = %v, want the default", d.idleTimeout)
	}
}

func TestWithIdleTimeout_SetsAPositiveValue(
	t *testing.T,
) {
	d := newDispatcher(asynxmd.DispatcherConfig[int]{}, WithIdleTimeout[int](time.Minute))
	defer d.Close(context.Background())
	if d.idleTimeout != time.Minute {
		t.Fatalf("idleTimeout = %v, want 1m", d.idleTimeout)
	}
}
