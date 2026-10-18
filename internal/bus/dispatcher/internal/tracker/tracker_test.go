package tracker_test

import (
	"context"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/tracker"
	asynxmd "github.com/char2cs/asynx/models"
)

func newJob(
	aggregateID string,
	version int64,
) jobs.Job[string] {
	request := asynxmd.DispatchRequest[string]{
		Event: asynxmd.Event[string]{AggregateID: aggregateID, Version: version},
	}
	return jobs.New(request, context.Background(), func() {})
}

func closed(
	ch <-chan struct{},
) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func idleReturns(
	tr tracker.Tracker[string],
) <-chan struct{} {
	returned := make(chan struct{})
	go func() {
		tr.Idle()
		close(returned)
	}()
	return returned
}

func TestTracker_ForUnknownEvent(t *testing.T) {
	tr := tracker.New[string]()

	if _, ok := tr.For("agg", 1); ok {
		t.Fatal("found an event that was never tracked")
	}
}

func TestTracker_ForLifecycle(t *testing.T) {
	tr := tracker.New[string]()
	job := newJob("agg", 1)
	tr.Track(job)

	ticket, ok := tr.For("agg", 1)
	if !ok {
		t.Fatal("tracked event not found")
	}
	if closed(ticket.Final) {
		t.Fatal("final closed before completion")
	}
	if _, ok := tr.For("agg", 2); ok {
		t.Fatal("found another version")
	}

	job.Complete(nil)

	if _, ok := tr.For("agg", 1); ok {
		t.Fatal("completed event still tracked")
	}
	if !closed(ticket.Final) {
		t.Fatal("ticket final not closed")
	}
}

func TestTracker_RetrackedEventReplacesKey(t *testing.T) {
	tr := tracker.New[string]()
	old := newJob("agg", 1)
	replacement := newJob("agg", 1)
	tr.Track(old)
	tr.Track(replacement)

	old.Complete(nil)

	ticket, ok := tr.For("agg", 1)
	if !ok {
		t.Fatal("the old job removed the job that replaced it")
	}
	if ticket.Final != replacement.Ticket().Final {
		t.Fatal("ticket is not the replacement's")
	}
	replacement.Complete(nil)
	if _, ok := tr.For("agg", 1); ok {
		t.Fatal("replacement still tracked after completion")
	}
}

func TestTracker_IdleWithNothingInFlight(t *testing.T) {
	tr := tracker.New[string]()

	select {
	case <-idleReturns(tr):
	case <-time.After(time.Second):
		t.Fatal("Idle blocked with nothing in flight")
	}
}

func TestTracker_IdleWaitsForJobsInFlight(t *testing.T) {
	tr := tracker.New[string]()
	first := newJob("a", 1)
	second := newJob("b", 1)
	tr.Track(first)
	tr.Track(second)
	returned := idleReturns(tr)

	first.Complete(nil)
	select {
	case <-returned:
		t.Fatal("Idle returned while a job was in flight")
	case <-time.After(50 * time.Millisecond):
	}

	second.Complete(nil)
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("Idle did not return after every job completed")
	}
}

func TestTracker_IdleIgnoresJobsTrackedAfterItStarted(t *testing.T) {
	tr := tracker.New[string]()
	early := newJob("a", 1)
	tr.Track(early)
	returned := idleReturns(tr)
	time.Sleep(20 * time.Millisecond)
	late := newJob("b", 1)
	tr.Track(late)

	early.Complete(nil)

	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("Idle waited for a job tracked after it was called")
	}
	late.Complete(nil)
}
