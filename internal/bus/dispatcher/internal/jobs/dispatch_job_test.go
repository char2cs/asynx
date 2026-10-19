package jobs_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	asynxmd "github.com/char2cs/asynx/models"
)

func TestJob_MarkSettledClosesSettledOnce(t *testing.T) {
	job, _ := newJob(t, eventRequest())
	ticket := job.Ticket()

	if closed(ticket.Settled) {
		t.Fatal("settled before MarkSettled")
	}
	job.MarkSettled()
	job.MarkSettled()

	if !closed(ticket.Settled) {
		t.Fatal("not settled after MarkSettled")
	}
	if ticket.Unrun() != nil {
		t.Fatalf("unrun = %v, want nil", ticket.Unrun())
	}
}

func TestJob_MarkUnrunRecordsReason(t *testing.T) {
	job, _ := newJob(t, eventRequest())
	ticket := job.Ticket()

	job.MarkUnrun(asynxmd.ErrDeliveryParked)

	if !closed(ticket.Settled) {
		t.Fatal("not settled after MarkUnrun")
	}
	if !errors.Is(ticket.Unrun(), asynxmd.ErrDeliveryParked) {
		t.Fatalf("unrun = %v", ticket.Unrun())
	}
}

func TestJob_SettleHappensOnce(t *testing.T) {
	job, _ := newJob(t, eventRequest())
	ticket := job.Ticket()

	job.MarkSettled()
	job.MarkUnrun(asynxmd.ErrDeliveryParked)

	if ticket.Unrun() != nil {
		t.Fatalf("a settled job was marked unrun: %v", ticket.Unrun())
	}
}

func TestJob_MarkUnrunFirstWins(t *testing.T) {
	job, _ := newJob(t, eventRequest())
	ticket := job.Ticket()

	job.MarkUnrun(asynxmd.ErrDeliveryParked)
	job.MarkSettled()
	job.MarkUnrun(errors.New("later"))

	if !errors.Is(ticket.Unrun(), asynxmd.ErrDeliveryParked) {
		t.Fatalf("unrun = %v", ticket.Unrun())
	}
}

func TestJob_CompleteRunsOnce(t *testing.T) {
	job, cancels := newJob(t, eventRequest())
	ticket := job.Ticket()
	var callbacks atomic.Int32
	job.OnComplete(func() { callbacks.Add(1) })
	first := errors.New("first")

	job.Complete(first)
	job.Complete(errors.New("second"))

	if !closed(ticket.Final) {
		t.Fatal("final not closed")
	}
	if !errors.Is(ticket.Err(), first) {
		t.Fatalf("err = %v, want first", ticket.Err())
	}
	if callbacks.Load() != 1 || cancels.Load() != 1 {
		t.Fatalf("callbacks=%d cancels=%d, want 1 and 1", callbacks.Load(), cancels.Load())
	}
	if job.Context().Err() == nil {
		t.Fatal("context not cancelled")
	}
}

func TestJob_CompleteWithoutCallback(t *testing.T) {
	job, _ := newJob(t, eventRequest())

	job.Complete(nil)

	if !closed(job.Ticket().Final) {
		t.Fatal("final not closed")
	}
}

func TestJob_ConcurrentCompleteRunsOnce(t *testing.T) {
	job, cancels := newJob(t, eventRequest())
	var callbacks atomic.Int32
	job.OnComplete(func() { callbacks.Add(1) })
	var wg sync.WaitGroup

	for range 16 {
		wg.Go(func() { job.Complete(nil) })
	}
	wg.Wait()

	if callbacks.Load() != 1 || cancels.Load() != 1 {
		t.Fatalf("callbacks=%d cancels=%d, want 1 and 1", callbacks.Load(), cancels.Load())
	}
}
