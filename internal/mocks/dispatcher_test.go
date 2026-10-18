package mocks

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/char2cs/asynx/models"
)

type fakeOrder struct{}

func TestDispatcher_EnqueueRecordsTheRequestAndSettlesTheTicket(t *testing.T) {
	d := NewDispatcher[fakeOrder]()
	request := models.DispatchRequest[fakeOrder]{Event: models.Event[fakeOrder]{AggregateID: "a", Version: 1}}

	ticket, err := d.Enqueue(context.Background(), request)
	if err != nil {
		t.Fatalf("Enqueue = %v", err)
	}
	<-ticket.Settled
	<-ticket.Final
	if ticket.Err() != nil || ticket.Unrun() != nil {
		t.Errorf("ticket = (%v, %v), want settled clean", ticket.Err(), ticket.Unrun())
	}
	if got := d.Requests(); len(got) != 1 || got[0].Event.Version != 1 {
		t.Errorf("Requests = %v, want the one request", got)
	}
}

func TestDispatcher_FailEnqueueRefusesAndRecordsNothing(t *testing.T) {
	d := NewDispatcher[fakeOrder]()
	sentinel := errors.New("enqueue failed")
	d.FailEnqueue(sentinel)

	if _, err := d.Enqueue(context.Background(), models.DispatchRequest[fakeOrder]{}); !errors.Is(err, sentinel) {
		t.Fatalf("Enqueue = %v, want the sentinel", err)
	}
	if len(d.Requests()) != 0 {
		t.Errorf("Requests = %v, want none", d.Requests())
	}

	d.FailEnqueue(nil)
	if _, err := d.Enqueue(context.Background(), models.DispatchRequest[fakeOrder]{}); err != nil {
		t.Errorf("Enqueue after restoring = %v", err)
	}
}

func TestDispatcher_FactoryKeepsTheConfigAndReturnsTheFake(t *testing.T) {
	d := NewDispatcher[fakeOrder]()

	got, err := d.Factory()(models.DispatcherConfig[fakeOrder]{MaxAttempts: 7})
	if err != nil || got != models.Dispatcher[fakeOrder](d) {
		t.Fatalf("Factory = (%v, %v), want the fake", got, err)
	}
	if d.Config().MaxAttempts != 7 {
		t.Errorf("Config = %+v, want MaxAttempts 7", d.Config())
	}
}

func TestDispatcher_ParkResumeSupersedeFollowTheContract(t *testing.T) {
	d := NewDispatcher[fakeOrder]()

	d.Park("a", 3, 4)
	d.Park("a")
	gen := d.Parks()["a"]
	if gen == 0 {
		t.Fatal("a is not parked")
	}
	if lifted, _ := d.ResumeUndelivered("a", gen+1); lifted {
		t.Error("a stale generation lifted the park")
	}
	if lifted, _ := d.ResumeUndelivered("a", gen); !lifted {
		t.Error("the current generation did not lift the park")
	}

	d.Park("b")
	d.Supersede("b")
	if len(d.Parks()) != 0 {
		t.Errorf("Parks = %v, want none", d.Parks())
	}
	if calls := d.ParkCalls(); len(calls) != 3 || !slices.Equal(calls[0].Undelivered, []int64{3, 4}) {
		t.Errorf("ParkCalls = %v", calls)
	}
}

func TestDispatcher_InterruptAndCloseAreRecordedInOrder(t *testing.T) {
	d := NewDispatcher[fakeOrder]()
	sentinel := errors.New("close failed")
	d.FailClose(sentinel)

	d.Interrupt()
	select {
	case <-d.Interrupted():
	default:
		t.Fatal("Interrupted is not closed after Interrupt")
	}
	if err := d.Close(context.Background()); !errors.Is(err, sentinel) {
		t.Errorf("Close = %v, want the sentinel", err)
	}
	d.WaitIdle()
	if got := d.Calls(); !slices.Equal(got, []string{"Interrupt", "Close", "WaitIdle"}) {
		t.Errorf("Calls = %v", got)
	}
}

func TestDispatcher_TheOtherQueriesAnswerNothing(t *testing.T) {
	d := NewDispatcher[fakeOrder]()

	if _, ok := d.TicketFor("a", 1); ok {
		t.Error("TicketFor found a ticket")
	}
	if d.Unrun("a", 1) != nil || d.InHandler(context.Background(), "a") {
		t.Error("Unrun or InHandler answered something")
	}
}
