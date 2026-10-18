package models_test

import (
	"errors"
	"testing"

	"github.com/char2cs/asynx/models"
)

func TestSubscriberError_NamesTheSubscriberAndKeepsTheCause(t *testing.T) {
	cause := errors.New("billing down")
	err := error(&models.SubscriberError{Subscriber: "OrderUpdated#2", Err: cause})

	if got, want := err.Error(), "subscriber OrderUpdated#2: billing down"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(err, cause) {
		t.Fatal("errors.Is does not see the cause")
	}
	var sub *models.SubscriberError
	if !errors.As(err, &sub) || sub.Subscriber != "OrderUpdated#2" {
		t.Fatal("errors.As does not find the SubscriberError")
	}
}
