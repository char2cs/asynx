package mockstores

import (
	"context"
	"errors"
	asynxmd "github.com/char2cs/asynx/models"
	"testing"
)

func TestErrStore_Append_ReturnsErr(
	t *testing.T,
) {
	sentinel := errors.New("fail")
	es := &ErrStore{Err: sentinel}
	if err := es.Append(
		context.Background(),
		"agg1",
		1,
		nil,
	); !errors.Is(
		err,
		sentinel,
	) {
		t.Errorf(
			"Append = %v, want sentinel",
			err,
		)
	}
}

func TestErrStore_ReadFrom_ReturnsErr(
	t *testing.T,
) {
	sentinel := errors.New("fail")
	es := &ErrStore{Err: sentinel}
	_, err := es.ReadFrom(
		context.Background(),
		"agg1",
		1,
	)
	if !errors.Is(
		err,
		sentinel,
	) {
		t.Errorf(
			"ReadFrom = %v, want sentinel",
			err,
		)
	}
}

func TestErrStore_ReadRange_ReturnsErr(
	t *testing.T,
) {
	sentinel := errors.New("fail")
	es := &ErrStore{Err: sentinel}
	_, err := es.ReadRange(
		context.Background(),
		"agg1",
		1,
		10,
	)
	if !errors.Is(
		err,
		sentinel,
	) {
		t.Errorf(
			"ReadRange = %v, want sentinel",
			err,
		)
	}
}

func TestErrStore_ImplementsStore(
	t *testing.T,
) {
	var _ asynxmd.Store = &ErrStore{}
}
