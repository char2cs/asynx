package mockstores

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/storage"
	"testing"
)

type txProbe struct {
	t       *testing.T
	ctx     context.Context
	wantErr bool
}

func (p txProbe) run(
	tx storage.Tx,
) error {
	p.checkAppend(tx)
	p.checkDelete(tx)
	p.checkNoops(tx)

	return nil
}

func (p txProbe) checkAppend(
	tx storage.Tx,
) {
	err := tx.Append(
		p.ctx,
		"a",
		1,
		nil,
	)
	if (err != nil) != p.wantErr {
		p.t.Errorf(
			"Append err = %v",
			err,
		)
	}
}

func (p txProbe) checkDelete(
	tx storage.Tx,
) {
	err := tx.DeleteStream(
		p.ctx,
		"a",
	)
	if (err != nil) != p.wantErr {
		p.t.Errorf(
			"DeleteStream err = %v",
			err,
		)
	}
}

func (p txProbe) checkNoops(
	tx storage.Tx,
) {
	if tx.DropDeliveries(
		p.ctx,
		"a",
	) != nil ||
		tx.PutReceipt(
			p.ctx,
			"c",
			event.Receipt{},
		) != nil ||
		tx.EnqueueDelivery(
			p.ctx,
			"a",
			1,
		) != nil ||
		tx.Native() != nil {
		p.t.Error("no-op tx methods should succeed")
	}
}

func checkStubs(
	t *testing.T,
	s storage.Store,
	wantErr bool,
) {
	ctx := context.Background()
	_, found, rerr := s.GetReceipt(
		ctx,
		"c",
	)
	pending, perr := s.PendingDeliveries(
		ctx,
		1,
	)
	aerr := s.AckDelivery(
		ctx,
		"a",
		1,
	)
	if found ||
		pending != nil ||
		(rerr != nil) != wantErr ||
		(perr != nil) != wantErr ||
		(aerr != nil) != wantErr {
		t.Errorf(
			"receipt/delivery stubs: found=%v pending=%v errs=%v,%v,%v",
			found,
			pending,
			rerr,
			perr,
			aerr,
		)
	}
}

type atomicCase struct {
	name  string
	store storage.Store
}

func (c atomicCase) run(
	t *testing.T,
) {
	wantErr := c.name == "ErrStore"
	probe := txProbe{
		t:       t,
		ctx:     context.Background(),
		wantErr: wantErr,
	}

	err := c.store.Atomically(
		probe.ctx,
		probe.run,
	)
	if err != nil {
		t.Errorf(
			"Atomically = %v",
			err,
		)
	}

	checkStubs(
		t,
		c.store,
		wantErr,
	)
}

func TestMockStores_AtomicallyDelegatesAppendAndDelete(
	t *testing.T,
) {
	sentinel := errors.New("fail")
	stores := map[string]storage.Store{
		"Store":            &Store{},
		"ErrStore":         &ErrStore{Err: sentinel},
		"CorruptBlobStore": &CorruptBlobStore{},
	}
	for name, s := range stores {
		t.Run(
			name,
			atomicCase{name: name, store: s}.run,
		)
	}
}

func TestMockStores_DeleteBehaviour(
	t *testing.T,
) {
	ctx := context.Background()
	if err := (&Store{}).Delete(
		ctx,
		"a",
	); err != nil {
		t.Errorf(
			"Store.Delete = %v",
			err,
		)
	}

	if err := (&CorruptBlobStore{}).Delete(
		ctx,
		"a",
	); err != nil {
		t.Errorf(
			"CorruptBlobStore.Delete = %v",
			err,
		)
	}

	sentinel := errors.New("fail")
	if err := (&ErrStore{Err: sentinel}).Delete(
		ctx,
		"a",
	); !errors.Is(
		err,
		sentinel,
	) {
		t.Errorf(
			"ErrStore.Delete = %v",
			err,
		)
	}
}
