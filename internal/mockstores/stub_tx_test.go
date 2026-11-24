package mockstores

import (
	"context"
	"errors"
	asynxmd "github.com/char2cs/asynx/models"
	"testing"
)

func TestMockStores_AtomicallyDelegatesAppendAndDelete(
	t *testing.T,
) {
	ctx := context.Background()
	sentinel := errors.New("fail")

	for name, s := range map[string]asynxmd.Store{
		"Store":            &Store{},
		"ErrStore":         &ErrStore{Err: sentinel},
		"CorruptBlobStore": &CorruptBlobStore{},
	} {
		t.Run(
			name,
			func(
				t *testing.T,
			) {
				wantErr := name == "ErrStore"
				err := s.Atomically(
					ctx,
					func(
						tx asynxmd.Tx,
					) error {
						if err := tx.Append(
							ctx,
							"a",
							1,
							nil,
						); (err != nil) != wantErr {
							t.Errorf(
								"Append err = %v",
								err,
							)
						}

						if err := tx.DeleteStream(
							ctx,
							"a",
						); (err != nil) != wantErr {
							t.Errorf(
								"DeleteStream err = %v",
								err,
							)
						}

						if tx.DropDeliveries(
							ctx,
							"a",
						) != nil ||
							tx.PutReceipt(
								ctx,
								"c",
								asynxmd.Receipt{},
							) != nil ||
							tx.EnqueueDelivery(
								ctx,
								"a",
								1,
							) != nil ||
							tx.Native() != nil {
							t.Error("no-op tx methods should succeed")
						}

						return nil
					},
				)
				if err != nil {
					t.Errorf(
						"Atomically = %v",
						err,
					)
				}

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
			},
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
