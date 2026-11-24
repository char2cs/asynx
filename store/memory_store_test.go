package store

import (
	"context"
	"errors"
	asynxmd "github.com/char2cs/asynx/models"
	"sync"
	"sync/atomic"
	"testing"
)

func TestSetError_CausesNextAppendToFail(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()
	sentinel := errors.New("injected")

	s.SetError(
		"agg1",
		sentinel,
	)

	err := s.Append(
		ctx,
		"agg1",
		1,
		[]byte("data"),
	)
	if !errors.Is(
		err,
		sentinel,
	) {
		t.Fatalf(
			"err = %v, want sentinel",
			err,
		)
	}
}

func TestSetError_IsOneShot_SubsequentAppendSucceeds(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()

	s.SetError(
		"agg1",
		errors.New("once"),
	)

	s.Append(
		ctx,
		"agg1",
		1,
		[]byte("x"),
	)

	if err := s.Append(
		ctx,
		"agg1",
		2,
		[]byte("ok"),
	); err != nil {
		t.Errorf(
			"second Append after SetError: %v (want nil)",
			err,
		)
	}
}

func TestSetError_OnlyAffectsTargetAggregate(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()

	s.SetError(
		"agg1",
		errors.New("fail agg1"),
	)

	if err := s.Append(
		ctx,
		"agg2",
		1,
		[]byte("data"),
	); err != nil {
		t.Errorf(
			"Append to agg2 failed unexpectedly: %v",
			err,
		)
	}
}

func TestNew_ReturnsEmptyStore(
	t *testing.T,
) {
	s := New()
	blobs, err := s.ReadFrom(
		context.Background(),
		"agg1",
		0,
	)
	if err != nil {
		t.Fatalf(
			"ReadFrom: %v",
			err,
		)
	}

	if len(blobs) != 0 {
		t.Errorf(
			"expected empty store, got %d blobs",
			len(blobs),
		)
	}
}

func TestAppend_StoresBlob(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()

	if err := s.Append(
		ctx,
		"agg1",
		1,
		[]byte("data"),
	); err != nil {
		t.Fatalf(
			"Append: %v",
			err,
		)
	}

	blobs, _ := s.ReadFrom(
		ctx,
		"agg1",
		1,
	)
	if len(blobs) != 1 ||
		string(blobs[0]) != "data" {
		t.Errorf(
			"got %v, want [data]",
			blobs,
		)
	}
}

func TestAppend_DuplicateVersion_ReturnsErrPipelineFailed(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()

	s.Append(
		ctx,
		"agg1",
		1,
		[]byte("first"),
	)

	err := s.Append(
		ctx,
		"agg1",
		1,
		[]byte("second"),
	)
	if !errors.Is(
		err,
		asynxmd.ErrPipelineFailed,
	) {
		t.Errorf(
			"err = %v, want ErrPipelineFailed",
			err,
		)
	}
}

func TestAppend_DifferentAggregates_DoNotConflict(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()

	if err := s.Append(
		ctx,
		"agg1",
		1,
		[]byte("a"),
	); err != nil {
		t.Fatalf(
			"Append agg1: %v",
			err,
		)
	}

	if err := s.Append(
		ctx,
		"agg2",
		1,
		[]byte("b"),
	); err != nil {
		t.Fatalf(
			"Append agg2: %v",
			err,
		)
	}
}

func TestAppend_VersionsAreIndependentPerAggregate(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()

	appendVersions(
		t,
		s,
		3,
	)

	blobs, _ := s.ReadFrom(
		ctx,
		"agg1",
		1,
	)
	if len(blobs) != 3 {
		t.Errorf(
			"expected 3 blobs, got %d",
			len(blobs),
		)
	}
}

func TestReadFrom_ReturnsAllBlobsFromVersion(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()

	s.Append(
		ctx,
		"agg1",
		1,
		[]byte("v1"),
	)
	s.Append(
		ctx,
		"agg1",
		2,
		[]byte("v2"),
	)
	s.Append(
		ctx,
		"agg1",
		3,
		[]byte("v3"),
	)

	blobs, err := s.ReadFrom(
		ctx,
		"agg1",
		2,
	)
	if err != nil {
		t.Fatalf(
			"ReadFrom: %v",
			err,
		)
	}

	if len(blobs) != 2 {
		t.Fatalf(
			"expected 2 blobs, got %d",
			len(blobs),
		)
	}

	if string(blobs[0]) != "v2" ||
		string(blobs[1]) != "v3" {
		t.Errorf(
			"got %v, want [v2 v3]",
			blobs,
		)
	}
}

func TestReadFrom_FromZero_ReturnsAll(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()

	s.Append(
		ctx,
		"agg1",
		1,
		[]byte("v1"),
	)
	s.Append(
		ctx,
		"agg1",
		2,
		[]byte("v2"),
	)

	blobs, _ := s.ReadFrom(
		ctx,
		"agg1",
		0,
	)
	if len(blobs) != 2 {
		t.Errorf(
			"expected 2 blobs, got %d",
			len(blobs),
		)
	}
}

func TestReadFrom_UnknownAggregate_ReturnsEmpty(
	t *testing.T,
) {
	s := New()
	blobs, err := s.ReadFrom(
		context.Background(),
		"ghost",
		1,
	)
	if err != nil {
		t.Fatalf(
			"ReadFrom: %v",
			err,
		)
	}

	if len(blobs) != 0 {
		t.Errorf(
			"expected empty, got %d blobs",
			len(blobs),
		)
	}
}

func TestReadFrom_VersionBeyondAll_ReturnsEmpty(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()

	s.Append(
		ctx,
		"agg1",
		1,
		[]byte("v1"),
	)

	blobs, _ := s.ReadFrom(
		ctx,
		"agg1",
		99,
	)
	if len(blobs) != 0 {
		t.Errorf(
			"expected empty, got %d blobs",
			len(blobs),
		)
	}
}

func TestReadRange_ReturnsUpToCount(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()

	s.Append(
		ctx,
		"agg1",
		1,
		[]byte("v1"),
	)
	s.Append(
		ctx,
		"agg1",
		2,
		[]byte("v2"),
	)
	s.Append(
		ctx,
		"agg1",
		3,
		[]byte("v3"),
	)

	blobs, err := s.ReadRange(
		ctx,
		"agg1",
		1,
		2,
	)
	if err != nil {
		t.Fatalf(
			"ReadRange: %v",
			err,
		)
	}

	if len(blobs) != 2 {
		t.Fatalf(
			"expected 2 blobs, got %d",
			len(blobs),
		)
	}

	if string(blobs[0]) != "v1" ||
		string(blobs[1]) != "v2" {
		t.Errorf(
			"got %v, want [v1 v2]",
			blobs,
		)
	}
}

func TestReadRange_CountExceedsAvailable_ReturnsAll(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()

	s.Append(
		ctx,
		"agg1",
		1,
		[]byte("v1"),
	)
	s.Append(
		ctx,
		"agg1",
		2,
		[]byte("v2"),
	)

	blobs, err := s.ReadRange(
		ctx,
		"agg1",
		1,
		100,
	)
	if err != nil {
		t.Fatalf(
			"ReadRange: %v",
			err,
		)
	}

	if len(blobs) != 2 {
		t.Errorf(
			"expected 2 blobs, got %d",
			len(blobs),
		)
	}
}

func TestReadRange_FromMidRange(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()

	s.Append(
		ctx,
		"agg1",
		1,
		[]byte("v1"),
	)
	s.Append(
		ctx,
		"agg1",
		2,
		[]byte("v2"),
	)
	s.Append(
		ctx,
		"agg1",
		3,
		[]byte("v3"),
	)

	blobs, _ := s.ReadRange(
		ctx,
		"agg1",
		2,
		10,
	)
	if len(blobs) != 2 {
		t.Fatalf(
			"expected 2 blobs from v2, got %d",
			len(blobs),
		)
	}

	if string(blobs[0]) != "v2" {
		t.Errorf(
			"first blob = %q, want v2",
			blobs[0],
		)
	}
}

func TestReadRange_UnknownAggregate_ReturnsEmpty(
	t *testing.T,
) {
	s := New()
	blobs, err := s.ReadRange(
		context.Background(),
		"ghost",
		1,
		10,
	)
	if err != nil {
		t.Fatalf(
			"ReadRange: %v",
			err,
		)
	}

	if len(blobs) != 0 {
		t.Errorf(
			"expected empty, got %d blobs",
			len(blobs),
		)
	}
}

func TestReadRange_CountZero_ReturnsEmpty(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()

	s.Append(
		ctx,
		"agg1",
		1,
		[]byte("v1"),
	)

	blobs, err := s.ReadRange(
		ctx,
		"agg1",
		1,
		0,
	)
	if err != nil {
		t.Fatalf(
			"ReadRange: %v",
			err,
		)
	}

	if len(blobs) != 0 {
		t.Errorf(
			"expected empty for count=0, got %d blobs",
			len(blobs),
		)
	}
}

func TestAppend_OutOfOrder_ReadsInVersionOrder(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()

	s.Append(
		ctx,
		"agg1",
		3,
		[]byte("v3"),
	)
	s.Append(
		ctx,
		"agg1",
		1,
		[]byte("v1"),
	)
	s.Append(
		ctx,
		"agg1",
		2,
		[]byte("v2"),
	)

	blobs, err := s.ReadFrom(
		ctx,
		"agg1",
		1,
	)
	if err != nil {
		t.Fatalf(
			"ReadFrom: %v",
			err,
		)
	}

	if len(blobs) != 3 {
		t.Fatalf(
			"expected 3 blobs, got %d",
			len(blobs),
		)
	}

	if string(blobs[0]) != "v1" ||
		string(blobs[1]) != "v2" ||
		string(blobs[2]) != "v3" {
		t.Errorf(
			"got %v, want [v1 v2 v3]",
			blobs,
		)
	}
}

func TestAppend_CancelledContext_ReturnsError(
	t *testing.T,
) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.Append(
		ctx,
		"agg1",
		1,
		[]byte("data"),
	)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

func TestReadFrom_CancelledContext_ReturnsError(
	t *testing.T,
) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := s.ReadFrom(
		ctx,
		"agg1",
		1,
	)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

func TestReadRange_CancelledContext_ReturnsError(
	t *testing.T,
) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := s.ReadRange(
		ctx,
		"agg1",
		1,
		10,
	)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

func TestConcurrentAppend_UniqueVersions_AllSucceed(
	t *testing.T,
) {
	s := New()
	const n = 50

	var wg sync.WaitGroup
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go appendAsync(
			&wg,
			s,
			int64(i),
		)
	}

	wg.Wait()

	blobs, err := s.ReadFrom(
		context.Background(),
		"agg1",
		1,
	)
	if err != nil {
		t.Fatalf(
			"ReadFrom: %v",
			err,
		)
	}

	if len(blobs) != n {
		t.Errorf(
			"expected %d blobs after concurrent appends, got %d",
			n,
			len(blobs),
		)
	}
}

func appendVersions(
	t *testing.T,
	s Memory,
	n int64,
) {
	t.Helper()

	for i := int64(1); i <= n; i++ {
		appendVersion(
			t,
			s,
			i,
		)
	}
}

func appendVersion(
	t *testing.T,
	s Memory,
	i int64,
) {
	t.Helper()

	err := s.Append(
		context.Background(),
		"agg1",
		i,
		[]byte("x"),
	)
	if err != nil {
		t.Fatalf(
			"Append agg1 v%d: %v",
			i,
			err,
		)
	}
}

func appendAsync(
	wg *sync.WaitGroup,
	s Memory,
	version int64,
) {
	defer wg.Done()

	err := s.Append(
		context.Background(),
		"agg1",
		version,
		[]byte("data"),
	)
	if err != nil {
		return
	}
}

func countAppendSuccess(
	wg *sync.WaitGroup,
	success *atomic.Int64,
	s Memory,
) {
	defer wg.Done()

	err := s.Append(
		context.Background(),
		"agg1",
		1,
		[]byte("data"),
	)
	if err == nil {
		success.Add(1)
	}
}

func TestConcurrentAppend_SameVersion_ExactlyOneSucceeds(
	t *testing.T,
) {
	s := New()
	const n = 20

	var (
		wg      sync.WaitGroup
		success atomic.Int64
	)
	for range n {
		wg.Add(1)
		go countAppendSuccess(
			&wg,
			&success,
			s,
		)
	}

	wg.Wait()

	if success.Load() != 1 {
		t.Errorf(
			"expected exactly 1 success, got %d",
			success.Load(),
		)
	}
}

func TestDelete_RemovesAllEntries(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()

	s.Append(
		ctx,
		"agg1",
		1,
		[]byte("v1"),
	)
	s.Append(
		ctx,
		"agg1",
		2,
		[]byte("v2"),
	)

	if err := s.Delete(
		ctx,
		"agg1",
	); err != nil {
		t.Fatalf(
			"Delete: %v",
			err,
		)
	}

	blobs, err := s.ReadFrom(
		ctx,
		"agg1",
		1,
	)
	if err != nil {
		t.Fatalf(
			"ReadFrom after Delete: %v",
			err,
		)
	}

	if len(blobs) != 0 {
		t.Errorf(
			"expected 0 blobs after Delete, got %d",
			len(blobs),
		)
	}
}

func TestDelete_NonExistentAggregate_NoError(
	t *testing.T,
) {
	s := New()
	if err := s.Delete(
		context.Background(),
		"ghost",
	); err != nil {
		t.Errorf(
			"Delete on non-existent aggregate: %v (want nil)",
			err,
		)
	}
}

func TestDelete_DoesNotAffectOtherAggregates(
	t *testing.T,
) {
	s := New()
	ctx := context.Background()

	s.Append(
		ctx,
		"agg1",
		1,
		[]byte("v1"),
	)
	s.Append(
		ctx,
		"agg2",
		1,
		[]byte("v2"),
	)

	s.Delete(
		ctx,
		"agg1",
	)

	blobs, err := s.ReadFrom(
		ctx,
		"agg2",
		1,
	)
	if err != nil {
		t.Fatalf(
			"ReadFrom agg2: %v",
			err,
		)
	}

	if len(blobs) != 1 {
		t.Errorf(
			"expected 1 blob for agg2, got %d",
			len(blobs),
		)
	}
}

func TestDelete_CancelledContext_ReturnsError(
	t *testing.T,
) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := s.Delete(
		ctx,
		"agg1",
	); err == nil {
		t.Error("expected error for cancelled context, got nil")
	}
}

func TestReads_RejectCancelledContext(
	t *testing.T,
) {
	s := New()
	dead, cancel := context.WithCancel(context.Background())
	cancel()

	if _, _, err := s.GetReceipt(
		dead,
		"c",
	); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Errorf(
			"GetReceipt err = %v",
			err,
		)
	}

	if _, err := s.PendingDeliveries(
		dead,
		1,
	); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Errorf(
			"PendingDeliveries err = %v",
			err,
		)
	}

	if err := s.AckDelivery(
		dead,
		"a",
		1,
	); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Errorf(
			"AckDelivery err = %v",
			err,
		)
	}

	if err := s.Delete(
		dead,
		"a",
	); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Errorf(
			"Delete err = %v",
			err,
		)
	}
}

func TestReads_FailOnACancelledContext(
	t *testing.T,
) {
	s := New()
	dead, cancel := context.WithCancel(context.Background())
	cancel()

	if _, _, err := s.GetReceipt(
		dead,
		"c",
	); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Errorf(
			"GetReceipt = %v, want context.Canceled",
			err,
		)
	}

	if _, err := s.PendingDeliveries(
		dead,
		1,
	); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Errorf(
			"PendingDeliveries = %v, want context.Canceled",
			err,
		)
	}

	if err := s.AckDelivery(
		dead,
		"a",
		1,
	); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Errorf(
			"AckDelivery = %v, want context.Canceled",
			err,
		)
	}
}

func TestGetReceipt_ReturnsWhatWasStored(
	t *testing.T,
) {
	ctx := context.Background()
	s := New()
	put := func(
		tx asynxmd.Tx,
	) error {
		return tx.PutReceipt(
			ctx,
			"c1",
			asynxmd.Receipt{Accepted: true},
		)
	}
	err := s.Atomically(
		ctx,
		put,
	)
	if err != nil {
		t.Fatal(err)
	}

	r, found, err := s.GetReceipt(
		ctx,
		"c1",
	)
	isStored := found && r.Accepted
	if err != nil ||
		!isStored {
		t.Fatalf(
			"GetReceipt = %+v, %v, %v",
			r,
			found,
			err,
		)
	}

	if _, found, _ := s.GetReceipt(
		ctx,
		"other",
	); found {
		t.Fatal("found a receipt that was never stored")
	}
}

func TestAckDelivery_RemovesOnlyTheNamedEntry(
	t *testing.T,
) {
	ctx := context.Background()
	s := New()
	enqueue := func(
		tx asynxmd.Tx,
	) error {
		return errors.Join(
			tx.EnqueueDelivery(
				ctx,
				"a",
				1,
			),
			tx.EnqueueDelivery(
				ctx,
				"a",
				2,
			),
		)
	}
	err := s.Atomically(
		ctx,
		enqueue,
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.AckDelivery(
		ctx,
		"a",
		1,
	); err != nil {
		t.Fatal(err)
	}

	if err := s.AckDelivery(
		ctx,
		"a",
		9,
	); err != nil {
		t.Fatalf(
			"acking an unknown entry = %v, want nil",
			err,
		)
	}

	pending, _ := s.PendingDeliveries(
		ctx,
		10,
	)
	if len(pending) != 1 ||
		pending[0].Version != 2 {
		t.Fatalf(
			"pending = %v, want only a v2",
			pending,
		)
	}
}
