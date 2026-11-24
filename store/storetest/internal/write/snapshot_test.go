package write_test

import (
	"context"
	"github.com/char2cs/asynx/v2/store"
	"github.com/char2cs/asynx/v2/store/storetest/internal/write"
	"testing"
)

func payloadOfWriter(
	writer int,
) []byte {
	return []byte{byte('a' + writer)}
}

func TestSnapshotPuts_WriterStoresItsPayloadAtItsOwnVersion(
	t *testing.T,
) {
	s := store.NewSnapshots()
	put := write.SnapshotPuts(
		s,
		"agg",
		payloadOfWriter,
	)

	err := put(2)
	if err != nil {
		t.Fatalf(
			"put = %v, want nil",
			err,
		)
	}

	got, found, err := s.Get(
		context.Background(),
		"agg",
	)
	if err != nil ||
		!found ||
		string(got) != "c" {
		t.Fatalf(
			"Get = %q, %v, %v; want the payload of writer 2",
			got,
			found,
			err,
		)
	}
}
