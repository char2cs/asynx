package storetest

import (
	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store/storetest/internal/race"
	"github.com/char2cs/asynx/store/storetest/internal/write"
	"testing"
)

const (
	secondVersion = 2
	thirdVersion  = 3
)

func appendCommitsAndReads(
	t testing.TB,
	s models.Store,
) {
	f := newFixture(
		t,
		s,
	)
	f.commit(write.Blobs(
		"s1",
		"a",
		"b",
		"c",
	)...)

	f.wantBlobsFrom(
		"s1",
		secondVersion,
		"b",
		"c",
	)
}

func readRangeLimits(
	t testing.TB,
	s models.Store,
) {
	const windowSize = 2
	f := newFixture(
		t,
		s,
	)
	f.commit(write.Blobs(
		"s1",
		"1",
		"2",
		"3",
		"4",
	)...)

	f.wantBlobsInRange(
		"s1",
		secondVersion,
		windowSize,
		"2",
		"3",
	)
}

func deleteStream(
	t testing.TB,
	s models.Store,
) {
	f := newFixture(
		t,
		s,
	)
	f.commit(write.Blobs(
		"s1",
		"a",
	)...)
	f.commit(write.Blobs(
		"s2",
		"b",
	)...)
	f.commit(
		write.EraseStream("s1"),
		write.EraseStream("never-existed"),
	)

	f.wantBlobsFrom(
		"s1",
		1,
	)
	f.wantBlobsFrom(
		"s2",
		1,
		"b",
	)
	f.commit(write.Blobs(
		"s1",
		"again",
	)...)
	f.wantBlobsFrom(
		"s1",
		1,
		"again",
	)
}

func readsOfUnknown(
	t testing.TB,
	s models.Store,
) {
	const anyCount = 5
	f := newFixture(
		t,
		s,
	)
	f.wantBlobsFrom(
		"nope",
		1,
	)
	f.wantBlobsInRange(
		"nope",
		1,
		anyCount,
	)
}

func committedVisible(
	t testing.TB,
	s models.Store,
) {
	f := newFixture(
		t,
		s,
	)
	f.commit(write.Blob(
		"s1",
		1,
		[]byte("a"),
	))
	f.commit(write.Blob(
		"s1",
		secondVersion,
		[]byte("b"),
	))

	f.wantBlobsFrom(
		"s1",
		1,
		"a",
		"b",
	)
}

func outOfOrderAppends(
	t testing.TB,
	s models.Store,
) {
	f := newFixture(
		t,
		s,
	)
	for _, version := range []int64{thirdVersion, 1, secondVersion} {
		f.commit(write.Blob(
			"s1",
			version,
			[]byte{byte('0' + version)},
		))
	}

	f.wantBlobsFrom(
		"s1",
		1,
		"1",
		"2",
		"3",
	)
	f.wantBlobsFrom(
		"s1",
		secondVersion,
		"2",
		"3",
	)
	f.wantBlobsInRange(
		"s1",
		secondVersion,
		1,
		"2",
	)
}

func concurrentSameVersion(
	t testing.TB,
	s models.Store,
) {
	const writers = 16
	f := newFixture(
		t,
		s,
	)
	writeVersionOne := func(
		i int,
	) error {
		return f.try(write.Blob(
			"s1",
			1,
			[]byte{byte(i)},
		))
	}

	results := race.All(
		writers,
		writeVersionOne,
	)

	f.wantOneWinner(results)
	if blobs := f.blobs(
		"s1",
		1,
	); len(blobs) != 1 {
		t.Fatalf(
			"stream holds %d records, want 1",
			len(blobs),
		)
	}
}

func concurrentDistinctVersions(
	t testing.TB,
	s models.Store,
) {
	const writers = 20
	f := newFixture(
		t,
		s,
	)
	writeOwnVersion := func(
		i int,
	) error {
		version := int64(i + 1)
		return f.try(
			write.Blob(
				"s1",
				version,
				[]byte{byte(version)},
			),
			write.Delivery(deliveryOf(
				"s1",
				version,
			)),
		)
	}

	results := race.All(
		writers,
		writeOwnVersion,
	)

	f.wantAllCommitted(results)
	f.wantVersionsInOrder(
		"s1",
		writers,
	)
	f.wantPendingCount(writers)
}

func appendScenarios() map[string]scenario[models.Store] {
	return map[string]scenario[models.Store]{
		"AppendCommitsAndReadFromReturnsInOrder":       appendCommitsAndReads,
		"ReadRangeLimitsCount":                         readRangeLimits,
		"DeleteStreamIsIdempotentAndScoped":            deleteStream,
		"ReadsOfUnknownStreamAreEmpty":                 readsOfUnknown,
		"CommitInsideAtomicallyIsVisibleAfterwards":    committedVisible,
		"AppendsOutOfVersionOrderReadInOrder":          outOfOrderAppends,
		"ConcurrentAppendsOfOneVersionAdmitOne":        concurrentSameVersion,
		"ConcurrentAppendsOfDistinctVersionsAllCommit": concurrentDistinctVersions,
	}
}
