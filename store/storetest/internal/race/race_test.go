package race

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

const timeout = 5 * time.Second

type laneErrors []error

func (l laneErrors) errorOfLane(
	i int,
) error {
	return l[i]
}

type meetingPoint struct {
	arrivals sync.WaitGroup
	done     chan struct{}
}

func newMeetingPoint(
	lanes int,
) *meetingPoint {
	m := &meetingPoint{done: make(chan struct{})}
	m.arrivals.Add(lanes)
	return m
}

func (m *meetingPoint) waitForEveryLane(
	int,
) error {
	m.arrivals.Done()
	m.arrivals.Wait()
	return nil
}

func (m *meetingPoint) runLanes(
	lanes int,
) {
	All(
		lanes,
		m.waitForEveryLane,
	)
	close(m.done)
}

type unexpectedLaneRun struct{ t *testing.T }

func (u unexpectedLaneRun) fail(
	int,
) error {
	u.t.Error("run was called for zero lanes")
	return nil
}

func TestAll_ReturnsEachLanesErrorByIndex(
	t *testing.T,
) {
	errThird := errors.New("third")
	want := laneErrors{nil, nil, errThird}

	got := All(
		3,
		want.errorOfLane,
	)
	if !slices.Equal(
		got,
		want,
	) {
		t.Fatalf(
			"All = %v, want the error of each lane at its own index",
			got,
		)
	}
}

func TestAll_RunsTheLanesConcurrently(
	t *testing.T,
) {
	const lanesCount = 8
	meeting := newMeetingPoint(lanesCount)
	go meeting.runLanes(lanesCount)

	select {
	case <-meeting.done:
	case <-time.After(timeout):
		t.Fatal("the lanes never all ran at once, so they did not overlap")
	}
}

func TestAll_ZeroLanesReturnsNothing(
	t *testing.T,
) {
	unexpected := unexpectedLaneRun{t}

	if got := All(
		0,
		unexpected.fail,
	); len(got) != 0 {
		t.Fatalf(
			"All(0) = %v, want no results",
			got,
		)
	}
}
