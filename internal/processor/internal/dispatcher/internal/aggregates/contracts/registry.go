package contracts

// Registry holds one record per aggregate under one mutex: the FIFO of the
// jobs queued for the aggregate, the chain of turns that runs them one at a
// time, the halt channel that interrupts a delivery in progress, and the park
// (its generation, its cause and the versions it left undelivered).
//
// # Lock rule
//
// One mutex guards every record and the registry's own state (the generation
// counter and the closed flag). Every method takes it once, changes the
// record entirely under that hold and releases it, so no other goroutine ever
// sees a half-changed record: a park, a lift, an enqueue or a finish happens
// entirely before or entirely after another. There is no second lock. The
// jobs' MarkUnrun and Complete, and the hooks, may run under the mutex, and
// must not call back into the Registry. Because every queued job may be
// completed under it, a completion callback, such as the tracker's, takes
// only leaf locks.
//
// A record lives while it has jobs in flight, a park or undelivered versions;
// a lifted park or a finished turn leaves no empty record behind. Generations
// come from one counter shared by every aggregate, so a park that is lifted
// and imposed again never repeats a generation a caller has listed.
//
// # Park state table
//
// The code mirrors this table. A durable job is one whose Acked reports true;
// any other job is never held back by a park.
//
//	| State             | Event                                      | Next state    | Effect                                                                                   |
//	|-------------------|--------------------------------------------|---------------|------------------------------------------------------------------------------------------|
//	| Running           | Park(agg, cause, v...)                     | Parked(gen+1) | record cause; close halt; queued durable jobs leave memory, recorded undelivered, completed ErrDeliveryParked |
//	| Parked(g)         | Park(...)                                  | Parked(g)     | merge versions; keep first non-nil cause                                                 |
//	| Parked(g)         | Lift(agg, g)                               | Running       | return and clear undelivered; clear cause; replace halt channel                          |
//	| Parked(g)         | Lift(agg, other)                           | Parked(g)     | no-op, lifted=false                                                                      |
//	| Parked(g)         | Enqueue durable                            | Parked(g)     | not queued; recorded; job unrun and completed ErrDeliveryParked                          |
//	| Running or Parked | Supersede                                  | Running       | drop queued durable jobs; clear record                                                   |
//	| Running           | ParkIfCurrent(halt), halt still open       | Parked(gen+1) | as Park                                                                                  |
//	| any               | ParkIfCurrent(halt), halt closed           | unchanged     | someone else already settled it                                                          |
//	| any               | Inspect                                    | unchanged     | read parked, generation, cause, undelivered; zero status unless parked                   |
//
// Enqueue and Lift are commands that write what they found into a value the
// caller passes (an Admission, a LiftOutcome), in the same hold of the mutex
// that changed the record, because a separate query could be overtaken between
// the two calls. Begin, SettleIfParked and the park methods are single
// test-and-sets under the mutex for the same reason.
type Registry[J any] interface {
	Backlog[J]
	Parking[J]
	Winddown
}
