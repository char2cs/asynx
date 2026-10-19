// Package aggregates is the dispatcher's per-aggregate registry: for every
// aggregate it knows of, one record holding the FIFO of queued jobs, the wake
// channel of the aggregate's worker, the halt channel that interrupts a delivery
// in progress, and the park (its generation, its cause and the versions it
// left undelivered).
//
// # Lock rule
//
// One mutex guards every record and the registry's own state (the generation
// counter and the closed flag). Every operation of Registry takes it once and
// does all its work under that single hold, so no other goroutine ever sees a
// half-changed record: a park, a lift, an enqueue or a retirement happens
// entirely before or entirely after another. There is no second lock. The jobs'
// MarkUnrun and Complete, and the Hooks, may run under the mutex: they must not
// call back into the Registry.
//
// A record lives while it has queued jobs, a worker, a park or undelivered
// versions; a lifted park or a retired worker leaves no empty record behind.
//
// # Park state table
//
// The code mirrors this table. "Durable" means a job whose Acked reports
// true; any other job is never held back by a park.
//
//	| State             | Event                                         | Next state    | Effect                                                                                       |
//	|-------------------|-----------------------------------------------|---------------|----------------------------------------------------------------------------------------------|
//	| Running           | Park(agg, cause, v...)                        | Parked(gen+1) | record cause; close halt; queued durable jobs leave memory, recorded undelivered, completed ErrDeliveryParked |
//	| Parked(g)         | Park(...)                                     | Parked(g)     | merge versions; keep first non-nil cause                                                     |
//	| Parked(g)         | Lift(agg, g)                                  | Running       | return and clear undelivered; clear cause; replace halt channel                              |
//	| Parked(g)         | Lift(agg, other)                              | Parked(g)     | no-op, lifted=false                                                                          |
//	| Parked(g)         | Enqueue durable                               | Parked(g)     | not queued; recorded; job unrun and completed ErrDeliveryParked                              |
//	| Running or Parked | Supersede                                     | Running       | drop queued durable jobs; clear record                                                       |
//	| Running           | worker: ParkIfCurrent(halt), halt still open  | Parked(gen+1) | as Park                                                                                      |
//	| any               | worker: ParkIfCurrent(halt), halt closed      | unchanged     | someone else already settled it                                                              |
//	| any               | Inspect                                       | unchanged     | read parked, generation, cause, undelivered; zero status unless parked                       |
//
// Generations come from one counter shared by every aggregate, so a park that
// is lifted and imposed again never repeats a generation a caller has listed.
package aggregates

// New returns an empty Registry. The hooks run under the registry mutex, when
// Enqueue gives an aggregate a worker (Spawn) and when it queues a job (Admit);
// either may be nil.
func New[J Item](
	hooks Hooks[J],
) Registry[J] {
	return &aggregateRegistry[J]{
		records: make(map[string]*record[J]),
		hooks:   hooks,
	}
}
