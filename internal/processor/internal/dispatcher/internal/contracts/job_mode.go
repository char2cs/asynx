package contracts

// JobMode tells which kind of delivery a job asks for.
type JobMode interface {
	// Durable reports whether the job runs durable subscribers: it carries an
	// Ack or it is a deletion marker.
	Durable() bool

	// Acked reports whether the job carries an Ack. Only such a job is held
	// back by a park: refused while its aggregate is parked, dropped from the
	// queue when a park is imposed, and parking its aggregate when it gives
	// up. A deletion marker or redelivery with a nil Ack is durable but not
	// acked.
	Acked() bool

	// Forget reports whether the job is a deletion marker, which every durable
	// subscriber receives whatever its pattern.
	Forget() bool
}
