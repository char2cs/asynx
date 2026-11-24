package contracts

// JobDelivery is what a job asks of its delivery: the kind of delivery it
// wants and what the handlers and the acknowledgement do with it.
type JobDelivery interface {
	JobMode
	JobHandling
}
