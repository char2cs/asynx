package domain

// OrderStatus represents the lifecycle state of an order.
type OrderStatus string

const (
	// OrderPending is the status of an order that was created and not yet confirmed.
	OrderPending OrderStatus = "Pending"
	// OrderConfirmed is the status of an order that was confirmed.
	OrderConfirmed OrderStatus = "Confirmed"
	// OrderShipped is the status of an order that was shipped.
	OrderShipped OrderStatus = "Shipped"
)
