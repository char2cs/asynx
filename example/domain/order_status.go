package domain

// OrderStatus represents the lifecycle state of an order.
type OrderStatus string

const (
	OrderPending   OrderStatus = "Pending"
	OrderConfirmed OrderStatus = "Confirmed"
	OrderShipped   OrderStatus = "Shipped"
)
