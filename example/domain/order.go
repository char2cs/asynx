package domain

// OrderStatus represents the lifecycle state of an order.
type OrderStatus string

const (
	OrderPending   OrderStatus = "Pending"
	OrderConfirmed OrderStatus = "Confirmed"
	OrderShipped   OrderStatus = "Shipped"
)

// Order is the aggregate root for the order bounded context.
type Order struct {
	ID     string      `json:"id"`
	Status OrderStatus `json:"status"`
	Total  float64     `json:"total"`
	Items  int         `json:"items"`
	ShipTo string      `json:"ship_to"`
}
