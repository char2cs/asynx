package domain

import "testing"

func TestOrderStatusConstants(t *testing.T) {
	if OrderPending != "Pending" {
		t.Errorf("OrderPending = %q, want %q", OrderPending, "Pending")
	}
	if OrderConfirmed != "Confirmed" {
		t.Errorf("OrderConfirmed = %q, want %q", OrderConfirmed, "Confirmed")
	}
	if OrderShipped != "Shipped" {
		t.Errorf("OrderShipped = %q, want %q", OrderShipped, "Shipped")
	}
}

func TestOrderStruct(t *testing.T) {
	order := Order{
		ID:     "ORD-001",
		Status: OrderPending,
		Total:  99.99,
		Items:  2,
		ShipTo: "123 Main St",
	}

	if order.ID != "ORD-001" {
		t.Errorf("ID = %q, want %q", order.ID, "ORD-001")
	}
	if order.Status != OrderPending {
		t.Errorf("Status = %q, want %q", order.Status, OrderPending)
	}
	if order.Total != 99.99 {
		t.Errorf("Total = %v, want %v", order.Total, 99.99)
	}
	if order.Items != 2 {
		t.Errorf("Items = %d, want %d", order.Items, 2)
	}
	if order.ShipTo != "123 Main St" {
		t.Errorf("ShipTo = %q, want %q", order.ShipTo, "123 Main St")
	}
}
