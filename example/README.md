# Asynx Example: E-Commerce Order System

This example demonstrates how to use **asynx** for command processing, event sourcing, and event-driven architecture in a real-world e-commerce order system.

## Overview

The example implements a complete order lifecycle:
1. **Create Order** - New order entry with validation
2. **Confirm Order** - Order confirmation and warehouse notification
3. **Ship Order** - Order fulfillment and customer notification

## Project Structure

```
example/
├── main.go                 # Orchestration & entry point
├── bus.go                  # Event bus implementation
├── README.md               # This file
│
├── domain/                 # Domain Model (Aggregate & Value Objects)
│   └── order.go           # Order aggregate definition
│
├── commands/              # Command Definitions
│   └── commands.go        # All commands (Create, Confirm, Ship)
│
└── projections/           # Event Subscribers (Read Models & Side Effects)
    └── projections.go     # Reporters & Notifiers
```

## Architecture Components

### 1. Domain Model (`Order` Aggregate)

The `Order` struct represents the aggregate state:
```go
type Order struct {
    ID        string
    Status    OrderStatus    // Pending → Confirmed → Shipped
    Total     float64
    Items     int
    ShipTo    string
}
```

### 2. Commands

Commands drive state transitions. Each command:
- Defines what action the user wants to perform
- Implements validation against current state
- Specifies the event to emit

**Example: CreateOrderCmd**
```go
type CreateOrderCmd struct {
    ID     string
    Total  float64
    Items  int
    ShipTo string
}

// Validation ensures order doesn't already exist and amounts are positive
func (c CreateOrderCmd) Validate(current *Order) error {
    if c.Total <= 0 { return asynxmd.ErrValidation }
    if current != nil { return asynxmd.ErrValidation } // Already exists
    return nil
}

// Emit produces the new state
func (c CreateOrderCmd) EmitEvent(current *Order) Order {
    return Order{
        ID: c.ID,
        Status: OrderPending,
        Total: c.Total,
        // ...
    }
}
```

### 3. Event Bus

The `SimpleBus` implements `asynxmd.Bus` to publish events to subscribers:
```go
bus.Subscribe("*", reporter.Handle)   // All events → reporter
bus.Subscribe("*", notifier.Handle)   // All events → notifier
```

### 4. Projections (Event Handlers)

Projections react to events and update read models or trigger side effects:

**OrderReporter** - Prints events
```go
fmt.Printf("[EVENT] %s - Order %s: %s → %s\n",
    event.EventName,
    event.AggregateID,
    event.PreviousAggregate.Status,
    event.Aggregate.Status,
)
```

**OrderNotifier** - Maintains notification outbox
```go
case "OrderShipped":
    n.outbox = append(n.outbox, fmt.Sprintf(
        "📦 Order %s shipped! Customer notified.",
        event.Aggregate.ID,
    ))
```

## How It Works

### Setup Phase
```go
// Use the public asynx.Builder API to create an Asynx instance
ax, err := asynx.New[Order]().
    WithEventStore(memoryStore).           // Your data store
    WithSnapshotStore(memoryStore).        // Optional snapshot store
    WithShardingOpts(asynx.ShardingOpts{
        Shards: 4,  // Number of worker shards
    }).
    Build()
```

The Builder automatically:
- Creates an in-process event bus (ChannelBus)
- Sets up the event sourcing layer
- Configures the command processor with worker shards

### Execution Phase

**Subscribe to Events:**
```go
// Pattern matching uses regex
// "Order.*" matches: OrderCreated, OrderConfirmed, OrderShipped
ax.Subscribe("Order.*", reporter.Handle)
ax.Subscribe("Order.*", notifier.Handle)
```

**Submit Command:**
```go
cmd := CreateOrderCmd{
    ID:     "ORD-001",
    Total:  99.99,
    Items:  2,
    ShipTo: "123 Main St",
}

err := ax.Send(ctx, cmd)
```

**What Happens Inside asynx:**

1. **Processor routes** the command to a shard (deterministic by aggregate ID)
2. **Processor validates** the command against current state
3. **EventStore persists** the new event to storage
4. **Bus publishes** the event asynchronously
5. **Projections** react to events (send emails, update dashboards, etc.)

**Wait for Processing:**
```go
proc.Wait()  // Block until all async handlers complete
```

### Query Phase

```go
order, err := es.Get(ctx, "ORD-001")
// order.Status == OrderShipped (final state from all events)
```

## Key Concepts

### Commands
- **Request** a state change
- **Validate** business rules
- **Emit** events on success
- **Must be** idempotent (same command = same result)

### Events
- **Record** what happened
- **Immutable** (never changed)
- **Replayed** to reconstruct state
- **Published** for subscribers

### Aggregates
- **Root entity** for a bounded context (Order in this case)
- **Validates** commands
- **Applies** events to rebuild state
- **Guarantees** consistency within boundaries

### Projections
- **React** to events
- **Build** read models
- **Trigger** side effects (emails, notifications)
- **Eventually consistent** (not guaranteed immediate)

## Running the Example

```bash
cd example
go run .
```

**Important**: This example uses only the **public asynx API**:
- `asynx.New[T]()` - Builder for creating Asynx instances
- `asynx.ShardingOpts` - Configuration for sharding
- `models.Store`, `models.Bus`, `models.Command` - Public interfaces

The example uses the public `store` package for the in-memory store, which is provided for testing/example purposes.

**Expected Output:**
```
=== Asynx E-Commerce Order Example ===

Step 1: Setting up infrastructure...
Step 2: Setting up event subscribers...
Step 3: Executing commands...
✓ Order ORD-001 created
✓ Order ORD-002 created
✓ Order ORD-003 created

[EVENT] OrderCreated - Order ORD-001: Pending → Pending
[EVENT] OrderCreated - Order ORD-002: Pending → Pending
[EVENT] OrderCreated - Order ORD-003: Pending → Pending

✓ Order ORD-001 confirmed
✓ Order ORD-002 confirmed
✓ Order ORD-003 confirmed

[EVENT] OrderConfirmed - Order ORD-001: Pending → Confirmed
[EVENT] OrderConfirmed - Order ORD-002: Pending → Confirmed
[EVENT] OrderConfirmed - Order ORD-003: Pending → Confirmed

✓ Order ORD-001 shipped
✓ Order ORD-002 shipped
✓ Order ORD-003 shipped

[EVENT] OrderShipped - Order ORD-001: Confirmed → Shipped
[EVENT] OrderShipped - Order ORD-002: Confirmed → Shipped
[EVENT] OrderShipped - Order ORD-003: Confirmed → Shipped

Step 4: Verifying final state...
Order ORD-001:
{
  "id": "ORD-001",
  "status": "Shipped",
  "total": 99.99,
  "items": 2,
  "ship_to": "123 Main St"
}
...

Step 5: Notifications/Outbox:
1. 📧 Confirmation email sent to customer for order ORD-001
2. 🎉 Order ORD-001 confirmed! Warehouse notified.
3. 📦 Order ORD-001 shipped! Customer notified.
...

✅ Example completed successfully!
```

## Integration Patterns

### 1. Error Handling

```go
if err := proc.Submit(ctx, cmd); err != nil {
    if errors.Is(err, asynxmd.ErrValidation) {
        // Validation failed - show user error message
        return fmt.Errorf("invalid order data")
    }
    // Storage error or other issue
    return fmt.Errorf("failed to process order")
}
```

### 2. Event Replay

Rebuild aggregate state from scratch:
```go
var order Order
es.Replay(ctx, "ORD-001", 1, 0, func(ctx context.Context, e asynxmd.Event[Order]) {
    order = e.Aggregate
})
```

### 3. Snapshots

For high-volume aggregates, take snapshots to skip replaying many events:
```go
func (c ShipOrderCmd) ShouldSnapshot() bool { return true }
```

### 4. Concurrent Commands

asynx handles concurrent commands for the same aggregate:
```go
// These run serially (same shard), preventing conflicts
go proc.Submit(ctx, ConfirmOrderCmd{ID: "ORD-001"})
go proc.Submit(ctx, ShipOrderCmd{ID: "ORD-001"})
```

## What This Example Teaches

✅ How to define **Commands** with validation
✅ How to implement **Aggregates** that emit events
✅ How to set up **EventStore** for persistence
✅ How to publish events via **Bus**
✅ How to react to events with **Projections**
✅ How to handle **concurrent commands** safely
✅ How to query **current state** from events
✅ How to build **event-driven systems**

## Next Steps

1. **Replace `SimpleBus`** with a real message broker (Kafka, RabbitMQ)
2. **Add database storage** (PostgreSQL, MongoDB) via `Store` interface
3. **Implement projections** in separate services (read-only models)
4. **Add API layer** to expose commands via HTTP/gRPC
5. **Scale horizontally** by distributing shards across instances
