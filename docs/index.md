# Asynx

An event sourcing + CQRS framework for Go. Developers express intent through commands — Asynx handles events, state, and history automatically. Bring your infrastructure, Asynx handles the rest.

## Philosophy

Most event sourcing frameworks fail developers by making them think in events. Asynx flips that — developers think in commands, Asynx handles events internally. Events in Asynx are not something the developer defines or emits directly. They are the CDC (Change Data Capture) diff of an aggregate's state, produced automatically by Asynx after a command succeeds.

The "bring your own" philosophy extends beyond the store — the bus and the store are all pluggable interfaces. Asynx ships sensible defaults for each. Developers swap them out only when their infrastructure demands it.

Asynx makes no consistency guarantees beyond what your Store implementation provides. If you bring a strongly consistent store, you get strong consistency. If you bring an eventually consistent one, that's your choice and your responsibility.

## Getting Started

- **[Full Specification](spec/overview.md)** - Complete architecture and module documentation
- **GitHub** - [char2cs/asynx](https://github.com/char2cs/asynx)

## Key Concepts

- **Commands** - Pure functions that express intent. Developers implement validation and state transition logic.
- **Events** - Automatically generated CDC diffs of state changes. Never manually emitted by developers.
- **Aggregates** - Domain entities managed by Asynx. State is reconstructed by replaying events.
- **Projections** - Eventual-consistency callbacks that subscribe to events and build read models.
- **Event Store** - The single persistence boundary. Everything durable goes here.
- **Bus** - In-process or external event dispatcher. Pluggable for multi-node deployments.

## Quick Links

- [Module Architecture](spec/overview.md#module-map)
- [Command Interface](spec/overview.md#command)
- [Builder Configuration](spec/overview.md#builder-configuration)
- [Error Handling](spec/overview.md#error-surface)
