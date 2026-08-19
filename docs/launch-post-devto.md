---
title: "Event-Driven Architecture in Go: 7 patterns that actually survive production"
published: false
description: "Outbox, inbox, event sourcing, sagas and JetStream — distilled from a real reference implementation, with a runnable example."
tags: go, microservices, architecture, eventdriven
canonical_url: https://github.com/reinaldosaraiva/eda-golang
cover_image:
---

# Event-Driven Architecture in Go: 7 patterns that actually survive production

I recently went deep into the codebase of
[*Event-Driven Architecture in Golang*](https://github.com/PacktPublishing/Event-Driven-Architecture-in-Golang)
(Michael Stack, Packt) — the "MallBots" reference app that evolves, chapter by
chapter, from a modular monolith into event-sourced microservices on NATS
JetStream and Postgres.

Instead of writing yet another "what is EDA" post, I distilled the patterns
that actually hold up — the ones I'd adopt, review for, and teach — into a
community repository with a **runnable example**:

👉 **[github.com/reinaldosaraiva/eda-golang](https://github.com/reinaldosaraiva/eda-golang)**

Here are the highlights.

## 1. Middleware is the one extensibility seam

Every cross-cutting concern in the reference code is a decorator over a small
interface — never a special path in business logic:

```go
type MessagePublisherMiddleware func(next MessagePublisher) MessagePublisher
```

Outbox, inbox/dedup, tracing, metrics, snapshots: all middleware. Business code
calls the plain interface; production wiring composes the decorators. When the
book adds OpenTelemetry in chapter 12, *no handler changes* — trace context
just rides the message metadata. That's the payoff.

## 2. Outbox + inbox: honest at-least-once

The naive "write to DB, then publish to the broker" is a dual-write bug
waiting to happen. The fix is two-sided:

- **Outbox (producer):** the event is written to an `outbox` table **inside
  the same transaction** as the state change. A relay publishes it and marks
  it published. Publish-then-mark means duplicates **will** happen.
- **Inbox (consumer):** insert the message ID into an `inbox` table **inside
  the same transaction as the state change**. Unique violation = already
  processed = ack and skip.

Correctness comes from the inbox, not from "exactly-once" marketing. The repo
ships this as a runnable single file:

```bash
git clone https://github.com/reinaldosaraiva/eda-golang
cd eda-golang/examples/outbox-inbox
docker compose up -d && go run .
```

You'll see a simulated redelivery get absorbed by the inbox dedup:

```
PRODUCER order order-1 placed (state + outbox committed atomically)
RELAY published ff85d040-...
CONSUMER order order-1 confirmed (msg ff85d040-...)
CHAOS   republished envelope ff85d040-... (past the broker dedup window)
CONSUMER duplicate ff85d040-... ignored (inbox dedup)
```

One subtlety the example makes concrete: put the idempotency key **in the
payload** (an envelope ID), not only in a broker header. JetStream's own dedup
window expires (default 2 minutes); the business key doesn't.

## 3. Event sourcing: command methods record, only ApplyEvent mutates

```go
// Command method: validate + record. NO mutation here.
func (o *Order) Cancel() error {
    if o.Status != OrderPending {
        return ErrOrderNotCancellable
    }
    o.AddEvent(OrderCancelledEvent, &OrderCancelled{Reason: "requested"})
    return nil
}

// The ONLY mutation path — used by replay AND by Save.
func (o *Order) ApplyEvent(evt ddd.Event) error { ... }
```

One mutation path means replay and live writes can never diverge. Bonus:
event-store PK `(stream_id, stream_name, stream_version)` gives you optimistic
concurrency for free.

## 4. Domain events ≠ integration events

Domain events are Go structs serialized as JSON and dispatched in-process.
Integration events are **protobuf** contracts published to the broker,
translated by an explicit anti-corruption layer. Never serialize your domain
model straight onto the wire — every consumer becomes coupled to your
internals.

Keep the contracts *as code*: subjects, message names and type registrations
live next to the `.proto` files. Consumers import a package, not a wiki page.

## 5. Saga state is data, not goroutines

Orchestrated sagas don't need a workflow engine: persist
`SagaContext{ID, Data, Step, Done, Compensating}` after **every** step,
correlate replies via message metadata headers (`SAGA_ID`), and compensate by
walking the step list backward. The saga survives process restarts because
it's a row in a table, not a suspended goroutine.

## 6. JetStream specifics worth copying

- Publish with `nats.MsgId(msg.ID())` → broker-side dedup
- Durable + queue-group consumers: `GroupName("ordering-baskets")`
- Explicit ack with the handler's `context.WithTimeout` **equal to `AckWait`** —
  otherwise redeliveries pile up while the first attempt still runs
- `MaxDeliver` caps redelivery; `Term()` is the poor-man's DLQ

## 7. Monolith → microservices is a deploy-time decision

If modules are bounded contexts from day one — own tables (`servicename.*`
schemas), own entrypoint, communication only via gRPC + events — then
splitting is a deployment change, not a rewrite. The reference repo runs the
same tree as a monolith or as seven services via docker-compose profiles.
Schema-per-service in one Postgres instance is the pragmatic intermediate
step.

---

## Common pitfalls (the short list)

- Dual write without outbox → lost events
- Trusting "exactly-once" → make consumers idempotent instead
- Mutating state outside `ApplyEvent` → replay/live divergence
- Domain structs on the wire → coupling
- Saga state in memory → one restart and it's gone
- `AckWait` shorter than handler timeout → duplicate storms

The full guide — including CQRS, the testing pyramid (fakes, Pact message
pacts, godog e2e), and observability via W3C TraceContext over message
metadata — is in the repo:

**[github.com/reinaldosaraiva/eda-golang](https://github.com/reinaldosaraiva/eda-golang)**

It also ships a `golang-pro` agent skill (for Kimi Code / Claude Code and
compatible harnesses) encoding these patterns for AI-assisted development.

Stars, issues, and PRs welcome — especially Kafka/RabbitMQ/Watermill variants
of the example. What EDA pattern burned you in production? Drop it in the
comments.
