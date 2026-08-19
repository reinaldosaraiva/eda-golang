# Event-Driven Architecture in Go — Best Practices

A community-driven, opinionated guide to building event-driven systems in Go:
event sourcing, CQRS, sagas, transactional messaging (outbox/inbox), and
NATS JetStream — distilled from real reference implementations and production
experience.

> Inspired by and distilling lessons from the
> [Event-Driven Architecture in Golang](https://github.com/PacktPublishing/Event-Driven-Architecture-in-Golang)
> reference application (Michael Stack, Packt 2022) — the "MallBots" app that
> evolves from a modular monolith into event-sourced microservices.

## Why EDA in Go

Go's concurrency primitives, small interfaces, and explicit error handling make
it a natural fit for asynchronous, eventually consistent systems. But EDA done
wrong is worse than no EDA: hidden coupling, lost messages, and "exactly-once"
myths. This guide collects the patterns that actually hold up.

## Table of Contents

- [Core Principles](#core-principles)
- [Patterns](#patterns)
  - [Middleware as the extensibility seam](#1-middleware-as-the-extensibility-seam)
  - [Transactional messaging: Outbox + Inbox](#2-transactional-messaging-outbox--inbox)
  - [Event sourcing](#3-event-sourcing)
  - [Domain events vs integration events](#4-domain-events-vs-integration-events)
  - [CQRS](#5-cqrs)
  - [Saga orchestration](#6-saga-orchestration)
  - [NATS JetStream specifics](#7-nats-jetstream-specifics)
- [Monolith to Microservices](#monolith-to-microservices-as-a-deploy-time-decision)
- [Runnable Examples](#runnable-examples)
- [Testing Pyramid for EDA](#testing-pyramid-for-eda)
- [Observability](#observability)
- [Common Pitfalls](#common-pitfalls)
- [Contributing](#contributing)
- [License](#license)

## Core Principles

1. **At-least-once is honest.** Design for duplicates from day one. Correctness
   comes from idempotent consumers, not from broker promises.
2. **Business code never knows about infrastructure.** Messaging, tracing, and
   persistence concerns are decorators over small interfaces.
3. **The domain model is not the wire format.** Domain events and integration
   events are different things with different serializers.
4. **State transitions have exactly one code path.** Replay and live writes
   must mutate state through the same function.
5. **Structure for the split before you split.** Bounded contexts with their
   own tables make monolith→microservices a deployment change, not a rewrite.

## Patterns

### 1. Middleware as the extensibility seam

Every cross-cutting concern should be a decorator over a small interface —
never a special code path in business logic:

```go
type MessagePublisher interface {
    Publish(ctx context.Context, msgs ...Message) error
}

type MessagePublisherMiddleware func(next MessagePublisher) MessagePublisher
```

Outbox, inbox (dedup), tracing, metrics, and snapshots all compose this way:

```go
publisher := amotel.TraceMiddleware(
    amprom.MetricsMiddleware(
        tm.OutboxPublisher(outboxStore, jetstreamPublisher),
    ),
)
```

Business code calls `publisher.Publish(...)` and stays oblivious.

### 2. Transactional messaging: Outbox + Inbox

The naive approach — write to the database, then publish to the broker — is a
dual-write problem: one of the two will eventually fail without the other.

**Outbox** (producer side): the publisher middleware writes the message to an
`outbox` table **inside the request's `*sql.Tx` instead of the broker**. The
message commits if and only if the state change commits. A background relay
publishes and marks:

```sql
CREATE TABLE outbox (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    subject      TEXT NOT NULL,
    payload      BYTEA NOT NULL,
    published_at TIMESTAMPTZ
);
CREATE INDEX unpublished_idx ON outbox (id) WHERE published_at IS NULL;
```

Publish-then-mark is at-least-once — duplicates **will** happen. That is fine,
because of the inbox.

**Inbox** (consumer side): the handler middleware inserts the incoming message
ID into an `inbox` table **in the same transaction as the state change**. A
unique-violation means "already processed": ack and skip.

```go
func InboxHandler(store InboxStore, next MessageHandler) MessageHandler {
    return MessageHandlerFunc(func(ctx context.Context, msg IncomingMessage) error {
        if err := store.Insert(ctx, msg.ID()); errors.Is(err, ErrDuplicateMessage) {
            return nil // dedup: ack without reprocessing
        }
        return next.HandleMessage(ctx, msg)
    })
}
```

**Key insight:** the same `*sql.Tx` must reach the repository, the outbox, and
the inbox. A small request-scoped DI container (`AddScoped`/`Scoped(ctx)`) is
enough — no need for fx/dig.

> Production note: a polling relay adds latency. For lower latency, stream the
> outbox with logical replication / CDC (Debezium, `pg_logical_slot`) — the
> pattern is the same.

### 3. Event sourcing

**Command methods validate and record; only `ApplyEvent` mutates state.**

```go
// Command method: validate + record. NO state mutation here.
func (o *Order) Cancel() error {
    if o.Status != OrderPending {
        return ErrOrderNotCancellable
    }
    o.AddEvent(OrderCancelledEvent, &OrderCancelled{Reason: "requested"})
    return nil
}

// The ONLY mutation path — used by replay AND by Save.
func (o *Order) ApplyEvent(evt ddd.Event) error {
    switch e := evt.Payload().(type) {
    case *OrderCreated:
        o.Status = OrderPending
        o.Items = e.Items
    case *OrderCancelled:
        o.Status = OrderCancelled
    }
    return nil
}
```

- **Optimistic concurrency for free:** event store primary key
  `(stream_id, stream_name, stream_version)`; stamp every event's metadata with
  `aggregate-id` / `aggregate-version`.
- **Generic repository + type registry** removes per-aggregate boilerplate
  while keeping strong typing — a good use of Go generics, applied sparingly.
- **Snapshots are store middleware** with versioned snapshot types
  (`OrderV1`, `OrderV2`): snapshot schema evolution mirrors event evolution.
  Keep the snapshot trigger a swappable strategy function.

### 4. Domain events vs integration events

| | Domain events | Integration events |
|---|---|---|
| Audience | Inside the module | Other services |
| Serializer | JSON (flexible) | Protobuf (strict contract) |
| Transport | In-process dispatcher | Message broker |
| Stability | Free to evolve | Versioned, backwards-compatible |

Never serialize your domain model straight onto the wire. Use an explicit
anti-corruption handler that translates domain events into integration events.

**Contracts as code:** keep subjects, message names, and type registrations
next to the `.proto` files in a shared `xxxpb` package — consumers import the
package, not a wiki page. Naming convention:

```
subject:      mallbots.ordering.events.Order
message name: ordersapi.OrderCreated
```

### 5. CQRS

- Split the application layer into `Commands` and `Queries`, one handler per
  use case. Command handlers get repository + publisher; query handlers get a
  read repository only.
- True read-model CQRS is a **separate service** fed by integration events
  into its own denormalized tables — not a flag on the same model.

### 6. Saga orchestration

**Saga state is data, not goroutines.** Persist the saga context after every
step; correlate replies via message metadata headers; compensate by walking
the step list backward:

```go
saga := sec.New[*CreateOrderData]("CreateOrder", repo, publisher).
    AddStep().
        Action(AuthorizeCustomerCmd).
        OnActionReply(customerAuthorized).
    AddStep().
        Action(CreateShoppingListCmd).
        OnActionReply(shoppingListCreated).
        Compensation(CancelShoppingListCmd).
    // ...
```

- Persist `SagaContext{ID, Data, Step, Done, Compensating}` after **every**
  step — the saga survives process restarts with no workflow engine.
- Command handlers auto-reply with `REPLY_OUTCOME: SUCCESS|FAILURE` headers,
  correlated by `SAGA_ID`.
- A failure reply flips `Compensating` and walks backward; a failure *while
  compensating* is a hard error that needs human attention (alert on it).

### 7. NATS JetStream specifics

- Publish with `nats.MsgId(msg.ID())` → server-side dedup; wrap in a bounded
  retry loop.
- Consumers: **durable + queue group** per subscriber
  (`GroupName("ordering-baskets")` → one delivery per group).
- `AckExplicitPolicy` with `AckWait`, and set the handler's
  `context.WithTimeout` **equal to `AckWait`** — otherwise redeliveries arrive
  while the first attempt is still working.
- `MaxDeliver` caps redelivery; `Term()` (terminate) is the poor-man's DLQ.
- Filter by message name server-side; ack-and-skip non-matching messages early.

## Monolith to Microservices as a Deploy-Time Decision

Structure modules as bounded contexts from day one:

- Own tables (`servicename.*` schemas), own entrypoint (`cmd/service`), own
  protobuf contract package.
- One `Module` interface; a `cmd/mallbots`-style binary composes all modules
  in-process, while each `*/cmd/service/main.go` runs one.
- Communication only via gRPC (sync reads) and events (workflows).

Then splitting is a deployment change, not a code change: docker-compose
profiles (`monolith` vs `microservices`) over the same tree; schema-per-service
in one Postgres instance is a pragmatic intermediate step.

## Testing Pyramid for EDA

| Level | Tool | Asserts |
|-------|------|---------|
| Unit | fakes over mocks (`FakeEventPublisher.Last()`) | Aggregates record the right events; handlers call the right commands |
| Contract | Pact **message** pacts + pact-broker | Published event schemas match consumer expectations |
| E2E | godog/Cucumber via the generated OpenAPI client | Whole workflows, black-box, against either deployment profile |

Prefer fakes over mocks for messaging: a fake publisher that records published
events makes event assertions trivial.

## Observability

- Propagate **W3C TraceContext through message metadata** — end-to-end traces
  across the broker without touching handler code (another payoff of the
  middleware seam).
- Metrics as publisher/subscriber middleware (sent/received counters per
  message name); expose `/metrics` and `/liveness` on a shared mux.
- Trace database transactions; record errors as span attributes/events.

## Common Pitfalls

- **Dual write without outbox** — DB commits, broker publish fails, event lost.
- **Trusting "exactly-once"** — redelivery happens; make consumers idempotent.
- **Mutating state outside `ApplyEvent`** — replay and live writes diverge.
- **Domain structs on the wire** — couples every consumer to your internals.
- **Saga state in memory** — one restart and the workflow is lost.
- **`AckWait` shorter than handler timeout** — duplicate processing storms.
- **Splitting services before boundaries exist** — split the schema first,
  the deployment later.

## Runnable Examples

- [`examples/outbox-inbox`](examples/outbox-inbox/) — transactional outbox +
  idempotent inbox with Postgres and NATS JetStream, in a single commented
  file. `docker compose up -d && go run .` — watch a simulated redelivery get
  absorbed by the inbox dedup.

## Included: `golang-pro` Agent Skill

This repository ships the [`golang-pro`](.agents/skills/golang-pro/SKILL.md)
skill — a ready-to-use specialist skill for AI coding agents (Kimi Code,
Claude Code, and compatible harnesses) covering Go concurrency, design
patterns, gRPC, eBPF, and the EDA patterns documented in this README
([references/event-driven-architecture.md](.agents/skills/golang-pro/references/event-driven-architecture.md)).

**Install (user scope — available in all projects):**

```bash
mkdir -p ~/.agents/skills
cp -R .agents/skills/golang-pro ~/.agents/skills/
```

**Install (project scope):** copy `.agents/skills/golang-pro` into your
project's `.agents/skills/` directory.

## Contributing

Contributions are welcome — corrections, additional patterns (Kafka, RabbitMQ,
Watermill implementations), real-world war stories, and translations. Please
open an issue or pull request.

## Acknowledgments

- [Event-Driven Architecture in Golang](https://github.com/PacktPublishing/Event-Driven-Architecture-in-Golang)
  by Michael Stack (Packt, 2022) — the reference implementation behind most of
  the patterns distilled here.
- [Enterprise Integration Patterns](https://www.enterpriseintegrationpatterns.com/)
  — the vocabulary for messaging patterns.

## License

[MIT](LICENSE) © Reinaldo Saraiva
