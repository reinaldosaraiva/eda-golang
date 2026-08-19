# Event-Driven Architecture in Go

Learnings extracted from [PacktPublishing/Event-Driven-Architecture-in-Golang](https://github.com/PacktPublishing/Event-Driven-Architecture-in-Golang)
(Michael Stack, Packt 2022) — the "MallBots" reference app, evolved chapter by
chapter from modular monolith to event-driven microservices. Stack: Go 1.18+,
NATS JetStream, Postgres, gRPC + grpc-gateway, buf, goose, Pact, godog,
OpenTelemetry, Prometheus.

Use this when designing or reviewing Go systems with async messaging,
event sourcing, CQRS, sagas, or a monolith→microservices migration.

## The one big idea: middleware is the extensibility seam

Every cross-cutting concern in the repo is a decorator over a small interface —
never a special code path in business logic:

```go
type MessagePublisherMiddleware func(next MessagePublisher) MessagePublisher
type MessageHandlerMiddleware  func(next MessageHandler) MessageHandler
type AggregateStoreMiddleware  func(next AggregateStore) AggregateStore
```

Outbox, inbox (dedup), tracing, metrics, and snapshots are all implemented this
way. Adopt this shape for any messaging or store abstraction you build:
business code calls the plain interface; production wiring wraps it.

## Transactional messaging (outbox + inbox)

The honest at-least-once design. Teach this instead of "exactly-once" claims.

- **Outbox as publisher middleware** (`internal/tm/outbox_middleware.go`):
  `tm.OutboxPublisher(store)` wraps the real broker publisher and writes the
  message to an `outbox` table **inside the request's `*sql.Tx` instead of the
  broker**. Atomicity = the message commits iff the state change commits.
- **Relay** (`internal/tm/outbox_processor.go`): polls
  `WHERE published_at IS NULL` (partial index `unpublished_idx`), publishes,
  then `MarkPublished`. Publish-then-mark is at-least-once — duplicates WILL
  happen. (Repo uses 333 ms polling; production alternative: logical
  replication/CDC, e.g. Debezium or `pg_logical_slot`.)
- **Inbox / idempotent consumer** (`internal/tm/inbox_middleware.go`):
  `tm.InboxHandler(store)` inserts the incoming message ID into an `inbox`
  table **in the same tx as the state change**; `pgerrcode.UniqueViolation`
  maps to `ErrDuplicateMessage` → return nil (ack, skip reprocessing).
- Correctness lives in the **inbox unique-PK dedup**, not in the broker.
- The same `*sql.Tx` reaches repo, outbox, and inbox via a request-scoped DI
  container (see below).

## Event sourcing

- **Command methods validate and record; only `ApplyEvent` mutates state.**
  Aggregate methods do checks and `AddEvent(name, payload)`; all state
  transitions live in one `ApplyEvent` switch (see
  `ordering/internal/domain/order.go`). `Save` re-applies pending events
  before persisting — one mutation path for both replay and live writes.
- **Generic repository + type registry** (`internal/es`,
  `internal/registry`): `AggregateRepository[T EventSourcedAggregate]` with
  `Load` (registry factory `Build(name, SetID(id))` → replay events) and
  `Save` (apply → persist → `CommitEvents`). Generics used sparingly, boilerplate
  gone, typing kept.
- **Optimistic concurrency for free**: event store PK
  `(stream_id, stream_name, stream_version)`; every event metadata stamped with
  `aggregate-name`, `aggregate-id`, `aggregate-version` (= PendingVersion+1).
- **Snapshots as store middleware** (`postgres/snapshot_store.go`): wraps the
  event store; load latest snapshot → `ApplySnapshot` → replay newer events
  (`stream_version > aggregate.Version()`). Snapshot types are versioned like
  events (`OrderV1`) — schema evolution mirrors event evolution. Snapshot
  trigger is a swappable strategy func (repo's `shouldSnapshot` N=3 is
  demo-grade; pick a real policy).

## Domain events ≠ integration events

- Domain events: Go structs, **JSON** (`serdes.JsonSerde`), dispatched
  in-process via generic `ddd.EventDispatcher[T]` (`Subscribe`/`Publish`).
- Integration events: **protobuf** (`serdes.ProtoSerde`), published to the
  broker by a dedicated anti-corruption handler
  (`handlers/domain_events.go`) that translates domain → wire format.
- Never serialize your domain model straight onto the wire.
- **Contracts as code in `xxxpb` packages**: subject constants, message names,
  and `Registrations(registry)` live next to the `.proto`; consumers import the
  package, not a wiki page.
- Naming convention: subjects `mallbots.<service>.events.<Aggregate>` /
  `mallbots.<service>.commands`; message names `<service>api.EventName`
  (e.g. `ordersapi.OrderCreated`).

## CQRS

- Application layer splits `Commands` / `Queries` interfaces, one handler
  struct per use case (`application/commands`, `application/queries`); command
  handlers get repo + publisher, query handlers get repo only.
- True read-model CQRS is a **separate module** (`search`) fed by integration
  events into its own denormalized tables — not a flag on the same model.

## Saga orchestration

- **Saga state is data, not goroutines** (`internal/sec`, `cosec` module):
  persist `SagaContext{ID, Data, Step, Done, Compensating}` after every step;
  replies correlate via message metadata headers (`SAGA_ID`, `SAGA_NAME`).
- Fluent definition: `AddStep().Action(...).OnActionReply(...).Compensation(...)`;
  orchestrator walks steps forward on success replies and backward
  (compensations) on failure replies; failure while compensating = hard error.
- Command handlers auto-reply with `REPLY_OUTCOME: SUCCESS|FAILURE` headers.
- Survives process restarts via the saga table — no workflow engine needed.

## NATS JetStream specifics worth copying

- One stream (`mallbots`, subjects `mallbots.>`) created at boot.
- Publish: `PublishMsgAsync` with `nats.MsgId(msg.ID())` → server-side dedup;
  bounded retry loop.
- Consume: durable + queue group per subscriber (`GroupName("ordering-baskets")`
  ⇒ `nats.Bind(stream, group)`), `AckExplicitPolicy` with `AckWait`, and the
  handler context timeout set **equal to AckWait**.
- `MaxDeliver` caps redelivery; `Term()` (exposed as `Kill()`) is the
  poor-man's DLQ. Message-name filter acks-and-skips non-matching messages early.

## Modular monolith → microservices as a deploy-time decision

- Modules are bounded contexts from day one: own tables
  (`servicename.*` schemas), own `cmd/service` entrypoint, communication only
  via gRPC + events.
- One `system.Module` interface; `cmd/mallbots` composes all modules
  in-process, each `*/cmd/service/main.go` runs one. Splitting = changing
  deployment, not code. docker-compose profiles `monolith|microservices` over
  the same tree; schema-per-service in one PG instance is the pragmatic
  intermediate step.
- Sync cross-service reads stay gRPC (`RPC_SERVICES` env map).

## Supporting patterns

- **Scoped DI container** (`internal/di`): `AddSingleton/AddScoped(key, fn)`,
  `Scoped(ctx)` caches a child container in ctx, `di.Get(ctx, key)`. This is
  how one `*sql.Tx` threads through gRPC handlers, message handlers, and
  domain-event handlers without globals or fx/dig.
- **Graceful shutdown `waiter`** (`internal/waiter`): `signal.NotifyContext` +
  errgroup; HTTP `Shutdown`, gRPC `GracefulStop` with forced-`Stop` timeout
  fallback, NATS `Drain`. Copy-paste worthy.
- **Migrations**: goose + `embed.FS` per module (`migrations/`); run in tests
  too via the embedded FS.

## Testing pyramid for EDA

- **Unit**: fakes over mocks — `am.NewFakeEventPublisher()` (assert
  `Last()` published event), `domain.NewFakeXRepository()`,
  `es.FakeAggregateRepository[T]`.
- **Contract**: Pact **message** pacts — provider verifies published events
  against a pact-broker (`*_contract_test.go`), consumer-side pacts for
  integration-event handlers.
- **E2E**: godog/Cucumber `.feature` files driving the system through the
  generated OpenAPI client (build tag `e2e`), black-box against either
  deployment profile.

## Observability (the middleware payoff)

- W3C TraceContext injected/extracted through **message metadata**
  (`internal/amotel`, `MetadataCarrier`) → end-to-end traces across NATS
  without touching handlers.
- Prometheus sent/received counters as publisher/subscriber middleware
  (`internal/amprom`); `/metrics` + `/liveness` on the shared mux; pg tx spans
  (`postgresotel`); error→span attributes (`errorsotel.ErrAttrs`).

## Known caveats in the reference code (don't copy blindly)

- Outbox relay is polling-based (333 ms) — consider CDC for lower latency.
- `shouldSnapshot` (every 3 events) is demo-grade.
- Saga orchestrator `Start` has an error-shadowing bug
  (`sec/orchestrator.go:52-55` returns the wrong `err`).
- JetStream publish retries give up silently after 5 tries (logged TODOs).
