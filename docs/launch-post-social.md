# Social launch copy — eda-golang

Short variants of the [dev.to launch post](launch-post-devto.md). Repo:
https://github.com/reinaldosaraiva/eda-golang

---

## X / Twitter (single post)

Event-Driven Architecture in Go, minus the "exactly-once" myths:

outbox+inbox, event sourcing, sagas, JetStream — distilled from a real
reference codebase, with a runnable example (docker compose up && go run .)

https://github.com/reinaldosaraiva/eda-golang

#golang #eventdriven #microservices

## X / Twitter (thread, 5 posts)

1/
I went deep into the "Event-Driven Architecture in Golang" reference codebase
(modular monolith → event-sourced microservices) and distilled the patterns
that actually survive production.

Repo + runnable example:

https://github.com/reinaldosaraiva/eda-golang

#golang

2/
Outbox + inbox: honest at-least-once.

The event commits in the SAME tx as the state change. Duplicates are absorbed
by an inbox unique-key dedup — also in the same tx.

Correctness comes from the inbox, not from broker promises.

3/
Put the idempotency key in the PAYLOAD (envelope ID), not only in a broker
header.

JetStream's dedup window expires (2 min default). The business key doesn't.

The example proves it: a redelivery past the window gets ignored by the inbox.

4/
Event sourcing rule #1: command methods validate and record events; ONLY
ApplyEvent mutates state.

One mutation path = replay and live writes can never diverge.

PK (stream_id, stream_name, stream_version) = free optimistic concurrency.

5/
Saga state is data, not goroutines.

Persist SagaContext after every step; correlate replies via message headers;
compensate by walking steps backward.

No workflow engine needed — and it survives restarts.

Full guide + code: https://github.com/reinaldosaraiva/eda-golang

---

## LinkedIn

**Event-Driven Architecture in Go: the patterns that actually survive production**

I spent time dissecting the reference codebase of "Event-Driven Architecture
in Golang" (Packt) — an app that evolves from a modular monolith to
event-sourced microservices on NATS JetStream and Postgres — and distilled
what I'd actually adopt, review for, and teach.

The result is an open community guide with a runnable example:

🔗 https://github.com/reinaldosaraiva/eda-golang

Highlights:

✅ Outbox + inbox: honest at-least-once delivery — the event commits in the
same transaction as the state change, and the consumer dedups by unique key
in the same transaction as its own state change

✅ Event sourcing with a single mutation path (only ApplyEvent touches state)
and free optimistic concurrency

✅ Sagas as data, not goroutines — persisted context, header correlation,
compensation by walking steps backward

✅ Domain events ≠ integration events — JSON in-process, protobuf contracts
on the wire, anti-corruption layer between them

✅ Monolith → microservices as a deploy-time decision, not a rewrite

The runnable example (Postgres + JetStream, one commented file) simulates a
duplicate delivery and shows the inbox absorbing it:

  docker compose up -d && go run .

Feedback, issues and PRs are very welcome — especially Kafka/RabbitMQ/
Watermill variants of the example.

#golang #eventdriven #microservices #softwarearchitecture #distributedSystems
