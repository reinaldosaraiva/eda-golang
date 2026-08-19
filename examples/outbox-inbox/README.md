# Example: Transactional Outbox + Idempotent Inbox

A runnable, single-file demonstration of the two patterns that make
event-driven systems honest — see the [root README](../../README.md#2-transactional-messaging-outbox--inbox)
for the theory.

- **Outbox (producer):** the event is written to an `outbox` table inside the
  same Postgres transaction as the state change. A relay publishes it to NATS
  JetStream and marks it published (at-least-once by design).
- **Inbox (consumer):** the consumer inserts the message ID into an `inbox`
  table inside the same transaction as the state change. Duplicates are
  detected by unique violation and safely ignored.

## Run

```bash
docker compose up -d   # Postgres 16 + NATS with JetStream
go run .
```

Expected output (abridged):

```
PRODUCER order order-1 placed (state + outbox committed atomically)
PRODUCER order order-2 placed (state + outbox committed atomically)
PRODUCER order order-3 placed (state + outbox committed atomically)
RELAY published 7d3c... 
CONSUMER order order-1 confirmed (msg 7d3c...)
...
CHAOS   republished envelope 7d3c... (simulating redelivery past the broker dedup window)
CONSUMER duplicate 7d3c... ignored (inbox dedup)
```

Inspect the state afterwards:

```bash
docker compose exec postgres psql -U eda -c \
  "SELECT 'orders' t, count(*) FROM orders UNION ALL SELECT 'outbox', count(*) FROM outbox UNION ALL SELECT 'inbox', count(*) FROM inbox;"
docker compose exec postgres psql -U eda -c "TABLE orders;"
```

## What to look at in `main.go`

| Function | Pattern |
|----------|---------|
| `placeOrder` | state change + outbox insert in ONE transaction; envelope carries the business idempotency key |
| `relay` | publish-then-mark, `nats.MsgId` broker-side dedup, partial index poll |
| `handle` | inbox dedup in the same tx as the state change; unique violation = ack & skip |
| `republishDuplicate` | redelivers the same envelope past the broker dedup window — the inbox is what catches it |

> Note the envelope: the idempotency key lives **in the payload**, not in a
> broker header. JetStream's own dedup window expires (default 2 minutes);
> the business key does not.

## Cleanup

```bash
docker compose down -v
```
