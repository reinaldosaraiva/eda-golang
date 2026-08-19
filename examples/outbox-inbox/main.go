// Command outbox-inbox demonstrates the two patterns that make event-driven
// systems honest:
//
//	OUTBOX (producer): the event is written to an `outbox` table INSIDE the
//	same database transaction as the state change. A background relay
//	publishes it to NATS JetStream and marks it published. Publish-then-mark
//	is at-least-once: duplicates WILL happen.
//
//	INBOX (consumer): the consumer inserts the incoming message ID into an
//	`inbox` table INSIDE the same transaction as the state change. A unique
//	violation means "already processed": ack and skip.
//
// Run it:
//
//	docker compose up -d          # Postgres + NATS (JetStream)
//	go run .
//
// You will see orders placed, the relay publishing them, the consumer
// processing them — and a duplicated delivery being safely ignored.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
)

const (
	streamName = "ORDERS"
	subject    = "orders.events"
	relayEvery = 300 * time.Millisecond
)

// OrderPlaced is the integration event payload.
type OrderPlaced struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

// Envelope is the wire format. ID is the BUSINESS idempotency key: it lives
// in the payload, so it survives beyond the broker's own dedup window.
type Envelope struct {
	ID      string      `json:"id"`
	Name    string      `json:"name"` // e.g. ordersapi.OrderPlaced
	Payload OrderPlaced `json:"payload"`
}

func main() {
	log.SetFlags(log.Ltime | log.Lmsgprefix)
	ctx := context.Background()

	db := mustPostgres(ctx)
	defer db.Close()

	js := mustJetStream()

	// Start the outbox relay (producer side) and the inbox consumer.
	go relay(ctx, db, js)
	go consume(ctx, db, js)
	time.Sleep(time.Second) // let the consumer subscription come up

	// Place a few orders. Each one writes order + outbox in ONE transaction.
	for i, amount := range []float64{99.90, 149.50, 29.99} {
		id := fmt.Sprintf("order-%d", i+1)
		if err := placeOrder(ctx, db, id, amount); err != nil {
			log.Fatalf("place order: %v", err)
		}
		log.Printf("PRODUCER order %s placed (state + outbox committed atomically)", id)
	}

	// Give the relay time to publish, then demonstrate at-least-once:
	// republish order-1's event with the SAME message ID. The inbox dedups it.
	time.Sleep(2 * time.Second)
	republishDuplicate(ctx, db, js)

	time.Sleep(2 * time.Second)
	log.Println("done — check the tables: SELECT * FROM orders / outbox / inbox")
}

// ---------------------------------------------------------------------------
// Producer: state change + outbox in a single transaction
// ---------------------------------------------------------------------------

func placeOrder(ctx context.Context, db *sql.DB, orderID string, amount float64) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO orders (id, amount, status) VALUES ($1, $2, 'PLACED')`, orderID, amount); err != nil {
		return err
	}

	msgID := uuid.NewString()
	payload, _ := json.Marshal(Envelope{
		ID:      msgID,
		Name:    "ordersapi.OrderPlaced",
		Payload: OrderPlaced{OrderID: orderID, Amount: amount},
	})
	// The event goes to the outbox table, NOT to the broker. If this
	// transaction rolls back, no phantom event can ever be published.
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO outbox (id, subject, payload) VALUES ($1, $2, $3)`,
		msgID, subject, payload); err != nil {
		return err
	}

	return tx.Commit()
}

// ---------------------------------------------------------------------------
// Relay: publish-then-mark (at-least-once, on purpose)
// ---------------------------------------------------------------------------

func relay(ctx context.Context, db *sql.DB, js nats.JetStreamContext) {
	for {
		rows, err := db.QueryContext(ctx,
			`SELECT id, subject, payload FROM outbox WHERE published_at IS NULL ORDER BY created_at LIMIT 50`)
		if err != nil {
			log.Printf("RELAY query: %v", err)
			time.Sleep(relayEvery)
			continue
		}
		type msg struct {
			id, subject string
			payload     []byte
		}
		var batch []msg
		for rows.Next() {
			var m msg
			if err := rows.Scan(&m.id, &m.subject, &m.payload); err == nil {
				batch = append(batch, m)
			}
		}
		rows.Close()

		for _, m := range batch {
			// nats.MsgId gives broker-side dedup within the stream's window.
			if _, err := js.Publish(m.subject, m.payload, nats.MsgId(m.id)); err != nil {
				log.Printf("RELAY publish %s: %v (will retry)", m.id, err)
				continue
			}
			// Crash between publish and mark = duplicate redelivery later.
			// That is FINE: the inbox on the consumer side absorbs it.
			if _, err := db.ExecContext(ctx,
				`UPDATE outbox SET published_at = now() WHERE id = $1`, m.id); err != nil {
				log.Printf("RELAY mark %s: %v", m.id, err)
			}
			log.Printf("RELAY published %s", m.id)
		}
		time.Sleep(relayEvery)
	}
}

// ---------------------------------------------------------------------------
// Consumer: inbox dedup in the same transaction as the state change
// ---------------------------------------------------------------------------

func consume(ctx context.Context, db *sql.DB, js nats.JetStreamContext) {
	// Durable + explicit ack: redelivery happens on crash, so handlers MUST
	// be idempotent.
	sub, err := js.Subscribe(subject, func(m *nats.Msg) {
		if err := handle(ctx, db, m); err != nil {
			log.Printf("CONSUMER error (nak, will redeliver): %v", err)
			m.Nak() //nolint:errcheck
			return
		}
		m.Ack() //nolint:errcheck
	}, nats.Durable("orders-inbox"), nats.ManualAck(), nats.AckWait(10*time.Second), nats.MaxDeliver(5))
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck
	<-ctx.Done()
}

func handle(ctx context.Context, db *sql.DB, m *nats.Msg) error {
	var env Envelope
	if err := json.Unmarshal(m.Data, &env); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	msgID := env.ID // business idempotency key, independent of broker headers
	evt := env.Payload

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	// Dedup FIRST, inside the same tx as the state change.
	if _, err := tx.ExecContext(ctx, `INSERT INTO inbox (id) VALUES ($1)`, msgID); err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			log.Printf("CONSUMER duplicate %s ignored (inbox dedup)", msgID)
			return nil // already processed: ack without reprocessing
		}
		return err
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE orders SET status = 'CONFIRMED' WHERE id = $1`, evt.OrderID); err != nil {
		return err
	}

	log.Printf("CONSUMER order %s confirmed (msg %s)", evt.OrderID, msgID)
	return tx.Commit()
}

// republishDuplicate simulates an at-least-once redelivery: the stored event
// is published again WITHOUT nats.MsgId, so the broker's own dedup window
// does not drop it. The same envelope ID reaches the consumer — and the
// inbox is what guarantees correctness.
func republishDuplicate(ctx context.Context, db *sql.DB, js nats.JetStreamContext) {
	var id string
	var payload []byte
	err := db.QueryRowContext(ctx,
		`SELECT id, payload FROM outbox ORDER BY created_at LIMIT 1`).Scan(&id, &payload)
	if err != nil {
		return
	}
	if _, err := js.Publish(subject, payload); err == nil {
		log.Printf("CHAOS   republished envelope %s (simulating redelivery past the broker dedup window)", id)
	}
}

// ---------------------------------------------------------------------------
// Infrastructure setup
// ---------------------------------------------------------------------------

func mustPostgres(ctx context.Context) *sql.DB {
	dsn := envOr("DATABASE_URL", "postgres://eda:eda@localhost:55432/eda?sslmode=disable")
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}
	for i := 0; i < 30; i++ {
		if err = db.PingContext(ctx); err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		log.Fatalf("ping postgres: %v", err)
	}
	schema := `
CREATE TABLE IF NOT EXISTS orders (
    id         TEXT PRIMARY KEY,
    amount     NUMERIC(10,2) NOT NULL,
    status     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS outbox (
    id           TEXT PRIMARY KEY,
    subject      TEXT NOT NULL,
    payload      BYTEA NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox (created_at) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS inbox (
    id           TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);`
	if _, err := db.ExecContext(ctx, schema); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	return db
}

func mustJetStream() nats.JetStreamContext {
	nc, err := nats.Connect(envOr("NATS_URL", "nats://localhost:14222"))
	if err != nil {
		log.Fatalf("connect nats: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{"orders.>"},
	}); err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		log.Fatalf("add stream: %v", err)
	}
	return js
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
