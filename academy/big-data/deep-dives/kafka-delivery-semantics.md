# Deep Dive — Kafka Partitions, Offsets and Duplicate Delivery

**Scenario:** A payment-approved event appears twice in downstream order notifications after a restart. You are assigned to make processing safe under replays.

## 1. A partition is a durable log

A Kafka topic consists of partitions, and each event has an offset within its partition. Order is guaranteed **inside one partition**. A producer can key related events by order_id so all updates to one order generally map to the same partition.

Producer durability includes acknowledgement configuration, in-sync replicas and broker health. Acknowledging a write is not the same as guaranteeing a downstream email was sent.

## 2. Consumer groups and offsets

A consumer group assigns each partition to at most one member at a time. Members can rebalance after a failure. The committed offset says which records the group considers processed, not which business side effects actually reached an external database.

~~~text
Offset 41: payment-approved(event_id=e-101)
    ↓
Consumer reads event
    ↓
Consumer inserts notification row (database commit)
    ↓
PROCESS CRASH — offset 42 not committed
    ↓
Consumer restarts from 41 → event replay
~~~

If the consumer inserts again, the user gets two notifications. **Idempotency must cover the external side effect**.

## 3. Reliable inbox pattern

~~~sql
CREATE TABLE processed_events (
    consumer_name TEXT NOT NULL,
    event_id TEXT NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (consumer_name, event_id)
);
~~~

In the **same DB transaction**, attempt to insert the event ID, then apply the business mutation only if the insert is new. Commit the Kafka offset after a successful DB commit. The DB transaction cannot atomically commit the Kafka offset unless a more complex coordination approach is used; duplicates may still be delivered, but the effect should be idempotent.

## 4. Real-world trade-offs

- At-most-once: may lose effects, avoids replay after early commit.
- At-least-once: may duplicate delivery, safer with idempotent sink.
- Kafka transactions: useful for some consume-transform-produce workflows, but do **not** automatically make HTTP emails or other external systems exactly once.
- Poison records: repeated failure can block a partition. Use bounded retries, dead-letter policy, alerting and replay tools.

## 5. Work ticket: simulate and prove

- [ ] Draw producer, topic, partition key, consumer group, external DB and commit boundary.
- [ ] Send two records with the same event_id; assert one business effect.
- [ ] Crash after DB commit but before offset commit; restart from prior offset.
- [ ] Show dedup counts, offset lag and retry metrics.
- [ ] Explain why offset commit order matters and which failure is impossible to avoid fully.
- [ ] Document topic retention and the backfill/replay window.

## Interview

Why can't a horizontally scaled consumer group process one partition with ten consumers simultaneously while preserving its standard ordering semantics?

Source: https://kafka.apache.org/documentation/
