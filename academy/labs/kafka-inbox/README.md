# Lab 06 — Kafka Consumer Recovery and Inbox Deduplication

**Professional ticket EVT-301:** A consumer sends duplicate order-paid notifications after a restart. The team needs a replay-safe sink and proof that acknowledging an event at the wrong time causes data loss or duplicate side effects.

This lab has two stages:

1. **No Docker:** test the database inbox and duplicate-handling logic with pure Python.
2. **Real Kafka:** use a single local open-source Apache Kafka broker, publish events, crash after a database commit, then resume consumption.

## Architecture

~~~mermaid
flowchart LR
  P[Order events producer] --> K[(Kafka topic: academy-orders)]
  K --> C[Consumer with manual offsets]
  C --> DB[(SQLite inbox + notifications)]
  C --> ACK[Commit Kafka offset after DB commit]
~~~

The notifications table models a business side effect; it does not actually send email or charge anyone.

## Stage A — tests, zero dependencies

~~~bash
cd academy/labs/kafka-inbox
python -m unittest discover -s tests -v
~~~

## Stage B — run Apache Kafka on localhost

Prerequisites: Docker Desktop/Compose, Python 3.11+, and internet access to obtain open-source images/dependencies. Kafka uses plaintext and a one-node KRaft configuration for learning; **never expose it on public networks**.

~~~bash
cd academy/labs/kafka-inbox
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic academy-orders --partitions 1 --replication-factor 1
python -m pip install -r requirements-kafka.txt
python produce.py
python consume.py --crash-after-first
python consume.py --max-records 3
python inbox.py --database output/inbox.db
docker compose down
~~~

The first consumer is intentionally expected to exit nonzero after committing the first event to SQLite **without committing its Kafka offset**. After restart, the record is replayed, but its event ID is already in the inbox; the second run must not create a duplicate notification.

The fixtures include one repeated event ID. Expected logical notifications: **two**.

If Docker Kafka setup fails, inspect container logs with docker compose logs kafka, and consult the official Kafka container guide. An actual Kafka/Compose integration run has not been independently verified in this repository.

## How it works

Kafka **offset commits** track consumer progress. A consumer can successfully commit a database transaction and then crash before it commits its offset. The broker will deliver that record again. The sink uses a durable UNIQUE event_id constraint and a *single SQLite transaction* to record the event ID and the notification together. A repeated event has no second business side effect.

This is **at-least-once delivery plus idempotent sink behavior**, not universal exactly-once delivery. SMTP calls, payments and external APIs would need their own idempotency or outbox workflows.

## Your engineering assignment

- [ ] Explain why committing the Kafka offset before the DB transaction may lose business work.
- [ ] Explain why committing the DB transaction before the offset may cause redelivery.
- [ ] Draw topic, partition, offset, consumer group and sink transaction boundary.
- [ ] Repeat the crash/restart with a real Kafka broker and record observed offsets.
- [ ] Change the event key to order_id and reason about per-order event ordering.
- [ ] Add a poison-message policy: bounded retry, quarantine/DLQ, metric and alert.
- [ ] Add an inbox retention strategy that is consistent with replay horizon.
- [ ] Describe what changes with multiple partitions and consumers.

**Acceptance:** Python tests pass, two unique notifications after replay, and a written incident timeline. Never represent a successful unit test as proof that Kafka Docker integration was executed.

## Official resources

- https://kafka.apache.org/documentation/
- https://kafka.apache.org/44/getting-started/docker/
- https://kafka-python.readthedocs.io/
