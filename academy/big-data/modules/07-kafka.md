# Kafka partitions and offsets

> **Real engineering ticket:** Order email delivered twice after restart

## Context and user impact
A consumer committed an email side effect but crashed before committing its Kafka offset.

## Concepts to understand deeply
Kafka is a partitioned append-only log. Order is guaranteed within a partition. Consumer groups coordinate ownership, offsets enable replay, and redelivery is expected in at-least-once processing. External side effects need an inbox/dedup table.

## What to implement
Practice with [Kafka coursework](../../../Apache%20Kafka/). Define an event_id and partition key, consumer group, explicit commit policy and dead-letter handling.

## Reference lab
[Kafka coursework](../../../Apache%20Kafka/)

**Hands-on protocol:** (1) draw the architecture; (2) write the first version; (3) run and record actual outputs; (4) implement tests; (5) cause a failure; (6) fix and rerun; (7) commit the work in a practice branch. Do not copy the reference code into your answer without understanding it.

## Failure injection
Crash between DB insert and offset commit; replay and prove no duplicated logical effect.

## Ticket acceptance criteria
Replayed events do not duplicate business state, and recovery is auditable.

## Evidence checklist
- [ ] Architecture sketch shows data flow, boundaries and state.
- [ ] Reproducible run commands and required versions are documented.
- [ ] A test demonstrates the intended contract.
- [ ] A failure/recovery observation is recorded, not fabricated.
- [ ] Security, privacy, monitoring and cost implications are explained.

## Interview challenge
**Why use order_id as partition key for event ordering?**

**Upstream documentation:** https://kafka.apache.org/documentation/

[Back to Big Data curriculum](../README.md)
