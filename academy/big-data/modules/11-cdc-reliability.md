# Change data capture and replay

> **Real engineering ticket:** Nightly extraction misses order cancellations

## Context and user impact
The nightly snapshot creates stale analytics when source rows are updated or deleted.

## Concepts to understand deeply
CDC reads commit logs like PostgreSQL WAL, often starting with a snapshot before changes. Update/delete semantics, event order, schema evolution, checkpointing and dedup govern materialized state. At-least-once replay needs sink-side idempotence.

## What to implement
Explore [CDC reference project](https://github.com/Elghouali/cdc-pipeline-bigdata), trace DB → Debezium → Kafka → Spark → Iceberg and propose a last-write-wins version rule.

## Reference lab
[CDC pipeline](https://github.com/Elghouali/cdc-pipeline-bigdata)

**Hands-on protocol:** (1) draw the architecture; (2) write the first version; (3) run and record actual outputs; (4) implement tests; (5) cause a failure; (6) fix and rerun; (7) commit the work in a practice branch. Do not copy the reference code into your answer without understanding it.

## Failure injection
Restart after sink commit but before checkpoint and demonstrate expected replay behavior.

## Ticket acceptance criteria
Silver state correct after repeated update/delete events; reconnect and recovery documented.

## Evidence checklist
- [ ] Architecture sketch shows data flow, boundaries and state.
- [ ] Reproducible run commands and required versions are documented.
- [ ] A test demonstrates the intended contract.
- [ ] A failure/recovery observation is recorded, not fabricated.
- [ ] Security, privacy, monitoring and cost implications are explained.

## Interview challenge
**How is a CDC delete different from a missing row in an append-only feed?**

**Upstream documentation:** https://debezium.io/documentation/

[Back to Big Data curriculum](../README.md)
