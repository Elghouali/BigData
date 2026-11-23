# SQL, transactions and indexing

> **Real engineering ticket:** Duplicate invoices appear after API retries

## Context and user impact
A payment API times out after writing and a retry creates another invoice.

## Concepts to understand deeply
ACID is a set of transaction guarantees. Atomicity prevents partial updates; isolation addresses concurrent anomalies; durability protects committed records. Business request IDs differ from surrogate IDs; unique constraints enforce concurrency-safe idempotency. Indexes speed selective reads but cost space and write overhead.

## What to implement
Create payment and order tables with foreign keys. Use UNIQUE(idempotency_key), explicit transactions, upsert and EXPLAIN QUERY PLAN. Identify the grain of each table.

## Reference lab
[SQLite ETL](../../labs/sql-etl/)

**Hands-on protocol:** (1) draw the architecture; (2) write the first version; (3) run and record actual outputs; (4) implement tests; (5) cause a failure; (6) fix and rerun; (7) commit the work in a practice branch. Do not copy the reference code into your answer without understanding it.

## Failure injection
Make the second INSERT violate uniqueness during a transaction; verify rollback and correct totals.

## Ticket acceptance criteria
No duplicate business transaction; foreign keys enforced; report query and index plan documented.

## Evidence checklist
- [ ] Architecture sketch shows data flow, boundaries and state.
- [ ] Reproducible run commands and required versions are documented.
- [ ] A test demonstrates the intended contract.
- [ ] A failure/recovery observation is recorded, not fabricated.
- [ ] Security, privacy, monitoring and cost implications are explained.

## Interview challenge
**Why does SELECT-then-INSERT fail under concurrency without a database constraint?**

**Upstream documentation:** https://www.postgresql.org/docs/current/transaction-iso.html

[Back to Big Data curriculum](../README.md)
