# Data lifecycle and contracts

> **Real engineering ticket:** Daily retail order feed contains malformed records

## Context and user impact
Finance gets different daily revenue figures because invalid records and cancelled orders are mixed with paid orders.

## Concepts to understand deeply
Bronze retains raw source evidence; Silver enforces typed schema and business rules; Gold publishes approved KPIs. Data-quality dimensions include uniqueness, validity, completeness, freshness and consistency. A data contract is a promise about fields, types, business meaning, ownership and expected delivery.

## What to implement
Design an explicit CSV schema, a bad-record quarantine, a business key and an idempotent load. Distinguish paid revenue from gross order amount. Inspect source → staging → curated → analytical output.

## Reference lab
[SQLite ETL](../../labs/sql-etl/)

**Hands-on protocol:** (1) draw the architecture; (2) write the first version; (3) run and record actual outputs; (4) implement tests; (5) cause a failure; (6) fix and rerun; (7) commit the work in a practice branch. Do not copy the reference code into your answer without understanding it.

## Failure injection
Introduce a blank customer ID and invalid price, then run twice. Prove records are quarantined and revenue stays unchanged.

## Ticket acceptance criteria
All bad rows have a reject_reason; repeated loads do not double facts; raw data remains inspectable.

## Evidence checklist
- [ ] Architecture sketch shows data flow, boundaries and state.
- [ ] Reproducible run commands and required versions are documented.
- [ ] A test demonstrates the intended contract.
- [ ] A failure/recovery observation is recorded, not fabricated.
- [ ] Security, privacy, monitoring and cost implications are explained.

## Interview challenge
**When is a 1% rejected-row rate acceptable, and who decides?**

**Upstream documentation:** https://docs.python.org/3/library/csv.html

[Back to Big Data curriculum](../README.md)
