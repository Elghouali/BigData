# Airflow DAGs and idempotent backfills

> **Real engineering ticket:** Gold transform runs before source files arrive

## Context and user impact
A scheduled reporting pipeline succeeds despite missing latest data.

## Concepts to understand deeply
Orchestrators track dependencies and execution state, not inherent data correctness. Tasks require input checks, retries with bounded backoff, timeouts and date parameters. Backfills rerun historical intervals; idempotent writes prevent double counts.

## What to implement
Design DAG ingest → validate → silver → gold → quality gate; run tasks in local sequence first, then translate to Airflow.

## Reference lab
[Airflow official tutorial](https://airflow.apache.org/docs/)

**Hands-on protocol:** (1) draw the architecture; (2) write the first version; (3) run and record actual outputs; (4) implement tests; (5) cause a failure; (6) fix and rerun; (7) commit the work in a practice branch. Do not copy the reference code into your answer without understanding it.

## Failure injection
Remove upstream input, inspect failure/retry; then restore source and backfill one date.

## Ticket acceptance criteria
Reruns do not double count, failure has alert owner, and backfill targets right interval.

## Evidence checklist
- [ ] Architecture sketch shows data flow, boundaries and state.
- [ ] Reproducible run commands and required versions are documented.
- [ ] A test demonstrates the intended contract.
- [ ] A failure/recovery observation is recorded, not fabricated.
- [ ] Security, privacy, monitoring and cost implications are explained.

## Interview challenge
**Why is a backfill different from retrying a transient failed task?**

**Upstream documentation:** https://airflow.apache.org/docs/

[Back to Big Data curriculum](../README.md)
