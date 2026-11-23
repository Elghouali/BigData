# Partial failure and consistency

> **Real engineering ticket:** Inventory reservation timed out during checkout

## Context and user impact
The client got a timeout but the remote inventory service may already have committed.

## Concepts to understand deeply
Network timeouts are ambiguous. Partitioning divides data by key; replication copies data across nodes. Strong versus eventual consistency influences user-visible behavior; retries need idempotency and bounded deadlines. State machines plus reconciliation repair uncertain outcomes.

## What to implement
Design checkout states and a client/server request-id scheme. Draw inventory, payment and order boundaries and identify where compensation is required.

## Reference lab
[HTTP networking](../../labs/networking/)

**Hands-on protocol:** (1) draw the architecture; (2) write the first version; (3) run and record actual outputs; (4) implement tests; (5) cause a failure; (6) fix and rerun; (7) commit the work in a practice branch. Do not copy the reference code into your answer without understanding it.

## Failure injection
Simulate a timeout after state update; retry and demonstrate why an idempotent operation differs from blind POST.

## Ticket acceptance criteria
One request ID produces at most one logical order; unknown outcomes have a repair path.

## Evidence checklist
- [ ] Architecture sketch shows data flow, boundaries and state.
- [ ] Reproducible run commands and required versions are documented.
- [ ] A test demonstrates the intended contract.
- [ ] A failure/recovery observation is recorded, not fabricated.
- [ ] Security, privacy, monitoring and cost implications are explained.

## Interview challenge
**What does a 504 response actually tell a client about the remote database commit?**

**Upstream documentation:** https://martinfowler.com/articles/patterns-of-distributed-systems/

[Back to Big Data curriculum](../README.md)
