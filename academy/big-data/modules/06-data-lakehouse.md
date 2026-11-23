# Parquet and lakehouse table formats

> **Real engineering ticket:** Vendor schema update breaks analytics

## Context and user impact
A partner adds new fields and changes numeric precision without warning.

## Concepts to understand deeply
Parquet is column-oriented and supports compression and predicate pushdown. Partition pruning limits IO; high cardinality leads to tiny files. Bronze/Silver/Gold are conventions, not transaction guarantees. Iceberg adds snapshots, evolving schema and metadata for reliable concurrent tables.

## What to implement
Change a CSV schema and document a migration. Compare file sizes and query plans, and distinguish a plain Parquet folder from an Iceberg table.

## Reference lab
[PySpark lakehouse](../../../projects/retail-pyspark-lakehouse/)

**Hands-on protocol:** (1) draw the architecture; (2) write the first version; (3) run and record actual outputs; (4) implement tests; (5) cause a failure; (6) fix and rerun; (7) commit the work in a practice branch. Do not copy the reference code into your answer without understanding it.

## Failure injection
Add incompatible decimal formatting; isolate records safely and propose rollback.

## Ticket acceptance criteria
Schema version documented, no silent precision loss, snapshots/migration strategy explained.

## Evidence checklist
- [ ] Architecture sketch shows data flow, boundaries and state.
- [ ] Reproducible run commands and required versions are documented.
- [ ] A test demonstrates the intended contract.
- [ ] A failure/recovery observation is recorded, not fabricated.
- [ ] Security, privacy, monitoring and cost implications are explained.

## Interview challenge
**Why might partitioning by order ID make analytics slower?**

**Upstream documentation:** https://iceberg.apache.org/docs/latest/

[Back to Big Data curriculum](../README.md)
