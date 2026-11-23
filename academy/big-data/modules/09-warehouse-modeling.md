# Dimensional models and KPIs

> **Real engineering ticket:** Sales and marketing report different active-customer counts

## Context and user impact
Two departments use incompatible definitions and cannot reconcile dashboards.

## Concepts to understand deeply
Facts capture events/measures at an explicit grain; dimensions describe entities and context. Star schemas improve analytical usability. SCD1 overwrites current attributes; SCD2 retains history and changes what past reports mean.

## What to implement
Design fact_orders and dimensions for customer/product/date/region; use [Hive coursework](../../../Data%20Warehouse%20Distribu%C3%A9%20avec%20Apache%20Hive/). Define gross and net paid revenue.

## Reference lab
[Hive coursework](../../../Data%20Warehouse%20Distribu%C3%A9%20avec%20Apache%20Hive/)

**Hands-on protocol:** (1) draw the architecture; (2) write the first version; (3) run and record actual outputs; (4) implement tests; (5) cause a failure; (6) fix and rerun; (7) commit the work in a practice branch. Do not copy the reference code into your answer without understanding it.

## Failure injection
Change customer region mid-month and compare SCD1 versus SCD2 metrics.

## Ticket acceptance criteria
Every fact has one declared grain and each KPI has a written definition.

## Evidence checklist
- [ ] Architecture sketch shows data flow, boundaries and state.
- [ ] Reproducible run commands and required versions are documented.
- [ ] A test demonstrates the intended contract.
- [ ] A failure/recovery observation is recorded, not fabricated.
- [ ] Security, privacy, monitoring and cost implications are explained.

## Interview challenge
**Why must grain be fixed before fact table column design?**

**Upstream documentation:** https://www.postgresql.org/docs/current/ddl.html

[Back to Big Data curriculum](../README.md)
