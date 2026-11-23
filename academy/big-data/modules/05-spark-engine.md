# Spark execution, DAG and shuffle

> **Real engineering ticket:** Daily revenue Spark job misses 07:00 deadline

## Context and user impact
A dimension join introduces huge shuffle and one worker becomes a straggler.

## Concepts to understand deeply
Spark builds lazy transformations and executes jobs as stages/tasks. Shuffle is an exchange of records among partitions, often during joins and aggregations. Data skew, excess partitions and file sizes affect runtime. Increasing executors cannot remove all bottlenecks.

## What to implement
Use [PySpark lakehouse](../../../projects/retail-pyspark-lakehouse/) and add a small dimension table. Compare explain() plans and record timings for regular versus broadcast joins.

## Reference lab
[PySpark lakehouse](../../../projects/retail-pyspark-lakehouse/)

**Hands-on protocol:** (1) draw the architecture; (2) write the first version; (3) run and record actual outputs; (4) implement tests; (5) cause a failure; (6) fix and rerun; (7) commit the work in a practice branch. Do not copy the reference code into your answer without understanding it.

## Failure injection
Make one key dominate input; measure skew and explain a mitigation.

## Ticket acceptance criteria
Measured baseline and improvement, clear Spark physical plan and honest hardware/runtime notes.

## Evidence checklist
- [ ] Architecture sketch shows data flow, boundaries and state.
- [ ] Reproducible run commands and required versions are documented.
- [ ] A test demonstrates the intended contract.
- [ ] A failure/recovery observation is recorded, not fabricated.
- [ ] Security, privacy, monitoring and cost implications are explained.

## Interview challenge
**Why may doubling workers not improve a skewed join?**

**Upstream documentation:** https://spark.apache.org/docs/latest/sql-performance-tuning.html

[Back to Big Data curriculum](../README.md)
