# HDFS blocks and replication

> **Real engineering ticket:** Data-lake archive exceeds single-server capacity

## Context and user impact
A company must store large append-only event logs while tolerating a worker disk failure.

## Concepts to understand deeply
NameNode holds namespace and block metadata; DataNodes store bytes. Blocks spread across nodes with configured replication. Small files exhaust metadata capacity; HDFS is optimized for large sequential access, not transactional row updates.

## What to implement
Use [HDFS coursework](../../../HDFS/); sketch a file split into blocks and distributed across three nodes. Upload a synthetic file, examine placement and checksums.

## Reference lab
[HDFS coursework](../../../HDFS/)

**Hands-on protocol:** (1) draw the architecture; (2) write the first version; (3) run and record actual outputs; (4) implement tests; (5) cause a failure; (6) fix and rerun; (7) commit the work in a practice branch. Do not copy the reference code into your answer without understanding it.

## Failure injection
Stop one DataNode if your local cluster supports it; document the observed read behavior and under-replication.

## Ticket acceptance criteria
Correct NameNode/DataNode roles; block-replica diagram; evidence from real commands.

## Evidence checklist
- [ ] Architecture sketch shows data flow, boundaries and state.
- [ ] Reproducible run commands and required versions are documented.
- [ ] A test demonstrates the intended contract.
- [ ] A failure/recovery observation is recorded, not fabricated.
- [ ] Security, privacy, monitoring and cost implications are explained.

## Interview challenge
**Why do a million tiny files burden the NameNode?**

**Upstream documentation:** https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html

[Back to Big Data curriculum](../README.md)
