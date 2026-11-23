# Engineering Portfolio and Interview Rubric

Each final project must show *evidence*, not claims.

## Project README template
1. Business problem and expected users.
2. Functional and nonfunctional requirements (SLO, RPO/RTO, throughput, data latency).
3. Data contract, schema, expected failures.
4. System diagram with components, data flow, trust boundaries.
5. Why each technology was chosen and a rejected alternative.
6. Run/test instructions, expected output, and actual output.
7. Security, observability, cost and safe cleanup.
8. Screenshots, metrics and an honest known-limitations section.

## Architecture decision record template

**Decision:** Why choose batch ingestion rather than streaming?  
**Context:** business latency goal, throughput, operations budget.  
**Options:** batch with Spark / streaming with Kafka and Spark.  
**Trade-offs:** freshness, cost, operations, failure handling.  
**Decision & consequences:** record what was chosen and why.  
**Revisit when:** workload or SLO changes.

## Example interview questions

- How do duplicate Kafka events happen, and what prevents double charging?
- Why can two consumers in the same group not process the same partition simultaneously?
- What happens to a query when Parquet files are partitioned by a high-cardinality column?
- How does logical replication differ from periodic polling?
- When does a VPC route table matter, and what does a security group protect?
- How do you determine whether an autoscaler will improve throughput?
- How would you prove your backups are restorable?
- Why is “exactly once” more complicated across a database and an external API?
- What operational metrics tell you your ingestion is behind?
- How can an S3 lifecycle rule accidentally destroy important data?

## Scoring

| Level | Evidence |
|---|---|
| 0 | Can name the tool, cannot explain mechanism |
| 1 | Can run tutorial unchanged |
| 2 | Can rebuild and test independently |
| 3 | Can debug failure and explain trade-offs |
| 4 | Can design for scale, security, cost and reliability with evidence |

Aim for at least level 3 in the core topics before presenting yourself as production-ready.
