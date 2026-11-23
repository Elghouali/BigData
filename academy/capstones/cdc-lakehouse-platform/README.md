# Capstone P3 — Near Real-Time CDC Lakehouse Operations

**Ticket DATA-304:** "Our operations and finance teams need order, payment and refund changes to appear in analytics promptly, survive worker restarts and remain queryable after a source schema change."

**Reference system:** [CDC Pipeline BigData](https://github.com/Elghouali/cdc-pipeline-bigdata), already containing PostgreSQL → Debezium/Kafka → Spark Structured Streaming → Iceberg/MinIO → Trino → APIs/dashboard and observability files. This capstone adds the **training brief, operational acceptance criteria and game days**, not a duplicate untested codebase.

## Logical architecture

~~~mermaid
flowchart LR
 PG[(PostgreSQL OLTP)] --> D[Debezium CDC]
 D --> K[(Kafka topics)]
 K --> B[Spark Bronze ingestion]
 B --> S[Spark Silver upserts/deletes]
 S --> G[Spark Gold business KPIs]
 G --> I[(Iceberg tables on object storage)]
 I --> T[Trino SQL]
 T --> API[Analytics API + dashboard]
~~~

## Stakeholder requirements

- Data freshness: propose an SLO (e.g. source commit to visible Gold within 5 minutes), then **measure it**. A target is not a measurement.
- Correctness: updates and cancels must be reflected in Silver/Gold without double counting.
- Recovery: a worker restart must not lose logical transactions; repeated CDC event processing must be idempotent.
- Security: credentials via local environment or approved secrets tooling, never committed. Browser dashboard must not expose administrative credentials.
- Operations: expose broker lag, connector state, Spark jobs, SQL query success, freshness, rejected record counts and volume.

## Sequence of assignments

1. **Architecture review:** locate the source DB, connector, topics, Spark layers, table metadata, query engine and dashboard in the linked repository. Explain each role.
2. **Prepare environment:** inspect .env.example, compose and resource requirements. Choose a machine with sufficient RAM. Do not run with exposed demo passwords.
3. **Happy path:** start the reference stack following its README. Record real container health, topic samples, Spark job status, Iceberg tables and SQL query results.
4. **Data contract:** define orders, payments, returns and a stable event key. Write the expected state from create → update → cancel → replay.
5. **Fault drill A — connector:** stop/change connector, restore it, then verify topic offsets and downstream materialization.
6. **Fault drill B — Spark:** interrupt a processing job, restart from checkpoint and compare the final materialized state.
7. **Fault drill C — duplicates:** deliver a repeated event and prove no extra net revenue.
8. **Fault drill D — schema change:** add a nullable source column, document compatibility and rerun a query.
9. **Observability:** collect real freshness/lag/quality data; write alert thresholds and runbooks.
10. **Final review:** present 5-minute architecture walkthrough with recovery evidence and known limitations.

## Acceptance and evidence

- [ ] All relevant services are inspected (not just containers "up").
- [ ] Queries from Gold match independent source reconciliations for a sample.
- [ ] Cancels/updates and duplicates have expected semantics.
- [ ] Recovery drill captured with before/after evidence and an incident timeline.
- [ ] Data freshness measured from timestamps; no fabricated latency claims.
- [ ] Secrets scanned and local credentials rotated if previously exposed.
- [ ] README gives reproducible commands, screenshots, cleanup and cost implications.

## Professional extension

Implement orchestrated backfills, a data contract registry, metric alerts, row-level access control and deploy a constrained cloud test environment with a reviewed Terraform plan. This is a multi-week advanced project; the live stack has **not been independently launched or verified as part of this academy contribution**.
