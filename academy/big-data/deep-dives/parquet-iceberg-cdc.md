# Deep Dive — From Raw CDC Events to Reliable Lakehouse Tables

**Scenario:** Finance wants an accurate net-revenue dashboard even when source orders are edited, cancelled, or deleted.

## 1. Three different data contracts

**Bronze** retains source event evidence. A CDC message should carry source table, primary key, operation, source commit position/time and payload. It is append-oriented and valuable for replay.

**Silver** represents clean business entities. It must select the latest valid version for each business key and implement delete semantics. A duplicated Kafka delivery must not duplicate a row.

**Gold** contains published business metrics, e.g. net paid revenue by date and product. Gold is a derived product; it can be rebuilt from correct Silver state.

## 2. Why Parquet alone is not a database transaction

Parquet is a **file format**, optimized for analytical column scans. A plain folder of Parquet files does not by itself give multi-writer transactions, atomic snapshot visibility, safe deletes or guaranteed schema/partition evolution.

A lakehouse table format (such as **Apache Iceberg**) adds metadata and snapshots. Readers can see a consistent table snapshot while writers commit new metadata; a failed operation may leave orphan files that require cleanup. Partition evolution and schema evolution are metadata-aware rather than manual directory renaming.

## 3. CDC example

~~~text
order 42: INSERT status=placed amount=100
order 42: UPDATE status=paid amount=100
order 42: UPDATE status=cancelled amount=100
order 42: replay UPDATE status=paid from older log position
~~~

The final Silver row must respect **source event ordering/version**, not processing arrival order. A naïve last-arrival-wins rule could resurrect a cancelled order. Use a commit position or source change version; document its stability and ordering scope.

## 4. Professional job ticket

- [ ] Define event envelope fields and table keys.
- [ ] Explain snapshot bootstrap versus live WAL tailing.
- [ ] Implement a deterministic upsert/soft-delete policy.
- [ ] Create a test with out-of-order updates and duplicate events.
- [ ] Compare a Gold aggregate before and after a cancellation.
- [ ] Show recovery from a lost consumer checkpoint without inflating totals.
- [ ] Document compaction, small-file cost, metadata retention and vacuum risk.

## 5. Architecture and operational evidence

Use [the existing CDC pipeline](https://github.com/Elghouali/cdc-pipeline-bigdata) to trace PostgreSQL → Debezium → Kafka → Spark → Iceberg → Trino. Capture real topic offsets, connector state, Spark metrics, Iceberg table snapshots and final SQL results. Do not claim “exactly once” without proving the guarantees across the boundary in question.

## Interview

Why is a source DELETE event different from a missing row in a daily full extract? When can a CDC event be safely replayed?

Sources: https://iceberg.apache.org/docs/latest/ ; https://debezium.io/documentation/ ; https://parquet.apache.org/docs/
