# Track A — Big Data Engineering (Beginner → Advanced)

**Goal:** become capable of building data ingestion, storage, processing, streaming and analytics systems—and explaining their failure semantics.

## Core learning path

| Module | What you must deeply understand | Practice |
|---|---|---|
| [01](modules/01-data-lifecycle.md) | data lifecycle, contracts, formats, schemas, quality | [SQLite ETL lab](../labs/sql-etl/) |
| [02](modules/02-sql-databases.md) | relational model, keys, transactions, indexing, OLTP/OLAP | SQLite ETL and SQL queries |
| [03](modules/03-distributed-systems.md) | latency, consistency, replication, partitions, failures | diagrams + failure thought experiment |
| [04](modules/04-hdfs-hadoop.md) | blocks, NameNode/DataNode, fault tolerance | [HDFS coursework](../../HDFS/) |
| [05](modules/05-spark-engine.md) | DAG, lazy evaluation, stages, jobs, shuffle | [Spark coursework](../../AppacheSparkSQL/) |
| [06](modules/06-data-lakehouse.md) | Parquet, partition pruning, Bronze/Silver/Gold, Iceberg | [PySpark project](../../projects/retail-pyspark-lakehouse/) |
| [07](modules/07-kafka.md) | topics, partitions, offsets, replays, delivery | [Kafka replay/inbox lab](../labs/kafka-inbox/) and [Kafka coursework](../../Apache%20Kafka/) |
| [08](modules/08-stream-processing.md) | event time, watermarks, windows, idempotency | [Streaming simulation](../labs/stream-processing/) |
| [09](modules/09-warehouse-modeling.md) | Hive, star schemas, facts, dimensions, SCD | [Hive coursework](../../Data%20Warehouse%20Distribu%C3%A9%20avec%20Apache%20Hive/) |
| [10](modules/10-orchestration.md) | DAGs, retries, backfills, dependency scheduling | [Airflow docs](https://airflow.apache.org/docs/) |
| [11](modules/11-cdc-reliability.md) | WAL, CDC, schema evolution, data quality, reprocessing | [CDC lakehouse](https://github.com/Elghouali/cdc-pipeline-bigdata) |
| [12](modules/12-production-platforms.md) | security, governance, observability, cost and SLO | [freshness incident lab](../labs/data-observability/) |

## Deep concept guides

When a production-style task raises a question about *why* a technology behaves the way it does, study these detailed mechanism walkthroughs before copying code:

- [Spark execution: logical plans, shuffle, skew and tuning](deep-dives/spark-internals.md)
- [Kafka partitions, offsets, replays and exactly-once boundaries](deep-dives/kafka-delivery-semantics.md)
- [CDC, Parquet and Iceberg table semantics](deep-dives/parquet-iceberg-cdc.md)

## How to study one module
1. Read the **mental model** and follow a record through the system.
2. Explain its **internal mechanism**, with a diagram.
3. Run the linked hands-on lab and observe output.
4. Cause the named failure and diagnose it.
5. Answer the checkpoint in your own words.
6. Rebuild independently on a practice branch.

**Full project:** [Build and defend a CDC analytical platform](../capstones/README.md).

### Existing repository integration
The original [BigData exercises](../../README.md), [PySpark lakehouse](../../projects/retail-pyspark-lakehouse/), and [cloud object store](../../projects/cloud-object-store-api/) remain available as separate practice material. They are used here as progressively harder engineering exercises, not hidden behind endless theory.
