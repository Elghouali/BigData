# Big Data Engineering — Practical Labs & Exercises

**A collection of academic exercises and experiments in distributed storage, stream processing, and SQL analytics.**

[![Hadoop](https://img.shields.io/badge/Hadoop-HDFS-FFCC00?logo=apachehadoop&logoColor=black)](https://hadoop.apache.org/)
[![Spark](https://img.shields.io/badge/Apache_Spark-SQL_%26_RDD-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Kafka-Streams-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Hive](https://img.shields.io/badge/Apache_Hive-Data_Warehouse-FDEE21)](https://hive.apache.org/)

> **Scope:** coursework, labs, notes, and small practice projects. Individual folders may have different prerequisites. This repository is not presented as a single production-ready application.

## Learning areas

| Topic | Material |
| --- | --- |
| **HDFS** | [HDFS lab](HDFS/) with a Docker Compose file, configuration, and lab reports |
| **Spark SQL** | [Spark SQL exercises](AppacheSparkSQL/) with working folders and supporting notes |
| **Apache Hive** | [Distributed data warehouse lab](Data%20Warehouse%20Distribu%C3%A9%20avec%20Apache%20Hive/) and related reports |
| **Apache Kafka** | [Kafka exercises](Apache%20Kafka/) including a Kafka Streams folder |
| **Kafka Streams** | [Mini-project material](Kafka%20Streams%20Mini%20p/) |
| **Spark RDD** | [RDD manipulation exercises](Manipulation%20des%20RDD%20de%20nombres%20avec%20Apache%20Spark%20Objectif/) |
| **Producer example** | [Web producer source](web-producer/) |

## New hands-on portfolio projects

Start with the [guided Big Data & Cloud labs](projects/):

| Guided project | Stack and objective |
| --- | --- |
| [Retail PySpark Lakehouse](projects/retail-pyspark-lakehouse/) | PySpark · Parquet · data quality · Bronze/Silver/Gold analytics |
| [Cloud Object Storage API](projects/cloud-object-store-api/) | Python · FastAPI · S3 SDK · local object storage · optional Terraform AWS S3 |
| [Advanced project ideas](projects/FUTURE_IDEAS.md) | Streaming analytics, cloud-native pipelines, disaster recovery |

Each lab includes a working-reference design, tests, exercises, and a checklist for rebuilding it yourself. The local-first design avoids mandatory cloud charges.

## What this repository demonstrates

- Understanding of **distributed file systems** and HDFS.
- Experience exploring **batch processing** and SQL on Apache Spark.
- Introductory work on **data warehousing** with Apache Hive.
- Practice with **event streams**, producers, and Kafka Streams concepts.

## How to explore

Each topic is a separate folder. Start with a lab report or README in that folder, then inspect the matching code and configuration. For HDFS, review `HDFS/docker-compose.yaml` before running any containers; environments and required images may have changed since the assignment.

For a more integrated data platform project, see [CDC Pipeline — Near Real-Time Lakehouse](https://github.com/Elghouali/cdc-pipeline-bigdata).

## Next improvements

- [ ] Add setup and expected outputs to each lab folder
- [ ] Document software versions and environment requirements
- [ ] Add reproducible datasets or links to permitted sources
- [ ] Remove IDE-generated files from version control
- [ ] Include tested run commands and screenshots where appropriate

**Author:** [Zakariae Elghouali](https://github.com/Elghouali) — Computer Engineering, Big Data & Cloud Computing.
