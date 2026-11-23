# 20-Week Learning Roadmap

Designed for **~10–15 focused hours/week**. Go slower if necessary. Every fourth week is a review and a real deliverable, not more tutorials.

| Week | Big Data focus | Cloud focus | Proof of work |
|---|---|---|---|
| 1 | Linux, Python, data lifecycle | Cloud mental models and shared responsibility | Local tooling and concept notes |
| 2 | SQL, constraints, transactions | OSI/TCP/IP, DNS, HTTP | SQL ETL lab working |
| 3 | Modeling: OLTP vs OLAP, facts/dimensions | VMs, containers, images | 2 architecture diagrams |
| 4 | Data-quality test & review | Network request failure lab | Review #1: own implementation |
| 5 | HDFS blocks, replication, NameNode | Object/block/file storage | HDFS original coursework |
| 6 | Spark execution, RDD vs DataFrame | S3/GCS/Blob and identity | Spark sample query explained |
| 7 | Spark SQL, joins, partitions, shuffle | IAM, least privilege, credentials | PySpark tests + partition experiment |
| 8 | Parquet + Bronze/Silver/Gold | Terraform state/plan/apply/destroy | Review #2: reproducible lakehouse |
| 9 | Kafka log, partitions, offsets | Compute options and load balancing | Streaming simulation lab |
| 10 | Producer/consumer and failure semantics | Docker networking and Compose | Kafka coursework with fault test |
| 11 | Batch vs streaming, event time | Autoscaling, failure domains | Architecture trade-off essay |
| 12 | Data warehouse, Hive, Iceberg | Observability logs, metrics, traces | Review #3: data platform design |
| 13 | Airflow DAGs and retries | VPC networking, subnets, routing | Orchestration design |
| 14 | CDC and schema evolution | IAM boundaries, encryption, secrets | CDC lab experiment and threat model |
| 15 | Data contracts, CI/CD, tests | Kubernetes basics and declarative desired state | CI passes + recovery scenario |
| 16 | Catalog, lineage, governance | High availability, backup/restore, RPO/RTO | Review #4: resiliency plan |
| 17 | Capstone architecture | AWS/GCP/Azure service mapping | Diagrams and ADRs |
| 18 | Capstone implementation | IaC and cloud budget plan | End-to-end local demonstration |
| 19 | Capstone fault injection | Monitoring and security review | Fault/recovery evidence |
| 20 | Refactor, documentation | Cost and operations review | Final portfolio presentation |

## At every week

- Theory (25%): explain mechanisms and trade-offs.
- Hands-on (50%): write and run code, SQL, IaC or containers.
- Troubleshooting (15%): inject a failure and recover.
- Interview and journal (10%): answer questions, commit evidence.

## Gate criteria

**Foundation gate (week 4):** SQLite pipeline reruns safely, tests pass, bad data has an explicit rejection reason.

**Data engineering gate (week 8):** Spark Bronze/Silver/Gold flows are reproducible; can explain partition/shuffle and show measured results.

**Distributed gate (week 12):** can show duplicate delivery, offset/consumer group behavior, and reason about recovery and ordering.

**Platform gate (week 16):** draws a secure, observable, recoverable cloud architecture; shows an IaC plan without deploying or spending money.

**Capstone gate (week 20):** runnable artifact, tests, README, diagrams, cost/security limitations, and recorded proof of failure and recovery.

Move forward only when you can explain **why** the code works, not only show that it runs.
