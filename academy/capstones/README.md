# End-to-End Professional Engineering Projects

These are **business-facing projects**, not disconnected tutorials. Each has a request from a fictional stakeholder, expected deliverables, test criteria, incident handling and a final review. Only synthetic data is used.

| Project | Business problem | Deliverables | Execution |
| --- | --- | --- | --- |
| [P1: Retail Analytics Data Product](retail-analytics-data-product/) | Finance needs trustworthy daily revenue despite invalid order feeds | Ingest, validate, SQL storage, KPIs, quality gate, JSON API, tests, runbook | Runnable locally with Python standard library |
| [P2: Cloud Data Publishing](cloud-data-publishing/) | Analytics output must be published to durable object storage and verified | Artifact generation, HTTP upload, download checksum, local MinIO, recovery | Runnable locally; requires P1 and existing S3 API lab |
| [P3: CDC Lakehouse Platform](cdc-lakehouse-platform/) | Business needs near-real-time orders and cancellations | PostgreSQL CDC, Kafka, Spark, Iceberg, dashboard, data contracts, SLOs | Guided operational exercises using existing full CDC project |
| [P4: Cloud Resilience & IaC](cloud-resilience/) | Company needs secure/recoverable data platform and cost guardrails | Multicloud architecture, IAM, Terraform plan, backups and DR drills | Design, local labs, and optional budgeted deployment |

## Career evidence

A project is portfolio-ready only if you can produce all of the following, from your own run:

1. Written business problem, bounded scope and acceptance criteria
2. Event/data flow diagram and exact technology role
3. Reproducible deployment, with versioned configuration and a cleanup procedure
4. Automated tests plus real outputs/screenshots
5. Security assumptions and realistic failure modes
6. Incident drill and recovery evidence
7. Measurement (freshness, latency, data-quality or resource usage)
8. Architectural decisions with alternatives and trade-offs
9. Approximate operating costs and IAM plan
10. A 5-minute demo and interview-ready explanation

## Suggested learning order

Start with P1, then P2, then complete the Big Data + Cloud modules necessary for P3. P4 is the reliability and cloud design stage. Do **not** claim P3/P4 are fully deployed just because a reference repository contains code.

**Security warning:** Project 2's local cloud API has no authentication; keep it on localhost. Terraform can incur charges if applied against AWS/GCP/Azure.
