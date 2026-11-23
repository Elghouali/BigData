# Big Data + Cloud Engineering Academy

**From first principles to portfolio-grade data platforms.**
An open-source, lab-first learning program designed for a Computer Engineering student who wants to understand the **why**, **how**, **failure modes**, and **trade-offs** behind each technology—not just copy commands.

> This academy is a set of **two standalone learning tracks inside the existing BigData repository**. The connected GitHub integration cannot create brand-new repository containers; these directories can be moved into dedicated repositories later. Existing coursework and portfolio projects are not modified.

## Pick your track

| Track | Start here | Core topics |
|---|---|---|
| **Big Data Engineering** | [Big Data curriculum](big-data/README.md) | Linux, Python, SQL, HDFS, Hadoop, Spark, Parquet, Kafka, Hive, modeling, Airflow, lakehouses, reliability |
| **Cloud Computing** | [Cloud curriculum](cloud-computing/README.md) | Networking, VMs, scaling, object storage, IAM, Docker, IaC, Kubernetes, HA/DR, observability, cost, multi-cloud |
| **Integrated projects** | [Capstones](capstones/README.md) | Build, test, secure, recover, document, and defend an end-to-end platform |

**Recommended starting point:** [START HERE](START_HERE.md) → [20-week roadmap](ROADMAP_20_WEEKS.md) → [progress tracker](PROGRESS.md).

## Academy command-line navigation

The **[learner CLI](HOW_TO_USE.md)** makes this a usable local learning platform instead of only a collection of READMEs. With Python 3.11+, run from the repository root:

~~~bash
python academy/academy.py doctor
python academy/academy.py list
python academy/academy.py test all
python academy/academy.py mark sql-etl --status in-progress
python academy/academy.py progress
~~~

The CLI records **self-reported** progress in a local ignored JSON file and never installs tools or creates cloud resources. Passing a reference lab test alone does not prove personal mastery.

## Learning sequence

~~~mermaid
flowchart TD
 A[Linux, Git, Python, SQL] --> B[Data lifecycle + modeling]
 A --> C[Network, compute, storage]
 B --> D[Spark + Kafka + lakehouse]
 C --> E[Containers, IAM, IaC]
 D --> F[Data pipelines + quality + orchestration]
 E --> G[Cloud architecture + observability]
 F --> H[Integrated capstone]
 G --> H
 H --> I[Proof: tests, diagrams, operations and trade-offs]
~~~

## Real labs you can run

| Lab | Prerequisites | Run mode |
|---|---|---|
| [SQL ETL from scratch](labs/sql-etl/) | Python only (stdlib) | laptop, no Docker |
| [HTTP + networking + retries](labs/networking/) | Python only; Docker optional | laptop or Compose |
| [Stream processing fundamentals](labs/stream-processing/) | Python only (stdlib) | laptop, no Kafka |
| [Data freshness and SLO monitoring](labs/data-observability/) | Python + SQLite | local incident / alerts |
| [Kafka replay-safe consumer](labs/kafka-inbox/) | Python + optional Apache Kafka Docker | local broker / recovery |
| [Retail PySpark lakehouse](../projects/retail-pyspark-lakehouse/) | Docker or Python 3.11 + Java 17 | local Spark |
| [Cloud S3 object-storage API](../projects/cloud-object-store-api/) | Docker/Compose | local MinIO |
| [Terraform state lab](labs/terraform-local/) | Terraform CLI (optional) | local only |
| [Original BigData exercises](../README.md) | see each university exercise | HDFS / Hive / Kafka / Spark |

Every lab has **concepts → commands → observation → challenge → verification**. The included code is a reference implementation. **Rebuild it yourself in a separate branch** to truly learn.

## More than tutorials: deep mechanisms and workplace tickets

For difficult technologies, use the [Spark/Kafka/lakehouse deep dives](big-data/README.md#deep-concept-guides) and [networking/IAM/reliability deep dives](cloud-computing/README.md#deep-concept-guides). Each includes what happens internally, a realistic incident, trade-offs, failure injection and an interview exercise.

Use the [engineering ticket backlog](WORK_TICKETS.md) for tasks resembling real Data Engineer and Cloud Engineer assignments, and the [four end-to-end projects](capstones/README.md) to combine those skills into deliverables.

## Academy build status and extension plan

The [build roadmap](BUILD_ROADMAP.md) lists implemented modules and labs, what passed unit tests, and what still needs **live Kafka/MinIO, Airflow, Docker and cloud integration testing**. The [lab authoring standard](LAB_TEMPLATE.md) defines the quality requirements for every future project.

The curriculum is ready to study from, but advanced distributed-system and cloud deployment exercises are **not all integration verified**.

## 4-stage mastery rubric

1. **Explain** a system using no buzzwords: inputs, outputs, states, contracts, assumptions.
2. **Build** it from scratch without copying the reference implementation.
3. **Break and recover** it: bad inputs, missing services, retries, wrong credentials, duplicate events.
4. **Defend decisions**: scale, consistency, security, operational cost, alternative designs.

A working demo is **not** the same as production readiness. Automated test results are not claims about cloud deployment.

## Time and resources

- First three stdlib labs: ~1–2 GB RAM and Python 3.11+; no cloud billing.
- Spark lab: Docker Desktop or Python 3.11 with Java 17; give Docker sufficient memory.
- Kafka and lakehouse full stacks: 8 GB+ RAM allocated to containers is often needed; start with the simple labs before larger stacks.
- Cloud lab: localhost-only Compose; do **not** expose demo credentials to the internet.
- Optional cloud deployment: budget first, least-privilege IAM, review Terraform plan, destroy unused resources.

## Sources and licenses

Original course text and training code here are authored for this academy; exercises link to official **Apache, Python, Linux, Docker, HashiCorp and cloud vendor documentation**, not copied third-party textbooks. Follow the respective open-source projects' licenses for their code and container images. See [Resources](RESOURCES.md) and [Security](SECURITY.md).

## Goal

By the end you should be able to design, implement, troubleshoot, test and **explain** a reproducible local data platform and translate it into AWS, Google Cloud and Azure architectures, with evidence you personally produced.
