# Capstone P4 — Secure & Recoverable Cloud Data Platform

**Ticket CLOUD-410:** "Move analytics workloads from one engineer's machine to a reproducible and recoverable cloud architecture without violating the security budget."

This capstone is a **design-and-test project**. Do not create paid cloud resources just because Terraform files are present.

## Requirements (assumptions to validate with stakeholders)

- Read-only analytics dashboard over a validated data product.
- A private object-storage bucket and least-privilege identity for data publishing.
- The public frontend does not have direct credentials to query the database.
- A written recovery plan with explicit **RPO** and **RTO** targets (proposed: 15 min and 60 min, respectively; not verified capabilities).
- Logs, metrics, alerts and data freshness instrumentation.
- Infrastructure described in version control and reviewed before changes.

## Platform architecture to design

~~~mermaid
flowchart TD
 U[Analyst browser] --> LB[HTTPS entry point]
 LB --> API[Private analytics API]
 API --> SQL[(Managed SQL / warehouse)]
 E[Batch / streaming ingest] --> OBJ[(Private object storage)]
 OBJ --> ETL[Data transforms + quality gate]
 ETL --> SQL
 IAM[Workload identity + scoped IAM] -.-> API & ETL & OBJ
 OBS[Logs, metrics, traces] -.-> API & ETL
~~~

## Sprint-based real-world tickets

| Sprint | Ticket | Deliverable |
|---|---|---|
| 1 | CLOUD-411 — Select infrastructure | AWS/GCP/Azure design comparison + ADR |
| 2 | CLOUD-412 — Build IAM model | Trust boundaries, read/write role matrix, credential rotation plan |
| 3 | CLOUD-413 — Infrastructure as Code | Local Terraform lab; inspect cloud S3 example plan without applying |
| 4 | CLOUD-414 — Restore drill | Backup, delete/corrupt local DB, restore and verify checksums |
| 5 | CLOUD-415 — Observability | Three SLIs, proposed SLOs, runbook and alert routing |
| 6 | CLOUD-416 — Cost review | Hypothetical monthly cost worksheet and a safe shutdown policy |

## Suggested practice

- [Terraform local state lab](../../labs/terraform-local/): understand desired state, drift and destroy with **zero cloud cost**.
- [Cloud S3 lab](../../../projects/cloud-object-store-api/): review bucket encryption, public access controls and local development API.
- [P1 retail data product](../retail-analytics-data-product/): create a reproducible SQLite backup/restore with row-count and SHA validation.
- [P2 artifact publishing](../cloud-data-publishing/): verify uploaded objects and plan secret-free workload identity.

## Completion conditions

Supply architecture diagrams, real local restoration timestamps, a Terraform plan, IAM privilege matrix, cost assumptions and a final security review. Claim cloud deployment only after testing it in an account you control and documenting actual cost and functionality.
