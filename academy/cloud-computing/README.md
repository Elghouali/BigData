# Track B — Cloud Computing (First Principles → Multi-Cloud)

**Goal:** understand cloud architectures and use cases independently of vendor names, then implement equivalent concepts across AWS, GCP and Azure.

## Cloud learning path

| Module | First-principles concept | Lab |
|---|---|---|
| [01](modules/01-cloud-mental-model.md) | abstraction, shared responsibility, IaaS/PaaS/SaaS, regions/zones | cost and failure diagram |
| [02](modules/02-linux-networking.md) | DNS, TCP, HTTP, ports, CIDR, firewalls | [HTTP + networking lab](../labs/networking/) |
| [03](modules/03-compute-containers.md) | VM, container, serverless, images and schedulers | Dockerfile and Compose |
| [04](modules/04-storage-databases.md) | object/block/file, database durability, replication | [S3 API lab](../../projects/cloud-object-store-api/) |
| [05](modules/05-iam-security.md) | identity vs authorization, least privilege, secrets | security review |
| [06](modules/06-virtual-network.md) | VPC/VNet, subnets, routing, NAT, LB, private endpoints | draw subnet route paths |
| [07](modules/07-infrastructure-as-code.md) | IaC plan/state/drift, desired state | [Terraform local lab](../labs/terraform-local/) |
| [08](modules/08-kubernetes.md) | pods, deployments, services, desired state, probes | manifest and failure exercise |
| [09](modules/09-reliability.md) | HA, DR, RTO/RPO, scaling, failure domains | restore drill design |
| [10](modules/10-observability.md) | logs, metrics, traces, SLI/SLO | latency/health measurement |
| [11](modules/11-cloud-data-platforms.md) | object storage, streaming, compute, orchestrator, warehouse | integrated Big Data capstone |
| [12](modules/12-multicloud-cost.md) | AWS/GCP/Azure equivalences, billing, vendor lock-in | architecture decision record |

## Vendor mapping

| Capability | AWS | Google Cloud | Azure |
|---|---|---|---|
| Virtual machines | EC2 | Compute Engine | Azure Virtual Machines |
| Object storage | S3 | Cloud Storage | Blob Storage |
| Managed containers | ECS/EKS | GKE / Cloud Run | AKS / Container Apps |
| Serverless functions | Lambda | Cloud Run functions | Azure Functions |
| Relational DB | RDS | Cloud SQL | Azure Database for PostgreSQL |
| Analytics warehouse | Redshift | BigQuery | Microsoft Fabric Warehouse / Synapse |
| Observability | CloudWatch | Cloud Monitoring / Logging | Azure Monitor |
| Identity and permissions | IAM | IAM | Entra ID + Azure RBAC |

**Service names are not enough:** every choice needs a security, reliability, performance and cost argument.

## Core hands-on habit

Start local, learn the principle, build a minimal system, inject failure, then choose the managed cloud equivalent. Optional vendor-specific lab instructions are **not** permission to provision billable resources.

**Full project:** [Cloud-hosted analytical architecture design](../capstones/README.md).
