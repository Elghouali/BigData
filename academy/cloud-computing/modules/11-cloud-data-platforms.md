# Cloud-native analytical architectures

> **Professional task:** Data platform must scale across teams

## Business problem
An e-commerce firm wants ingestion, transformation and self-service analytics without running a huge Hadoop cluster.

## Learn the underlying concepts
Map logical functions—source, landing, stream, compute, catalog, warehouse, governance—to managed or self-managed services. Cost comes from storage, network egress, compute, requests and operations labor. Managed solutions reduce some toil but introduce constraints and service coupling.

## Assignment
Draw AWS/GCP/Azure alternatives for [retail lakehouse](../../../projects/retail-pyspark-lakehouse/) + [object store](../../../projects/cloud-object-store-api/). Document IAM, lifecycle, observability and costs.

## Lab or source implementation
[Capstone](../../capstones/README.md)

## Failure drill
Assume one region outage and downstream schema break. Write a recovery and data replay plan.

## Definition of done
Architecture shows trust boundaries, failure domains, budget and data quality gates.

**Work as an engineer:** capture exact configuration, command outputs, a before/after observation, test evidence, and a one-page architecture decision. Do not turn an untested reference into a production claim.

## Review checklist
- [ ] I can explain this mechanism without naming vendor products first.
- [ ] I can draw component boundaries, inputs, outputs and security controls.
- [ ] I tested or simulated a failure and recorded real evidence.
- [ ] I proposed AWS, GCP and Azure equivalent patterns with trade-offs.
- [ ] I considered ongoing operations, recovery and billing.

## Interview challenge
**When is a managed warehouse preferable to Spark on self-managed VMs?**

**Official reference:** https://cloud.google.com/architecture

[Return to Cloud curriculum](../README.md)
