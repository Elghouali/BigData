# Cloud models, regions and shared responsibility

> **Professional task:** Select hosting for a seasonal retail API

## Business problem
A retailer expects 10× traffic during the holiday season with a two-person operations team.

## Learn the underlying concepts
Cloud is a set of on-demand infrastructure abstractions with responsibility and cost trade-offs, not an automatic reliability guarantee. IaaS manages virtual resources, PaaS manages more runtime responsibilities, and SaaS supplies an application. Region, availability zone and fault domain influence blast radius. Shared responsibility changes with service abstraction.

## Assignment
Write an ADR comparing two VMs, a managed container service, and serverless functions. Include load patterns, availability requirements, portability and costs as hypotheses.

## Lab or source implementation
[Cloud object storage lab](../../../projects/cloud-object-store-api/)

## Failure drill
Assume one zone fails during a peak. Show whether the architecture actually survives it.

## Definition of done
Diagrams include AZ boundaries, deployment owner, recovery and cost notes.

**Work as an engineer:** capture exact configuration, command outputs, a before/after observation, test evidence, and a one-page architecture decision. Do not turn an untested reference into a production claim.

## Review checklist
- [ ] I can explain this mechanism without naming vendor products first.
- [ ] I can draw component boundaries, inputs, outputs and security controls.
- [ ] I tested or simulated a failure and recorded real evidence.
- [ ] I proposed AWS, GCP and Azure equivalent patterns with trade-offs.
- [ ] I considered ongoing operations, recovery and billing.

## Interview challenge
**When is a VM a better choice than serverless for this workload?**

**Official reference:** https://docs.aws.amazon.com/wellarchitected/latest/framework/welcome.html

[Return to Cloud curriculum](../README.md)
