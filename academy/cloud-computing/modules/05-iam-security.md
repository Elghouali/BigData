# Identity, IAM and secret management

> **Professional task:** Storage bucket accidentally exposed publicly

## Business problem
An intern used a broad credential and accidentally opened a dataset to the internet.

## Learn the underlying concepts
Authentication establishes who a caller is; authorization decides which operation is permitted. Least privilege constrains actions/resources. Temporary scoped credentials beat embedded long-lived secrets. Encryption in transit, at rest, logging and resource boundaries are complementary; none compensates for unrestricted IAM.

## Assignment
Write a hypothetical read-only policy for one data prefix, and a distinct write-only producer policy. Inspect Terraform bucket public access blocks. Do not publish credentials.

## Lab or source implementation
[Terraform/cloud lab](../../../projects/cloud-object-store-api/)

## Failure drill
Model stolen access key and enumerate blast radius; show rotation and incident response steps.

## Definition of done
Permissions are narrowly scoped; trusted roles, logging and rotation described; no real secret committed.

**Work as an engineer:** capture exact configuration, command outputs, a before/after observation, test evidence, and a one-page architecture decision. Do not turn an untested reference into a production claim.

## Review checklist
- [ ] I can explain this mechanism without naming vendor products first.
- [ ] I can draw component boundaries, inputs, outputs and security controls.
- [ ] I tested or simulated a failure and recorded real evidence.
- [ ] I proposed AWS, GCP and Azure equivalent patterns with trade-offs.
- [ ] I considered ongoing operations, recovery and billing.

## Interview challenge
**Does encrypting a public bucket prevent an authorized anonymous read?**

**Official reference:** https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html

[Return to Cloud curriculum](../README.md)
