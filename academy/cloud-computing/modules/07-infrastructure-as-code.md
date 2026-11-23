# Terraform state, plan and drift

> **Professional task:** Manual cloud configuration differs by environment

## Business problem
Production works but staging cannot be recreated after a disaster.

## Learn the underlying concepts
Declarative IaC describes desired resource state, while a provider translates it into API operations. Terraform state maps resources to real IDs; plan estimates changes. State may contain secrets and needs protection. Drift happens when remote resources change outside IaC; apply can change or destroy real resources.

## Assignment
Run [local Terraform lab](../../labs/terraform-local/) with init → validate → plan → apply → plan → destroy, using the local provider only.

## Lab or source implementation
[Local Terraform](../../labs/terraform-local/)

## Failure drill
Edit the managed output manually and observe refresh/plan drift, then restore desired state.

## Definition of done
State file excluded from Git, plan reviewed before apply, cleanup demonstrated.

**Work as an engineer:** capture exact configuration, command outputs, a before/after observation, test evidence, and a one-page architecture decision. Do not turn an untested reference into a production claim.

## Review checklist
- [ ] I can explain this mechanism without naming vendor products first.
- [ ] I can draw component boundaries, inputs, outputs and security controls.
- [ ] I tested or simulated a failure and recorded real evidence.
- [ ] I proposed AWS, GCP and Azure equivalent patterns with trade-offs.
- [ ] I considered ongoing operations, recovery and billing.

## Interview challenge
**Why is deleting the state file not the same as destroying resources?**

**Official reference:** https://developer.hashicorp.com/terraform/language

[Return to Cloud curriculum](../README.md)
