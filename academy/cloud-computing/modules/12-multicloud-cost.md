# Multi-cloud trade-offs and FinOps

> **Professional task:** Cloud bill surprises engineering manager

## Business problem
A demo environment runs all weekend and egress/idle compute costs exceed budget.

## Learn the underlying concepts
Cloud bills reflect provisioned resources, runtime, storage, network traffic and managed-service pricing. Equivalent AWS/GCP/Azure products differ in billing dimensions and IAM models. Portability is not always worth its integration complexity. Budgets/alerts reduce surprises but may not hard-stop charges.

## Assignment
Design three architecture variants for the capstone (AWS, GCP, Azure). Build a cost worksheet with explicit assumptions: GB/month, requests, compute hours, egress, operational effort. No deployment required.

## Lab or source implementation
[Project roadmap](../../capstones/README.md)

## Failure drill
Model 5× data growth and estimate the dominant new cost. Document shutdown/cleanup runbook.

## Definition of done
No guessed prices represented as quotes; assumptions and links to price calculators recorded.

**Work as an engineer:** capture exact configuration, command outputs, a before/after observation, test evidence, and a one-page architecture decision. Do not turn an untested reference into a production claim.

## Review checklist
- [ ] I can explain this mechanism without naming vendor products first.
- [ ] I can draw component boundaries, inputs, outputs and security controls.
- [ ] I tested or simulated a failure and recorded real evidence.
- [ ] I proposed AWS, GCP and Azure equivalent patterns with trade-offs.
- [ ] I considered ongoing operations, recovery and billing.

## Interview challenge
**Why is an 'always free tier' assumption unsafe for production budgeting?**

**Official reference:** https://learn.microsoft.com/en-us/azure/architecture/guide/

[Return to Cloud curriculum](../README.md)
