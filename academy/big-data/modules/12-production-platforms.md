# Data quality SLOs and observability

> **Real engineering ticket:** ETL dashboards green while reports are stale

## Context and user impact
Pipelines are operationally green but business data is two days behind.

## Concepts to understand deeply
Data freshness/completeness/correctness are user-facing SLIs. SLOs express expected thresholds; actionable alerts require owner and runbook. Logs are events, metrics are trends, traces correlate causal paths. Lineage supports impact analysis and recovery.

## What to implement
Add a runbook to the [capstone](../../capstones/README.md): input partition freshness, reject ratio, duplicate rate and job duration; show real metrics where observed.

## Reference lab
[Capstone handbook](../../capstones/README.md)

**Hands-on protocol:** (1) draw the architecture; (2) write the first version; (3) run and record actual outputs; (4) implement tests; (5) cause a failure; (6) fix and rerun; (7) commit the work in a practice branch. Do not copy the reference code into your answer without understanding it.

## Failure injection
Delete one expected source partition and explain how quality gate blocks publication.

## Ticket acceptance criteria
Three SLIs with concrete definitions, an escalation owner and reproducible recovery.

## Evidence checklist
- [ ] Architecture sketch shows data flow, boundaries and state.
- [ ] Reproducible run commands and required versions are documented.
- [ ] A test demonstrates the intended contract.
- [ ] A failure/recovery observation is recorded, not fabricated.
- [ ] Security, privacy, monitoring and cost implications are explained.

## Interview challenge
**How can a successful CI workflow coexist with a failed data freshness SLO?**

**Upstream documentation:** https://opentelemetry.io/docs/

[Back to Big Data curriculum](../README.md)
