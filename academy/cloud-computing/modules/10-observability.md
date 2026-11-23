# SLIs, SLOs, logs, metrics and traces

> **Professional task:** Checkout latency doubled unnoticed

## Business problem
CPU average looks fine, but user requests are failing intermittently.

## Learn the underlying concepts
Logs record discrete events, metrics quantify system behavior, traces link distributed calls. Four Golden Signals: latency, traffic, errors and saturation. An SLI is a measurable user experience property; an SLO is the objective. Alerts should be actionable and describe owner/response.

## Assignment
Record API status counts, p50/p95 latency and errors using the networking lab. Define a 99.9% availability SLO as a *requirement*, not a measured fact.

## Lab or source implementation
[HTTP retry lab](../../labs/networking/)

## Failure drill
Introduce 20% 503 failures, see error rate, and write an on-call runbook.

## Definition of done
SLI formula and alert window documented, real measurements distinguished from goals.

**Work as an engineer:** capture exact configuration, command outputs, a before/after observation, test evidence, and a one-page architecture decision. Do not turn an untested reference into a production claim.

## Review checklist
- [ ] I can explain this mechanism without naming vendor products first.
- [ ] I can draw component boundaries, inputs, outputs and security controls.
- [ ] I tested or simulated a failure and recorded real evidence.
- [ ] I proposed AWS, GCP and Azure equivalent patterns with trade-offs.
- [ ] I considered ongoing operations, recovery and billing.

## Interview challenge
**Why can a high CPU graph be less useful than request failure rate?**

**Official reference:** https://opentelemetry.io/docs/

[Return to Cloud curriculum](../README.md)
