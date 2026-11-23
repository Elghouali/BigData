# Real-World Engineering Task Backlog

Treat these as tasks assigned by a lead engineer. Deliver a pull request with tests, screenshots, an ADR and an incident note, not just runnable code.

| Priority | Ticket | Role | Reference | Acceptance |
|---|---|---|---|---|
| P0 | DATA-101 Fix duplicate daily invoices | Data Engineer | [SQL ETL](labs/sql-etl/) | Safe reruns and an explicit uniqueness rule |
| P0 | DATA-102 Publish trusted revenue report | Data Engineer | [Retail E2E](capstones/retail-analytics-data-product/) | Quality gate, Gold report and API tests |
| P0 | CLOUD-201 Recover a failing internal API | Cloud Engineer | [Networking](labs/networking/) | Bounded retries with an incident timeline |
| P1 | DATA-203 Handle late streaming payments | Streaming Engineer | [Event time](labs/stream-processing/) | Window totals and late policy verified |
| P1 | CLOUD-204 Upload and verify analytics artifact | Platform Engineer | [Cloud E2E](capstones/cloud-data-publishing/) | Remote SHA256 matches source |
| P1 | DATA-305 Replay CDC after consumer restart | Data Engineer | [CDC E2E](capstones/cdc-lakehouse-platform/) | Duplicate-safe final state, recovery evidence |
| P1 | CLOUD-410 Make recovery reproducible | Cloud Engineer | [Cloud DR](capstones/cloud-resilience/) | RPO/RTO target, backup and tested restore |
| P2 | CLOUD-420 Compare cloud architecture costs | Solutions Engineer | [Multicloud](cloud-computing/modules/12-multicloud-cost.md) | Assumption-based AWS/GCP/Azure comparison |
| P2 | DATA-430 Add pipeline SLO dashboard | Platform Data Engineer | [Data SLOs](big-data/modules/12-production-platforms.md) | Freshness, completeness and quality SLIs |

## How to work a ticket

1. Translate the business request into a measurable acceptance criterion.
2. Capture the current system behavior **before changing code**.
3. Draw the minimal architecture and list known failure modes.
4. Implement the smallest change with tests and reproducible fixtures.
5. Inject a failure to test recovery, not just the happy path.
6. Document operational runbook and security controls.
7. Submit a PR and explain the design as if defending it in a code review.

You can create GitHub Issues from these tickets later; no ticket is marked complete unless the evidence exists.
