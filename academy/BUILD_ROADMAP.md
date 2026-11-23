# Academy Build Roadmap — Maintainer Checklist

**Purpose:** This file tracks what has actually been implemented and tested versus what remains an educational specification. Do not mark a workflow or cloud deployment as verified without real output from that exact component.

## Phase 1 — Academy structure (implemented)

- [x] Two principle-first tracks with 12 modules each: Big Data and Cloud Computing
- [x] Six deep technical guides (Spark, Kafka, CDC, networking, IAM, recovery and cost)
- [x] Twenty-week study plan, first-session onboarding and open-source documentation links
- [x] Career-oriented acceptance criteria, professional tasks and a progress journal
- [x] Python CLI for lab listing, dependency checks, testing and self-reported local progress
- [x] GitHub Actions automated tests for registered pure-Python labs

## Phase 2 — Foundations and operational reliability (implemented)

- [x] Python + SQLite idempotent CSV ingestion and invalid-row quarantine
- [x] HTTP 503 diagnosis with bounded GET retries
- [x] Event-time window simulation for late and duplicated transactions
- [x] Terraform local provider plan/state/drift lab (Terraform runtime not independently verified)
- [x] Retail data product: source -> data quality -> SQL Gold -> manifest + API
- [x] Failed quality gate does not overwrite previously published artifacts
- [x] Object-store publishing with upload/download checksum verification using fake HTTP tests
- [x] Data freshness and quality monitor with deterministic incident tests
- [x] Kafka inbox modeled with SQLite transaction and deterministic replay tests

## Phase 3 — Real distributed systems integration (not yet completely verified)

- [ ] Boot local single-node Kafka in Docker, execute the crash/restart workflow, and record broker offset evidence
- [ ] Boot MinIO + FastAPI in Docker and perform full P1 -> P2 integration test (not just mocked API tests)
- [ ] Validate Docker Compose behavior and cleanup on Windows and Linux
- [ ] Execute original HDFS/Hive coursework and document up-to-date, reproducible environment requirements
- [ ] Add measured Spark physical-plan and skew tuning experiments, not guessed benchmarks
- [ ] Add an Airflow DAG that calls a real idempotent job with a backfill/retry exercise
- [ ] Add a local monitoring/dashboard stack with reproducible freshness and failure alerts

## Phase 4 — Cloud and production engineering (future)

- [ ] Local Kubernetes deployment, readiness and rollback exercises
- [ ] More robust role-based access / secrets / TLS examples; remove unauthenticated assumptions before internet exposure
- [ ] Backup/restore drill with checksum, actual RPO/RTO observations, and incident timeline
- [ ] Cloud cost worksheet sourced from official AWS/GCP/Azure calculators
- [ ] Reviewed Terraform plans for optional cloud resources; no automatic cloud apply
- [ ] Verify recoverability and correctness on the advanced CDC lakehouse stack
- [ ] Publish video-independent, independently reproducible capstone demos and architecture review packets

## Quality gates for every new lab

1. State a realistic business problem and the engineering role responsible.
2. Explain **mechanism, state and failure modes** before commands.
3. Provide reproducible run and cleanup commands with version/OS assumptions.
4. Add automated checks where possible; label live integration separately.
5. Define a deliberate failure injection and its expected symptom.
6. Give acceptance criteria, evidence requirements, and an interview defense question.
7. Discuss security, cost and operational trade-offs.
8. Include a challenge for the learner to independently rebuild or extend the reference.

## Levels of verification

| Label | Meaning |
| --- | --- |
| **Unit verified** | Test suite passed against the reference code (does not exercise external services) |
| **Integration verified** | Full required external services ran and actual end-to-end output was inspected |
| **Deployment verified** | A specified environment was provisioned, exercised, monitored and safely cleaned |
| **Learning complete** | The learner rebuilt, tested, explained and defended the project independently |

Never promote an item to a stronger status by inference.

## Working policy

Build on the academy feature branch, use PR reviews, preserve university coursework, and keep all real credentials and private data out of the repository. The repository is an educational reference, not a certification authority.
