# Deep Dive — Reproducibility, Recovery and Cloud Cost

**Scenario:** The only VM hosting an analytics service fails; a team has Terraform code but no tested backup restore, and their cloud bill keeps increasing.

## 1. Terraform solves only part of the problem

IaC can recreate **infrastructure configuration**, but it does not automatically restore database contents. Terraform state maps declared resources to remote identities; a plan is a proposed change and should be reviewed. Manual cloud-console edits create drift. A state file may contain sensitive information and requires protected remote storage for teams.

Run [the local Terraform lab](../../labs/terraform-local/) to practice init → plan → apply → drift → destroy without provisioning cloud resources.

## 2. Backup vs replica vs versioning

- **Replica:** helps availability but may replicate accidental deletes or corruption.
- **Backup:** preserves a recovery point for restoration; must be verified, isolated and retained.
- **Versioning:** can help recover prior object versions but is not a complete backup policy and has cost implications.
- **RPO:** maximum tolerable data loss (time between last recoverable state and incident).
- **RTO:** maximum acceptable time to restore service after an outage.

Proposed requirements like RPO 15 min and RTO 60 min are not proof. Measure actual restore time and the restored data's newest timestamp.

## 3. A professional disaster-recovery drill

1. Take a consistent backup of the SQLite retail capstone DB.
2. Hash the backup file and store it outside the working data directory.
3. Deliberately corrupt a disposable copy, not the only real file.
4. Restore the copy, check the hash and query known KPI totals.
5. Record measured elapsed time and actual data freshness.
6. Write a runbook with command sequence, owner, validation, escalation and safe cleanup.

For PostgreSQL, practice pg_dump/pg_restore for logical backups, then separately study PITR/WAL recovery. One backup style does not substitute for every recovery requirement.

## 4. Cost is an engineering requirement

Cloud bills can include:
- Storage GB-month, object requests, versions and retrieval costs
- Compute instance/serverless runtime and attached disks
- Networking, NAT, cross-region replication and internet egress
- Managed service operational baseline and logs/monitoring ingestion

Always state assumptions and use official pricing calculators rather than quoting unverified “free” prices. Scale forecasts with dataset size, traffic and retention. A budget alert does not necessarily stop spending automatically.

## 5. Professional ticket acceptance

- [ ] IaC plan reviewed and stored as evidence; no cloud apply in a reference exercise.
- [ ] Local restore proven with checksums, row counts and actual time.
- [ ] Failure domains mapped: instance, zone, region and credential compromise.
- [ ] RPO/RTO targets stated separately from observed recovery results.
- [ ] Cost worksheet has explicit GB, runtime, traffic, replication and egress assumptions.
- [ ] Cleanup and credential-rotation runbooks written.

**Interview:** Why do Terraform scripts plus a running database replica not guarantee recovery from an accidental DELETE issued yesterday?

Sources: https://developer.hashicorp.com/terraform/docs ; https://docs.aws.amazon.com/wellarchitected/latest/reliability-pillar/welcome.html
