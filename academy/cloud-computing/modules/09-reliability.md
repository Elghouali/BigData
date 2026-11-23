# High availability, DR, RPO/RTO

> **Professional task:** Primary database unavailable during checkout

## Business problem
The business can tolerate 10 minutes outage and maximum 5 minutes lost data, but backups have never been tested.

## Learn the underlying concepts
Availability requires more than replicas: detect failures, fail over, manage state and avoid correlated faults. RTO limits restoration time; RPO limits tolerable data loss. Backups, replication and snapshots solve different failure cases. Regional HA and disaster recovery cost money.

## Assignment
Prepare a restore drill using a local SQLite or PostgreSQL backup. Measure actual restoration and validate row counts/checksum. Draw primary, standby and backup failure domains.

## Lab or source implementation
[SQL ETL](../../labs/sql-etl/)

## Failure drill
Corrupt/delete local database copy; restore from backup and verify reports.

## Definition of done
Restore procedure tested, measured RPO/RTO evidence and assumptions recorded.

**Work as an engineer:** capture exact configuration, command outputs, a before/after observation, test evidence, and a one-page architecture decision. Do not turn an untested reference into a production claim.

## Review checklist
- [ ] I can explain this mechanism without naming vendor products first.
- [ ] I can draw component boundaries, inputs, outputs and security controls.
- [ ] I tested or simulated a failure and recorded real evidence.
- [ ] I proposed AWS, GCP and Azure equivalent patterns with trade-offs.
- [ ] I considered ongoing operations, recovery and billing.

## Interview challenge
**Why is a replica not a substitute for an immutable backup against accidental deletes?**

**Official reference:** https://docs.aws.amazon.com/wellarchitected/latest/reliability-pillar/welcome.html

[Return to Cloud curriculum](../README.md)
