# Object, block and file storage

> **Professional task:** Build document archive for a company

## Business problem
An internal API needs durable report storage, not files attached to a temporary container filesystem.

## Learn the underlying concepts
Object stores use bucket + key and APIs, with different access and metadata model than POSIX files. Block volumes support filesystems/databases; network filesystems offer shared directories. Durability does not equal backup, and versioning is not a backup strategy by itself.

## Assignment
Run the [S3-compatible API](../../../projects/cloud-object-store-api/), upload, list, download and delete an object. Compare S3/GCS/Blob and define lifecycle/retention policy.

## Lab or source implementation
[Cloud object store](../../../projects/cloud-object-store-api/)

## Failure drill
Stop and restart API, verify objects persist; delete an object and attempt recovery under versioning assumptions.

## Definition of done
Data survives API restart, backups and retention documented, no publicly accessible bucket.

**Work as an engineer:** capture exact configuration, command outputs, a before/after observation, test evidence, and a one-page architecture decision. Do not turn an untested reference into a production claim.

## Review checklist
- [ ] I can explain this mechanism without naming vendor products first.
- [ ] I can draw component boundaries, inputs, outputs and security controls.
- [ ] I tested or simulated a failure and recorded real evidence.
- [ ] I proposed AWS, GCP and Azure equivalent patterns with trade-offs.
- [ ] I considered ongoing operations, recovery and billing.

## Interview challenge
**Why should a relational database not use S3 object storage as a drop-in database disk?**

**Official reference:** https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html

[Return to Cloud curriculum](../README.md)
