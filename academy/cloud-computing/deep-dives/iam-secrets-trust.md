# Deep Dive — Identity, IAM and Secrets in a Data Platform

**Scenario:** A data uploader leaked credentials and can list every bucket in the account. The security engineer wants least-privilege roles, clear ownership and recovery steps.

## 1. Identity is not permission

Authentication: prove who you are. Authorization: decide which operation that identity may perform. A **trust policy** decides who can assume a role; a **permission policy** defines which actions and resources that role may access. Multi-factor access, identity federation and short-lived credentials reduce operational risk but do not eliminate compromised privileges.

For a retail platform, separate service identities:
- Ingestor: write only to a particular raw prefix.
- Transformer: read raw, write curated prefix.
- Reporting API: read only curated/Gold; cannot delete raw source records.
- Human operator: time-limited audited elevation for incident response.

Do not share a single permanent admin access key across all services.

## 2. What “least privilege” means in practice

An example AWS-style **illustrative** action/resource matrix (not a complete deployable policy):

| Role | Allowed action | Scope |
| --- | --- | --- |
| raw-writer | PutObject | bucket/raw/* |
| silver-transform | GetObject / PutObject | bucket/raw/* and bucket/silver/* |
| dashboard-reader | GetObject | bucket/gold/* |
| ops-auditor | read logs/metadata | relevant audit stores |

S3 list permissions often require **bucket-level** resource access combined with prefix conditions, whereas object reads/writes target object ARNs. A resource policy can also grant access; examine both identity and resource policies plus explicit denies.

## 3. Secrets, rotation and encryption

Use workload identity or short-lived credentials when possible. Do not place access keys inside source, Dockerfiles, screenshots, notebooks or Terraform state. Store secrets in managed vaults or a protected local mechanism; restrict access and log changes.

A leaked secret demands **rotation/revocation**, review of audit logs and assessment of impact. Deleting a secret from the current Git commit does not erase Git history or revoke access.

Encrypt traffic in transit (TLS), stored objects (provider encryption) and secret material, but understand that an over-privileged authenticated caller can still access encrypted data through authorized APIs.

## 4. Real security review

- [ ] Draw trust boundaries from developer laptop → CI → runtime identity → storage.
- [ ] Write three least-privilege policies scoped by action and resource.
- [ ] Explain who can assume each role and how credentials expire.
- [ ] Review the local [object storage lab](../../../projects/cloud-object-store-api/) and identify why its demo secrets/API are unsafe on the internet.
- [ ] Draft a compromise runbook: disable identity, rotate, inspect logs, isolate data, document customer impact.
- [ ] Propose equivalent access design: AWS IAM, GCP service accounts/IAM, Azure managed identities/RBAC.

**Interview:** Why can an encrypted bucket still be insecure if the resource policy exposes public read access?

Sources: https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html ; https://cloud.google.com/iam/docs/overview ; https://learn.microsoft.com/azure/role-based-access-control/overview
