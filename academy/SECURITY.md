# Security, Cost and Lab Safety

- All datasets in these labs are synthetic. Do not insert real banking, identity, personal, or client data.
- Never commit API keys, access keys, private keys, database passwords, real .env files, or cloud credentials.
- Only commit example files with **obvious placeholders**. Check Git history if a credential was previously committed; removing the file from the latest commit does not revoke the leaked credential.
- Local lab credentials and unauthenticated development APIs are **not production-safe**. Bind containers to 127.0.0.1 where practical; never port-forward to public interfaces.
- Before cloud provisioning, check account permissions, set budget alerts, review the Terraform plan, and have a tested cleanup procedure. Don't run terraform apply just to complete the course.
- Use least-privilege IAM and short-lived identities where possible. Do not use root cloud credentials for routine work.
- Treat public code and images as untrusted dependencies. Pin versions, inspect Dockerfiles, run dependency checks and update regularly.
- Store sensitive Terraform state securely. State may contain secret material; never commit it to Git.
- Never publish student or classmate data. Use screenshots with secrets removed.
- The academy is a training environment. Passing unit tests is not proof of production-grade availability, security, scale or compliance.
