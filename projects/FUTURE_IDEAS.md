# Suggested Next Portfolio Projects

Choose a project only after you can explain and independently rebuild the two reference labs.

| Project | Architecture | Why recruiters care | Difficulty |
| --- | --- | --- | --- |
| **IoT Streaming Analytics** | MQTT → Kafka → Spark Structured Streaming → Iceberg → Grafana | Event time, watermarks, recovery, observability | Advanced |
| **Cloud-Native Data Platform** | Object storage → orchestrator → PySpark → SQL warehouse → dashboard | IaC, IAM, scheduling, cost controls | Advanced |
| **Multi-Cloud Disaster Recovery Demo** | Backup to S3/GCS/Blob, object checksums, restore drills, Terraform | RPO/RTO, security, resilience, portability | Advanced |

## Recommended execution order

1. **Retail PySpark Lakehouse** — implement quality rules, Parquet, Spark SQL, tests.
2. **Cloud Object Storage API** — local MinIO, S3 SDK, REST endpoints, Terraform plan.
3. **Integrate both** — upload Gold Parquet output through the API; log job manifests.
4. **Pick an advanced capstone** — add automation, CI, observability, and cloud costs.

## Portfolio evidence checklist

- [ ] Architecture diagram (not just a list of tools)
- [ ] README with a reproducible setup and resource requirements
- [ ] Real test output, without fabricated benchmarks
- [ ] Screenshots from a successful local run
- [ ] Security/limitations and a threat model
- [ ] CI pipeline and evidence of a passing run
- [ ] Design decisions and alternative technologies considered
- [ ] Cost estimate for optional cloud deployment

**Cloud budget rule:** begin with the local version and only deploy cloud resources when you are prepared to monitor charges and destroy them safely.
