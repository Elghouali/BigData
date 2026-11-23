# Capstone P2 — Publish a Trusted Daily Revenue Artifact to Object Storage

**Ticket DATA-207:** "Finance's daily Gold CSV is generated locally, but analysts must retrieve the same immutable artifact from our object-store interface. Upload it and verify that remote bytes match the source."

**Dependencies:** [P1 Retail Data Product](../retail-analytics-data-product/) and the local [Cloud Object Storage API lab](../../../projects/cloud-object-store-api/). The reference API uses FastAPI and MinIO and is explicitly for **localhost only**.

## End-to-end architecture

~~~mermaid
flowchart LR
 CSV[Daily Orders] --> ETL[Validation + SQL Gold]
 ETL --> G[Gold daily_kpis.csv]
 G --> P[Artifact publisher]
 P --> API[FastAPI S3 API]
 API --> S3[(MinIO local object storage)]
 P --> V[GET and SHA256 verification]
 G --> M[Data quality manifest]
 M --> P
~~~

## Run from the cloned repository

**Terminal 1:** Start object store and API.

~~~bash
cd projects/cloud-object-store-api
docker compose up --build -d
docker compose ps
~~~

**Terminal 2:** From the repository root:

~~~bash
cd academy/capstones/cloud-data-publishing
python run_all.py --endpoint http://127.0.0.1:8000
~~~

The pipeline creates the P1 dataset, checks the quality gate and uploads **both** the Gold CSV and the manifest. It downloads each object and verifies the SHA-256 hash before declaring success.

The default keys are reports/retail/daily_kpis.csv and reports/retail/manifest.json. The service persists objects in a Docker volume, so an API restart should not delete data.

To test only the publisher without Docker:

~~~bash
python -m unittest discover -s tests -v
~~~

The tests use a fake local HTTP server; they **do not contact AWS**.

## Professional acceptance criteria

- [ ] Gold artifact must be created from validated data; a failed quality gate blocks publication.
- [ ] Both files upload successfully and remote bytes match by SHA-256.
- [ ] A corrupt download is detected rather than silently accepted.
- [ ] Publishing fails safely when the API is unavailable.
- [ ] Compose containers are bound to localhost; no demo credentials exposed publicly.
- [ ] A business runbook explains how to safely republish and restore.
- [ ] Real screenshots: object list, data file, quality manifest, verification output.

## Rebuild and production upgrades

Reimplement the publisher yourself, then add: object versioning; metadata including schema version and source SHA; immutable daily object keys; idempotent publish semantics; authentication; retry backoff; and a reconciliation job. When moving to AWS/GCP/Azure, use workload identities and least privilege; do not embed access keys.

**Limitations:** The reference object-store API has no user authentication, presigned URLs, or production durability guarantees. No cloud services are deployed automatically.
