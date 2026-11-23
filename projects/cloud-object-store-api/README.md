# Cloud Object Storage API — Local S3 + AWS Infrastructure

**Cloud engineering lab:** build a REST API backed by S3-compatible object storage, run it locally with Docker Compose + MinIO, then inspect an optional Terraform configuration for AWS S3.

**Stack:** Python 3.11 · FastAPI · boto3 · MinIO · Docker Compose · pytest · Terraform

> This is a **local educational demo**, not a public upload service. It has no user authentication or content scanning. Do not expose the API or MinIO to the public internet.

## Architecture

~~~mermaid
flowchart LR
   C[Client: curl / browser] --> A[FastAPI object API]
   A --> S[S3 client: boto3]
   S --> M[(MinIO local object store)]
   S -. Optional AWS endpoint .-> AWS[(Amazon S3)]
~~~

The application uses the same S3 API against a local service or a properly configured cloud bucket. **Terraform creates only an S3 bucket and security controls, not a hosted API.**

## Run locally with Docker

~~~bash
cd projects/cloud-object-store-api
docker compose up --build -d
docker compose ps
~~~

- API docs: http://localhost:8000/docs
- Local MinIO console: http://localhost:9001
- Health: http://localhost:8000/health

The Compose project uses **fictional local development credentials** in the YAML and initializes the bucket called portfolio-lab. They must never be used in a public deployment. Bindings are restricted to localhost.

Try the REST API:

~~~bash
curl -X PUT http://localhost:8000/objects/demo/hello.txt -H "Content-Type: text/plain" --data-binary "hello cloud"
curl http://localhost:8000/objects
curl http://localhost:8000/objects/demo/hello.txt
curl -X DELETE http://localhost:8000/objects/demo/hello.txt
docker compose down
~~~

**Tests without any cloud account or Docker:**

~~~bash
python -m venv .venv
python -m pip install -r requirements.txt
python -m pytest -q
~~~

The API tests inject an in-memory fake object store, so they do not contact S3. For a real cloud endpoint you must set credentials and an endpoint according to your provider; never commit them to GitHub.

## API contract

| Endpoint | Purpose |
| --- | --- |
| GET /health | Process health only; does not verify S3 |
| PUT /objects/{key} | Upload raw bytes, maximum 1 MiB |
| GET /objects/{key} | Download object |
| GET /objects?prefix=... | List keys and sizes |
| DELETE /objects/{key} | Delete object |

Use meaningful keys (e.g. reports/2026-11/revenue.parquet). There is no application-level authentication, so run locally only.

## Optional AWS infrastructure (potentially billable)

The [Terraform folder](infra/terraform/) describes an **AWS S3 bucket** with public access blocked, versioning, and server-side encryption. It does not create IAM users, host FastAPI, or start an AWS deployment automatically.

~~~bash
cd infra/terraform
terraform init
terraform validate
terraform plan -var="bucket_name=YOUR_GLOBALLY_UNIQUE_BUCKET_NAME"
~~~

Do **not** run terraform apply unless you understand billing, IAM permissions, state management, and resource cleanup. AWS resources may incur charges. Use a cost budget. The bucket name must be globally unique. For learning, the Docker/MinIO path is sufficient.

| Concept | AWS | Google Cloud | Azure |
| --- | --- | --- | --- |
| Object storage | S3 | Cloud Storage | Blob Storage |
| Identity | IAM | IAM | Azure RBAC / Entra ID |
| Encryption | SSE-S3/KMS | Default encryption / Cloud KMS | Storage Service Encryption / Key Vault |
| Serverless API (future) | Lambda + API Gateway | Cloud Run | Azure Functions |

## Rebuild it yourself

1. Remove the reference implementation from your working branch and recreate the API from the OpenAPI contract.
2. Add a test for uploading an empty file and a test for oversized payloads.
3. Add authentication and deny anonymous write operations.
4. Add presigned download URLs with short expiration and explain their security trade-offs.
5. Add pagination to the list endpoint, metadata validation, logging, and metrics.
6. Export the PySpark lab's Gold Parquet data and upload it using this API.
7. Explain S3 consistency, versioning, lifecycle policy, cost, IAM, and least privilege.
8. Draw equivalent deployments on AWS, GCP, and Azure before provision.

**Definition of done:** all local endpoints work, tests pass, data persists between Compose restarts, and you can explain why public object storage is dangerous.
