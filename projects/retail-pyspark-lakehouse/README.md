# Retail Analytics — PySpark Lakehouse

Build an **end-to-end batch data pipeline**: CSV → Bronze (raw Parquet) → Silver (validated Parquet) + rejected rows → Gold (business KPIs).

**Stack:** Python 3.11 · PySpark 3.5 · Parquet · pytest · Docker

~~~mermaid
flowchart LR
  CSV[Retail CSV] --> S[PySpark ingestion]
  S --> B[(Bronze)]
  S --> Q{Data quality}
  Q -->|Valid| V[(Silver)]
  Q -->|Invalid| R[(Rejects)]
  V --> G[(Gold revenue)]
~~~

## Run

**Option A (recommended):** Docker Desktop installed.

~~~bash
cd projects/retail-pyspark-lakehouse
docker build -t retail-lakehouse .
docker run --rm retail-lakehouse
~~~

Inside Docker, outputs are written to /work/output. To retain them locally, mount a folder on the host (Docker Desktop bind mount), or use Option B.

**Option B:** Python 3.11 and Java 17 installed.

~~~bash
python -m venv .venv
python -m pip install -r requirements.txt
python -m src.pipeline --input data/orders.csv --output output
python -m pytest -q
~~~

## Quality contract

- Remove duplicate order IDs (the sample repeats an *identical* row).
- Reject missing IDs/customer IDs, invalid dates, nonpositive quantities, negative prices, and unsupported statuses.
- Keep valid cancelled orders in Silver but **exclude** them from paid revenue in Gold.
- Sum paid revenue and count orders by **order date and category**.

| Output | Description |
| --- | --- |
| output/bronze | All source rows, Parquet |
| output/silver | Valid, typed rows, date-partitioned Parquet |
| output/rejected | Invalid rows with reject_reason |
| output/gold | Daily category paid revenue and order count |

**Expected sample results:** 10 source rows, 1 duplicate removed, 4 valid unique orders, 5 rejects, and **325.00 MAD** total paid revenue.

## Rebuild-by-hand challenges

1. Implement CSV read with an explicit schema.
2. Implement the validation rules yourself, without looking at the reference.
3. Explain why cancelled orders are kept in Silver but not Gold.
4. Add a customer-ID format rule and its unit tests.
5. Replace order-ID deduplication with a deterministic latest-event rule (timestamp + window).
6. Generate one million synthetic rows and measure Spark partition/shuffle behavior.
7. Upload curated Parquet output to the [cloud object storage lab](../cloud-object-store-api/).

**Interview practice:** What is the distinction between ETL and ELT? Why retain rejected rows? When does Spark shuffle? How can partition choices make a query slower?

> Educational reference. A full local execution and integration test have not yet been independently verified in the connected environment.
