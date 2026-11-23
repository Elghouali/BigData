# Lab 01 — Retail Ingestion & SQL Quality Control

**Work ticket:** "The sales department's CSV feed has duplicates, invalid amounts and missing customers. Build a rerunnable ingestion service with auditable rejects and a correct paid-revenue query."

**Tools:** Python standard library, SQLite; no pip, Docker or cloud required.

## Run from this folder

~~~bash
python -m unittest discover -s tests -v
python pipeline.py --source data/orders.csv --database output/retail.db
python pipeline.py --source data/orders.csv --database output/retail.db
~~~

The final database must contain **4 valid unique orders**, **5 rejected source rows**, and **325.00 MAD** of paid revenue. The dataset contains **10 input rows**, including one exact duplicate that is not an extra order. Re-running must not inflate totals.

## Workflow

~~~mermaid
flowchart LR
 CSV[Daily orders CSV] --> P[Validate + deduplicate]
 P --> O[(SQLite clean orders)]
 P --> R[(SQLite rejected records)]
 O --> Q[SQL daily revenue report]
~~~

In a professional setting, raw rows, reject reasons, a unique business key and transactional commits make this pipeline auditable. Rejection is not a substitute for diagnosing faulty upstream feeds.

## Exercise progression
1. Draw a data contract for the CSV including type, nullable status, and valid range.
2. Run the reference code, inspect SQLite tables and explain every quality rule.
3. Build your version **without copying** from pipeline.py.
4. Add a new status, e.g. returned, and decide its revenue semantics.
5. Add a unit test for conflict: same order_id with different amount.
6. Record before/after counts when running twice.
7. Explain how you would implement the same transformation using PostgreSQL, Airflow and Spark.

## Professional acceptance criteria
- Contract written and raw input not modified
- No silent bad-row loss
- Typed money values with exact decimal representation
- Idempotent daily reprocessing
- Automated tests and actual output recorded
- Short incident note for one deliberately corrupted row
