# Lab 05 — On-Call Data Freshness and Quality Monitoring

**Professional incident OPS-205:** The scheduled ETL is green, but finance sees yesterday's dashboard. Build a measurable freshness and quality gate that tells an on-call engineer whether to publish or investigate.

This exercise uses Python and SQLite (standard library only), without a cloud account.

## Architecture

~~~mermaid
flowchart LR
  DB[(Gold SQL)] --> M[Monitor]
  MF[ETL run manifest] --> M
  CLOCK[Evaluation timestamp] --> M
  M --> OK[Healthy]
  M --> A[Alert + issues + metrics]
~~~

## Run from the repository root

Generate a verified data product first:

~~~bash
python academy/capstones/retail-analytics-data-product/run.py --output academy/capstones/retail-analytics-data-product/output
~~~

Then monitor it with the current date and default thresholds:

~~~bash
python academy/labs/data-observability/monitor.py --database academy/capstones/retail-analytics-data-product/output/orders.db --manifest academy/capstones/retail-analytics-data-product/output/manifest.json
~~~

The sample fixture is historical, so the **data freshness check may alert** today. That is intentional. To reproduce a known healthy historical run, pass --as-of 2026-11-03T00:00:00+00:00 to the monitor with a manifest whose generated_at_utc matches that instant. You can practice that deterministically with the unit-test fixtures.

~~~bash
cd academy/labs/data-observability
python -m unittest discover -s tests -v
~~~

## What the monitor observes

- **Job freshness:** time since generated_at_utc in the manifest, in minutes.
- **Data event freshness:** days since MAX(order_date) in the validated orders table.
- **Quality:** manifest quality_passed flag, reject_ratio and published KPI count.

The business event date is only a **teaching proxy** for source-feed freshness; an actual data platform should track source arrival timestamp, ingestion watermark, and published partition completeness separately.

## Engineering tasks

1. Explain why a successful Airflow task cannot prove up-to-date data.
2. Add a missing-daily-partition check using an expected business calendar.
3. Add a warning severity separate from critical.
4. Record SLA/SLO definitions and who owns each alert.
5. Add structured logging and a notification adapter (local by default).
6. Make a failure runbook: late source file, high reject ratio, stale Gold, recovery/backfill and verification.

**Acceptance:** deterministic tests prove healthy, stale-job, missing-metadata and stale-data alerts. Never send real secrets or customer data in alert payloads.
