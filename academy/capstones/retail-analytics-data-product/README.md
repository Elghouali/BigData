# Capstone P1 — Trustworthy Retail Analytics Data Product

**Ticket FIN-101: "Our sales team receives daily order CSVs with duplicated IDs and invalid amounts. Build one reliable and auditable endpoint for paid revenue. It must safely rerun if our scheduler retries."**

## System architecture

~~~mermaid
flowchart LR
  CSV[Orders CSV] --> ING[Validated ingestion]
  ING --> RAW[(Raw + rejected rows)]
  ING --> SQL[(SQLite valid orders)]
  SQL --> G[(Daily Gold KPIs)]
  G --> API[Read-only HTTP JSON API]
  ING --> Q[Data-quality gate + manifest]
  Q --> API
~~~

### Why this resembles a professional ticket

The business cares about accuracy, reproducibility and traceability—not the novelty of the framework. This local implementation separates raw, validated and aggregated data; validates input and reports quality statistics; and lets another service read KPIs. Later you will migrate storage to PostgreSQL, orchestration to Airflow and transforms to Spark.

## Run (Python 3.11+; no dependencies)

~~~bash
cd academy/capstones/retail-analytics-data-product
python -m unittest discover -s tests -v
python run.py --output output
python api.py --database output/orders.db --manifest output/manifest.json
~~~

In another terminal:

~~~bash
curl http://127.0.0.1:8090/health
curl http://127.0.0.1:8090/metrics
curl http://127.0.0.1:8090/kpis
curl "http://127.0.0.1:8090/kpis?category=Electronics"
~~~

Produced artifacts:
- output/orders.db — raw, valid, rejected and Gold tables
- output/daily_kpis.csv — portable Gold export
- output/manifest.json — input hash, row counts, revenue and the quality decision

**Expected fixture:** 4 accepted unique orders, 5 rejected, 1 exact duplicate, 3 paid orders, 325.00 MAD total paid revenue. The sample's reject ratio is intentionally high for training, so the default demo gate is lenient (0.60). **Production must set a threshold with the business and operations owners.**

## Your professional implementation tasks

- [ ] Rebuild the data contract and reject policy yourself.
- [ ] Add a daily partition input for a different date without overwriting previous results.
- [ ] Add test coverage for a conflicting order ID and malformed money value.
- [ ] Introduce a strict 0.10 rejection threshold; demonstrate the publish step is blocked.
- [ ] Run twice: show stable KPIs and unchanged logical order count.
- [ ] Add structured logs and an alert when freshness exceeds your defined SLO.
- [ ] Write a runbook for source arrival failure and reprocessing.
- [ ] Explain security requirements before making the API public.

## Interview defense

Explain *exactly* how this service avoids inflated revenue, why it uses integer cents instead of floating-point money, what the quality gate protects, and how you would scale the design from laptop to cloud.

**Not production-ready:** no authentication, scheduler, persistent backups or distributed transactions. HTTP binds to localhost only. Tests in CI are a learning gate, not evidence of load capacity or cloud deployment.
