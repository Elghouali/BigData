# Lab 03 — Streaming Fraud Window Simulation

**Professional task:** A fraud dashboard must count mobile purchases by event time despite duplicates and delayed delivery. Learn the mechanism in pure Python before switching to Kafka/Spark.

Run from this directory:

~~~bash
python -m unittest discover -s tests -v
python stream.py --input data/events.csv --lateness 30
python stream.py --input data/events.csv --lateness 120
~~~

Business contract: one event_id per logical transaction; one-minute tumbling windows; the watermark before a record is maximum prior event time minus allowed lateness. Duplicate IDs are rejected; events older than the watermark are tracked separately.

## Your assignments
1. Observe the effect of late events on revenue counts.
2. Add out-of-order timestamps and duplicate event IDs with different payloads.
3. Design a dead-letter topic or correction mechanism for late financial transactions.
4. Translate the model to Kafka consumer groups and Spark Structured Streaming with watermarks.
5. Explain the alert-freshness versus historical-correctness trade-off.
6. Add processing lag and reject metrics for an on-call dashboard.

**Acceptance:** Test output, failure observation, an architecture diagram and a written late-data policy.
