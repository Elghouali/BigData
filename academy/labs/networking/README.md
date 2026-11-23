# Lab 02 — Production HTTP Incident & Retries

**Engineering ticket:** The reporting API intermittently returns HTTP 503, and client retries overload it.

Run from this folder using Python 3.11+ (no packages needed):

~~~bash
python -m unittest discover -s tests -v
python server.py --port 8089
~~~

In another terminal: python client.py --url http://127.0.0.1:8089/flaky

Endpoints: /health always 200; /flaky alternates 503 and 200; /down always 503. The client retries **GET only** with a bounded budget. Never blindly retry a payment POST without an idempotency key.

## Professional acceptance
- State whether a failure occurred during DNS, TCP, TLS, proxy, or application handling.
- Distinguish timeout, connection refused and HTTP 503.
- Show failed and recovered calls with actual outputs.
- Explain retry amplification, backoff, jitter and deadlines.
- Add your own total deadline and structured logs.
- Write an incident runbook with customer impact and recovery actions.
