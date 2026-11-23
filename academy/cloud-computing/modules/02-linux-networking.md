# Linux, DNS, TCP, TLS and HTTP

> **Professional task:** Intermittent HTTP 502 from a reverse proxy

## Business problem
Front end users cannot load a backend endpoint but a health checker sometimes passes.

## Learn the underlying concepts
DNS resolution, TCP handshake, optional TLS verification, HTTP routing and application processing are separate stages. A 502 is sent by an intermediary when an upstream response is invalid; a timeout may happen earlier. Retry budgets must account for latency amplification.

## Assignment
Use [networking lab](../../labs/networking/) and draw client → DNS → proxy → API → DB. Record status and delay; distinguish service unavailability from a wrong URL.

## Lab or source implementation
[HTTP lab](../../labs/networking/)

## Failure drill
Return 503 every second call; retry an idempotent GET with bounded backoff. Never blindly retry payments.

## Definition of done
Clear incident timeline, retry limit, failure stage and ownership documented.

**Work as an engineer:** capture exact configuration, command outputs, a before/after observation, test evidence, and a one-page architecture decision. Do not turn an untested reference into a production claim.

## Review checklist
- [ ] I can explain this mechanism without naming vendor products first.
- [ ] I can draw component boundaries, inputs, outputs and security controls.
- [ ] I tested or simulated a failure and recorded real evidence.
- [ ] I proposed AWS, GCP and Azure equivalent patterns with trade-offs.
- [ ] I considered ongoing operations, recovery and billing.

## Interview challenge
**Why can DNS resolution succeed while application traffic still times out?**

**Official reference:** https://developer.mozilla.org/en-US/docs/Web/HTTP

[Return to Cloud curriculum](../README.md)
