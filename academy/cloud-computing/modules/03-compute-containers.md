# Compute: VM, containers and serverless

> **Professional task:** API container OOM-killed while generating reports

## Business problem
The backend exhausts memory, restarts and causes cascading retries.

## Learn the underlying concepts
A VM emulates hardware with a guest OS. A container packages an isolated process but shares the host kernel; resource limits come from cgroups and namespaces. A function platform invokes code and may scale per request. CPU throttling, memory pressure and graceful termination determine real behavior.

## Assignment
Containerize the academy HTTP API, set a memory limit, inspect logs and compare cold start/steady-state. Describe requests, limits and horizontal scaling assumptions.

## Lab or source implementation
[Networking lab](../../labs/networking/)

## Failure drill
Lower memory below job needs and distinguish an OOM kill from an application exception.

## Definition of done
Docker build reproducible; health checks meaningful; limits and restart policy justified.

**Work as an engineer:** capture exact configuration, command outputs, a before/after observation, test evidence, and a one-page architecture decision. Do not turn an untested reference into a production claim.

## Review checklist
- [ ] I can explain this mechanism without naming vendor products first.
- [ ] I can draw component boundaries, inputs, outputs and security controls.
- [ ] I tested or simulated a failure and recorded real evidence.
- [ ] I proposed AWS, GCP and Azure equivalent patterns with trade-offs.
- [ ] I considered ongoing operations, recovery and billing.

## Interview challenge
**Why will horizontal replicas not solve a shared database bottleneck?**

**Official reference:** https://docs.docker.com/get-started/

[Return to Cloud curriculum](../README.md)
