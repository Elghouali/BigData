# Kubernetes controllers and service discovery

> **Professional task:** Release causes pods to enter CrashLoopBackOff

## Business problem
A new container version cannot become ready, causing service outage.

## Learn the underlying concepts
A Deployment declares desired replicas; its controller reconciles observed state. Pods run containers; Services give stable networking; readiness controls traffic and liveness controls restart. Resource requests guide placement. Rolling updates and rollback are operational processes, not guarantees of bug-free deploys.

## Assignment
Define Deployment and Service YAML for the training API, with read-only secrets, probes and resource limits. Apply only to a local cluster (kind/minikube) when available.

## Lab or source implementation
[Kubernetes concepts](https://kubernetes.io/docs/concepts/)

## Failure drill
Deliberately configure the wrong readiness path; explain why the endpoint stops receiving traffic.

## Definition of done
YAML valid, no public credentials, rollback and rollout monitoring plan written.

**Work as an engineer:** capture exact configuration, command outputs, a before/after observation, test evidence, and a one-page architecture decision. Do not turn an untested reference into a production claim.

## Review checklist
- [ ] I can explain this mechanism without naming vendor products first.
- [ ] I can draw component boundaries, inputs, outputs and security controls.
- [ ] I tested or simulated a failure and recorded real evidence.
- [ ] I proposed AWS, GCP and Azure equivalent patterns with trade-offs.
- [ ] I considered ongoing operations, recovery and billing.

## Interview challenge
**What is the difference between a liveness probe failure and a readiness failure?**

**Official reference:** https://kubernetes.io/docs/concepts/workloads/controllers/deployment/

[Return to Cloud curriculum](../README.md)
