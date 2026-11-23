# Virtual networks, routes, NAT and load balancing

> **Professional task:** Private database unreachable from app subnet

## Business problem
After moving services to a private subnet, the API has no route to the database.

## Learn the underlying concepts
CIDR defines address ranges, subnetting separates routing domains, route tables choose network paths. Security groups and firewall ACLs constrain allowed packets. NAT enables outbound connections from private hosts; load balancers direct traffic among healthy backends. DNS resolution and reachability must be debugged separately.

## Assignment
Draw public load balancer + private application + private database across two zones. For each hop list IP/port/route/firewall. Map to VPC/VNet and vendor equivalents.

## Lab or source implementation
[Networking lab](../../labs/networking/)

## Failure drill
Remove one required route or ingress rule in your diagram and show the exact failure symptom and investigation steps.

## Definition of done
No public DB endpoint, scoped ports, return path valid, clear DNS and routing assumptions.

**Work as an engineer:** capture exact configuration, command outputs, a before/after observation, test evidence, and a one-page architecture decision. Do not turn an untested reference into a production claim.

## Review checklist
- [ ] I can explain this mechanism without naming vendor products first.
- [ ] I can draw component boundaries, inputs, outputs and security controls.
- [ ] I tested or simulated a failure and recorded real evidence.
- [ ] I proposed AWS, GCP and Azure equivalent patterns with trade-offs.
- [ ] I considered ongoing operations, recovery and billing.

## Interview challenge
**Why is adding another load balancer not a remedy for exhausted backend CPU?**

**Official reference:** https://docs.aws.amazon.com/vpc/latest/userguide/what-is-amazon-vpc.html

[Return to Cloud curriculum](../README.md)
