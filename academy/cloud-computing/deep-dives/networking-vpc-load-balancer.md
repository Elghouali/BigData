# Deep Dive — Cloud Networking by Following One Request

**Scenario:** A new application deploys successfully but requests return HTTP 502. The database runs in a private subnet and only some app instances can connect.

## 1. The path matters

~~~mermaid
flowchart LR
 Browser[Browser DNS+TLS] --> LB[Public HTTPS load balancer]
 LB --> App[Private app subnet]
 App --> DB[(Private DB subnet)]
 App --> NAT[NAT / controlled outbound]
~~~

Example address plan (documentation only):
- VPC/VNet: 10.20.0.0/16
- Public load balancer subnet AZ-A: 10.20.0.0/24
- Private application subnet AZ-A: 10.20.10.0/24
- Private database subnet AZ-A: 10.20.20.0/24
- An equivalent separate-AZ group would need **non-overlapping** CIDRs.

A subnet is a network segment, but its *public/private* characteristic depends on routing and exposure, not merely a label. Routes determine reachability; a security group or firewall independently determines permitted packets. A NAT gateway supports outbound traffic from private nodes; it does not ordinarily provide unsolicited inbound connectivity.

## 2. Debug one failed request

1. Did DNS resolve to the expected load balancer?
2. Did TCP establish a connection on port 443?
3. Did TLS terminate with a valid certificate?
4. Does the load balancer see a healthy target? Is its health path correct?
5. Is there a route from LB to app and the necessary security-group/firewall rule?
6. Can the app resolve and reach the database on its actual port (e.g. PostgreSQL 5432)?
7. Is the app ready, authenticated, and within timeout budget?
8. Are there correlated failures in a zone, or a shared exhausted database?

A 502 can originate at a proxy even if DNS works. A 504 is often a timeout at an intermediary; neither uniquely identifies the root cause.

## 3. Scaling is not a magic cure

Adding application replicas can increase throughput **only if** the bottleneck is horizontally scalable. If the DB is saturated, adding many replicas may generate more queries and worsen p95 latency. If the load balancer checks the wrong path, it may route to zero healthy targets even with idle instances.

## 4. Real incident exercise

- [ ] Draw routes and rules for browser → LB → API → PostgreSQL.
- [ ] Show exactly which source, destination, protocol and port each rule allows.
- [ ] Remove app ingress in your diagram and predict the failure symptom.
- [ ] Replace a healthy target with an unready one; explain probe impact.
- [ ] Write a diagnostic plan using application logs, LB target status, flow logs and DB connection metrics.
- [ ] Translate the design to AWS VPC/ALB/EC2, GCP VPC/Cloud Load Balancing and Azure VNet/Application Gateway. Describe differences rather than claiming 1:1 identical behavior.

**Interview:** Why doesn't a private subnet require NAT for a private DB connection inside the same routable virtual network?

Sources: https://docs.aws.amazon.com/vpc/latest/userguide/what-is-amazon-vpc.html ; https://cloud.google.com/vpc/docs/overview ; https://learn.microsoft.com/azure/virtual-network/virtual-networks-overview
