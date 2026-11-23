# Template — Professional Engineering Lab

Copy this structure when authoring a new lab. Replace placeholders with actual business requirements, runnable code and test evidence.

## Ticket: ROLE-123 — Concrete customer or operator problem

**Who reports it?** Business stakeholder / platform on-call / data consumer.
**Impact:** Incorrect metric, outage, duplicated side effect, lost event, security or cost risk.
**Requirements:** State measurable SLIs/SLOs, schema, throughput, latency or recovery expectations.

## What you must understand

1. **First principles:** what need does this technology solve? Which alternatives exist?
2. **Mechanism:** data flow, internal state, communication, persistence and consistency.
3. **Failure modes:** what breaks, what is observed, and what can be retried?
4. **Trade-offs:** performance, correctness, operations, security, cloud portability and cost.

## Architecture

Include a Mermaid diagram and describe trust, state and failure boundaries.

## Reproduction

- Supported OS and version matrix
- Exact setup, run, test and cleanup commands
- Synthetic inputs and expected outputs
- Explicitly stated unverified or manual steps

## Learner challenge

1. Run reference and record real results.
2. Reimplement from the requirements on a new branch.
3. Add tests covering both successful and failed operations.
4. Deliberately inject a failure and recover.
5. Collect observations and write an architecture decision record.
6. Explain an AWS/GCP/Azure pattern, if applicable.

## Acceptance criteria

- [ ] Correct result and reproducibility
- [ ] Verified negative tests and recovery
- [ ] Observability and runbook
- [ ] No leaked credentials or personally identifiable data
- [ ] Performance and cost claims backed by measurements or labeled assumptions
- [ ] README, tests, and a code review summary

## Interview checkpoint

Ask one demanding question requiring reasoning, not a vocabulary definition.

## Reference materials

Link official or openly licensed documentation with version information. Never paste copyrighted chapters without permission.
