# Event time, watermarks and windows

> **Real engineering ticket:** Late mobile transactions spoil a fraud dashboard

## Context and user impact
Events are delayed, duplicated and out of order, while analysts expect per-minute counts.

## Concepts to understand deeply
Event time describes when business activity happened; processing time describes arrival. Watermarks bound allowed lateness, trading completeness versus operational state size. Dedup keys prevent replays from inflating windows.

## What to implement
Run [stream simulation](../../labs/stream-processing/) with 60-second windows. Vary allowed-lateness threshold and inspect accepted versus late events.

## Reference lab
[Stream simulation](../../labs/stream-processing/)

**Hands-on protocol:** (1) draw the architecture; (2) write the first version; (3) run and record actual outputs; (4) implement tests; (5) cause a failure; (6) fix and rerun; (7) commit the work in a practice branch. Do not copy the reference code into your answer without understanding it.

## Failure injection
Send an event eight minutes late and a duplicate ID. Observe where each goes.

## Ticket acceptance criteria
Counts deterministic under duplicates, documented late-data policy and source timestamps.

## Evidence checklist
- [ ] Architecture sketch shows data flow, boundaries and state.
- [ ] Reproducible run commands and required versions are documented.
- [ ] A test demonstrates the intended contract.
- [ ] A failure/recovery observation is recorded, not fabricated.
- [ ] Security, privacy, monitoring and cost implications are explained.

## Interview challenge
**Who sets the acceptable lateness threshold?**

**Upstream documentation:** https://spark.apache.org/docs/latest/streaming/

[Back to Big Data curriculum](../README.md)
