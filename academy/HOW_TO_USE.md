# How to Use Your Engineering Academy

The academy lives in source control and can be run locally on a laptop. The Python learning CLI does not install dependencies, launch cloud instances, or create paid resources.

## Setup

Clone Elghouali/BigData. Until the academy pull request is merged, check out the feature/academy-data-cloud-from-zero branch. Run these commands from the repository root:

~~~bash
python academy/academy.py doctor
python academy/academy.py list
python academy/academy.py list --track cloud-computing
python academy/academy.py test sql-etl
~~~

Use Python 3.11 or later. Pure Python labs require no third-party packages. Spark, MinIO and Terraform labs have their own prerequisites.

## Learning tracker

~~~bash
python academy/academy.py mark sql-etl --status in-progress --note "Read data contracts and reviewed validation rules"
python academy/academy.py progress
python academy/academy.py mark sql-etl --status done --note "Independently implemented and tested"
~~~

Your progress is written to academy/.academy-progress.json, ignored by Git. This is **self-reported**, not an automatic certification. Keep real test outputs, failure traces and design decisions in your practice branch.

## Run automated tests

~~~bash
python academy/academy.py test all
~~~

Runs the registered Python stdlib test suites. Manual labs and external containers are not started automatically. Run those explicitly and capture evidence. To validate one lab, use its ID from the list command.

## Build like a professional engineer

1. Select one business ticket from WORK_TICKETS.md.
2. Restate the acceptance criteria and draw the architecture.
3. Read the theory, then build your own implementation branch.
4. Create repeatable tests; reproduce a failure.
5. Record observed output, mitigation, security and cost trade-offs.
6. Write a pull request with the tests, an ADR and a short operational runbook.

The reference implementations are examples, not completed personal projects. Learning means being able to reproduce and defend the approach without copying them.

## Safe use

Keep any training credentials and HTTP services local. Never commit real secrets, personally identifiable data, Terraform state or local progress data. Do not use cloud infrastructure templates without reviewing access policies, expected costs and cleanup.
