# START HERE — Your first 90 minutes

Do **not** start with Kafka, Kubernetes, or the biggest Docker Compose stack. Prove the foundations first.

## 0–15 min: environment

- Install Git and Python 3.11+.
- Clone this repository and create your own feature branch.
- Check: python --version, git --version.
- Read [Security](SECURITY.md). Do not commit local secret files.
- Read the GitHub Actions checks; green CI is evidence for *that specific tested commit*, not proof of deployment.

Try the [academy CLI](HOW_TO_USE.md) from the repository root to list available labs and check tool availability:

~~~bash
python academy/academy.py doctor
python academy/academy.py list --track big-data
~~~

## 15–30 min: conceptual diagnostic

On paper, answer **without searching**:
1. What happens between typing a URL and receiving an HTTP response?
2. What is the difference between a process, container, VM, and server?
3. What is the difference between a database transaction and a streaming event?
4. Why is CSV input unsafe without schema validation?
5. Explain partitioning vs replication.
6. What happens if a consumer processes an event but crashes before acknowledging it?

If you cannot explain, that is your starting gap, not a reason to skip ahead.

## 30–65 min: first working lab

~~~bash
cd academy/labs/sql-etl
python -m unittest discover -s tests -v
python pipeline.py --source data/orders.csv --database output/retail.db
~~~

Inspect the printed JSON summary. Open the SQLite database in a SQLite viewer or with Python. Then intentionally modify a row's quantity to zero and rerun. Explain the change in rejected row counts.

## 65–90 min: repeatability

Run the ETL twice. Why is the result not doubled? Find the exact line responsible. Write down three ways the pipeline could still fail.

## Learning contract

For each module:
1. Explain the mental model out loud in 2 minutes.
2. Draw an architecture and trace one request/event through it.
3. Follow the lab, then **rebuild** it in your own branch.
4. Make one failure intentionally; write how you recovered.
5. Run automated checks and attach **real** output to your notes.
6. Answer the module's single checkpoint question before moving forward.

Use [PROGRESS.md](PROGRESS.md) as your lab journal. The reference code is only a teacher; your own reproduction is the deliverable.

## Common setup notes

- Windows: use PowerShell and Python's launcher (py) if necessary. Install Docker Desktop/WSL2 only for the container labs.
- macOS/Linux: use a virtual environment for Python packages.
- Python 3.14: the pure-standard-library labs should work. The existing PySpark 3.5 project targets Python 3.11; use its provided container.
- A company laptop, school network, proxy or low RAM can affect Docker and package installs. Do not pretend a failed setup succeeded.

**Next:** [20-week roadmap](ROADMAP_20_WEEKS.md).
