# Reliable Job Runner

[![tests](https://github.com/SohamMessi10/reliable-job-runner/actions/workflows/tests.yml/badge.svg)](https://github.com/SohamMessi10/reliable-job-runner/actions/workflows/tests.yml)

A background job queue that retries temporary failures, gives up on permanent ones, recovers
jobs from crashed workers, and never lets a stale worker overwrite a newer attempt.

> **Status:** in development. The job state machine and SQLite storage with safe claiming are done. See
> [`docs/design.md`](docs/design.md) for the plan and [`PROGRESS.md`](PROGRESS.md) for status.

## Setup

Requires Python 3.11 or newer.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
python -m pytest
```
