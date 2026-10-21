"""One competing worker, run as a separate process by test_store.py.

    python race_worker.py DB_PATH WORKER_ID SIGNAL_DIR

Opens the store, creates SIGNAL_DIR/ready-WORKER_ID, then waits for SIGNAL_DIR/go.
The test creates "go" only after every worker is ready, so they all start
claiming at the same moment instead of the first process to finish starting up
draining the queue alone. Each job takes a couple of milliseconds of simulated
work, so claims interleave. Prints the ids it claimed as JSON on stdout.
"""

import json
import sys
import time
from pathlib import Path

from jobrunner import JobStore


def main() -> None:
    db_path, worker, signals = sys.argv[1], sys.argv[2], Path(sys.argv[3])
    claimed = []
    with JobStore.open(db_path) as store:
        (signals / f"ready-{worker}").touch()
        deadline = time.monotonic() + 30
        while not (signals / "go").exists():
            if time.monotonic() > deadline:
                sys.exit("start signal never arrived")
            time.sleep(0.0005)
        while True:
            job = store.claim_next(worker, now=1_000, lease_ms=60_000)
            if job is None:
                break
            claimed.append(job.job_id)
            time.sleep(0.002)  # simulated work, outside any transaction
            store.complete(job.job_id, job.token, now=1_001, result={"by": worker})
    print(json.dumps(claimed))


if __name__ == "__main__":
    main()
