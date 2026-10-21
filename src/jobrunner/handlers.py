"""Demo job handlers.

summarize_csv reads a CSV file and writes a JSON summary next to it. It is
idempotent: the same input always produces the same output, written with an
atomic rename, so running it twice (which at-least-once delivery allows) just
replaces the file with an identical one.
"""

from __future__ import annotations

import csv
import json
import os
import tempfile
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from jobrunner.worker import JobContext, Permanent

HEARTBEAT_EVERY_ROWS = 50_000


def summarize_csv(payload: Any, ctx: JobContext) -> dict:
    """Payload: {"input": "data.csv", "output": "summary.json"}.

    Writes {"rows", "columns", "numeric": {column: {"min", "max", "sum"}}} to the output and
    returns the same summary. Numbers are summed as Decimal and written as text, so the
    result is exact and identical on every run.
    """
    if not isinstance(payload, dict) or set(payload) != {"input", "output"}:
        raise Permanent('payload must be {"input": <csv path>, "output": <json path>}')
    source, target = Path(payload["input"]), Path(payload["output"])
    if source.resolve() == target.resolve():
        raise Permanent("output must be a different file from the input")
    if not target.parent.is_dir():
        raise Permanent(f"output folder {target.parent} does not exist")

    try:
        with source.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle, strict=True)
            header = next(reader, None)
            if not header:
                raise Permanent(f"{source} is empty")
            numeric: dict[str, dict[str, Decimal]] = {}
            not_numeric: set[str] = set()
            rows = 0
            for row in reader:
                if not row:
                    continue
                if len(row) != len(header):
                    raise Permanent(f"{source} line {reader.line_num} has {len(row)} fields, expected {len(header)}")
                rows += 1
                if rows % HEARTBEAT_EVERY_ROWS == 0:
                    ctx.heartbeat()  # a big file: keep the lease alive
                for column, raw in zip(header, row):
                    if column in not_numeric or raw.strip() == "":
                        continue
                    try:
                        value = Decimal(raw)
                    except InvalidOperation:
                        not_numeric.add(column)
                        numeric.pop(column, None)
                        continue
                    if not value.is_finite():
                        not_numeric.add(column)
                        numeric.pop(column, None)
                        continue
                    stats = numeric.setdefault(column, {"min": value, "max": value, "sum": Decimal(0)})
                    stats["min"], stats["max"] = min(stats["min"], value), max(stats["max"], value)
                    stats["sum"] += value
    except FileNotFoundError:
        raise Permanent(f"input file {source} does not exist") from None
    except UnicodeDecodeError:
        raise Permanent(f"{source} is not UTF-8 text") from None
    except csv.Error as exc:
        raise Permanent(f"{source} is not a readable CSV: {exc}") from None

    summary = {
        "rows": rows,
        "columns": header,
        "numeric": {c: {k: str(v) for k, v in numeric[c].items()} for c in header if c in numeric},
    }
    _write_atomically(target, json.dumps(summary, indent=2) + "\n")
    return summary


def _write_atomically(path: Path, text: str) -> None:
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


HANDLERS = {"summarize_csv": summarize_csv}
