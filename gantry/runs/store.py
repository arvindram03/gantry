# SPDX-License-Identifier: Apache-2.0
"""Where run records are kept.

SQLite by default: a file, no server, and it outlives the process — which is
the whole requirement. The evidence is stored as JSON in one column rather than
shredded across tables, because the bundle is already the schema and a v0 store
should not become a second place the model is defined.

The indexed columns are the ones anyone actually searches by. Everything else
stays in the document.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from gantry.evidence import EvidenceBundle, Observation, ObservationSource
from gantry.runs.model import RunRecord
from gantry.verifier import CheckResult

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    engine TEXT NOT NULL,
    operation TEXT NOT NULL,
    decision TEXT NOT NULL,
    native_execution_id TEXT,
    proposal_hash TEXT,
    recorded_at TEXT NOT NULL,
    evidence TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS runs_recorded_at ON runs (recorded_at DESC);
CREATE INDEX IF NOT EXISTS runs_decision ON runs (decision);
"""


class RunStore(Protocol):
    """Persistence for run records, keyed by Gantry run id."""

    def record(self, evidence: EvidenceBundle) -> RunRecord: ...

    def get(self, run_id: str) -> RunRecord | None: ...

    def list(self, *, limit: int = 50, decision: str | None = None) -> Sequence[RunRecord]: ...


class MemoryRunStore:
    """Process-local storage, for tests and for callers who want none."""

    def __init__(self) -> None:
        self._runs: dict[str, RunRecord] = {}

    def record(self, evidence: EvidenceBundle) -> RunRecord:
        run = RunRecord(evidence=evidence, recorded_at=datetime.now(UTC))
        self._runs[evidence.run_id] = run
        return run

    def get(self, run_id: str) -> RunRecord | None:
        return self._runs.get(run_id)

    def list(self, *, limit: int = 50, decision: str | None = None) -> Sequence[RunRecord]:
        runs = sorted(self._runs.values(), key=lambda run: run.recorded_at, reverse=True)
        if decision is not None:
            runs = [run for run in runs if run.decision == decision]
        return tuple(runs[:limit])


class SQLiteRunStore:
    """Run records in a SQLite file.

    One connection guarded by a lock rather than one per call: SQLite objects
    are not safe to share across threads by default, and a run store is written
    once per execution, so contention is not the thing to optimise for.
    """

    def __init__(self, path: str | Path = ".gantry/runs.db") -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._connection = sqlite3.connect(str(self._path), check_same_thread=False)
        self._connection.executescript(_SCHEMA)
        self._connection.commit()

    def record(self, evidence: EvidenceBundle) -> RunRecord:
        recorded_at = datetime.now(UTC)
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO runs (run_id, engine, operation, decision, native_execution_id,
                                  proposal_hash, recorded_at, evidence)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    decision = excluded.decision,
                    recorded_at = excluded.recorded_at,
                    evidence = excluded.evidence
                """,
                (
                    evidence.run_id,
                    evidence.engine,
                    evidence.operation,
                    evidence.decision,
                    evidence.native_execution_id,
                    evidence.proposal_hash,
                    recorded_at.isoformat(),
                    evidence.to_json(),
                ),
            )
            self._connection.commit()
        return RunRecord(evidence=evidence, recorded_at=recorded_at)

    def get(self, run_id: str) -> RunRecord | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT evidence, recorded_at FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        return None if row is None else _row_to_record(row)

    def list(self, *, limit: int = 50, decision: str | None = None) -> Sequence[RunRecord]:
        query = "SELECT evidence, recorded_at FROM runs"
        parameters: tuple[object, ...] = ()
        if decision is not None:
            query += " WHERE decision = ?"
            parameters = (decision,)
        query += " ORDER BY recorded_at DESC LIMIT ?"
        with self._lock:
            rows = self._connection.execute(query, (*parameters, int(limit))).fetchall()
        return tuple(_row_to_record(row) for row in rows)

    def close(self) -> None:
        with self._lock:
            self._connection.close()


def _row_to_record(row: tuple[str, str]) -> RunRecord:
    return RunRecord(
        evidence=_bundle_from_dict(json.loads(row[0])),
        recorded_at=datetime.fromisoformat(row[1]),
    )


def _bundle_from_dict(payload: dict[str, object]) -> EvidenceBundle:
    """Rebuild a bundle from stored JSON.

    Round-tripping matters more than it looks: a record nobody can read back is
    a log line, not evidence.
    """
    return EvidenceBundle(
        run_id=str(payload["run_id"]),
        engine=str(payload["engine"]),
        operation=str(payload["operation"]),
        decision=str(payload["decision"]),
        native_execution_id=_optional_str(payload.get("native_execution_id")),
        proposal_hash=_optional_str(payload.get("proposal_hash")),
        inputs=tuple(str(value) for value in _sequence(payload.get("inputs"))),
        outputs=tuple(str(value) for value in _sequence(payload.get("outputs"))),
        started_at=_optional_time(payload.get("started_at")),
        finished_at=_optional_time(payload.get("finished_at")),
        execution=_mapping(payload.get("execution")),
        observations=tuple(
            Observation(
                name=str(item["name"]),
                value=item.get("value"),
                source=ObservationSource(str(item.get("source", "gantry"))),
                unit=_optional_str(item.get("unit")),
                observed_at=datetime.fromisoformat(str(item["observed_at"])),
            )
            for item in _sequence(payload.get("observations"))
        ),
        checks=tuple(
            CheckResult(
                name=str(item["check"]),
                ok=bool(item["passed"]),
                expected=item.get("expected"),
                actual=item.get("observed"),
                message=_optional_str(item.get("message")),
                metadata=_mapping(item.get("metadata")),
                supported=bool(item.get("supported", True)),
                source=_optional_str(item.get("source")),
            )
            for item in _sequence(payload.get("checks"))
        ),
    )


def _sequence(value: object) -> list[dict[str, object]]:
    return list(value) if isinstance(value, list) else []


def _mapping(value: object) -> dict[str, object]:
    return {str(key): item for key, item in value.items()} if isinstance(value, dict) else {}


def _optional_str(value: object) -> str | None:
    return None if value is None else str(value)


def _optional_time(value: object) -> datetime | None:
    return None if value is None else datetime.fromisoformat(str(value))


_default: RunStore = MemoryRunStore()


def configure(store: RunStore) -> None:
    """Replace the process-default store.

    In-memory unless a caller says otherwise: writing a file to someone's
    working directory because they imported a library is not a default worth
    having.
    """
    global _default
    _default = store


def record(evidence: EvidenceBundle | None) -> RunRecord | None:
    """Persist a run. `None` evidence records nothing, so callers need no guard."""
    return None if evidence is None else _default.record(evidence)


def get(run_id: str) -> RunRecord | None:
    return _default.get(run_id)


def recent(*, limit: int = 50, decision: str | None = None) -> Sequence[RunRecord]:
    """Recent runs, newest first, optionally only those with one decision."""
    return _default.list(limit=limit, decision=decision)


__all__ = [
    "MemoryRunStore",
    "RunStore",
    "SQLiteRunStore",
    "configure",
    "get",
    "recent",
    "record",
]
