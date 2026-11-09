# SPDX-License-Identifier: Apache-2.0
"""A durable run store in a SQLite file.

A file, no server, and it outlives the process — which is the whole
requirement. The run is stored as JSON in one column rather than shredded
across tables: the model is already the schema, and a v0 store should not
become a second place it is defined. The indexed columns are the ones anyone
searches by.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Sequence
from pathlib import Path

from gantry.runs.model import Run
from gantry.runs.serialization import evidence_from_dict, run_from_dict
from gantry.runs.status import RunStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    kind TEXT NOT NULL,
    engine TEXT NOT NULL,
    actor TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    document TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS runs_created_at ON runs (created_at DESC);
CREATE INDEX IF NOT EXISTS runs_status ON runs (status);
"""


class SQLiteRunStore:
    """Run records in a SQLite file.

    One connection behind a lock rather than one per call: SQLite objects are
    not safe to share between threads by default, and a run is written a
    handful of times per operation, so contention is not the thing to optimise.
    """

    def __init__(self, path: str | Path = ".gantry/runs.db") -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._connection = sqlite3.connect(str(self._path), check_same_thread=False)
        self._connection.executescript(_SCHEMA)
        self._connection.commit()

    def create(self, run: Run) -> None:
        self._write(run, insert=True)

    def update(self, run: Run) -> None:
        self._write(run, insert=False)

    def compare_and_set(self, run_id: str, expected: RunStatus, updated: Run) -> bool:
        """One statement, so the check and the write cannot be separated.

        `WHERE status = ?` is the whole mechanism: two hosts confirming the same
        run both run this, SQLite serialises them, and the second one updates no
        rows. Only the caller that changed a row may start work.
        """
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE runs SET status = ?, updated_at = ?, document = ?
                WHERE id = ? AND status = ?
                """,
                (
                    updated.status.value,
                    updated.updated_at.isoformat(),
                    updated.to_json(),
                    run_id,
                    expected.value,
                ),
            )
            self._connection.commit()
            return cursor.rowcount == 1

    def get(self, run_id: str) -> Run | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT document FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
        return None if row is None else run_from_dict(json.loads(row[0]))

    def recent(self, *, limit: int = 50, status: str | None = None) -> Sequence[Run]:
        query = "SELECT document FROM runs"
        parameters: tuple[object, ...] = ()
        if status is not None:
            query += " WHERE status = ?"
            parameters = (status,)
        query += " ORDER BY created_at DESC, id DESC LIMIT ?"
        with self._lock:
            rows = self._connection.execute(query, (*parameters, int(limit))).fetchall()
        return tuple(run_from_dict(json.loads(row[0])) for row in rows)

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def _write(self, run: Run, *, insert: bool) -> None:
        document = run.to_json()
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO runs (id, status, kind, engine, actor, created_at, updated_at, document)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    status = excluded.status,
                    updated_at = excluded.updated_at,
                    document = excluded.document
                """,
                (
                    run.id,
                    run.status.value,
                    run.operation.kind.value,
                    run.operation.engine,
                    run.actor.label,
                    run.created_at.isoformat(),
                    run.updated_at.isoformat(),
                    document,
                ),
            )
            self._connection.commit()


__all__ = ["SQLiteRunStore", "evidence_from_dict", "run_from_dict"]
