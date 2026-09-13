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
from datetime import datetime
from pathlib import Path

from gantry.actor import ActorRef, ActorType
from gantry.confirmation.model import ConfirmationReason, ConfirmationRecord
from gantry.confirmation.status import ConfirmationReasonCode, ConfirmationStatus
from gantry.evidence import EvidenceBundle, Observation, ObservationSource
from gantry.runs.model import (
    AdmissionRecord,
    ExecutionRecord,
    OperationKind,
    OperationRef,
    ProposalRecord,
    ProposalStorage,
    QueryResultRef,
    ResourceRef,
    Run,
)
from gantry.runs.status import RunStatus
from gantry.verifier import CheckResult, CheckSource, VerificationResult

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


def run_from_dict(payload: dict[str, object]) -> Run:
    """Rebuild a run from its stored form.

    Round-tripping is the property the whole feature rests on: a record nobody
    can read back is a log line.
    """
    actor_payload = _mapping(payload.get("actor"))
    operation = _mapping(payload.get("operation"))
    return Run(
        id=str(payload["id"]),
        status=RunStatus(str(payload["status"])),
        actor=ActorRef(
            type=ActorType(str(actor_payload.get("type", "unknown"))),
            id=_optional_str(actor_payload.get("id")),
            session_id=_optional_str(actor_payload.get("session_id")),
            metadata=_mapping(actor_payload.get("metadata")),
        ),
        operation=OperationRef(
            kind=OperationKind(str(operation.get("kind", "query"))),
            engine=str(operation.get("engine", "unknown")),
            provider=_optional_str(operation.get("provider")),
        ),
        proposal=_proposal(payload.get("proposal")),
        inputs=_refs(payload.get("inputs")),
        outputs=_refs(payload.get("outputs")),
        result_ref=_result_ref(payload.get("result_ref")),
        admission=_admission(payload.get("admission")),
        confirmation=_confirmation(payload.get("confirmation")),
        execution=_execution(payload.get("execution")),
        verification=_verification(payload.get("verification")),
        evidence=_evidence(payload.get("evidence")),
        created_at=_time(payload.get("created_at")) or datetime.now(),
        updated_at=_time(payload.get("updated_at")) or datetime.now(),
        completed_at=_time(payload.get("completed_at")),
    )


def _proposal(value: object) -> ProposalRecord | None:
    data = _mapping(value)
    if not data:
        return None
    return ProposalRecord(
        hash=str(data.get("hash", "")),
        kind=str(data.get("kind", "sql")),
        body=_optional_str(data.get("body")),
        storage=ProposalStorage(str(data.get("storage", "full"))),
        agent_verification=tuple(str(item) for item in _sequence(data.get("agent_verification"))),
    )


def _refs(value: object) -> tuple[ResourceRef, ...]:
    return tuple(
        ResourceRef(system=str(item.get("system", "")), resource=str(item.get("resource", "")))
        for item in _sequence(value)
        if isinstance(item, dict)
    )


def _result_ref(value: object) -> QueryResultRef | None:
    data = _mapping(value)
    if not data:
        return None
    return QueryResultRef(
        rows=_optional_int(data.get("rows")) or 0,
        inline=bool(data.get("inline", True)),
        truncated=bool(data.get("truncated", False)),
    )


def _admission(value: object) -> AdmissionRecord | None:
    data = _mapping(value)
    if not data:
        return None
    decided = _time(data.get("decided_at"))
    return AdmissionRecord(
        allowed=bool(data.get("allowed", False)),
        reasons=tuple(str(item) for item in _sequence(data.get("reasons"))),
        policy=_optional_str(data.get("policy")),
        policy_hash=_optional_str(data.get("policy_hash")),
        matched_rules=tuple(str(item) for item in _sequence(data.get("matched_rules"))),
        codes=tuple(str(item) for item in _sequence(data.get("codes"))),
        request=_mapping(data.get("request")) or None,
        **({"decided_at": decided} if decided is not None else {}),
    )


def _confirmation(value: object) -> ConfirmationRecord | None:
    data = _mapping(value)
    if not data:
        return None
    return ConfirmationRecord(
        status=ConfirmationStatus(str(data.get("status", "not_required"))),
        run_id=str(data.get("run_id", "")),
        proposal_hash=_optional_str(data.get("proposal_hash")),
        reasons=tuple(
            ConfirmationReason(
                code=ConfirmationReasonCode(
                    str(item.get("code", ConfirmationReasonCode.CUSTOM_POLICY_REQUIREMENT.value))
                ),
                message=str(item.get("message", "confirmation required")),
                rule=_optional_str(item.get("rule")),
                details=_mapping(item.get("details")) or None,
            )
            for item in _sequence(data.get("reasons"))
        ),
        required_at=_time(data.get("required_at")),
        confirmed_at=_time(data.get("confirmed_at")),
        declined_at=_time(data.get("declined_at")),
        metadata=_mapping(data.get("metadata")),
    )


def _execution(value: object) -> ExecutionRecord | None:
    data = _mapping(value)
    if not data:
        return None
    return ExecutionRecord(
        status=str(data.get("status", "PENDING")),
        native_id=_optional_str(data.get("native_id")),
        submitted_at=_time(data.get("submitted_at")),
        started_at=_time(data.get("started_at")),
        finished_at=_time(data.get("finished_at")),
        metrics=_mapping(data.get("metrics")),
    )


def _verification(value: object) -> VerificationResult | None:
    data = _mapping(value)
    if not data:
        return None
    return VerificationResult(
        ok=bool(data.get("passed", False)),
        checks=tuple(
            CheckResult(
                name=str(item.get("check", "")),
                ok=bool(item.get("passed", False)),
                expected=item.get("expected"),
                actual=item.get("observed"),
                message=_optional_str(item.get("message")),
                supported=bool(item.get("supported", True)),
                source=_check_source(item.get("source")),
            )
            for item in _sequence(data.get("checks"))
            if isinstance(item, dict)
        ),
    )


def _evidence(value: object) -> EvidenceBundle | None:
    data = _mapping(value)
    if not data:
        return None
    return EvidenceBundle(
        run_id=str(data.get("run_id", "")),
        engine=str(data.get("engine", "")),
        operation=str(data.get("operation", "")),
        decision=str(data.get("decision", "")),
        native_execution_id=_optional_str(data.get("native_execution_id")),
        proposal_hash=_optional_str(data.get("proposal_hash")),
        inputs=tuple(str(item) for item in _sequence(data.get("inputs"))),
        outputs=tuple(str(item) for item in _sequence(data.get("outputs"))),
        started_at=_time(data.get("started_at")),
        finished_at=_time(data.get("finished_at")),
        execution=_mapping(data.get("execution")),
        observations=tuple(
            Observation(
                name=str(item["name"]),
                value=item.get("value"),
                source=ObservationSource(str(item.get("source", "gantry"))),
                unit=_optional_str(item.get("unit")),
                observed_at=_time(item.get("observed_at")) or datetime.now(),
            )
            for item in _sequence(data.get("observations"))
            if isinstance(item, dict)
        ),
        checks=tuple(
            CheckResult(
                name=str(item.get("check", "")),
                ok=bool(item.get("passed", False)),
                expected=item.get("expected"),
                actual=item.get("observed"),
                message=_optional_str(item.get("message")),
                supported=bool(item.get("supported", True)),
                source=_check_source(item.get("source")),
            )
            for item in _sequence(data.get("checks"))
            if isinstance(item, dict)
        ),
    )


def _sequence(value: object) -> list[dict[str, object]]:
    return list(value) if isinstance(value, list) else []


def _mapping(value: object) -> dict[str, object]:
    return {str(key): item for key, item in value.items()} if isinstance(value, dict) else {}


def _check_source(value: object) -> CheckSource | str | None:
    """Restore `CheckSource` where the stored value is one.

    The source field carries two kinds of answer — who asked for the check, and
    where the observation came from. Only the first is an enum, and a round
    trip that flattened it to a string made `check.source is CheckSource.AGENT`
    quietly false for a run read back from disk.
    """
    if value is None:
        return None
    text = str(value)
    try:
        return CheckSource(text)
    except ValueError:
        return text


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _optional_str(value: object) -> str | None:
    return None if value is None else str(value)


def _time(value: object) -> datetime | None:
    return None if value is None else datetime.fromisoformat(str(value))


evidence_from_dict = _evidence


__all__ = ["SQLiteRunStore", "evidence_from_dict", "run_from_dict"]
