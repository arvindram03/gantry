"""The metadata schema.

Durable control state lives outside worker processes so workers stay
disposable. Nothing here holds customer row data: the data plane keeps that,
and the control plane keeps only what it needs to plan, checkpoint, verify and
audit.

Every table that records a decision - a state transition, a policy evaluation -
is append-only. Evidence that can be rewritten is not evidence.
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

# Explicit naming so Alembic autogenerates stable constraint names.
metadata = MetaData(
    naming_convention={
        "ix": "ix_%(column_0_label)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s",
        "pk": "pk_%(table_name)s",
    }
)

TIMESTAMP = DateTime(timezone=True)


dataset_versions = Table(
    "dataset_versions",
    metadata,
    Column("name", String(253), primary_key=True),
    Column("version", Integer, primary_key=True),
    Column("content_hash", String(71), nullable=False),
    Column("manifest", JSONB, nullable=False),
    Column("registered_at", TIMESTAMP, nullable=False),
    CheckConstraint("version >= 1", name="version_positive"),
    # Registration is idempotent by content, so one hash appears at most once
    # per dataset. Without this, a race could mint two versions of one manifest
    # and split the provenance pins that reference it.
    UniqueConstraint("name", "content_hash", name="uq_dataset_versions_content"),
)


operations = Table(
    "operations",
    metadata,
    Column("name", String(253), primary_key=True),
    Column("operation_type", String(32), nullable=False),
    Column("state", String(32), nullable=False),
    Column("current_plan_version", Integer, nullable=True),
    Column("created_at", TIMESTAMP, nullable=False),
    Column("updated_at", TIMESTAMP, nullable=False),
    # Optimistic concurrency: two workers cannot both advance one Operation.
    Column("row_version", Integer, nullable=False, server_default="1"),
)


plan_versions = Table(
    "plan_versions",
    metadata,
    Column("operation", String(253), primary_key=True),
    Column("version", Integer, primary_key=True),
    Column("content_hash", String(71), nullable=False),
    Column("guarantee_fingerprint", String(71), nullable=False),
    Column("plan", JSONB, nullable=False),
    # The Dataset versions this plan was compiled against. Execution resolves
    # manifests through these rather than reading whatever is latest, so a
    # rediscovery between planning and execution cannot change what runs.
    Column("dataset_pins", JSONB, nullable=False, server_default="[]"),
    Column("created_at", TIMESTAMP, nullable=False),
    ForeignKeyConstraint(["operation"], ["operations.name"], name="fk_plan_versions_operation"),
    CheckConstraint("version >= 1", name="version_positive"),
)


state_transitions = Table(
    "state_transitions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("operation", String(253), nullable=False),
    Column("from_state", String(32), nullable=False),
    Column("to_state", String(32), nullable=False),
    Column("actor", String(32), nullable=False),
    Column("reason", Text, nullable=False),
    Column("plan_version", Integer, nullable=True),
    Column("occurred_at", TIMESTAMP, nullable=False),
    ForeignKeyConstraint(["operation"], ["operations.name"], name="fk_state_transitions_operation"),
    CheckConstraint("length(btrim(reason)) > 0", name="reason_not_blank"),
    Index("ix_state_transitions_operation_time", "operation", "occurred_at"),
)


checkpoints = Table(
    "checkpoints",
    metadata,
    Column("operation", String(253), primary_key=True),
    Column("scope", String(32), primary_key=True),
    Column("scope_id", String(512), primary_key=True),
    Column("position_kind", String(32), nullable=False),
    Column("position_value", Text, nullable=False),
    # When the side effect was confirmed durable, not when this row was written.
    Column("committed_at", TIMESTAMP, nullable=False),
    ForeignKeyConstraint(["operation"], ["operations.name"], name="fk_checkpoints_operation"),
)


verification_results = Table(
    "verification_results",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("operation", String(253), nullable=False),
    Column("plan_version", Integer, nullable=False),
    Column("check_name", String(64), nullable=False),
    Column("scope", String(512), nullable=True),
    Column("status", String(32), nullable=False),
    Column("severity", String(32), nullable=False),
    Column("evidence", JSONB, nullable=False),
    Column("observed_at", TIMESTAMP, nullable=False),
    ForeignKeyConstraint(
        ["operation"], ["operations.name"], name="fk_verification_results_operation"
    ),
    Index("ix_verification_results_operation", "operation", "plan_version"),
)


results = Table(
    "results",
    metadata,
    Column("name", String(253), primary_key=True),
    Column("operation", String(253), nullable=False),
    Column("kind", String(32), nullable=False),
    Column("status", String(32), nullable=False),
    Column("provenance", JSONB, nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("created_at", TIMESTAMP, nullable=False),
    ForeignKeyConstraint(["operation"], ["operations.name"], name="fk_results_operation"),
)


audit_log = Table(
    "audit_log",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("operation", String(253), nullable=True),
    Column("actor", String(32), nullable=False),
    Column("action", String(64), nullable=False),
    Column("detail", JSONB, nullable=False),
    Column("occurred_at", TIMESTAMP, nullable=False),
    Index("ix_audit_log_time", "occurred_at"),
)


tasks = Table(
    "tasks",
    metadata,
    Column("operation", String(253), primary_key=True),
    Column("node_id", String(64), primary_key=True),
    Column("plan_version", Integer, nullable=False),
    Column("depends_on", ARRAY(String), nullable=False, server_default="{}"),
    Column("state", String(32), nullable=False),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("lease_owner", String(128), nullable=True),
    # A lease is the only thing standing between a dead worker and a stuck
    # task: when it expires the task returns to the queue on its own.
    Column("lease_expires_at", TIMESTAMP, nullable=True),
    Column("last_error", Text, nullable=True),
    ForeignKeyConstraint(["operation"], ["operations.name"], name="fk_tasks_operation"),
    Index("ix_tasks_leasable", "operation", "state"),
)


dead_letters = Table(
    "dead_letters",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("operation", String(253), nullable=False),
    Column("dataset", String(253), nullable=False),
    Column("key_text", String(1024), nullable=True),
    Column("source_lsn", BigInteger, nullable=True),
    Column("reason", Text, nullable=False),
    # The whole event, so it can be replayed once the cause is fixed. A
    # dead-letter queue that discards the payload is a counter.
    Column("payload", JSONB, nullable=False),
    Column("occurred_at", TIMESTAMP, nullable=False),
    Column("replayed_at", TIMESTAMP, nullable=True),
    ForeignKeyConstraint(["operation"], ["operations.name"], name="fk_dead_letters_operation"),
    Index("ix_dead_letters_pending", "operation", "replayed_at"),
)
