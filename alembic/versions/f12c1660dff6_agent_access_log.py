"""agent access log

The durable record of what agents asked for and what policy answered.
Append-only, and it records refusals as carefully as grants: a trail holding
only what was permitted reads as clean history while an agent probes every rung
on every Dataset and is turned away each time.

Revision ID: f12c1660dff6
Revises: 9556321f11ae
Create Date: 2026-09-05 08:12:11.856474
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "f12c1660dff6"
down_revision: str | None = "9556321f11ae"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "access_log",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("dataset", sa.String(length=253), nullable=False),
        sa.Column("rung", sa.String(length=32), nullable=False),
        sa.Column("decision", sa.String(length=16), nullable=False),
        sa.Column("principal", sa.String(length=253), nullable=False),
        sa.Column("grounds", postgresql.ARRAY(sa.String()), server_default="{}", nullable=False),
        sa.Column(
            "redacted_fields", postgresql.ARRAY(sa.String()), server_default="{}", nullable=False
        ),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("row_limit", sa.Integer(), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_access_log")),
    )
    op.create_index("ix_access_log_dataset", "access_log", ["dataset", "observed_at"], unique=False)
    op.create_index("ix_access_log_denied", "access_log", ["decision", "observed_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_access_log_denied", table_name="access_log")
    op.drop_index("ix_access_log_dataset", table_name="access_log")
    op.drop_table("access_log")
