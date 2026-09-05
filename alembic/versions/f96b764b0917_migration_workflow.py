"""migration workflow

Migration is a workflow composed from Movements, so these tables hold the
workflow and nothing else: which Movements it drives, where the cutover has
got to, and every transition with the actor who caused it. Execution state
stays on the Operations beneath it.

`migration_transitions` is append-only, like every other table that records a
decision. It carries the gate report a cutover was approved against, whether
the gates passed or failed - what a post-mortem asks is what was believed at
the time.

Revision ID: f96b764b0917
Revises: f12c1660dff6
Create Date: 2026-09-05 10:28:53.637941
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "f96b764b0917"
down_revision: str | None = "f12c1660dff6"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "migrations",
        sa.Column("name", sa.String(length=253), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("movements", postgresql.ARRAY(sa.String()), server_default="{}", nullable=False),
        sa.Column("spec", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("row_version", sa.Integer(), server_default="1", nullable=False),
        sa.PrimaryKeyConstraint("name", name=op.f("pk_migrations")),
    )
    op.create_table(
        "migration_transitions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("migration", sa.String(length=253), nullable=False),
        sa.Column("from_state", sa.String(length=32), nullable=False),
        sa.Column("to_state", sa.String(length=32), nullable=False),
        sa.Column("actor", sa.String(length=16), nullable=False),
        sa.Column("actor_id", sa.String(length=253), nullable=True),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("evidence", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["migration"], ["migrations.name"], name="fk_migration_transitions"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_migration_transitions")),
    )
    op.create_index(
        "ix_migration_transitions_migration",
        "migration_transitions",
        ["migration", "occurred_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_migration_transitions_migration", table_name="migration_transitions")
    op.drop_table("migration_transitions")
    op.drop_table("migrations")
