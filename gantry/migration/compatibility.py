# SPDX-License-Identifier: Apache-2.0
"""Whether a target can hold what the source will send it.

Prepare is where a migration should fail. Cutover is where it fails loudly and
at the worst possible moment, and the six hours of snapshot between the two are
the expensive part. So this runs before anything moves, and it refuses in the
shape the Analysis validator uses — a list of structured failures naming the
column and what would fix it, not a stack trace.

The asymmetry that makes this worth doing carefully: **compatibility is
directional.** A target column can be wider than the source's and be fine; the
reverse silently truncates or overflows. A target column can be nullable when
the source is not; the reverse rejects rows the source considers valid. Every
rule below is written in the direction data actually flows.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import StrEnum

from gantry.core.dataset import DatasetManifest
from gantry.core.schema import FieldSchema


class CompatibilityCheck(StrEnum):
    """What was checked, so a failure says which rule refused."""

    TABLE_EXISTS = "table_exists"
    COLUMN_PRESENT = "column_present"
    TYPE_COMPATIBLE = "type_compatible"
    WIDTH_SUFFICIENT = "width_sufficient"
    NULLABILITY = "nullability"
    KEY_PRESENT = "key_present"


class CompatibilityRepair(StrEnum):
    """What kind of change would fix it.

    Coarse, like the Analysis repairs, and for the same reason: an agent needs
    to know whether to alter the target, change the spec, or rediscover — not
    to be handed a migration script it would run without understanding.
    """

    ALTER_TARGET = "alter_target"
    CREATE_TARGET = "create_target"
    EDIT_SPEC = "edit_spec"
    REDISCOVER = "rediscover"


@dataclass(frozen=True)
class CompatibilityFailure:
    """One reason the target cannot hold the source."""

    check: CompatibilityCheck
    dataset: str
    column: str | None
    problem: str
    repair: CompatibilityRepair

    def describe(self) -> str:
        where = f"{self.dataset}.{self.column}" if self.column else self.dataset
        return f"{self.check.value}: {where} — {self.problem}"


@dataclass
class CompatibilityReport:
    """Whether a migration may move data into this target."""

    dataset: str
    failures: list[CompatibilityFailure] = field(default_factory=list)
    # Columns the target has and the source does not. Not a failure: a target
    # may carry its own bookkeeping. Reported because a column nobody
    # remembers adding is worth seeing before a cutover, not after.
    extra_columns: tuple[str, ...] = ()

    @property
    def compatible(self) -> bool:
        return not self.failures

    @property
    def repairs(self) -> tuple[CompatibilityRepair, ...]:
        return tuple(sorted({failure.repair for failure in self.failures}))

    def describe(self) -> str:
        if self.compatible:
            extra = (
                f" ({len(self.extra_columns)} extra target columns)" if self.extra_columns else ""
            )
            return f"{self.dataset}: compatible{extra}"
        return "; ".join(failure.describe() for failure in self.failures)


# Types the source may widen into. Written one-directionally on purpose: an
# entry says "a source column of this type fits in a target column of any of
# these", and never the reverse.
_WIDENS_TO: dict[str, frozenset[str]] = {
    "smallint": frozenset({"smallint", "integer", "bigint", "numeric", "real", "double precision"}),
    "integer": frozenset({"integer", "bigint", "numeric", "double precision"}),
    "bigint": frozenset({"bigint", "numeric"}),
    "real": frozenset({"real", "double precision", "numeric"}),
    "double precision": frozenset({"double precision", "numeric"}),
    "numeric": frozenset({"numeric"}),
    "text": frozenset({"text", "character varying"}),
    "character varying": frozenset({"character varying", "text"}),
    "character": frozenset({"character", "character varying", "text"}),
    "timestamp without time zone": frozenset(
        {"timestamp without time zone", "timestamp with time zone"}
    ),
    "timestamp with time zone": frozenset({"timestamp with time zone"}),
    "date": frozenset({"date", "timestamp without time zone", "timestamp with time zone"}),
    "boolean": frozenset({"boolean"}),
    "uuid": frozenset({"uuid", "text"}),
    "jsonb": frozenset({"jsonb", "json"}),
    "json": frozenset({"json", "jsonb"}),
    "bytea": frozenset({"bytea"}),
}

_PRECISION = re.compile(r"^(?P<base>[a-z ]+?)\s*\((?P<args>[\d,\s]+)\)$")

# Aliases the catalog and hand-written specs both produce.
_CANONICAL = {
    "int": "integer",
    "int2": "smallint",
    "int4": "integer",
    "int8": "bigint",
    "int16": "bigint",
    "float4": "real",
    "float8": "double precision",
    "bool": "boolean",
    "varchar": "character varying",
    "char": "character",
    "decimal": "numeric",
    "timestamp": "timestamp without time zone",
    "timestamptz": "timestamp with time zone",
}


@dataclass(frozen=True)
class _Type:
    """A declared type split into its base and its precision arguments."""

    base: str
    args: tuple[int, ...]

    @classmethod
    def parse(cls, declared: str) -> _Type:
        text = declared.strip().lower()
        match = _PRECISION.match(text)
        if match is None:
            return cls(base=_CANONICAL.get(text, text), args=())
        base = match.group("base").strip()
        args = tuple(int(part) for part in match.group("args").split(",") if part.strip())
        return cls(base=_CANONICAL.get(base, base), args=args)

    def describe(self) -> str:
        return self.base + (f"({', '.join(str(a) for a in self.args)})" if self.args else "")


def check_field(
    dataset: str, source: FieldSchema, target: FieldSchema
) -> list[CompatibilityFailure]:
    """Compare one column, in the direction data flows."""
    failures: list[CompatibilityFailure] = []
    from_type = _Type.parse(source.type)
    to_type = _Type.parse(target.type)

    if from_type.base != to_type.base:
        widens = _WIDENS_TO.get(from_type.base, frozenset({from_type.base}))
        if to_type.base not in widens:
            failures.append(
                CompatibilityFailure(
                    check=CompatibilityCheck.TYPE_COMPATIBLE,
                    dataset=dataset,
                    column=source.name,
                    problem=(
                        f"source is {from_type.describe()}, target is {to_type.describe()}; "
                        f"values would not survive the conversion"
                    ),
                    repair=CompatibilityRepair.ALTER_TARGET,
                )
            )
            return failures

    # Precision only narrows a value when the target declares less of it.
    # A target with no declared precision is unbounded and always sufficient.
    if from_type.args and to_type.args and _narrower(from_type.args, to_type.args):
        failures.append(
            CompatibilityFailure(
                check=CompatibilityCheck.WIDTH_SUFFICIENT,
                dataset=dataset,
                column=source.name,
                problem=(
                    f"source is {from_type.describe()}, target is {to_type.describe()}; "
                    f"the target is narrower and would truncate"
                ),
                repair=CompatibilityRepair.ALTER_TARGET,
            )
        )

    # A nullable source into a NOT NULL target rejects rows the source calls
    # valid. The reverse is fine: a nullable target simply never sees a null.
    if source.nullable and not target.nullable:
        failures.append(
            CompatibilityFailure(
                check=CompatibilityCheck.NULLABILITY,
                dataset=dataset,
                column=source.name,
                problem="source allows nulls, target does not; rows would be rejected",
                repair=CompatibilityRepair.ALTER_TARGET,
            )
        )
    return failures


def check_manifest(
    source: DatasetManifest, target: DatasetManifest | None, *, target_name: str
) -> CompatibilityReport:
    """Compare a whole Dataset against the target it will be written to.

    `target` is None when the table does not exist yet, which is not a failure
    the operator has to fix — the target adapter creates it. It is reported so
    a Prepare phase can say what it is about to create rather than creating it
    silently.
    """
    report = CompatibilityReport(dataset=source.name)

    if target is None:
        report.failures.append(
            CompatibilityFailure(
                check=CompatibilityCheck.TABLE_EXISTS,
                dataset=source.name,
                column=None,
                problem=f"target table {target_name!r} does not exist yet",
                repair=CompatibilityRepair.CREATE_TARGET,
            )
        )
        return report

    if not source.dataset_schema.fields:
        report.failures.append(
            CompatibilityFailure(
                check=CompatibilityCheck.COLUMN_PRESENT,
                dataset=source.name,
                column=None,
                problem="the source has no discovered fields to compare",
                repair=CompatibilityRepair.REDISCOVER,
            )
        )
        return report

    by_name = {field.name: field for field in target.dataset_schema.fields}
    for source_field in source.dataset_schema.fields:
        target_field = by_name.get(source_field.name)
        if target_field is None:
            report.failures.append(
                CompatibilityFailure(
                    check=CompatibilityCheck.COLUMN_PRESENT,
                    dataset=source.name,
                    column=source_field.name,
                    problem=f"target {target_name!r} has no such column",
                    repair=CompatibilityRepair.ALTER_TARGET,
                )
            )
            continue
        report.failures.extend(check_field(source.name, source_field, target_field))

    # The key has to exist on the target, because idempotent writes detect a
    # conflict on it. Without it, a replayed batch duplicates instead of
    # upserting - which is the guarantee the whole runtime rests on.
    for key in source.dataset_schema.keys:
        if key not in by_name:
            report.failures.append(
                CompatibilityFailure(
                    check=CompatibilityCheck.KEY_PRESENT,
                    dataset=source.name,
                    column=key,
                    problem=(
                        f"key column missing from target {target_name!r}; "
                        f"idempotent writes need it to detect a conflict"
                    ),
                    repair=CompatibilityRepair.ALTER_TARGET,
                )
            )

    source_names = {field.name for field in source.dataset_schema.fields}
    report.extra_columns = tuple(sorted(set(by_name) - source_names))
    return report


def _narrower(source: tuple[int, ...], target: tuple[int, ...]) -> bool:
    """Whether the target's precision cannot hold the source's.

    Compared position by position: for `numeric(p, s)` that is precision then
    scale, and for `varchar(n)` just the length. A target declaring fewer
    arguments than the source is treated as unbounded in the ones it omits.
    """
    return any(t < s for s, t in zip(source, target, strict=False))
