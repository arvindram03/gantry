# SPDX-License-Identifier: Apache-2.0
"""Rules that grant authority: `gantry.allow.query(sources=["analytics.*"])`.

A rule grants only what it names. Sources left out mean any source may be read;
destinations left out mean nothing may be written, which is why every writing
operation here requires them.

`require_confirmation=True` still allows the operation. It asks the host to tell
the user before it runs — a separate question from authority, answered in
`run.confirmation` rather than by refusing.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from gantry.confirmation.status import ConfirmationReasonCode
from gantry.policy.model import PolicyRule
from gantry.policy.rules import allow as _allow
from gantry.runs.model import OperationKind


def query(
    *,
    name: str | None = None,
    require_confirmation: bool = False,
    confirmation_code: ConfirmationReasonCode | str | None = None,
    confirmation_message: str | None = None,
    actors: Sequence[str] | None = None,
    engines: Sequence[str] | None = None,
    sources: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    environments: Sequence[str] | None = None,
    constraints: Mapping[str, float] | None = None,
) -> PolicyRule:
    """Allow reading. Omitting `sources` allows any source to be read.

    `destinations` is for the one case where a query writes — a pipeline ending
    in `$out`, an `INSERT` submitted through a query operation. Omitting it
    grants no write authority, so a read policy stays a read policy.
    """
    return _allow(
        OperationKind.QUERY,
        name=name,
        require_confirmation=require_confirmation,
        confirmation_code=confirmation_code,
        confirmation_message=confirmation_message,
        actors=actors,
        engines=engines,
        sources=sources,
        destinations=destinations,
        environments=environments,
        constraints=constraints,
    )


def materialize(
    *,
    destinations: Sequence[str],
    name: str | None = None,
    require_confirmation: bool = False,
    confirmation_code: ConfirmationReasonCode | str | None = None,
    confirmation_message: str | None = None,
    actors: Sequence[str] | None = None,
    engines: Sequence[str] | None = None,
    sources: Sequence[str] | None = None,
    environments: Sequence[str] | None = None,
    constraints: Mapping[str, float] | None = None,
) -> PolicyRule:
    """Allow a materialization that lands in `destinations`."""
    return _allow(
        OperationKind.MATERIALIZE,
        name=name,
        require_confirmation=require_confirmation,
        confirmation_code=confirmation_code,
        confirmation_message=confirmation_message,
        actors=actors,
        engines=engines,
        sources=sources,
        destinations=destinations,
        environments=environments,
        constraints=constraints,
    )


def batch(
    *,
    destinations: Sequence[str],
    name: str | None = None,
    require_confirmation: bool = False,
    confirmation_code: ConfirmationReasonCode | str | None = None,
    confirmation_message: str | None = None,
    actors: Sequence[str] | None = None,
    engines: Sequence[str] | None = None,
    sources: Sequence[str] | None = None,
    environments: Sequence[str] | None = None,
    constraints: Mapping[str, float] | None = None,
) -> PolicyRule:
    """Allow a batch job that writes into `destinations`."""
    return _allow(
        OperationKind.BATCH,
        name=name,
        require_confirmation=require_confirmation,
        confirmation_code=confirmation_code,
        confirmation_message=confirmation_message,
        actors=actors,
        engines=engines,
        sources=sources,
        destinations=destinations,
        environments=environments,
        constraints=constraints,
    )


def stream(
    *,
    destinations: Sequence[str],
    name: str | None = None,
    require_confirmation: bool = False,
    confirmation_code: ConfirmationReasonCode | str | None = None,
    confirmation_message: str | None = None,
    actors: Sequence[str] | None = None,
    engines: Sequence[str] | None = None,
    sources: Sequence[str] | None = None,
    environments: Sequence[str] | None = None,
    constraints: Mapping[str, float] | None = None,
) -> PolicyRule:
    """Allow submitting a streaming job that writes into `destinations`."""
    return _allow(
        OperationKind.STREAM,
        name=name,
        require_confirmation=require_confirmation,
        confirmation_code=confirmation_code,
        confirmation_message=confirmation_message,
        actors=actors,
        engines=engines,
        sources=sources,
        destinations=destinations,
        environments=environments,
        constraints=constraints,
    )


__all__ = ["batch", "materialize", "query", "stream"]
