# SPDX-License-Identifier: Apache-2.0
"""Packaging: how a job is made runnable.

`Packaging` is a union with one member today. Adding a second is meant to be a
two-line change here plus a new model — and specifically *not* a change to the
job model, the runner protocol, or anything that submits work.
"""

from __future__ import annotations

from gantry.jobs.packaging.container import (
    DEFAULT_BEAM_IMAGE,
    DEFAULT_SQL_IMAGE,
    ContainerPackaging,
    beam_packaging,
    sql_client_packaging,
)
from gantry.jobs.packaging.kind import PackagingKind

# A union of one. Written as a union rather than as the concrete type so that
# `Packaging = ContainerPackaging | WasmPackaging` is the whole diff later.
Packaging = ContainerPackaging

__all__ = [
    "DEFAULT_BEAM_IMAGE",
    "DEFAULT_SQL_IMAGE",
    "ContainerPackaging",
    "Packaging",
    "PackagingKind",
    "beam_packaging",
    "sql_client_packaging",
]
