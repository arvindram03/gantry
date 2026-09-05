# SPDX-License-Identifier: Apache-2.0
"""The target adapter interface.

A target is responsible for two things the runtime cannot provide from outside:
making writes idempotent, and reporting durably what it did. Everything else -
retries, ordering, checkpoints - is the runtime's job, and all of it depends on
a replayed write producing no second effect.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from gantry.core.commit import CommitResult
from gantry.core.dataset import DatasetManifest


class TargetAdapter(Protocol):
    """Write-side adapter for one target system."""

    async def prepare(self, manifest: DatasetManifest, *, target: str) -> None:
        """Create or validate the target structure for a dataset.

        Idempotent: preparing an already-prepared target is a no-op, because
        Prepare runs again on every restart.
        """
        ...

    async def write_batch(
        self,
        manifest: DatasetManifest,
        *,
        target: str,
        rows: Sequence[Sequence[object]],
    ) -> CommitResult:
        """Write rows idempotently, keyed by the dataset's stable key.

        Must be safe to call twice with the same rows. The returned
        `CommitResult` attests the write is durable, and is the only thing that
        permits a checkpoint to advance.
        """
        ...
