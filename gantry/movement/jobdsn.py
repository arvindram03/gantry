# SPDX-License-Identifier: Apache-2.0
"""How a job reaches the databases.

Not the same question as how this process reaches them. The job runs somewhere
else — another container, another host — so a URL that works here may not work
there, and the driver prefix this process needs (`+asyncpg`) is meaningless to
`psql`.

Keeping it a named type rather than two strings is deliberate: a source and a
target DSN passed positionally are trivially swappable, and swapping them would
copy the target over the source.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncEngine

_DRIVER = re.compile(r"^postgresql\+\w+://")

SOURCE_DSN_ENV = "GANTRY_JOB_SOURCE_DSN"
TARGET_DSN_ENV = "GANTRY_JOB_TARGET_DSN"
NETWORK_ENV = "GANTRY_JOB_NETWORK"


@dataclass(frozen=True)
class JobConnections:
    """The DSNs and network a generated job runs against."""

    source: str
    target: str
    network: str | None = None

    @classmethod
    def from_env(cls, *, source_engine: AsyncEngine, target_engine: AsyncEngine) -> JobConnections:
        """Configuration first, this process's own URLs second.

        A job usually cannot reach a database at the address this process uses:
        `localhost` inside a container is the container. So the environment
        wins where it is set, and the derived values are a convenience for the
        case where the job really does share this network view.
        """
        derived = cls.derived(source_engine=source_engine, target_engine=target_engine)
        return cls(
            source=os.environ.get(SOURCE_DSN_ENV, derived.source),
            target=os.environ.get(TARGET_DSN_ENV, derived.target),
            network=os.environ.get(NETWORK_ENV) or None,
        )

    @classmethod
    def derived(
        cls, *, source_engine: AsyncEngine, target_engine: AsyncEngine, network: str | None = None
    ) -> JobConnections:
        """Take this process's own URLs, minus the async driver.

        Correct whenever the job shares this process's view of the network, and
        loudly wrong otherwise: the job cannot connect, so it fails, so no
        commit is attested and no checkpoint advances. The failure mode is a
        stalled operation with a connection error, never silent data loss.
        """
        return cls(
            source=_psql_dsn(source_engine),
            target=_psql_dsn(target_engine),
            network=network,
        )


def _psql_dsn(engine: AsyncEngine) -> str:
    """The engine's URL as `psql` would accept it, password included.

    `render_as_string(hide_password=False)` is required: this value is handed to
    the runner as a secret and never written into a job body.
    """
    return _DRIVER.sub("postgresql://", engine.url.render_as_string(hide_password=False))
