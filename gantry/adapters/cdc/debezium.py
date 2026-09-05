# SPDX-License-Identifier: Apache-2.0
"""Provisioning a Debezium connector.

Connector setup is part of Prepare and teardown is part of Finalize, both for
the same reason: a replication slot pins WAL on the source until it is dropped.
A migration that leaves one behind does not merely waste space - it eventually
takes the source database down. That makes teardown a correctness concern
rather than tidiness.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum

import httpx

DEFAULT_CONNECT_URL = "http://localhost:18083"


class ConnectorState(StrEnum):
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    FAILED = "FAILED"
    UNASSIGNED = "UNASSIGNED"
    ABSENT = "ABSENT"


class ConnectorError(Exception):
    """A connector could not be provisioned or inspected."""


@dataclass(frozen=True)
class ConnectorStatus:
    name: str
    state: ConnectorState
    tasks: tuple[ConnectorState, ...] = ()
    trace: str | None = None

    @property
    def healthy(self) -> bool:
        """Running, with every task running.

        A connector can report RUNNING while its only task has failed, which
        looks healthy and streams nothing.
        """
        return self.state is ConnectorState.RUNNING and all(
            task is ConnectorState.RUNNING for task in self.tasks
        )


@dataclass(frozen=True)
class DebeziumConfig:
    """What a Postgres connector needs to know."""

    name: str
    database_host: str
    database_port: int
    database_user: str
    database_password: str
    database_name: str
    tables: tuple[str, ...]
    topic_prefix: str
    slot_name: str
    publication_name: str
    # Emitting the existing rows again would duplicate work the snapshot phase
    # already did, and the runtime takes its own consistent snapshot. `never`
    # means the connector streams changes only.
    snapshot_mode: str = "never"

    def to_payload(self) -> dict[str, str]:
        return {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "plugin.name": "pgoutput",
            "database.hostname": self.database_host,
            "database.port": str(self.database_port),
            "database.user": self.database_user,
            "database.password": self.database_password,
            "database.dbname": self.database_name,
            "topic.prefix": self.topic_prefix,
            "table.include.list": ",".join(self.tables),
            "slot.name": self.slot_name,
            "publication.name": self.publication_name,
            "publication.autocreate.mode": "filtered",
            "snapshot.mode": self.snapshot_mode,
            # Keys and values as plain JSON without schemas: the runtime has
            # the schema already, from discovery, and the envelope is smaller.
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false",
            # Numerics as strings rather than Debezium's default encoded
            # decimal, so a value survives the round trip unambiguously.
            "decimal.handling.mode": "string",
            "tombstones.on.delete": "false",
            "topic.creation.enable": "true",
            "topic.creation.default.replication.factor": "1",
            "topic.creation.default.partitions": "1",
        }

    def topic_for(self, table: str) -> str:
        return f"{self.topic_prefix}.{table}"

    @property
    def topics(self) -> tuple[str, ...]:
        return tuple(self.topic_for(table) for table in self.tables)


class DebeziumConnectClient:
    """Talks to Kafka Connect's REST API."""

    def __init__(self, base_url: str = DEFAULT_CONNECT_URL, *, timeout: float = 30.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    async def ensure(self, config: DebeziumConfig) -> ConnectorStatus:
        """Create or update a connector. Idempotent.

        Prepare runs again on every restart, so this uses PUT on the config
        endpoint rather than POST: re-preparing must converge on the intended
        configuration rather than fail because something already exists.
        """
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.put(
                f"{self._base_url}/connectors/{config.name}/config",
                json=config.to_payload(),
            )
            if response.status_code >= 400:
                raise ConnectorError(
                    f"could not provision connector {config.name!r}: "
                    f"{response.status_code} {response.text}"
                )
        # Connect accepts the config before the connector exists, so reading
        # status immediately reports ABSENT. Returning that would tell a caller
        # provisioning failed when it is merely still happening.
        return await self.wait_until_running(config.name)

    async def wait_until_running(
        self, name: str, *, timeout_seconds: float = 60.0, interval: float = 0.5
    ) -> ConnectorStatus:
        """Poll until a connector and its tasks are running, or give up.

        A FAILED task is reported immediately rather than waited out: it
        carries the trace explaining why, and no amount of waiting fixes it.
        """
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        status = await self.status(name)
        while asyncio.get_running_loop().time() < deadline:
            if status.healthy:
                return status
            if status.state is ConnectorState.FAILED or ConnectorState.FAILED in status.tasks:
                raise ConnectorError(
                    f"connector {name!r} failed: {status.trace or 'no trace reported'}"
                )
            await asyncio.sleep(interval)
            status = await self.status(name)
        raise ConnectorError(
            f"connector {name!r} did not start within {timeout_seconds:.0f}s "
            f"(state {status.state.value}, tasks {[t.value for t in status.tasks]})"
        )

    async def status(self, name: str) -> ConnectorStatus:
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(f"{self._base_url}/connectors/{name}/status")
            if response.status_code == 404:
                return ConnectorStatus(name=name, state=ConnectorState.ABSENT)
            if response.status_code >= 400:
                raise ConnectorError(
                    f"could not read connector {name!r}: {response.status_code} {response.text}"
                )
            body = response.json()

        tasks = tuple(
            ConnectorState(task.get("state", "UNASSIGNED")) for task in body.get("tasks", [])
        )
        trace = next((task["trace"] for task in body.get("tasks", []) if task.get("trace")), None)
        return ConnectorStatus(
            name=name,
            state=ConnectorState(body.get("connector", {}).get("state", "UNASSIGNED")),
            tasks=tasks,
            trace=trace,
        )

    async def delete(self, name: str) -> bool:
        """Remove a connector. Returns whether one was there to remove.

        Deleting the connector releases the replication slot; leaving it in
        place pins WAL on the source indefinitely.
        """
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.delete(f"{self._base_url}/connectors/{name}")
        if response.status_code == 404:
            return False
        if response.status_code >= 400:
            raise ConnectorError(
                f"could not delete connector {name!r}: {response.status_code} {response.text}"
            )
        return True

    async def list(self) -> tuple[str, ...]:
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(f"{self._base_url}/connectors")
            response.raise_for_status()
            return tuple(response.json())
