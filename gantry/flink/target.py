# SPDX-License-Identifier: Apache-2.0
"""Configuration for an existing Flink cluster."""

from __future__ import annotations

import re
import ssl
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from gantry.target import ExecutionTarget

_ALLOWED_CONFIG = frozenset(
    {
        "api_version",
        "basic_auth",
        "default_catalog",
        "default_database",
        "execution_config",
        "headers",
        "jobmanager_endpoint",
        "output_refs",
        "request_timeout",
        "session_properties",
        "ssl_context",
        "submission_timeout",
        "token",
        "transport",
        "validation_timeout",
    }
)


@dataclass(frozen=True, slots=True)
class FlinkTarget:
    """Private connection configuration for SQL Gateway and JobManager REST."""

    endpoint: str
    config: Mapping[str, object] = field(default_factory=dict)
    name: str = "flink"

    def __post_init__(self) -> None:
        _validate_url(self.endpoint, "endpoint")
        if not self.name.strip():
            raise ValueError("Flink target name must not be empty")
        if not isinstance(self.config, Mapping):
            raise TypeError("Flink config must be a mapping")
        unknown = set(self.config) - _ALLOWED_CONFIG
        if unknown:
            raise ValueError(f"unknown Flink configuration fields: {', '.join(sorted(unknown))}")
        jobmanager = self.config.get("jobmanager_endpoint")
        if jobmanager is not None:
            if not isinstance(jobmanager, str):
                raise TypeError("Flink jobmanager_endpoint must be a string")
            _validate_url(jobmanager, "jobmanager_endpoint")
        for name in ("request_timeout", "submission_timeout", "validation_timeout"):
            value = self.config.get(name)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0
            ):
                raise ValueError(f"Flink {name} must be positive")
        version = self.config.get("api_version", "v2")
        if not isinstance(version, str) or re.fullmatch(r"v?[1-9][0-9]*", version) is None:
            raise ValueError("Flink api_version must look like 'v2'")
        for name in ("headers", "session_properties", "execution_config", "output_refs"):
            value = self.config.get(name)
            if value is not None and not isinstance(value, Mapping):
                raise TypeError(f"Flink {name} must be a mapping")
        for name in ("default_catalog", "default_database", "token"):
            value = self.config.get(name)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"Flink {name} must be a non-empty string")
        basic = self.config.get("basic_auth")
        if basic is not None and (
            not isinstance(basic, tuple)
            or len(basic) != 2
            or not all(isinstance(part, str) for part in basic)
        ):
            raise TypeError("Flink basic_auth must be a (username, password) tuple")
        context = self.config.get("ssl_context")
        if context is not None and not isinstance(context, ssl.SSLContext):
            raise TypeError("Flink ssl_context must be an ssl.SSLContext")

    @property
    def gateway_endpoint(self) -> str:
        return self.endpoint.rstrip("/")

    @property
    def jobmanager_endpoint(self) -> str:
        value = self.config.get("jobmanager_endpoint", self.endpoint)
        assert isinstance(value, str)
        return value.rstrip("/")

    @property
    def request_timeout(self) -> float:
        value = self.config.get("request_timeout", 30.0)
        assert isinstance(value, (int, float)) and not isinstance(value, bool)
        return float(value)

    def execution_target(self) -> ExecutionTarget:
        # Credentials and endpoints deliberately stay in this adapter-owned object.
        return ExecutionTarget(self.name, {"engine": "flink"})


def _validate_url(value: str, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Flink {name} must be a non-empty URL")
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"Flink {name} must be an http(s) URL")
