"""Registry errors."""

from __future__ import annotations


class RegistryError(Exception):
    """Base class for registry failures."""


class DatasetNotFoundError(RegistryError):
    def __init__(self, name: str) -> None:
        super().__init__(f"dataset {name!r} is not registered")
        self.name = name


class DatasetVersionNotFoundError(RegistryError):
    def __init__(self, name: str, version: int, available: int) -> None:
        super().__init__(f"dataset {name!r} has no version {version} (latest is {available})")
        self.name = name
        self.version = version
