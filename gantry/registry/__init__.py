# SPDX-License-Identifier: Apache-2.0
"""Dataset registry.

Datasets are registered, versioned and resolved by name. Manifests are
content-addressed, so registration is idempotent and a version number always
means the same bytes.
"""

from gantry.registry.base import DatasetRegistry
from gantry.registry.errors import (
    DatasetNotFoundError,
    DatasetVersionNotFoundError,
    RegistryError,
)
from gantry.registry.jsonfile import DEFAULT_REGISTRY_PATH, JsonFileDatasetRegistry
from gantry.registry.memory import InMemoryDatasetRegistry

__all__ = [
    "DEFAULT_REGISTRY_PATH",
    "DatasetNotFoundError",
    "DatasetRegistry",
    "DatasetVersionNotFoundError",
    "InMemoryDatasetRegistry",
    "JsonFileDatasetRegistry",
    "RegistryError",
]
