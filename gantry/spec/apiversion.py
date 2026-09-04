"""Accepted spec API versions.

The design documents disagree: the Movement example uses `gantry.io/v1alpha1`
while the Dataset and Analysis examples use `gantry.dev/v1alpha1`. Rather than
silently accept both forever, `gantry.dev` is canonical and `gantry.io` is
accepted as a deprecated alias so existing specs keep parsing.
"""

from __future__ import annotations

CANONICAL_API_VERSION = "gantry.dev/v1alpha1"

# Deprecated -> canonical.
API_VERSION_ALIASES: dict[str, str] = {
    "gantry.io/v1alpha1": CANONICAL_API_VERSION,
}

SUPPORTED_API_VERSIONS: frozenset[str] = frozenset({CANONICAL_API_VERSION, *API_VERSION_ALIASES})


def normalize_api_version(api_version: str) -> str:
    """Map a declared apiVersion onto its canonical form."""
    return API_VERSION_ALIASES.get(api_version, api_version)


def is_deprecated_api_version(api_version: str) -> bool:
    return api_version in API_VERSION_ALIASES
