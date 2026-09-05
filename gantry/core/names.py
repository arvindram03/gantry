# SPDX-License-Identifier: Apache-2.0
"""Validated identifiers shared by every Gantry resource.

Names are part of the public contract: they appear in specs, CLI arguments,
metric labels and provenance records, so they are constrained once here rather
than validated ad hoc at each boundary.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import StringConstraints

# Lowercase DNS-ish labels. Permits dots so datasets can carry qualified names
# such as `public.orders` without a second naming convention.
RESOURCE_NAME_PATTERN = r"^[a-z0-9]([a-z0-9._-]*[a-z0-9])?$"

ResourceName = Annotated[
    str,
    StringConstraints(pattern=RESOURCE_NAME_PATTERN, min_length=1, max_length=253),
]
"""Name of a Dataset, Movement, Analysis or Result."""

FieldName = Annotated[str, StringConstraints(min_length=1, max_length=253, strip_whitespace=True)]
"""Column or field name. Deliberately permissive - source systems are not ours to constrain."""

ContentHash = Annotated[str, StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$")]
"""Content address of an immutable object (manifest, plan, generated artifact)."""
