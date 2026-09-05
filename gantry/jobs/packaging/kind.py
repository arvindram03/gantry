# SPDX-License-Identifier: Apache-2.0
"""What kinds of packaging exist.

Separate from the packaging models so the job model can name a kind without
importing a container, which is what keeps the dependency arrow pointing the
right way.
"""

from __future__ import annotations

from enum import StrEnum


class PackagingKind(StrEnum):
    """How a job is made runnable.

    One member today. The enum exists so that adding `WASM` or `JAR` later
    touches this file and one new model, rather than every signature that
    currently assumes an image.
    """

    CONTAINER = "container"
