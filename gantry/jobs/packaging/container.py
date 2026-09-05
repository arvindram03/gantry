# SPDX-License-Identifier: Apache-2.0
"""Container packaging.

**This is the only module in the codebase that may name an image.** Packaging is
a thing that changes when the industry moves, and a `str` image reference in a
core signature would quietly decide that packaging is containers forever. A test
enforces the boundary rather than review.

A runner that declares support for this packaging may of course read these
fields — that is what supporting it means.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, model_validator

from gantry.jobs.packaging.kind import PackagingKind


class ContainerPackaging(BaseModel):
    """An OCI image, and how to invoke it."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal[PackagingKind.CONTAINER] = PackagingKind.CONTAINER
    image: str
    # Pinned by digest wherever possible. The digest is part of the job's
    # identity, so an image changing underneath a replay is a *different job*
    # rather than the same one behaving differently.
    digest: str | None = None
    command: tuple[str, ...] = ()
    # Non-secret configuration only. Anything here is retained with the job and
    # is meant to be read.
    environment: dict[str, str] = {}
    # Secrets by *name*. The runner resolves them; the values never enter the
    # job, which is provenance.
    secrets: tuple[str, ...] = ()
    network: str | None = None

    @model_validator(mode="after")
    def _check_packaging(self) -> ContainerPackaging:
        if not self.image.strip():
            raise ValueError("a container packaging needs an image")
        if self.digest is not None and not self.digest.startswith("sha256:"):
            raise ValueError(f"digest must be a sha256 reference, got {self.digest!r}")
        leaked = sorted(name for name in self.environment if _looks_secret(name))
        if leaked:
            raise ValueError(
                f"environment carries what look like secrets: {leaked}; "
                f"pass them by name through `secrets` so the values stay out of "
                f"the job body, which is retained as provenance"
            )
        return self

    @property
    def reference(self) -> str:
        """What actually gets run, digest-pinned when one is known."""
        return f"{self.image}@{self.digest}" if self.digest else self.image

    def identity(self) -> str:
        """The part of this packaging that belongs in the job's content hash.

        Secrets are named, not valued, so they are identity-bearing without
        being disclosing: changing *which* secret a job reads is a different
        job.
        """
        environment = ",".join(f"{k}={self.environment[k]}" for k in sorted(self.environment))
        return "|".join(
            (
                self.kind.value,
                self.reference,
                " ".join(self.command),
                environment,
                ",".join(sorted(self.secrets)),
            )
        )


_SECRET_HINTS = ("password", "passwd", "secret", "token", "apikey", "api_key", "credential")


def _looks_secret(name: str) -> bool:
    lowered = name.lower()
    return any(hint in lowered for hint in _SECRET_HINTS)
