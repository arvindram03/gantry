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
    # How the body is handed to the image: the body becomes the final argument.
    # A generated SQL script is a shell script and a generated Beam pipeline is
    # a Python program, and which one an image can run is a fact about the
    # image — so it lives with the packaging rather than being assumed by every
    # runner.
    interpreter: tuple[str, ...] = ("sh", "-c")
    # Non-secret configuration only. Anything here is retained with the job and
    # is meant to be read.
    environment: dict[str, str] = {}
    # Secrets by *name*. The runner resolves them; the values never enter the
    # job, which is provenance.
    secrets: tuple[str, ...] = ()
    network: str | None = None
    # Host paths made visible to the job, as (outside, inside) pairs. Needed by
    # a job whose target *is* a filesystem — an Iceberg warehouse, say. Part of
    # the packaging's identity, because a job pointed at a different warehouse
    # is a different job rather than the same one behaving differently.
    mounts: tuple[tuple[str, str], ...] = ()

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
                # A job pointed at a different warehouse is a different job.
                ",".join(f"{outside}:{inside}" for outside, inside in self.mounts),
                " ".join(self.interpreter),
            )
        )


_SECRET_HINTS = ("password", "passwd", "secret", "token", "apikey", "api_key", "credential")


def _looks_secret(name: str) -> bool:
    lowered = name.lower()
    return any(hint in lowered for hint in _SECRET_HINTS)


# The image a generated SQL script runs in. It needs a `psql` and nothing else,
# so the client image is the whole dependency. Pinned by tag here and by digest
# in anything that cares about reproducing a run exactly.
DEFAULT_SQL_IMAGE = "postgres:16-alpine"


def sql_client_packaging(
    *,
    secrets: tuple[str, ...],
    image: str = DEFAULT_SQL_IMAGE,
    network: str | None = None,
    mounts: tuple[tuple[str, str], ...] = (),
) -> ContainerPackaging:
    """Packaging for a generated SQL script.

    This exists so that the code *generating* a script does not have to know
    what a container is. When a second packaging mechanism arrives, callers
    swap this factory for another one and the generators do not change.
    """
    return ContainerPackaging(image=image, network=network, secrets=secrets, mounts=mounts)


# The image a generated Beam pipeline runs in. Unlike the SQL client image this
# one is built rather than pulled: it needs a JRE and pre-staged JARs that the
# official Beam SDK image does not carry. See `docker/beam/Dockerfile`.
DEFAULT_BEAM_IMAGE = "gantry/beam:2.76.0"


def beam_packaging(
    *,
    secrets: tuple[str, ...],
    image: str = DEFAULT_BEAM_IMAGE,
    network: str | None = None,
    mounts: tuple[tuple[str, str], ...] = (),
) -> ContainerPackaging:
    """Packaging for a generated Beam pipeline.

    The body is a Python program, so it is handed to `python -c` rather than to
    a shell. Same reason `sql_client_packaging` exists: the code generating a
    pipeline should not have to know what a container is.
    """
    return ContainerPackaging(
        image=image,
        network=network,
        secrets=secrets,
        mounts=mounts,
        interpreter=("python", "-c"),
    )
