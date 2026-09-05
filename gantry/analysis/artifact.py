# SPDX-License-Identifier: Apache-2.0
"""Generated executable artifacts.

The design document is explicit that generation is not execution: an artifact
is produced, versioned, retained, and only then validated and run. That makes
the generated SQL an inspectable object rather than a string that briefly
existed inside a function.

It matters for two reasons. A Result has to be able to answer *why do we
believe this*, and "some SQL we generated" is not an answer. And an agent that
proposes an Analysis needs its generated work to be reviewable before anyone
lets it run.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator

from gantry.core.names import ContentHash, ResourceName
from gantry.core.provenance import ArtifactKind, ArtifactRef


class ArtifactLanguage(StrEnum):
    SQL = "sql"


class GeneratedArtifact(BaseModel):
    """Executable work compiled from a specification.

    Content-addressed, so an artifact either is the one a Result was produced
    from or is a different artifact. Compilation time is excluded from the
    hash: recompiling the same Analysis must yield the same artifact, whenever
    it happens.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    analysis: ResourceName
    engine: str
    language: ArtifactLanguage = ArtifactLanguage.SQL
    body: str
    # What the artifact reads, recorded so provenance does not have to be
    # recovered by parsing the SQL back out.
    inputs: tuple[ResourceName, ...] = ()
    parameters: dict[str, str] = {}
    generated_at: datetime

    @model_validator(mode="after")
    def _check_artifact(self) -> GeneratedArtifact:
        if self.generated_at.tzinfo is None:
            raise ValueError("generated_at must be timezone-aware")
        if not self.body.strip():
            raise ValueError("an artifact with no body is not an artifact")
        return self

    def canonical(self) -> str:
        """The bytes the hash is taken over.

        Deliberately not the whole model: compilation time and anything else
        that varies between runs of the same input must not change identity.
        """
        parameters = "\n".join(
            f"{name}={self.parameters[name]}" for name in sorted(self.parameters)
        )
        return "\n".join(
            (
                f"analysis={self.analysis}",
                f"engine={self.engine}",
                f"language={self.language.value}",
                f"inputs={','.join(sorted(self.inputs))}",
                f"parameters={parameters}",
                "body=",
                self.body,
            )
        )

    @property
    def content_hash(self) -> ContentHash:
        digest = hashlib.sha256(self.canonical().encode("utf-8")).hexdigest()
        return f"sha256:{digest}"

    def to_ref(self) -> ArtifactRef:
        return ArtifactRef(
            kind=ArtifactKind.SQL,
            reference=f"{self.analysis}/{self.engine}",
            content_hash=self.content_hash,
        )

    def preview(self, lines: int = 20) -> str:
        """The first few lines, for showing an operator what will run."""
        body = self.body.strip().splitlines()
        shown = "\n".join(body[:lines])
        return shown if len(body) <= lines else f"{shown}\n… {len(body) - lines} more lines"


class CompilationError(Exception):
    """A specification that cannot be compiled.

    Raised with enough detail to repair the spec, because a planner or an agent
    reads these and has to know what to change.
    """

    def __init__(self, analysis: str, problem: str, *, hint: str | None = None) -> None:
        message = f"cannot compile analysis {analysis!r}: {problem}"
        if hint:
            message = f"{message} ({hint})"
        super().__init__(message)
        self.analysis = analysis
        self.problem = problem


class UnknownSignalError(CompilationError):
    """A signal the compiler has no definition for."""

    def __init__(self, analysis: str, signal: str, known: tuple[str, ...]) -> None:
        super().__init__(
            analysis,
            f"unknown signal {signal!r}",
            hint=f"known signals: {', '.join(known)}",
        )
        self.signal = signal


class SignalDefinition(BaseModel):
    """A named measurement, with the expression that computes it.

    Signals are declared rather than written as free SQL on purpose. An
    Analysis spec that accepted arbitrary expressions would be a query
    language, and this compiler is not one - it exists to compile a fixed
    vocabulary onto engines that already have query languages of their own.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    expression: str
    # Which normalised column the expression needs, so a spec asking for a
    # signal its inputs cannot support fails at compile time rather than at
    # execution time.
    requires: tuple[str, ...] = ()
    description: str = Field(default="")
