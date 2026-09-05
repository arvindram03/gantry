# SPDX-License-Identifier: Apache-2.0
"""Jobs, packaging, and the boundary between them.

Three things that change on different schedules. The test that matters most in
this file is the last one: it asserts that nothing outside two named modules
knows what a container is. That boundary is the likeliest failure of the design
and the quietest — an `image: str` in a signature decides that packaging is
containers forever, and nothing about the code looks wrong afterwards.
"""

from __future__ import annotations

import ast
from datetime import UTC, datetime
from pathlib import Path

import pytest
from gantry.jobs import (
    ContainerPackaging,
    Job,
    JobKind,
    JobState,
    PackagingKind,
)
from pydantic import ValidationError

AT = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
ROOT = Path(__file__).resolve().parents[2]


def packaging(**overrides: object) -> ContainerPackaging:
    base: dict[str, object] = {"image": "postgres:16-alpine"}
    base.update(overrides)
    return ContainerPackaging.model_validate(base)


def job(**overrides: object) -> Job:
    base: dict[str, object] = {
        "operation": "orders-snapshot",
        "kind": JobKind.SQL,
        "unit": "public.orders/00004",
        "body": "psql -c 'SELECT 1'",
        "packaging": packaging(),
        "generated_at": AT,
    }
    base.update(overrides)
    return Job.model_validate(base)


class TestJobIdentity:
    def test_the_same_job_hashes_the_same(self) -> None:
        assert job().content_hash == job().content_hash

    def test_generation_time_is_outside_the_hash(self) -> None:
        """Recompiling the same unit of work has to be idempotent."""
        later = job(generated_at=datetime(2027, 1, 1, tzinfo=UTC))
        assert later.content_hash == job().content_hash

    def test_a_different_body_is_a_different_job(self) -> None:
        assert job(body="psql -c 'SELECT 2'").content_hash != job().content_hash

    def test_a_different_unit_is_a_different_job(self) -> None:
        """Two partitions are two jobs even when the script text matches."""
        assert job(unit="public.orders/00005").content_hash != job().content_hash

    def test_a_different_image_is_a_different_job(self) -> None:
        """The packaging is part of the identity, which is what stops an image
        changing underneath a replay from looking like the same work behaving
        differently."""
        assert job(packaging=packaging(image="postgres:17-alpine")).content_hash != (
            job().content_hash
        )

    def test_a_digest_changes_the_identity(self) -> None:
        pinned = packaging(digest="sha256:" + "a" * 64)
        assert job(packaging=pinned).content_hash != job().content_hash

    def test_changing_which_secret_is_read_is_a_different_job(self) -> None:
        """Named, not valued — identity-bearing without being disclosing."""
        assert job(packaging=packaging(secrets=("PGPASSWORD",))).content_hash != (
            job().content_hash
        )


class TestJobValidation:
    def test_a_job_with_no_body_is_not_a_job(self) -> None:
        with pytest.raises(ValidationError, match="no body"):
            job(body="   ")

    def test_a_job_must_say_what_it_covers(self) -> None:
        with pytest.raises(ValidationError, match="which unit"):
            job(unit="")

    def test_generation_time_must_be_timezone_aware(self) -> None:
        with pytest.raises(ValidationError, match="timezone-aware"):
            job(generated_at=datetime(2026, 9, 5, 12, 0))


class TestPackaging:
    def test_an_image_is_required(self) -> None:
        with pytest.raises(ValidationError, match="needs an image"):
            packaging(image="  ")

    def test_a_digest_must_be_a_sha256_reference(self) -> None:
        with pytest.raises(ValidationError, match="sha256"):
            packaging(digest="latest")

    def test_the_reference_pins_to_the_digest_when_there_is_one(self) -> None:
        digest = "sha256:" + "b" * 64
        assert packaging(digest=digest).reference == f"postgres:16-alpine@{digest}"

    def test_the_reference_falls_back_to_the_tag(self) -> None:
        assert packaging().reference == "postgres:16-alpine"

    @pytest.mark.parametrize(
        "name", ["PGPASSWORD", "DB_SECRET", "api_key", "AUTH_TOKEN", "MY_CREDENTIAL"]
    )
    def test_environment_refuses_things_that_look_like_secrets(self, name: str) -> None:
        """The job body and its packaging are retained as provenance and meant
        to be read. A password in there is a password in the audit trail."""
        with pytest.raises(ValidationError, match="look like secrets"):
            packaging(environment={name: "hunter2"})

    def test_ordinary_environment_is_fine(self) -> None:
        assert packaging(environment={"PGHOST": "pg-source"}).environment["PGHOST"] == ("pg-source")


class TestJobState:
    @pytest.mark.parametrize("state", [JobState.SUCCEEDED, JobState.FAILED])
    def test_terminal_states_are_terminal(self, state: JobState) -> None:
        assert state.terminal

    @pytest.mark.parametrize("state", [JobState.PENDING, JobState.RUNNING])
    def test_the_others_are_not(self, state: JobState) -> None:
        assert not state.terminal


def test_packaging_kinds_are_open_to_extension() -> None:
    """One member today. The enum exists so adding WASM later touches this and
    one new model, rather than every signature that assumed an image."""
    assert PackagingKind.CONTAINER in tuple(PackagingKind)


# --- the boundary ----------------------------------------------------------

# The modules allowed to know what a container is. Everything else may name a
# packaging *kind* and nothing more.
_CONTAINER_AWARE = {
    "gantry/jobs/packaging/container.py",
    "gantry/jobs/packaging/kind.py",
    "gantry/jobs/runners/docker.py",
}

# Names that would mean packaging had leaked into a signature. Narrow on
# purpose — a guard that cries wolf gets deleted, and these three are the only
# words with no other meaning in this codebase.
#
# `registry` is out: it is the *Dataset* registry here, an older and unrelated
# idea. `digest` is out: it is a local in the artifact hasher. And `image` is
# checked as an identifier rather than as text, because `changes.py` says
# "before image" about CDC row images, which is a different word entirely.
_PACKAGING_NAMES = {"image", "container", "dockerfile"}


def _declared_names(tree: ast.AST) -> list[tuple[int, str]]:
    """Every field, parameter and assignment target in a module."""
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.arg):
            found.append((node.lineno, node.arg))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            found.append((node.lineno, node.target.id))
        elif isinstance(node, ast.Assign):
            found.extend(
                (target.lineno, target.id)
                for target in node.targets
                if isinstance(target, ast.Name)
            )
    return found


def test_packaging_does_not_leak_into_any_signature() -> None:
    """The likeliest failure of this design, and the quietest.

    An `image: str` field or parameter outside the packaging modules decides
    that packaging is containers forever, and nothing about the code looks
    wrong afterwards. Packaging is supposed to change when the industry moves;
    this test is what keeps that possible.

    Checked against the AST rather than by grepping, so that prose about
    containers is free and CDC's "before image" — a different word — does not
    register.
    """
    offenders: list[str] = []
    for path in sorted((ROOT / "gantry").rglob("*.py")):
        relative = path.relative_to(ROOT).as_posix()
        if relative in _CONTAINER_AWARE:
            continue
        tree = ast.parse(path.read_text())
        offenders += [
            f"{relative}:{line}: {name}"
            for line, name in _declared_names(tree)
            if name.lower().strip("_") in _PACKAGING_NAMES
        ]

    assert not offenders, (
        "packaging leaked outside "
        + ", ".join(sorted(_CONTAINER_AWARE))
        + ":\n  "
        + "\n  ".join(offenders)
    )


def test_the_boundary_test_can_actually_fail() -> None:
    """A guard that cannot fail is decoration.

    Synthesises the leak it is meant to catch and checks the detector sees it,
    because a test whose assertion has never been observed to fire is a test
    nobody has verified.
    """
    leaked = ast.parse("class Runner:\n    image: str\n")
    assert [name for _, name in _declared_names(leaked)] == ["image"]
