# SPDX-License-Identifier: Apache-2.0
"""What the distribution has to carry for its types to be visible.

`mypy --strict` passing in this repository says nothing about what a consumer
sees. Under PEP 561 a type checker must ignore an installed package that has no
`py.typed` marker, so without it every symbol Gantry exports resolves to `Any`
downstream and the `Typing :: Typed` classifier is a false claim.
"""

from __future__ import annotations

import importlib.resources


def test_the_package_ships_a_py_typed_marker() -> None:
    """Checked through the package's own resources rather than a repository
    path, so this fails the same way against an installed wheel that dropped
    the file as it does against a source tree missing it."""
    marker = importlib.resources.files("gantry").joinpath("py.typed")

    assert marker.is_file(), (
        "gantry/py.typed is missing; without it PEP 561 requires type checkers "
        "to treat every Gantry import as untyped"
    )
