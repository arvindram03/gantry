# SPDX-License-Identifier: Apache-2.0
"""The published capability matrix has to match the code it describes.

A matrix is a promise about what each adapter enforces, and a promise nobody
re-checks is the kind that quietly stops being true. This regenerates it from
the adapters and from `policy_errors` and fails when the committed page has
drifted.
"""

from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from capability_matrix import BEGIN, END, PAGE, POLICY_REQUIREMENTS, render  # noqa: E402


def test_the_published_matrix_matches_the_adapters() -> None:
    page = PAGE.read_text()
    start, end = page.index(BEGIN), page.index(END) + len(END)

    assert page[start:end] == render(), (
        "docs/api/capabilities.md is out of date; "
        "regenerate with `python scripts/capability_matrix.py --write`"
    )


def test_every_documented_refusal_is_a_message_enforcement_can_produce() -> None:
    """The refusals in the matrix are quoted from `policy_errors`. If one is
    reworded there and not here, the page starts describing behaviour that no
    longer exists."""
    source = (ROOT / "gantry" / "sql" / "enforcement.py").read_text()

    for field, _capability, message in POLICY_REQUIREMENTS:
        assert message in source, (
            f"the matrix says {field} is refused with {message!r}, "
            f"which enforcement.py no longer says"
        )
