# SPDX-License-Identifier: Apache-2.0
"""Every exported symbol reaches the published reference, and says something.

The first version of the reference passed `mkdocs build --strict` while
omitting `submit`, `wait` and `run` entirely, and while rendering forty-odd
dataclasses whose only documentation was their own signature repeated back as
prose. Neither gap is visible to a docs build: a page that documents nothing
builds perfectly. These two tests are what notices.
"""

from __future__ import annotations

import inspect
import pathlib
import re

import gantry
import gantry.sql

API_DIR = pathlib.Path(__file__).resolve().parents[1] / "docs" / "api"
MODULES = ((gantry, "gantry"), (gantry.sql, "gantry.sql"))


def _documentable() -> list[tuple[str, str, object]]:
    """Exported symbols that can carry a docstring of their own.

    Submodules re-exported for convenience (`gantry.sql`, `gantry.batch`) are
    documented by their own pages, and `__version__` is a string.
    """
    out = []
    for module, label in MODULES:
        for name in module.__all__:
            obj = getattr(module, name)
            if inspect.ismodule(obj) or isinstance(obj, str):
                continue
            out.append((label, name, obj))
    return out


def _own_docstring(obj: object) -> str | None:
    """The object's own docstring, never one inherited from a base.

    `inspect.getdoc` walks the MRO, so an exception subclass appears to be
    documented by `Exception` and a Protocol by `Protocol`. That is how
    `MaterializationError` passed an earlier version of this check.
    """
    own = obj.__dict__.get("__doc__") if hasattr(obj, "__dict__") else None
    if isinstance(own, str) and own.strip():
        return own
    if inspect.isclass(obj):
        return None
    doc = getattr(obj, "__doc__", None)
    return doc if isinstance(doc, str) and doc.strip() else None


def test_every_exported_symbol_has_its_own_docstring() -> None:
    undocumented = []
    for label, name, obj in _documentable():
        doc = _own_docstring(obj)
        if doc is None:
            undocumented.append(f"{label}.{name} (no docstring)")
        elif inspect.isclass(obj) and doc.strip().startswith(f"{obj.__name__}("):
            # A dataclass with no docstring gets one generated from its
            # signature. It renders as a paragraph and tells a reader nothing
            # the signature above it did not.
            undocumented.append(f"{label}.{name} (generated dataclass signature)")

    assert not undocumented, "these render in the API reference with no prose: " + ", ".join(
        sorted(undocumented)
    )


def test_every_exported_symbol_appears_on_a_reference_page() -> None:
    rendered = set()
    for page in API_DIR.glob("*.md"):
        rendered |= set(re.findall(r"^\s*:::\s*(\S+)", page.read_text(), re.M))

    missing = []
    for label, name, obj in _documentable():
        # mkdocstrings resolves either the re-export path or the defining one.
        paths = {f"{label}.{name}", f"{getattr(obj, '__module__', '')}.{name}"}
        if not paths & rendered:
            missing.append(f"{label}.{name}")

    assert not missing, "exported but on no docs/api page: " + ", ".join(sorted(missing))
