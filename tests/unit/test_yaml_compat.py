"""YAML 1.2 boolean handling.

PyYAML implements YAML 1.1, where `on`/`off`/`yes`/`no` are booleans. The join
block names its key field `on`, so this matters for real specs.
"""

from __future__ import annotations

import pytest
import yaml
from gantry.spec.yaml_compat import safe_load


@pytest.mark.parametrize("word", ["on", "off", "yes", "no"])
def test_yaml_11_boolean_words_stay_strings(word: str) -> None:
    assert safe_load(f"{word}: [a]") == {word: ["a"]}


@pytest.mark.parametrize("word", ["on", "off", "yes", "no"])
def test_this_differs_from_pyyaml_default(word: str) -> None:
    """Documents the behaviour being corrected."""
    assert list(yaml.safe_load(f"{word}: [a]")) != [word]


def test_real_booleans_still_parse() -> None:
    assert safe_load("a: true\nb: false\nc: True\nd: FALSE") == {
        "a": True,
        "b": False,
        "c": True,
        "d": False,
    }


def test_other_scalars_are_unaffected() -> None:
    assert safe_load("a: 1\nb: 1.5\nc: null\nd: text") == {
        "a": 1,
        "b": 1.5,
        "c": None,
        "d": "text",
    }
