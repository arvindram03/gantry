# SPDX-License-Identifier: Apache-2.0
"""A YAML loader aligned with YAML 1.2 booleans.

PyYAML implements YAML 1.1, which resolves `on`, `off`, `yes` and `no` as
booleans. That is a problem for specs: the join block names its key field `on`,
so `on: [request_id]` loads as `{True: ['request_id']}` and fails validation
with an error that points nowhere useful.

YAML 1.2 restricts booleans to `true` and `false`. This loader does the same,
so field names spelled `on`, `no`, `yes` or `off` survive as strings.
"""

from __future__ import annotations

import re
from typing import IO

import yaml

_BOOL_TAG = "tag:yaml.org,2002:bool"

# YAML 1.2 core schema: true/false only, in the three conventional casings.
_BOOL_PATTERN = re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$")


class GantryYamlLoader(yaml.SafeLoader):
    """SafeLoader with YAML 1.1's extra boolean spellings removed."""


GantryYamlLoader.yaml_implicit_resolvers = {
    first_char: [(tag, regexp) for tag, regexp in resolvers if tag != _BOOL_TAG]
    for first_char, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
GantryYamlLoader.add_implicit_resolver(_BOOL_TAG, _BOOL_PATTERN, list("tTfF"))


def safe_load(stream: str | IO[str]) -> object:
    """Parse YAML using the 1.2-aligned boolean rules."""
    return yaml.load(stream, Loader=GantryYamlLoader)
