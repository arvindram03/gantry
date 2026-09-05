# SPDX-License-Identifier: Apache-2.0
"""YAML spellings for verification requirements.

The design documents write these two ways. A Movement lists bare check names:

    verification:
      required: [row_count, chunk_checksum]

An Analysis lists parameterised checks:

    verify:
      - rowExpansion: {max: 1.1}
      - joinCoverage: {min: 0.95}

Both are the same requirement list, and both normalise onto the domain model in
`gantry.core.verification`. This module owns only the spellings, so a change to
spec syntax stops here.
"""

from __future__ import annotations

from gantry.core.verification import PARAMETERLESS_CHECKS, CheckName

# camelCase spellings from the Analysis examples map onto canonical names.
CHECK_ALIASES: dict[str, str] = {
    "rowCount": CheckName.ROW_COUNT,
    "chunkChecksum": CheckName.CHUNK_CHECKSUM,
    "primaryKeyUnique": CheckName.PRIMARY_KEY_UNIQUE,
    "foreignKeyIntegrity": CheckName.FOREIGN_KEY_INTEGRITY,
    "rowExpansion": CheckName.ROW_EXPANSION,
    "joinCoverage": CheckName.JOIN_COVERAGE,
    "temporalAlignment": CheckName.TEMPORAL_ALIGNMENT,
    "nullRate": CheckName.NULL_RATE,
}


def normalize_check_name(name: str) -> str:
    return CHECK_ALIASES.get(name, name)


def normalize_requirement(entry: object) -> object:
    """Normalise one requirement entry into a taggable mapping.

    Accepts a bare name (`row_count`) or a single-key mapping carrying
    parameters (`{rowExpansion: {max: 1.1}}`), and leaves an already-tagged
    mapping alone so parsed specs round-trip.
    """
    if isinstance(entry, str):
        return {"check": normalize_check_name(entry)}

    if not isinstance(entry, dict):
        raise ValueError(
            f"verification entry must be a check name or a single-key mapping, "
            f"got {type(entry).__name__}"
        )

    if "check" in entry:
        tagged: dict[str, object] = {str(k): v for k, v in entry.items()}
        tagged["check"] = normalize_check_name(str(tagged["check"]))
        return tagged

    if len(entry) != 1:
        raise ValueError(
            f"verification entry must have exactly one check name, got {sorted(entry)}"
        )

    raw_name, params = next(iter(entry.items()))
    name = normalize_check_name(str(raw_name))
    if params is None:
        return {"check": name}
    if not isinstance(params, dict):
        raise ValueError(f"parameters for {name!r} must be a mapping, got {type(params).__name__}")
    if name in PARAMETERLESS_CHECKS and params:
        raise ValueError(f"check {name!r} takes no parameters, got {sorted(params)}")
    return {"check": name, **{str(k): v for k, v in params.items()}}


def normalize_requirements(value: object) -> object:
    """Normalise a whole `verify` / `verification.required` list."""
    if not isinstance(value, list | tuple):
        return value
    return [normalize_requirement(entry) for entry in value]
