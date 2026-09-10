# SPDX-License-Identifier: Apache-2.0
"""Generate the capability matrix in `docs/api/capabilities.md` from the source.

A hand-written matrix is a promise nobody re-checks. This reads what each
adapter actually declares and what `policy_errors` actually requires, so the
page is wrong only if the code is.

    python scripts/capability_matrix.py          # print
    python scripts/capability_matrix.py --write  # update the page in place

`tests/test_capability_matrix.py` fails when the committed page and the source
disagree.
"""

from __future__ import annotations

import argparse
import ast
import dataclasses
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
ADAPTERS = ROOT / "gantry" / "sql" / "adapters"
PAGE = ROOT / "docs" / "api" / "capabilities.md"

BEGIN = "<!-- generated: capability matrix -->"
END = "<!-- /generated -->"

# Which capability a policy field needs before Gantry will admit a statement,
# and what it says when the adapter has not got it. Taken from
# `gantry/sql/enforcement.py`; the test checks these messages still appear there.
POLICY_REQUIREMENTS: tuple[tuple[str, str, str], ...] = (
    ("read_only", "read_only_session", "adapter cannot enforce a read-only session"),
    ("max_rows", "row_limit", "adapter cannot enforce the row limit"),
    (
        "timeout_seconds",
        "statement_timeout, or reconnect and cancellation",
        "adapter cannot enforce or monitor the statement timeout",
    ),
    ("max_bytes_scanned", "bytes_scanned", "adapter cannot enforce maximum bytes scanned"),
    ("max_cost_usd", "cost_limit", "adapter cannot enforce maximum cost"),
)

PROVIDERS = {
    "postgres": "PostgreSQL, Neon, Supabase",
    "bigquery": "BigQuery",
    "snowflake": "Snowflake",
    "duckdb": "DuckDB",
}


# Some adapters declare a capability conditionally — DuckDB and Snowflake can
# hold a read-only session only when the connection was opened read-only. An
# earlier version of this script read those as the dataclass default and printed
# a confident "no", which is the failure a generated matrix exists to avoid.
_CONDITIONS = {
    "self._read_only": "read-only conn",
    "not self._read_only": "writable conn",
}


def _value(node: ast.expr) -> str:
    if isinstance(node, ast.Constant):
        return "yes" if node.value else "no"
    return _CONDITIONS.get(ast.unparse(node), "conditional")


def declared(module: pathlib.Path) -> dict[str, str]:
    """Read the SQLCapabilities literal an adapter's `capabilities()` returns.

    Parsed rather than imported: instantiating an adapter needs its driver, and
    the matrix should be buildable without installing four of them.
    """
    tree = ast.parse(module.read_text())
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef) or node.name != "capabilities":
            continue
        for inner in ast.walk(node):
            if (
                isinstance(inner, ast.Call)
                and isinstance(inner.func, ast.Name)
                and inner.func.id == "SQLCapabilities"
            ):
                return {
                    keyword.arg: _value(keyword.value)
                    for keyword in inner.keywords
                    if keyword.arg is not None
                }
    raise SystemExit(f"no SQLCapabilities literal in {module}")


def capability_fields() -> tuple[str, ...]:
    from gantry.sql import SQLCapabilities

    return tuple(field.name for field in dataclasses.fields(SQLCapabilities))


def render() -> str:
    from gantry.sql import SQLCapabilities

    defaults = {f.name: f.default for f in dataclasses.fields(SQLCapabilities)}
    found = {name: declared(ADAPTERS / f"{name}.py") for name in PROVIDERS}

    lines = [BEGIN, ""]
    lines.append("### What a policy field needs")
    lines.append("")
    lines.append("| Policy field | Capability required | Refusal when absent |")
    lines.append("| --- | --- | --- |")
    for field, capability, message in POLICY_REQUIREMENTS:
        lines.append(f"| `{field}` | `{capability}` | {message} |")
    lines.append("")
    lines.append("### What each provider declares")
    lines.append("")
    header = " | ".join(PROVIDERS[name] for name in PROVIDERS)
    lines.append(f"| Capability | {header} |")
    lines.append("| --- |" + " --- |" * len(PROVIDERS))
    for capability in capability_fields():
        cells = [
            found[name].get(capability, "yes" if defaults[capability] else "no")
            for name in PROVIDERS
        ]
        lines.append(f"| `{capability}` | " + " | ".join(cells) + " |")
    lines.append("")
    lines.append(
        "A cell reading *read-only conn* or *writable conn* is declared "
        "conditionally: the adapter has the capability only when the connection "
        'was opened that way. `gantry.sql.connect("duckdb", path=..., '
        "read_only=True)` is what makes DuckDB able to hold a read-only session, "
        "and a connection that was not opened read-only is refused rather than "
        "trusted."
    )
    lines.append("")
    lines.append(END)
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="update the page in place")
    args = parser.parse_args()

    table = render()
    if not args.write:
        print(table)
        return 0

    text = PAGE.read_text()
    start, end = text.index(BEGIN), text.index(END) + len(END)
    PAGE.write_text(text[:start] + table + text[end:])
    print(f"updated {PAGE.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
