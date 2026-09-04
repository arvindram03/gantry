"""Dataset spec parsing, aliases and error reporting."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from gantry.core import AgentAccessPolicy
from gantry.spec import (
    CANONICAL_API_VERSION,
    DatasetSpec,
    SpecError,
    SpecValidationError,
    UnsupportedKindError,
    dataset_json_schema,
    is_deprecated_api_version,
    load_dataset_spec,
)

EXAMPLES = Path(__file__).resolve().parents[2] / "spec" / "examples"

MINIMAL = """
apiVersion: gantry.dev/v1alpha1
kind: Dataset
metadata:
  name: orders
physical:
  adapter: postgres
  reference: public.orders
"""


def parse(text: str) -> DatasetSpec:
    return load_dataset_spec("<test>", text=text)


def test_example_manifests_parse_and_convert() -> None:
    for path in sorted(EXAMPLES.glob("dataset-*.yaml")):
        spec = load_dataset_spec(path)
        manifest = spec.to_manifest()
        assert manifest.name == spec.metadata.name


def test_full_example_maps_every_block() -> None:
    manifest = load_dataset_spec(EXAMPLES / "dataset-checkout-logs.yaml").to_manifest()
    assert manifest.physical.adapter == "clickhouse"
    assert manifest.physical.estimated_bytes == 14_200_000_000_000
    assert manifest.physical.estimated_rows == 8_400_000_000
    assert manifest.dataset_schema.time_field == "event_time"
    assert manifest.dataset_schema.keys == ("request_id", "trace_id", "service")
    assert manifest.statistics.change_rate_per_second == 18000
    assert manifest.access.agent_policy is AgentAccessPolicy.AGGREGATE_OR_MASKED
    assert manifest.sensitive_fields == ("user_email", "payment_token")


def test_deprecated_api_version_is_accepted_and_flagged() -> None:
    spec = load_dataset_spec("<test>", text=MINIMAL.replace("gantry.dev", "gantry.io"))
    assert is_deprecated_api_version(spec.api_version)
    assert not is_deprecated_api_version(CANONICAL_API_VERSION)


def test_unknown_api_version_is_rejected() -> None:
    with pytest.raises(SpecValidationError, match="apiVersion"):
        parse(MINIMAL.replace("gantry.dev/v1alpha1", "example.com/v1"))


def test_time_field_accepts_either_spelling() -> None:
    via_semantics = parse(MINIMAL + "semantics:\n  timeField: created_at\n")
    via_schema = parse(MINIMAL + "schema:\n  timestamp: created_at\n")
    assert via_semantics.to_manifest().dataset_schema.time_field == "created_at"
    assert via_schema.to_manifest().dataset_schema.time_field == "created_at"


def test_conflicting_time_field_declarations_are_an_error() -> None:
    text = MINIMAL + "schema:\n  timestamp: a\nsemantics:\n  timeField: b\n"
    with pytest.raises(SpecValidationError, match="conflicting time field"):
        parse(text)


def test_agreeing_time_field_declarations_are_accepted() -> None:
    text = MINIMAL + "schema:\n  timestamp: created_at\nsemantics:\n  timeField: created_at\n"
    assert parse(text).to_manifest().dataset_schema.time_field == "created_at"


def test_errors_carry_yaml_field_paths() -> None:
    with pytest.raises(SpecValidationError) as caught:
        parse(MINIMAL.replace("  reference: public.orders", ""))
    assert any("physical.reference" in line for line in caught.value.errors)


def test_unparseable_size_names_its_field() -> None:
    with pytest.raises(SpecValidationError) as caught:
        parse(MINIMAL + "  estimatedBytes: 14.2 parsecs\n")
    assert any("estimatedBytes" in line for line in caught.value.errors)


def test_unknown_field_is_rejected_rather_than_ignored() -> None:
    with pytest.raises(SpecValidationError):
        parse(MINIMAL + "sensitiveFieldz: [oops]\n")


def test_wrong_kind_names_supported_kinds() -> None:
    with pytest.raises(UnsupportedKindError, match="Dataset"):
        parse(MINIMAL.replace("kind: Dataset", "kind: Movement"))


@pytest.mark.parametrize(
    ("text", "match"),
    [
        ("", "empty"),
        ("- a\n- b\n", "must be a mapping"),
        ("a: [unclosed\n", "not valid YAML"),
    ],
)
def test_malformed_documents_report_clearly(text: str, match: str) -> None:
    with pytest.raises(SpecError, match=match):
        parse(text)


def test_missing_file_is_reported() -> None:
    with pytest.raises(SpecError, match="no such file"):
        load_dataset_spec(Path("/nonexistent/dataset.yaml"))


def test_json_schema_is_generated_from_the_models() -> None:
    schema = json.loads(dataset_json_schema())
    assert schema["title"] == "Gantry Dataset"
    assert "apiVersion" in schema["properties"]
    assert "sensitiveFields" in schema["properties"]
