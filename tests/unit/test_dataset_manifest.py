"""Dataset manifests: content addressing and schema consistency."""

from __future__ import annotations

import pytest
from gantry.core import (
    AgentAccessPolicy,
    DatasetManifest,
    DatasetRef,
    DatasetSchema,
    FieldSchema,
    PhysicalRef,
)
from pydantic import ValidationError


def manifest(**overrides: object) -> DatasetManifest:
    base: dict[str, object] = {
        "name": "orders",
        "physical": PhysicalRef(adapter="postgres", reference="public.orders"),
        "dataset_schema": DatasetSchema(keys=("order_id",)),
    }
    base.update(overrides)
    return DatasetManifest.model_validate(base)


def test_content_hash_is_stable_across_equal_manifests() -> None:
    assert manifest().content_hash == manifest().content_hash


def test_content_hash_changes_with_content() -> None:
    enriched = manifest(statistics={"row_count": 100})
    assert enriched.content_hash != manifest().content_hash


def test_canonical_json_is_key_ordered() -> None:
    rendered = manifest().canonical_json()
    keys = [k for k in ("access", "dataset_schema", "name", "physical") if f'"{k}"' in rendered]
    assert keys == sorted(keys)


def test_default_agent_policy_is_not_permissive() -> None:
    assert manifest().access.agent_policy is AgentAccessPolicy.AGGREGATE_OR_MASKED


def test_keys_must_exist_once_fields_are_discovered() -> None:
    with pytest.raises(ValidationError, match="key fields not present"):
        DatasetSchema(
            keys=("missing_id",),
            fields=(FieldSchema(name="order_id", type="bigint"),),
        )


def test_keys_need_no_field_list_before_discovery() -> None:
    assert DatasetSchema(keys=("order_id",)).fields == ()


def test_sensitive_fields_must_exist_once_fields_are_discovered() -> None:
    with pytest.raises(ValidationError, match="sensitive fields not present"):
        manifest(
            dataset_schema=DatasetSchema(fields=(FieldSchema(name="order_id", type="bigint"),)),
            sensitive_fields=("user_email",),
        )


def test_dataset_ref_renders_pinned_and_floating_forms() -> None:
    assert str(DatasetRef(name="orders")) == "orders"
    assert not DatasetRef(name="orders").is_pinned
    assert str(DatasetRef(name="orders", version=3)) == "orders@3"
    assert DatasetRef(name="orders", version=3).is_pinned


def test_manifest_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        manifest(oops="typo")
