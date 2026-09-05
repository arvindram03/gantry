"""The progressive access ladder, and what it refuses.

The RFC's claim about these rules is specific: an LLM cannot override them.
That is a claim about *where* they are enforced, so what these tests pin is
that a decision depends on the policy and the manifest and nothing else - no
caller-supplied override, no per-request escape, no way to ask nicely.
"""

from __future__ import annotations

import pytest
from gantry.core import AgentAccessPolicy, DatasetManifest, DatasetSchema, FieldSchema, PhysicalRef
from gantry.core.dataset import AccessPolicy, DatasetStatistics
from gantry.policy.gate import AccessDeniedError, AccessGate, AccessRequest
from gantry.policy.ladder import AccessRung
from gantry.policy.redaction import REDACTED, redact_manifest, redact_rows
from gantry.policy.rules import (
    AccessDefaults,
    AccessRules,
    Decision,
    PiiMode,
    PiiRules,
    QueryRules,
    SampleRules,
    most_restrictive,
)

FIELDS = (
    FieldSchema(name="order_id", type="bigint"),
    FieldSchema(name="email", type="text"),
    FieldSchema(name="total", type="numeric"),
)


def manifest(**overrides: object) -> DatasetManifest:
    base: dict[str, object] = {
        "name": "orders",
        "physical": PhysicalRef(adapter="postgres", reference="public.orders"),
        "dataset_schema": DatasetSchema(keys=("order_id",), fields=FIELDS),
        "sensitive_fields": ("email",),
    }
    base.update(overrides)
    return DatasetManifest.model_validate(base)


def request(rung: AccessRung, **overrides: object) -> AccessRequest:
    base: dict[str, object] = {"rung": rung, "dataset": manifest()}
    base.update(overrides)
    return AccessRequest(**base)  # type: ignore[arg-type]


class TestTheLadderItself:
    def test_the_rungs_are_ordered_least_to_most_revealing(self) -> None:
        assert list(AccessRung) == sorted(AccessRung)
        assert AccessRung.DESCRIBE < AccessRung.PROFILE < AccessRung.QUERY
        assert AccessRung.QUERY < AccessRung.PARTITION < AccessRung.SAMPLE < AccessRung.RECORDS

    def test_describe_is_the_only_rung_that_returns_no_values(self) -> None:
        """Redaction starts at profile because a profile quotes the data."""
        assert not AccessRung.DESCRIBE.returns_values
        assert all(rung.returns_values for rung in AccessRung if rung > AccessRung.DESCRIBE)

    def test_rows_begin_at_sample(self) -> None:
        assert not AccessRung.PARTITION.returns_rows
        assert AccessRung.SAMPLE.returns_rows
        assert AccessRung.RECORDS.returns_rows


class TestDefaultPosture:
    """The RFC's default: rows deny, aggregates allow, metadata allow."""

    def test_metadata_and_aggregates_are_permitted_by_default(self) -> None:
        gate = AccessGate()
        assert gate.evaluate(request(AccessRung.DESCRIBE)).permitted
        assert gate.evaluate(request(AccessRung.QUERY, fields=("total",))).permitted

    def test_raw_rows_are_denied_by_default(self) -> None:
        gate = AccessGate()
        decision = gate.evaluate(request(AccessRung.RECORDS, reason="investigating"))
        assert not decision.permitted
        assert any("row access" in ground for ground in decision.grounds)

    def test_a_denial_says_which_rule_refused(self) -> None:
        """A denial that teaches nothing invites retrying at random."""
        gate = AccessGate()
        with pytest.raises(AccessDeniedError) as caught:
            gate.authorize(request(AccessRung.RECORDS))
        assert "records on orders: deny" in str(caught.value)
        assert caught.value.decision.rung is AccessRung.RECORDS


class TestDatasetStanceComposition:
    def test_a_dataset_can_tighten_the_global_policy(self) -> None:
        permissive = AccessRules(default=AccessDefaults(rows=Decision.ALLOW))
        closed = manifest(access=AccessPolicy(agent_policy=AgentAccessPolicy.DENY))
        decision = AccessGate(permissive).evaluate(
            AccessRequest(rung=AccessRung.DESCRIBE, dataset=closed)
        )
        assert not decision.permitted

    def test_a_dataset_cannot_loosen_the_global_policy(self) -> None:
        """The composition rule that needs no precedence table: strictest wins."""
        strict = AccessRules(default=AccessDefaults(aggregates=Decision.DENY))
        open_dataset = manifest(access=AccessPolicy(agent_policy=AgentAccessPolicy.ALLOW))
        decision = AccessGate(strict).evaluate(
            AccessRequest(rung=AccessRung.QUERY, dataset=open_dataset, fields=("total",))
        )
        assert not decision.permitted

    def test_aggregate_or_masked_permits_a_sample_only_masked(self) -> None:
        rules = AccessRules(default=AccessDefaults(rows=Decision.ALLOW))
        decision = AccessGate(rules).evaluate(
            request(AccessRung.SAMPLE, reason="checking a failure", rows_requested=10)
        )
        assert decision.permitted
        assert decision.decision is Decision.REDACT
        assert decision.redacted_fields == ("email",)

    def test_aggregate_or_masked_never_permits_exact_records(self) -> None:
        rules = AccessRules(default=AccessDefaults(rows=Decision.ALLOW))
        decision = AccessGate(rules).evaluate(request(AccessRung.RECORDS))
        assert decision.decision is Decision.REDACT
        assert decision.permitted

    def test_an_allow_dataset_with_permissive_rules_returns_records_unmasked(self) -> None:
        rules = AccessRules(
            default=AccessDefaults(rows=Decision.ALLOW), pii=PiiRules(mode=PiiMode.ALLOW)
        )
        open_dataset = manifest(access=AccessPolicy(agent_policy=AgentAccessPolicy.ALLOW))
        decision = AccessGate(rules).evaluate(
            AccessRequest(rung=AccessRung.RECORDS, dataset=open_dataset)
        )
        assert decision.decision is Decision.ALLOW
        assert decision.redacted_fields == ()


class TestPii:
    def test_a_request_that_names_no_fields_is_treated_as_asking_for_all(self) -> None:
        """Assuming the narrower thing would let an unspecified request past."""
        decision = AccessGate().evaluate(request(AccessRung.PROFILE))
        assert decision.redacted_fields == ("email",)

    def test_a_request_avoiding_sensitive_fields_is_not_redacted(self) -> None:
        decision = AccessGate().evaluate(request(AccessRung.QUERY, fields=("order_id", "total")))
        assert decision.decision is Decision.ALLOW
        assert decision.redacted_fields == ()

    def test_pii_deny_refuses_rather_than_masking(self) -> None:
        rules = AccessRules(pii=PiiRules(mode=PiiMode.DENY))
        decision = AccessGate(rules).evaluate(request(AccessRung.QUERY, fields=("email",)))
        assert not decision.permitted
        assert any("pii mode is deny" in ground for ground in decision.grounds)

    def test_describe_is_not_redacted_because_it_quotes_nothing(self) -> None:
        """Field names and types are the description; masking them would make
        the top rung useless without protecting anything."""
        decision = AccessGate().evaluate(request(AccessRung.DESCRIBE))
        assert decision.decision is Decision.ALLOW
        assert decision.redacted_fields == ()

    def test_a_dataset_with_nothing_sensitive_is_never_masked(self) -> None:
        plain = manifest(sensitive_fields=())
        decision = AccessGate().evaluate(AccessRequest(rung=AccessRung.PROFILE, dataset=plain))
        assert decision.decision is Decision.ALLOW


class TestSampleRules:
    def test_a_sample_is_capped_at_the_policy_maximum(self) -> None:
        rules = AccessRules(
            default=AccessDefaults(rows=Decision.ALLOW), samples=SampleRules(max_rows=50)
        )
        decision = AccessGate(rules).evaluate(
            request(AccessRung.SAMPLE, reason="triage", rows_requested=10_000)
        )
        assert decision.row_limit == 50
        assert any("capped at 50" in ground for ground in decision.grounds)

    def test_asking_for_fewer_rows_than_the_cap_is_honoured(self) -> None:
        rules = AccessRules(default=AccessDefaults(rows=Decision.ALLOW))
        decision = AccessGate(rules).evaluate(
            request(AccessRung.SAMPLE, reason="triage", rows_requested=5)
        )
        assert decision.row_limit == 5

    def test_a_sample_without_a_reason_is_refused_when_one_is_required(self) -> None:
        rules = AccessRules(default=AccessDefaults(rows=Decision.ALLOW))
        decision = AccessGate(rules).evaluate(request(AccessRung.SAMPLE, rows_requested=5))
        assert not decision.permitted
        assert any("requires a stated reason" in ground for ground in decision.grounds)

    def test_a_blank_reason_is_not_a_reason(self) -> None:
        rules = AccessRules(default=AccessDefaults(rows=Decision.ALLOW))
        decision = AccessGate(rules).evaluate(request(AccessRung.SAMPLE, reason="   "))
        assert not decision.permitted

    def test_a_zero_row_sample_policy_denies_sampling_outright(self) -> None:
        rules = AccessRules(
            default=AccessDefaults(rows=Decision.ALLOW), samples=SampleRules(max_rows=0)
        )
        decision = AccessGate(rules).evaluate(request(AccessRung.SAMPLE, reason="triage"))
        assert not decision.permitted

    def test_only_a_sample_carries_a_row_limit(self) -> None:
        assert AccessGate().evaluate(request(AccessRung.QUERY)).row_limit is None


class TestQueryRules:
    def test_a_query_over_the_byte_budget_is_refused(self) -> None:
        rules = AccessRules(queries=QueryRules(max_bytes_scanned="1GB"))
        decision = AccessGate(rules).evaluate(
            request(AccessRung.QUERY, fields=("total",), estimated_bytes=2_000_000_000)
        )
        assert not decision.permitted
        assert any("over the 1GB budget" in ground for ground in decision.grounds)

    def test_a_query_inside_the_budget_runs(self) -> None:
        rules = AccessRules(queries=QueryRules(max_bytes_scanned="1GB"))
        decision = AccessGate(rules).evaluate(
            request(AccessRung.QUERY, fields=("total",), estimated_bytes=1_000)
        )
        assert decision.permitted

    def test_an_unknown_estimate_is_not_treated_as_zero(self) -> None:
        """No estimate means the engine would not say, which is not the same
        as saying it is small."""
        rules = AccessRules(queries=QueryRules(max_bytes_scanned="1GB"))
        decision = AccessGate(rules).evaluate(request(AccessRung.QUERY, fields=("total",)))
        assert decision.permitted, "an unknown estimate should not itself deny"

    def test_a_group_smaller_than_the_minimum_is_refused(self) -> None:
        """An aggregate over a unique key is row access wearing a GROUP BY."""
        rules = AccessRules(queries=QueryRules(min_group_size=25))
        decision = AccessGate(rules).evaluate(
            request(AccessRung.QUERY, fields=("total",), group_size=1)
        )
        assert not decision.permitted
        assert any("smallest group is 1" in ground for ground in decision.grounds)

    def test_the_default_minimum_of_one_does_not_reject_anything(self) -> None:
        decision = AccessGate().evaluate(request(AccessRung.QUERY, fields=("total",), group_size=1))
        assert decision.permitted

    def test_the_budget_applies_to_partition_listing_too(self) -> None:
        rules = AccessRules(queries=QueryRules(max_bytes_scanned="1KB"))
        decision = AccessGate(rules).evaluate(
            request(AccessRung.PARTITION, fields=("total",), estimated_bytes=10_000)
        )
        assert not decision.permitted


class TestRedaction:
    def test_masking_replaces_the_value_and_keeps_the_column(self) -> None:
        """A dropped column looks like a Dataset without one; a masked column
        says something is there and policy withheld it."""
        decision = AccessGate(AccessRules(default=AccessDefaults(rows=Decision.ALLOW))).evaluate(
            request(AccessRung.SAMPLE, reason="triage")
        )
        rows = redact_rows([{"order_id": 1, "email": "a@b.c", "total": 10}], decision)
        assert rows == [{"order_id": 1, "email": REDACTED, "total": 10}]

    def test_an_unredacted_decision_passes_rows_through(self) -> None:
        decision = AccessGate().evaluate(request(AccessRung.QUERY, fields=("total",)))
        assert redact_rows([{"total": 10}], decision) == [{"total": 10}]

    def test_a_profile_drops_the_histogram_of_a_masked_field(self) -> None:
        """Enough boundaries and the distribution of a masked column is
        readable straight off the profile."""
        profiled = manifest(
            statistics=DatasetStatistics(
                histograms={"email": ("a@b.c", "z@y.x"), "total": ("1", "100")},
                null_rates={"email": 0.0},
            )
        )
        decision = AccessGate().evaluate(AccessRequest(rung=AccessRung.PROFILE, dataset=profiled))
        masked = redact_manifest(profiled, decision)
        assert "email" not in masked.statistics.histograms
        assert masked.statistics.histograms["total"] == ("1", "100")
        assert masked.statistics.null_rates == {"email": 0.0}, "a rate quotes nothing"

    def test_the_key_range_survives_when_the_key_is_not_sensitive(self) -> None:
        profiled = manifest(statistics=DatasetStatistics(key_min="1", key_max="900"))
        decision = AccessGate().evaluate(AccessRequest(rung=AccessRung.PROFILE, dataset=profiled))
        assert redact_manifest(profiled, decision).statistics.key_min == "1"

    def test_the_key_range_is_dropped_when_the_key_itself_is_sensitive(self) -> None:
        profiled = manifest(
            sensitive_fields=("order_id",),
            statistics=DatasetStatistics(key_min="1", key_max="900"),
        )
        decision = AccessGate().evaluate(AccessRequest(rung=AccessRung.PROFILE, dataset=profiled))
        masked = redact_manifest(profiled, decision)
        assert masked.statistics.key_min is None
        assert masked.statistics.key_max is None


def test_most_restrictive_prefers_the_strictest() -> None:
    assert most_restrictive(Decision.ALLOW, Decision.REDACT) is Decision.REDACT
    assert most_restrictive(Decision.REDACT, Decision.DENY) is Decision.DENY
    assert most_restrictive(Decision.ALLOW, Decision.ALLOW) is Decision.ALLOW


def test_rules_parse_the_rfcs_own_policy_document() -> None:
    rules = AccessRules.model_validate(
        {
            "default": {"rows": "deny", "aggregates": "allow", "metadata": "allow"},
            "pii": {"mode": "redact"},
            "samples": {"max_rows": 50, "require_reason": True},
            "queries": {"max_bytes_scanned": "100GB", "timeout": "5m"},
            "evidence": {"persist": True},
        }
    )
    assert rules.queries.max_bytes_scanned == 100 * 1000**3
    assert rules.queries.timeout is not None
    assert rules.queries.timeout.total_seconds() == 300
    assert rules.default.rows is Decision.DENY


def test_the_shipped_defaults_are_the_rfcs_defaults() -> None:
    """Nothing is permitted by default that the design document did not permit."""
    rules = AccessRules()
    assert rules.default.rows is Decision.DENY
    assert rules.default.aggregates is Decision.ALLOW
    assert rules.default.metadata is Decision.ALLOW
    assert rules.pii.mode is PiiMode.REDACT
    assert rules.samples.max_rows == 50
    assert rules.samples.require_reason
    assert rules.evidence.persist


class TestPolicyLoading:
    def test_the_shipped_policy_file_parses(self) -> None:
        from pathlib import Path

        from gantry.policy.loader import load_access_rules

        rules = load_access_rules(
            Path(__file__).resolve().parents[2] / "spec/policy/agent-access.yaml"
        )
        assert rules == AccessRules(queries=QueryRules(max_bytes_scanned="100GB", timeout="5m"))

    def test_a_bare_block_without_the_wrapper_key_is_accepted(self) -> None:
        from gantry.policy.loader import parse_access_rules

        rules = parse_access_rules({"samples": {"maxRows": 5}})
        assert rules.samples.max_rows == 5

    def test_an_unreadable_policy_does_not_fall_back_to_the_defaults(self) -> None:
        """A permissive policy silently substituted for an unreadable strict
        one is the worst failure available here."""
        from gantry.policy.loader import PolicyError, load_access_rules

        with pytest.raises(PolicyError):
            load_access_rules("/nonexistent/policy.yaml")

    def test_an_unknown_key_is_refused_rather_than_ignored(self) -> None:
        """A typo'd rule that is silently dropped reads as a rule in force."""
        from gantry.policy.loader import PolicyError, parse_access_rules

        with pytest.raises(PolicyError):
            parse_access_rules({"agentAccess": {"sampels": {"maxRows": 5}}})

    def test_a_policy_that_is_not_a_mapping_is_refused(self) -> None:
        from gantry.policy.loader import PolicyError, parse_access_rules

        with pytest.raises(PolicyError, match="must be a mapping"):
            parse_access_rules(["rows: deny"])
