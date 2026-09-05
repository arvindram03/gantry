"""Range splitting for mismatch localisation."""

from __future__ import annotations

from gantry.core.dataset import DatasetManifest, PhysicalRef
from gantry.core.schema import DatasetSchema, FieldSchema
from gantry.verification.localize import KeyRange, Localization, MismatchLocalizer


def manifest() -> DatasetManifest:
    return DatasetManifest(
        name="public.orders",
        physical=PhysicalRef(adapter="postgres", reference="public.orders"),
        dataset_schema=DatasetSchema(
            keys=("order_id",), fields=(FieldSchema(name="order_id", type="bigint"),)
        ),
    )


def localizer() -> MismatchLocalizer:
    return MismatchLocalizer(source_engine=None, target_engine=None)  # type: ignore[arg-type]


def test_a_range_halves() -> None:
    halves = localizer()._split(manifest(), "order_id", KeyRange("0", "100"))
    assert halves == (KeyRange("0", "50"), KeyRange("50", "100"))


def test_halving_converges() -> None:
    """Repeated halving must terminate rather than spin on a one-wide range."""
    current = KeyRange("1", "10000000")
    steps = 0
    while (halves := localizer()._split(manifest(), "order_id", current)) is not None:
        current = halves[0]
        steps += 1
        assert steps < 64
    assert steps <= 24, "halving 10M should take about log2(10M) steps"


def test_an_unbounded_range_cannot_be_halved() -> None:
    """There is no midpoint of an open range; enumerate instead."""
    assert localizer()._split(manifest(), "order_id", KeyRange(None, "100")) is None
    assert localizer()._split(manifest(), "order_id", KeyRange("0", None)) is None


def test_a_non_numeric_range_cannot_be_halved() -> None:
    assert localizer()._split(manifest(), "order_id", KeyRange("alpha", "omega")) is None


def test_an_adjacent_range_cannot_be_halved() -> None:
    assert localizer()._split(manifest(), "order_id", KeyRange("5", "6")) is None


def test_ranges_render_readably() -> None:
    assert KeyRange("1", "100").describe() == "[1, 100)"
    assert KeyRange(None, None).describe() == "[-inf, +inf)"


# --- reporting -------------------------------------------------------------


def test_a_localization_knows_whether_it_found_anything() -> None:
    assert not Localization().located
    assert Localization(differing_keys=["7"]).located
    assert Localization(missing_keys=["7"]).located
    assert Localization(extra_keys=["7"]).located


def test_a_localization_describes_what_it_found() -> None:
    found = Localization(differing_keys=["7654321"], comparisons=27)
    assert found.describe() == "1 differing in 27 comparisons"


def test_an_unresolved_localization_says_so() -> None:
    unresolved = Localization(ranges=[KeyRange("1", "100")], comparisons=3)
    assert "unresolved ranges" in unresolved.describe()
