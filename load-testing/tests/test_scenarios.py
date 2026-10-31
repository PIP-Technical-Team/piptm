from __future__ import annotations

import json
import random
from pathlib import Path

import pytest

from loadtest.scenarios import CatalogError, choose_scenario, load_catalog

ROOT = Path(__file__).resolve().parents[1]


def test_catalog_covers_tiers_transports_and_chains() -> None:
    scenarios = load_catalog(ROOT / "scenarios.json")
    assert {item.tier for item in scenarios} == {1, 2, 3}
    assert {item.transport for item in scenarios} == {"GET", "POST"}
    assert {item.downstream for item in scenarios} >= {
        "description_metadata", "description_recompute", "download_csv", "download_rds"
    }
    assert all("by" not in item.payload() for item in scenarios)


def test_get_payload_preserves_repeated_array_parameters() -> None:
    scenario = load_catalog(ROOT / "scenarios.json")[0]
    payload = scenario.payload(("COL_2019_GEIH_INC_ALL",))
    assert payload["pip_id"] == ["COL_2019_GEIH_INC_ALL"]
    assert payload["measures"] == ["mean", "gini"]


def test_weighted_selection_is_seeded() -> None:
    scenarios = load_catalog(ROOT / "scenarios.json")
    first = [choose_scenario(scenarios, 1, random.Random(9)).scenario_id for _ in range(3)]
    second = [choose_scenario(scenarios, 1, random.Random(9)).scenario_id for _ in range(3)]
    assert first == second


@pytest.mark.parametrize(
    "mutation",
    [
        lambda item: item.update({"survey_count": 16}),
        lambda item: item.update({"poverty_line": None, "analysis_var": "pov_status"}),
        lambda item: item.update({"slots": {"column": "gender", "superColumn": "area"}}),
        lambda item: item.update({"slots": {"column": "gender", "row": "gender"}}),
        lambda item: item.update({"survey_pool": []}),
        lambda item: item.update({"path": "/session/surveys"}),
    ],
)
def test_invalid_catalog_entries_fail_startup(tmp_path: Path, mutation) -> None:
    document = json.loads((ROOT / "scenarios.json").read_text(encoding="utf-8"))
    item = document["scenarios"][0]
    mutation(item)
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(CatalogError):
        load_catalog(path)
