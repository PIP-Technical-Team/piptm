from __future__ import annotations

import json
import random
from pathlib import Path

import pytest

from loadtest.scenarios import CatalogError, choose_scenario, load_catalog

ROOT = Path(__file__).resolve().parents[1]


def test_catalog_covers_tiers_transports_latency_classes_and_chains() -> None:
    scenarios = load_catalog(ROOT / "scenarios.json")
    assert len(scenarios) >= 12
    assert {item.tier for item in scenarios} == {1, 2, 3}
    assert {item.transport for item in scenarios} == {"GET", "POST"}
    assert {item.downstream for item in scenarios} >= {
        "none",
        "metadata_html",
        "metadata_markdown",
        "download_csv",
        "download_rds",
    }
    assert {item.latency_class for item in scenarios} == {"class-b", "class-c"}
    assert {item.expected_status for item in scenarios} == {200, 422}
    assert all(item.analysis_var != "gender" for item in scenarios)
    assert any(set(item.slots.keys()) == {"row"} for item in scenarios)
    assert all("by" not in item.payload() for item in scenarios)
    assert all("include_metadata" not in item.payload() for item in scenarios)


def test_row_only_and_independent_parent_child_slots_are_valid(tmp_path: Path) -> None:
    document = json.loads((ROOT / "scenarios.json").read_text(encoding="utf-8"))
    item = document["scenarios"][0]
    item["slots"] = {"row": "area"}
    item["required_covariates"] = ["area"]
    path = tmp_path / "row_only.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    loaded = load_catalog(path)
    assert loaded[0].slots == {"row": "area"}

    item["slots"] = {"column": "gender", "superColumn": "area"}
    item["required_covariates"] = ["gender", "area"]
    path.write_text(json.dumps(document), encoding="utf-8")
    loaded2 = load_catalog(path)
    assert loaded2[0].slots == {"column": "gender", "superColumn": "area"}


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
        lambda item: item.update(
            {"poverty_line": None, "analysis_var": "pov_status", "measures": ["headcount"]}
        ),
        lambda item: item.update({"slots": {"column": "pov_status"}, "poverty_line": None}),
        lambda item: item.update(
            {
                "slots": {"row": "gender", "superColumn": "area"},
                "required_covariates": ["gender", "area"],
            }
        ),
        lambda item: item.update(
            {
                "slots": {"column": "gender", "superRow": "area"},
                "required_covariates": ["gender", "area"],
            }
        ),
        lambda item: item.update(
            {"slots": {"column": "gender", "row": "gender"}, "required_covariates": ["gender"]}
        ),
        lambda item: item.update(
            {
                "analysis_var": "electricity",
                "measures": ["pop_share"],
                "slots": {"column": "electricity"},
                "required_covariates": ["electricity"],
            }
        ),
        lambda item: item.update({"analysis_var": "gender", "measures": ["pop_share"]}),
        lambda item: item.update({"analysis_var": "age", "measures": ["mean", "gini"]}),
        lambda item: item.update(
            {
                "analysis_var": "pov_status",
                "poverty_line": 2.15,
                "measures": ["headcount", "gini"],
            }
        ),
        lambda item: item.update({"analysis_var": "electricity", "measures": ["mean"]}),
        lambda item: item.update({"survey_pool": []}),
        lambda item: item.update({"path": "/session/surveys"}),
        lambda item: item.update({"downstream": "description_metadata"}),
        lambda item: item.update({"latency_class": "class-c"}),
        lambda item: item.update({"expected_status": 422, "downstream": "metadata_html"}),
        lambda item: item.update({"pop_share_threshold": 1.0}),
        lambda item: item.update(
            {"filter_base": {"age_group": ["25-64"]}, "required_covariates": ["age_group"]}
        ),
        lambda item: item.update({"filter_base": {"gender": [1]}, "required_covariates": []}),
        lambda item: item.update({"ppp": 0}),
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
