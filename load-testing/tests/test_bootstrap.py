from __future__ import annotations

from pathlib import Path

import pytest

from loadtest.bootstrap import bootstrap_catalog, compatible_live_scenarios
from loadtest.scenarios import CatalogError, load_catalog

ROOT = Path(__file__).resolve().parents[1]


class Response:
    status_code = 200
    headers = {"Content-Type": "application/json"}

    def __init__(self, data):
        self.data = data

    def json(self):
        return {"status": "success", "data": self.data, "errors": [], "warnings": [], "meta": {}}


class Client:
    def __init__(self, surveys):
        self.surveys = surveys

    def get(self, *_args, **_kwargs):
        return Response(self.surveys)


def test_live_bootstrap_intersects_required_dimensions() -> None:
    scenarios = load_catalog(ROOT / "scenarios.json")
    dimensions = ["gender", "area", "age_group", "educat4"]
    surveys = [
        {"pip_id": f"COL_{2000 + index:04d}_SURVEY_INC_ALL", "dimensions": dimensions}
        for index in range(15)
    ]
    live = compatible_live_scenarios(Client(surveys), scenarios, 1)
    assert {item.tier for item in live} == {1, 2, 3}
    assert all(item.survey_pool[0].startswith("COL_") for item in live)


def test_live_failure_can_use_explicit_curated_fallback() -> None:
    scenarios = load_catalog(ROOT / "scenarios.json")
    assert bootstrap_catalog(Client([]), scenarios, "live-with-curated-fallback", 1) == scenarios
    with pytest.raises(CatalogError):
        bootstrap_catalog(Client([]), scenarios, "live", 1)
