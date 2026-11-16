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
        return {
            "status": "success",
            "data": self.data,
            "errors": [],
            "warnings": [],
            "meta": {},
        }


class Client:
    def __init__(self, surveys, covariates=None):
        self.surveys = surveys
        self.covariates = covariates if covariates is not None else [
            {"varname": "gender"},
            {"varname": "area"},
            {"varname": "age_group"},
            {"varname": "educat4"},
            {"varname": "electricity"},
        ]
        self.paths: list[str] = []

    def get(self, path, *_args, **_kwargs):
        self.paths.append(path)
        if path == "/surveys-ui":
            return Response(self.surveys)
        if path in {"/covariates", "/categories"}:
            return Response(self.covariates)
        raise AssertionError(f"unexpected bootstrap path: {path}")


def test_live_bootstrap_uses_ui_endpoints_without_surveys_manifest() -> None:
    scenarios = load_catalog(ROOT / "scenarios.json")
    surveys = [
        {
            "pip_id": f"COL_{2000 + index:04d}_SURVEY_INC_ALL",
            "country_code": "COL",
            "year": 2000 + index,
        }
        for index in range(15)
    ]
    client = Client(surveys)
    live = compatible_live_scenarios(client, scenarios, 1)
    assert {item.tier for item in live} == {1, 2, 3}
    assert all(
        item.survey_pool[0].startswith("COL_")
        for item in live
        if item.expected_status == 200
    )
    assert "/surveys" not in client.paths
    assert "/surveys-ui" in client.paths
    assert any(item.expected_status == 422 for item in live)


def test_live_failure_can_use_explicit_curated_fallback() -> None:
    scenarios = load_catalog(ROOT / "scenarios.json")
    assert bootstrap_catalog(Client([]), scenarios, "live-with-curated-fallback", 1) == scenarios
    with pytest.raises(CatalogError):
        bootstrap_catalog(Client([]), scenarios, "live", 1)
