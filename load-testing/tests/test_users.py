from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from loadtest.scenarios import CatalogError, load_catalog
from loadtest.users import (
    AGREED_ENDPOINTS,
    TableMakerUser,
    get_query,
    table_request_name,
)

ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_ENDPOINTS = {"/description", "/session/surveys", "/surveys", "/lookup", "/dimensions"}


class _FakeContextResponse:
    def __init__(
        self,
        status_code: int = 200,
        payload=None,
        content_type: str = "application/json",
        content: bytes = b"x",
        disposition: str = "",
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = {
            "Content-Type": content_type,
            "Content-Disposition": disposition,
        }
        self.content = content
        self.text = content.decode(errors="ignore")
        self.marked_success = False
        self.failure_reason: str | None = None

    def json(self):
        return self._payload

    def success(self) -> None:
        self.marked_success = True

    def failure(self, reason: str) -> None:
        self.failure_reason = reason

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def get(self, path: str, name: str = "", **_kwargs):
        self.calls.append(("GET", path, name))
        if path == "/cell-definition":
            return _FakeContextResponse(
                payload={
                    "status": "success",
                    "data": {
                        "who_is_in_table": "All",
                        "what_a_cell_is": "Cell",
                        "what_is_shown": ["Mean"],
                        "notes": [],
                    },
                    "errors": [],
                    "warnings": [],
                    "meta": {},
                }
            )
        if path == "/table" and name.endswith(":expected422"):
            return _FakeContextResponse(
                status_code=422,
                payload={
                    "status": "error",
                    "data": None,
                    "errors": ["Missing dimension"],
                    "warnings": [],
                    "meta": {},
                },
            )
        if path == "/metadata":
            return _FakeContextResponse(
                content_type="text/html; charset=utf-8",
                content=b"<h1>Metadata</h1>",
                disposition='attachment; filename="table-metadata.html"',
            )
        return _FakeContextResponse(
            payload={
                "status": "success",
                "data": [
                    {
                        "pip_id": "COL_2019_GEIH_INC_ALL",
                        "measures": ["mean", "gini", "headcount", "poverty_gap"],
                        "layout": {"columns": None},
                        "cells": [
                            {
                                "measures": {
                                    "mean": "1.0",
                                    "gini": "0.4",
                                    "headcount": "0.1",
                                    "poverty_gap": "0.05",
                                }
                            }
                        ],
                    }
                ],
                "errors": [],
                "warnings": [],
                "meta": {},
            }
        )

    def post(self, path: str, name: str = "", **_kwargs):
        self.calls.append(("POST", path, name))
        if path == "/table/download":
            fmt = "csv" if name.endswith(":csv") else "rds"
            ctype = "text/csv; charset=utf-8" if fmt == "csv" else "application/octet-stream"
            return _FakeContextResponse(
                content_type=ctype,
                content=b"a,b\n1,2",
                disposition=f'attachment; filename="table.{fmt}"',
            )
        if path == "/metadata":
            return _FakeContextResponse(
                content_type="text/plain; charset=utf-8",
                content=b"# Metadata\n",
                disposition='attachment; filename="table-metadata.md"',
            )
        return _FakeContextResponse(
            payload={
                "status": "success",
                "data": [
                    {
                        "pip_id": "COL_2019_GEIH_INC_ALL",
                        "measures": ["mean", "median", "gini"],
                        "layout": {"columns": {}},
                        "cells": [{"measures": {"mean": "1.0", "median": "0.8", "gini": "0.4"}}],
                    }
                ],
                "errors": [],
                "warnings": [],
                "meta": {},
            }
        )


def test_get_query_repeats_arrays_and_encodes_filter() -> None:
    query = get_query(
        {"pip_id": ["A", "B"], "measures": ["mean", "gini"], "filter_base": {"gender": [0]}}
    )
    assert query.count(("pip_id", "A")) == 1
    assert ("measures", "gini") in query
    assert ("filter_base", '{"gender":[0]}') in query


def test_agreed_13_endpoints_and_no_forbidden_endpoints() -> None:
    expected = {
        "/health",
        "/releases",
        "/regions",
        "/countries",
        "/surveys-ui",
        "/categories",
        "/analysis-variables",
        "/statistics",
        "/covariates",
        "/cell-definition",
        "/table",
        "/metadata",
        "/table/download",
    }
    assert expected == AGREED_ENDPOINTS
    assert FORBIDDEN_ENDPOINTS.isdisjoint(AGREED_ENDPOINTS)
    user_src = (ROOT / "loadtest" / "users.py").read_text(encoding="utf-8")
    for endpoint in AGREED_ENDPOINTS:
        assert endpoint in user_src
    assert "include_metadata" not in user_src
    assert '"/description"' not in user_src
    assert '"/session' not in user_src


def test_request_names_are_low_cardinality_and_include_latency_class() -> None:
    scenarios = load_catalog(ROOT / "scenarios.json")
    names = {table_request_name(item) for item in scenarios}
    assert all("COL_" not in name and "?" not in name for name in names)
    assert any(":class-b:" in name for name in names)
    assert any(":class-c:" in name for name in names)


def test_user_executes_preview_table_metadata_download_and_expected_422() -> None:
    scenarios = {item.scenario_id: item for item in load_catalog(ROOT / "scenarios.json")}
    user = TableMakerUser.__new__(TableMakerUser)
    user.client = _FakeClient()

    # 1. Mixed 200 scenario calls /cell-definition -> /table -> /metadata
    s_meta = scenarios["tier1-poverty-column-metadata-html"]
    user._execute_scenario(s_meta, ("COL_2019_GEIH_INC_ALL",), include_preview=True)
    assert [c[1] for c in user.client.calls] == ["/cell-definition", "/table", "/metadata"]

    # 2. Download CSV scenario calls /table -> /table/download
    user.client.calls.clear()
    s_dl = scenarios["tier1-download-csv"]
    user._execute_scenario(s_dl, ("COL_2019_GEIH_INC_ALL",), include_preview=False)
    assert [c[1] for c in user.client.calls] == ["/table", "/table/download"]

    # 3. Expected-422 scenario calls /table and stops without downstream
    user.client.calls.clear()
    s_422 = scenarios["tier1-expected-422-missing-dim"]
    user._execute_scenario(s_422, ("COL_2019_GEIH_INC_ALL",), include_preview=False)
    assert [c[1] for c in user.client.calls] == ["/table"]


def test_stampede_selection_validates_pinned_scenario_id() -> None:
    scenarios = load_catalog(ROOT / "scenarios.json")
    user = TableMakerUser.__new__(TableMakerUser)
    selected = user._select_stampede_scenario(
        scenarios,
        SimpleNamespace(stampede_scenario_id="tier2-download-rds", stampede_tier=3),
    )
    assert selected.scenario_id == "tier2-download-rds"

    with pytest.raises(CatalogError):
        user._select_stampede_scenario(
            scenarios,
            SimpleNamespace(stampede_scenario_id="nonexistent-scenario", stampede_tier=3),
        )
    with pytest.raises(CatalogError):
        user._select_stampede_scenario(
            scenarios,
            SimpleNamespace(
                stampede_scenario_id="tier1-expected-422-missing-dim",
                stampede_tier=1,
            ),
        )


def test_locustfile_import_performs_no_network(monkeypatch) -> None:
    def forbidden(*_args, **_kwargs):
        raise AssertionError("network activity during import")

    monkeypatch.setattr("requests.sessions.Session.request", forbidden)
    spec = importlib.util.spec_from_file_location("offline_locustfile", ROOT / "locustfile.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    assert module.TableMakerUser
