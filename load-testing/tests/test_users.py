from __future__ import annotations

import importlib.util
from pathlib import Path

from loadtest.scenarios import load_catalog
from loadtest.users import get_query

ROOT = Path(__file__).resolve().parents[1]


def test_get_query_repeats_arrays_and_encodes_filter() -> None:
    query = get_query(
        {"pip_id": ["A", "B"], "measures": ["mean", "gini"], "filter_base": {"gender": [0]}}
    )
    assert query.count(("pip_id", "A")) == 1
    assert ("measures", "gini") in query
    assert ("filter_base", '{"gender":[0]}') in query


def test_request_names_are_low_cardinality() -> None:
    scenarios = load_catalog(ROOT / "scenarios.json")
    names = {f"table:tier{item.tier}:{item.transport.lower()}" for item in scenarios}
    assert all("COL_" not in name and "?" not in name for name in names)


def test_locustfile_import_performs_no_network(monkeypatch) -> None:
    def forbidden(*_args, **_kwargs):
        raise AssertionError("network activity during import")

    monkeypatch.setattr("requests.sessions.Session.request", forbidden)
    spec = importlib.util.spec_from_file_location("offline_locustfile", ROOT / "locustfile.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    assert module.TableMakerUser
