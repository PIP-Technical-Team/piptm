from __future__ import annotations

import pytest

from loadtest.api import (
    SemanticError,
    extract_approach_c,
    extract_description_metadata,
    validate_description,
    validate_download,
    validate_envelope,
    validate_table,
)


class Response:
    def __init__(self, payload=None, status=200, content_type="application/json", content=b"x"):
        self._payload = payload
        self.status_code = status
        self.headers = {"Content-Type": content_type, "Content-Disposition": ""}
        self.content = content
        self.text = content.decode(errors="ignore")

    def json(self):
        return self._payload


def envelope(data, errors=None):
    return {"status": "success", "data": data, "warnings": None, "errors": errors or [], "meta": {}}


def test_success_envelope_accepts_null_warnings() -> None:
    assert validate_envelope(Response(envelope([{"x": 1}]))) ["data"]


@pytest.mark.parametrize("payload", [
    {"status": "error", "data": None, "errors": ["bad"]},
    envelope([], []),
    envelope([1], ["semantic failure"]),
])
def test_semantic_failures_are_rejected(payload) -> None:
    with pytest.raises(SemanticError):
        validate_envelope(Response(payload))


def test_separate_chain_shapes() -> None:
    approach_c = [{
        "pip_id": "COL_2019_GEIH_INC_ALL",
        "analysis_var": "welfare",
        "measures": ["mean"],
        "layout": {"columns": {}},
        "cells": [{"measures": {"mean": "4.50"}}],
    }]
    assert extract_approach_c(envelope(approach_c), 1) == approach_c
    metadata_payload = envelope([{"measure": "mean", "value": "4.50"}])
    metadata_payload["meta"]["description_metadata"] = {"params": {"pip_id": ["X"]}}
    assert extract_description_metadata(metadata_payload)["params"]
    with pytest.raises(SemanticError):
        extract_approach_c(metadata_payload, 1)


def test_table_validator_rejects_wrong_surveys_and_garbage_rows() -> None:
    approach_c = envelope([{
        "pip_id": "COL_2019_GEIH_INC_ALL",
        "analysis_var": "welfare",
        "measures": ["mean"],
        "layout": {"columns": None},
        "cells": [{"measures": {"mean": "4.50"}}],
    }])
    validate_table(approach_c, ("COL_2019_GEIH_INC_ALL",), ("mean",), False)
    with pytest.raises(SemanticError):
        validate_table(approach_c, ("BRA_2021_PNAD_INC_ALL",), ("mean",), False)
    metadata = envelope([{"garbage": 1}])
    metadata["meta"]["description_metadata"] = {"params": {"pip_id": ["X"]}}
    with pytest.raises(SemanticError):
        validate_table(metadata, ("X",), ("mean",), True)


def test_description_and_download_headers() -> None:
    description = Response(
        content_type="text/html; charset=utf-8", content=b"<h2>Table Overview</h2>"
    )
    validate_description(description)
    download = Response(content_type="text/csv; charset=utf-8", content=b"a,b\n1,2")
    download.headers["Content-Disposition"] = 'attachment; filename="table.csv"'
    validate_download(download, "csv")
