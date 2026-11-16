from __future__ import annotations

import pytest

from loadtest.api import (
    SemanticError,
    extract_approach_c,
    validate_cell_definition,
    validate_download,
    validate_envelope,
    validate_expected_422,
    validate_metadata,
    validate_table,
)


class Response:
    def __init__(
        self,
        payload=None,
        status=200,
        content_type="application/json",
        content=b"x",
        disposition="",
    ):
        self._payload = payload
        self.status_code = status
        self.headers = {
            "Content-Type": content_type,
            "Content-Disposition": disposition,
        }
        self.content = content
        self.text = content.decode(errors="ignore")

    def json(self):
        return self._payload


def envelope(data, errors=None, warnings=None):
    return {
        "status": "success",
        "data": data,
        "warnings": warnings,
        "errors": errors or [],
        "meta": {},
    }


def test_success_envelope_accepts_null_warnings() -> None:
    assert validate_envelope(Response(envelope([{"x": 1}])))["data"]


@pytest.mark.parametrize(
    "payload",
    [
        {"status": "error", "data": None, "errors": ["bad"]},
        envelope([], []),
        envelope([1], ["semantic failure"]),
    ],
)
def test_semantic_failures_are_rejected(payload) -> None:
    with pytest.raises(SemanticError):
        validate_envelope(Response(payload))


def test_table_validator_accepts_partial_survey_exclusion_and_rejects_unrequested() -> None:
    single_returned = envelope(
        [
            {
                "pip_id": "COL_2019_GEIH_INC_ALL",
                "analysis_var": "welfare",
                "measures": ["mean"],
                "layout": {"columns": None},
                "cells": [{"measures": {"mean": "4.50"}}],
            }
        ],
        warnings=["BRA_2021_PNAD_INC_ALL excluded: missing dimension"],
    )
    validated = validate_table(
        single_returned,
        ("COL_2019_GEIH_INC_ALL", "BRA_2021_PNAD_INC_ALL"),
        ("mean",),
    )
    assert validated == single_returned["data"]
    assert (
        extract_approach_c(
            single_returned,
            ("COL_2019_GEIH_INC_ALL", "BRA_2021_PNAD_INC_ALL"),
        )
        == single_returned["data"]
    )
    with pytest.raises(SemanticError):
        validate_table(single_returned, ("PER_2019_ENAHO_INC_ALL",), ("mean",))
    with pytest.raises(SemanticError):
        validate_table(
            envelope(
                [
                    {
                        "pip_id": "COL_2019_GEIH_INC_ALL",
                        "analysis_var": "welfare",
                        "measures": ["mean"],
                        "layout": {"columns": None},
                        "cells": [{"measures": {}}],
                    }
                ]
            ),
            ("COL_2019_GEIH_INC_ALL",),
            ("mean",),
        )


def test_expected_422_validator_accepts_clean_422_and_rejects_200_or_500() -> None:
    err_422 = Response(
        payload={
            "status": "error",
            "data": None,
            "warnings": [],
            "errors": ["None of the selected surveys contain 'industrycat10_2_year'."],
            "meta": {},
        },
        status=422,
    )
    validate_expected_422(err_422)

    with pytest.raises(SemanticError):
        validate_expected_422(Response(envelope([{"x": 1}]), status=200))
    with pytest.raises(SemanticError):
        validate_expected_422(Response({"status": "error", "errors": ["crash"]}, status=500))


def test_cell_definition_metadata_and_download_validators() -> None:
    cell_def = Response(
        envelope(
            {
                "schema_version": "2.0",
                "who_is_in_table": "All individuals",
                "what_a_cell_is": "Each cell...",
                "what_is_shown": ["Mean"],
                "notes": [],
            }
        )
    )
    validate_cell_definition(cell_def)
    with pytest.raises(SemanticError):
        validate_cell_definition(Response(envelope({"who_is_in_table": "only one key"})))

    meta_html = Response(
        content_type="text/html; charset=utf-8",
        content=b"<!DOCTYPE html><html><body><h1>Metadata</h1></body></html>",
        disposition='attachment; filename="table-metadata.html"',
    )
    validate_metadata(meta_html, "html")
    with pytest.raises(SemanticError):
        validate_metadata(
            Response(
                content_type="text/html; charset=utf-8",
                content=b"<h1>Metadata</h1>",
                disposition='attachment; filename="wrong.html"',
            ),
            "html",
        )

    meta_md = Response(
        content_type="text/plain; charset=utf-8",
        content=b"# Metadata\n\n## Table Overview\n",
        disposition='attachment; filename="table-metadata.md"',
    )
    validate_metadata(meta_md, "markdown")
    with pytest.raises(SemanticError):
        validate_metadata(
            Response(
                content_type="text/plain; charset=utf-8",
                content=b"no heading here",
                disposition='attachment; filename="table-metadata.md"',
            ),
            "markdown",
        )

    download = Response(
        content_type="text/csv; charset=utf-8",
        content=b"a,b\n1,2",
        disposition='attachment; filename="table.csv"',
    )
    validate_download(download, "csv")

    download_rds = Response(
        content_type="application/octet-stream",
        content=b"RDS-BYTES",
        disposition='attachment; filename="table.rds"',
    )
    validate_download(download_rds, "rds")
    with pytest.raises(SemanticError):
        validate_download(
            Response(
                content_type="application/octet-stream",
                content=b"",
                disposition='attachment; filename="table.rds"',
            ),
            "rds",
        )
