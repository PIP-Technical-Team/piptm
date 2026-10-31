"""Semantic response validation and Table Maker request chains."""

from __future__ import annotations

import json
from typing import Any


class SemanticError(ValueError):
    """Raised when an HTTP response violates the API's success contract."""


def validate_envelope(response: Any, *, require_data: bool = True) -> dict[str, Any]:
    if response.status_code != 200:
        raise SemanticError(f"unexpected HTTP status {response.status_code}")
    content_type = response.headers.get("Content-Type", "").lower()
    if "json" not in content_type:
        raise SemanticError(f"expected JSON response, received {content_type or 'unknown'}")
    try:
        payload = response.json()
    except (ValueError, json.JSONDecodeError) as exc:
        raise SemanticError("response is not valid JSON") from exc
    if not isinstance(payload, dict) or payload.get("status") != "success":
        raise SemanticError("response status is not success")
    if payload.get("errors") not in (None, []):
        raise SemanticError("response contains semantic errors")
    if require_data and payload.get("data") in (None, [], {}):
        raise SemanticError("response data is empty")
    return payload


def extract_approach_c(payload: dict[str, Any], expected_surveys: int) -> list[dict[str, Any]]:
    data = payload.get("data")
    if not isinstance(data, list) or len(data) != expected_surveys:
        raise SemanticError("Approach C survey count does not match the request")
    for survey in data:
        if not isinstance(survey, dict) or not survey.get("layout") or not survey.get("cells"):
            raise SemanticError("download chain requires non-empty Approach C layout and cells")
    return data


def validate_table(
    payload: dict[str, Any],
    expected_surveys: tuple[str, ...],
    expected_measures: tuple[str, ...],
    metadata_mode: bool,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Validate the appropriate `/table` response mode and requested identifiers."""
    if metadata_mode:
        extract_description_metadata(payload)
        rows = payload.get("data")
        if isinstance(rows, dict):
            rows = [rows]
        if not isinstance(rows, list) or not rows:
            raise SemanticError("metadata table mode requires non-empty flat rows")
        required = {"pip_id", "measure", "value"}
        if any(not isinstance(row, dict) or not required.issubset(row) for row in rows):
            raise SemanticError("metadata table rows are missing required fields")
        returned_surveys = {str(row["pip_id"]) for row in rows}
        returned_measures = {str(row["measure"]) for row in rows}
    else:
        rows = extract_approach_c(payload, len(expected_surveys))
        required = {
            "pip_id", "analysis_var", "measures", "layout", "cells",
        }
        if any(not required.issubset(survey) for survey in rows):
            raise SemanticError("Approach C survey objects are missing required fields")
        returned_surveys = {str(survey["pip_id"]) for survey in rows}
        returned_measures = {
            str(measure) for survey in rows for measure in survey.get("measures", [])
        }
    if returned_surveys != set(expected_surveys):
        raise SemanticError("table response survey identifiers do not match the request")
    if not set(expected_measures).issubset(returned_measures):
        raise SemanticError("table response measures do not match the request")
    return rows


def extract_description_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    metadata = payload.get("meta", {}).get("description_metadata")
    if not isinstance(metadata, dict) or not metadata.get("params"):
        raise SemanticError("metadata chain requires meta.description_metadata with params")
    return metadata


def validate_description(response: Any) -> None:
    if response.status_code != 200:
        raise SemanticError(f"description returned HTTP {response.status_code}")
    content_type = response.headers.get("Content-Type", "").lower()
    if not any(value in content_type for value in ("text/html", "text/plain")):
        raise SemanticError("description returned an unexpected content type")
    if len(response.text.strip()) < 20:
        raise SemanticError("description response is empty")


def validate_download(response: Any, file_format: str, max_bytes: int = 30 * 1024 * 1024) -> None:
    if response.status_code != 200 or not response.content:
        raise SemanticError(f"download returned HTTP {response.status_code} or empty bytes")
    if len(response.content) > max_bytes:
        raise SemanticError("download response exceeds the retained-size guard")
    content_type = response.headers.get("Content-Type", "").lower()
    disposition = response.headers.get("Content-Disposition", "").lower()
    expected_type = "text/csv" if file_format == "csv" else "application/octet-stream"
    if expected_type not in content_type or f"table.{file_format}" not in disposition:
        raise SemanticError("download headers do not match the requested format")


def mark_response(response: Any, validator: Any, *args: Any) -> Any | None:
    """Run a semantic validator and report through Locust catch_response."""
    try:
        result = validator(response, *args)
    except SemanticError as exc:
        response.failure(str(exc))
        return None
    response.success()
    return result
