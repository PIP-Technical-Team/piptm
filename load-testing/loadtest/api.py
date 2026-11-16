"""Semantic response checks for Table Maker API endpoints."""

from __future__ import annotations

from typing import Any


class SemanticError(ValueError):
    """Raised when an HTTP response is syntactically valid but semantically wrong."""


def _scalar(value: Any) -> Any:
    if isinstance(value, list) and len(value) == 1:
        return value[0]
    return value


def _json_object(response: Any) -> dict[str, Any]:
    content_type = response.headers.get("Content-Type", "")
    if "application/json" not in content_type.lower():
        raise SemanticError(f"expected JSON response, got Content-Type={content_type!r}")
    try:
        payload = response.json()
    except Exception as exc:  # pragma: no cover - requests exception class varies
        raise SemanticError(f"invalid JSON response: {exc}") from exc
    if not isinstance(payload, dict):
        raise SemanticError("expected JSON object envelope")
    return payload


def validate_envelope(response: Any) -> dict[str, Any]:
    """Validate a standard Table Maker HTTP 200 JSON envelope.

    Args:
        response: HTTP response object from Locust or requests.

    Returns:
        Parsed top-level JSON envelope dictionary.

    Raises:
        SemanticError: If status code != 200, status != 'success', errors exist, or data is empty.
    """
    if response.status_code != 200:
        raise SemanticError(f"expected HTTP 200, got {response.status_code}")
    payload = _json_object(response)
    if _scalar(payload.get("status")) != "success":
        raise SemanticError(f"envelope status was {payload.get('status')!r}")
    errors = payload.get("errors")
    if errors not in (None, []):
        raise SemanticError(f"envelope contained errors: {errors}")
    data = payload.get("data")
    if data in (None, [], {}):
        raise SemanticError("envelope data is empty")
    return payload


def validate_expected_422(response: Any) -> dict[str, Any]:
    """Validate an expected HTTP 422 missing-dimension error envelope.

    Args:
        response: HTTP response object expected to carry status 422.

    Returns:
        Parsed JSON error envelope dictionary.

    Raises:
        SemanticError: If status != 422, status != 'error', or errors list is empty.
    """
    if response.status_code != 422:
        raise SemanticError(
            f"expected HTTP 422 for missing-dimension check, got {response.status_code}"
        )
    payload = _json_object(response)
    if _scalar(payload.get("status")) != "error":
        raise SemanticError(f"expected error status in 422 envelope, got {payload.get('status')!r}")
    errors = payload.get("errors")
    if not isinstance(errors, list) or not errors:
        raise SemanticError("expected non-empty errors array in 422 envelope")
    return payload


def extract_approach_c(
    payload: dict[str, Any], expected_surveys: tuple[str, ...] | int
) -> list[dict[str, Any]]:
    """Extract and validate the Approach C survey array from a `/table` envelope.

    Args:
        payload: Parsed top-level JSON envelope from `/table`.
        expected_surveys: Tuple of requested ``pip_id`` strings or max expected count.

    Returns:
        List of per-survey Approach C table dictionaries.

    Raises:
        SemanticError: If ``data`` is empty, exceeds requested surveys, or lacks layout/cells.
    """
    data = payload.get("data")
    if not isinstance(data, list) or not data:
        raise SemanticError("table response data must be a non-empty list")
    max_surveys = (
        len(expected_surveys) if isinstance(expected_surveys, tuple) else int(expected_surveys)
    )
    if not 1 <= len(data) <= max_surveys:
        raise SemanticError(f"expected 1..{max_surveys} survey objects, got {len(data)}")
    for item in data:
        if not isinstance(item, dict):
            raise SemanticError("survey table item must be an object")
        if (
            not isinstance(item.get("layout"), dict)
            or not isinstance(item.get("cells"), list)
            or not item["cells"]
        ):
            raise SemanticError("Approach C table item requires layout dict and non-empty cells")
    if isinstance(expected_surveys, tuple):
        returned = [str(_scalar(item.get("pip_id"))) for item in data]
        if not set(returned).issubset(set(expected_surveys)):
            raise SemanticError(
                f"returned surveys {returned} not subset of {list(expected_surveys)}"
            )
    return data


def validate_table(
    payload: dict[str, Any],
    expected_surveys: tuple[str, ...],
    expected_measures: tuple[str, ...],
) -> list[dict[str, Any]]:
    """Validate `/table` Approach C output and return the extracted survey list.

    Args:
        payload: Parsed top-level JSON envelope from `/table`.
        expected_surveys: Tuple of requested ``pip_id`` strings.
        expected_measures: Tuple of requested measure identifiers.

    Returns:
        Validated Approach C survey list from ``payload['data']``.

    Raises:
        SemanticError: If surveys, layout, cells, or requested measures are invalid.
    """
    data = extract_approach_c(payload, expected_surveys)
    expected_measure_set = set(expected_measures)
    for item in data:
        measures = set(item.get("measures") or [])
        if not expected_measure_set.issubset(measures):
            raise SemanticError("table item is missing requested measures")
        for cell in item["cells"]:
            if not isinstance(cell, dict) or not isinstance(cell.get("measures"), dict):
                raise SemanticError("Approach C cell is missing measures object")
            if not expected_measure_set.issubset(cell["measures"].keys()):
                raise SemanticError("Approach C cell is missing requested measure keys")
    return data


def validate_cell_definition(response: Any) -> dict[str, Any]:
    """Validate a `GET /cell-definition` response envelope.

    Args:
        response: HTTP response from `GET /cell-definition`.

    Returns:
        Parsed JSON envelope dictionary.

    Raises:
        SemanticError: If any of the four explanation keys are missing.
    """
    payload = validate_envelope(response)
    data = payload.get("data")
    required_keys = {"who_is_in_table", "what_a_cell_is", "what_is_shown", "notes"}
    if not isinstance(data, dict) or not required_keys.issubset(data.keys()):
        raise SemanticError("cell-definition response is missing required explanation sections")
    return payload


def validate_metadata(response: Any, fmt: str = "html") -> None:
    """Validate a `GET|POST /metadata` attachment response.

    Args:
        response: HTTP response from `/metadata`.
        fmt: Expected metadata format (``'html'`` or ``'markdown'``).

    Raises:
        SemanticError: If status code, headers, or body content do not match ``fmt``.
    """
    if response.status_code != 200:
        raise SemanticError(f"metadata returned HTTP {response.status_code}")
    body = response.text if hasattr(response, "text") else ""
    if not body.strip():
        raise SemanticError("metadata response body is empty")
    content_type = response.headers.get("Content-Type", "").lower()
    disposition = response.headers.get("Content-Disposition", "")
    if fmt == "html":
        if "text/html" not in content_type or ("<h" not in body and "<H" not in body):
            raise SemanticError("HTML metadata response lacked HTML content")
        if disposition and "table-metadata.html" not in disposition:
            raise SemanticError("HTML metadata response had unexpected Content-Disposition")
    elif fmt == "markdown":
        if "table-metadata.md" not in disposition or "#" not in body:
            raise SemanticError("Markdown metadata response lacked Markdown heading or filename")
    else:
        raise SemanticError(f"unsupported metadata format {fmt!r}")


def validate_download(response: Any, fmt: str) -> None:
    """Validate a `POST /table/download` CSV or RDS binary attachment response.

    Args:
        response: HTTP response from `/table/download`.
        fmt: Export format (``'csv'`` or ``'rds'``).

    Raises:
        SemanticError: If status, content-type, filename header, or body is invalid.
    """
    if response.status_code != 200:
        raise SemanticError(f"download returned HTTP {response.status_code}")
    if not getattr(response, "content", b""):
        raise SemanticError("download body is empty")
    content_type = response.headers.get("Content-Type", "").lower()
    disposition = response.headers.get("Content-Disposition", "")
    expected_type = "text/csv" if fmt == "csv" else "application/octet-stream"
    if expected_type not in content_type:
        raise SemanticError(f"expected {expected_type}, got {content_type!r}")
    if f'filename="table.{fmt}"' not in disposition:
        raise SemanticError(f"missing table.{fmt} Content-Disposition")
