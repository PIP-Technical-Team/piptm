"""Live discovery compatibility checks with explicit curated fallback."""

from __future__ import annotations

from typing import Any

try:
    from .api import SemanticError, validate_envelope
    from .scenarios import CatalogError, Scenario
except ImportError:  # Azure flattens uploaded helper files.
    from api import SemanticError, validate_envelope
    from scenarios import CatalogError, Scenario


DISCOVERY_ENDPOINTS = (
    "/health", "/releases", "/surveys-ui", "/categories", "/covariates",
    "/analysis-variables", "/statistics",
)


def compatible_live_scenarios(
    client: Any,
    scenarios: tuple[Scenario, ...],
    timeout: float,
) -> tuple[Scenario, ...]:
    surveys_response = client.get(
        "/surveys-ui", name="discovery:/surveys-ui", timeout=timeout
    )
    try:
        surveys = validate_envelope(surveys_response)["data"]
    except SemanticError as exc:
        raise CatalogError(f"live discovery failed: {exc}") from exc
    if not isinstance(surveys, list):
        raise CatalogError("live /surveys-ui data is not an array")
    dimensions = {
        item.get("pip_id"): set(item.get("dimensions") or [])
        for item in surveys
        if isinstance(item, dict) and item.get("pip_id")
    }
    resolved: list[Scenario] = []
    for scenario in scenarios:
        candidates = tuple(
            pip_id for pip_id, available in dimensions.items()
            if set(scenario.required_covariates).issubset(available)
        )
        if len(candidates) >= scenario.survey_count:
            resolved.append(scenario.with_pool(candidates))
    if {item.tier for item in resolved} != {1, 2, 3}:
        raise CatalogError("live discovery cannot construct compatible scenarios for every tier")
    return tuple(resolved)


def bootstrap_catalog(
    client: Any,
    scenarios: tuple[Scenario, ...],
    mode: str,
    timeout: float,
) -> tuple[Scenario, ...]:
    if mode == "curated":
        return scenarios
    try:
        return compatible_live_scenarios(client, scenarios, timeout)
    except (CatalogError, SemanticError, OSError):
        if mode == "live-with-curated-fallback":
            return scenarios
        raise
