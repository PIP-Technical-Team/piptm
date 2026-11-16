"""Live catalog compatibility checks using Page 1 and Page 2 UI endpoints."""

from __future__ import annotations

import logging
from typing import Any

try:
    from .api import SemanticError, validate_envelope
    from .scenarios import CatalogError, Scenario
except ImportError:
    from api import SemanticError, validate_envelope  # type: ignore[no-redef]
    from scenarios import CatalogError, Scenario  # type: ignore[no-redef]

LOGGER = logging.getLogger(__name__)


def compatible_live_scenarios(
    client: Any,
    scenarios: tuple[Scenario, ...],
    timeout: float,
) -> tuple[Scenario, ...]:
    """Filter and adapt catalog scenarios against live Page 1/2 discovery endpoints.

    Args:
        client: HTTP client supporting ``get(path, name=..., timeout=...)``.
        scenarios: Curated scenario tuple loaded from ``scenarios.json``.
        timeout: Per-request timeout in seconds.

    Returns:
        Tuple of scenarios compatible with the live deployment.

    Raises:
        CatalogError: If live responses are malformed or do not cover tiers 1-3.
    """
    surveys_response = client.get(
        "/surveys-ui", name="discovery:class-a:/surveys-ui", timeout=timeout
    )
    covariates_response = client.get(
        "/covariates", name="discovery:class-a:/covariates", timeout=timeout
    )
    categories_response = client.get(
        "/categories", name="discovery:class-a:/categories", timeout=timeout
    )
    surveys = validate_envelope(surveys_response)["data"]
    covariates = validate_envelope(covariates_response)["data"]
    categories = validate_envelope(categories_response)["data"]
    if not all(isinstance(item, list) for item in (surveys, covariates, categories)):
        raise CatalogError("live discovery endpoints did not return lists")

    live_pip_ids = [
        str(item.get("pip_id"))
        for item in surveys
        if isinstance(item, dict) and item.get("pip_id")
    ]
    if not live_pip_ids:
        raise CatalogError("live /surveys-ui returned no survey identifiers")

    known_vars = {
        str(item.get("varname"))
        for item in (*covariates, *categories)
        if isinstance(item, dict) and item.get("varname")
    }
    live_id_set = set(live_pip_ids)
    updated: list[Scenario] = []
    for scenario in scenarios:
        if scenario.expected_status == 422:
            updated.append(scenario)
            continue
        if known_vars and not set(scenario.required_covariates).issubset(known_vars):
            continue
        matching = tuple(pid for pid in scenario.survey_pool if pid in live_id_set)
        if len(matching) < scenario.survey_count and len(live_pip_ids) >= scenario.survey_count:
            candidate = tuple(live_pip_ids[: max(scenario.survey_count, 5)])
            if scenario.required_covariates:
                try:
                    cov_res = client.get(
                        "/covariates",
                        params=[("pip_id", pid) for pid in candidate[: scenario.survey_count]],
                        name="discovery:class-a:/covariates",
                        timeout=timeout,
                    )
                    cov_items = validate_envelope(cov_res)["data"]
                    common_vars = {
                        str(item.get("varname"))
                        for item in cov_items
                        if isinstance(item, dict) and item.get("varname")
                    }
                    if not set(scenario.required_covariates).issubset(common_vars):
                        continue
                except TypeError:
                    pass
            matching = candidate
        if len(matching) >= scenario.survey_count:
            updated.append(scenario.with_pool(matching))
    if {item.tier for item in updated if item.expected_status == 200} != {1, 2, 3}:
        raise CatalogError("live discovery could not build scenarios for tiers 1, 2, and 3")
    return tuple(updated)


def bootstrap_catalog(
    client: Any,
    scenarios: tuple[Scenario, ...],
    mode: str,
    timeout: float,
) -> tuple[Scenario, ...]:
    """Bootstrap the scenario catalog according to ``mode``.

    Args:
        client: HTTP client used when ``mode`` is ``'live'`` or ``'live-with-curated-fallback'``.
        scenarios: Curated scenario tuple.
        mode: Bootstrap mode (``'curated'``, ``'live'``, or ``'live-with-curated-fallback'``).
        timeout: Per-request timeout in seconds.

    Returns:
        Tuple of active scenarios for the run.

    Raises:
        CatalogError: If ``mode == 'live'`` and live discovery fails.
    """
    if mode == "curated":
        return scenarios
    try:
        return compatible_live_scenarios(client, scenarios, timeout)
    except (CatalogError, SemanticError, OSError) as exc:
        if mode == "live-with-curated-fallback":
            LOGGER.warning("Live catalog bootstrap failed (%s); using curated catalog.", exc)
            return scenarios
        raise CatalogError(f"live catalog bootstrap failed: {exc}") from exc
