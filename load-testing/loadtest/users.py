"""Locust virtual user journeys for the Table Maker API (Pages 1, 2, and 3)."""

from __future__ import annotations

import itertools
import json
import os
import random
from typing import Any

from locust import HttpUser, between, task

try:
    from .api import (
        SemanticError,
        validate_cell_definition,
        validate_download,
        validate_envelope,
        validate_expected_422,
        validate_metadata,
        validate_table,
    )
    from .config import Settings
    from .scenarios import CatalogError, Scenario, choose_scenario, load_catalog
except ImportError:
    from api import (  # type: ignore[no-redef]
        SemanticError,
        validate_cell_definition,
        validate_download,
        validate_envelope,
        validate_expected_422,
        validate_metadata,
        validate_table,
    )
    from config import Settings  # type: ignore[no-redef]
    from scenarios import (  # type: ignore[no-redef]
        CatalogError,
        Scenario,
        choose_scenario,
        load_catalog,
    )

SETTINGS = Settings.from_env()
_CACHED_SCENARIOS: tuple[Scenario, ...] | None = None
USER_SEQUENCE = itertools.count(1)

AGREED_ENDPOINTS: frozenset[str] = frozenset(
    {
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
)
DISCOVERY_ENDPOINTS: tuple[tuple[str, str], ...] = (
    ("/health", "discovery:class-a:/health"),
    ("/releases", "discovery:class-a:/releases"),
    ("/regions", "discovery:class-a:/regions"),
    ("/countries", "discovery:class-a:/countries"),
    ("/surveys-ui", "discovery:class-a:/surveys-ui"),
    ("/categories", "discovery:class-a:/categories"),
    ("/analysis-variables", "discovery:class-a:/analysis-variables"),
    ("/statistics", "discovery:class-a:/statistics"),
    ("/covariates", "discovery:class-a:/covariates"),
)


def _get_scenarios(environment: Any | None = None) -> tuple[Scenario, ...]:
    global _CACHED_SCENARIOS
    env_scenarios = getattr(environment, "piptm_scenarios", None) if environment else None
    if env_scenarios:
        return env_scenarios
    if _CACHED_SCENARIOS is None:
        _CACHED_SCENARIOS = load_catalog(SETTINGS.scenario_catalog)
    return _CACHED_SCENARIOS


def get_query(payload: dict[str, Any]) -> list[tuple[str, str]]:
    """Serialize a request dictionary into repeated query-string key/value pairs.

    Args:
        payload: Request parameter dictionary.

    Returns:
        List of ``(key, value)`` query parameter tuples with compact JSON ``filter_base``.
    """
    params: list[tuple[str, str]] = []
    for key, value in payload.items():
        if key == "filter_base" and isinstance(value, dict):
            params.append((key, json.dumps(value, separators=(",", ":"))))
        elif isinstance(value, list):
            params.extend((key, str(item)) for item in value)
        elif value is not None:
            params.append((key, str(value)))
    return params


def table_request_name(scenario: Scenario) -> str:
    """Build the low-cardinality Locust request name for a `/table` scenario.

    Args:
        scenario: Active ``Scenario`` instance.

    Returns:
        Request name encoding tier, latency class, transport, and optional ``:expected422``.
    """
    suffix = ":expected422" if scenario.expected_status == 422 else ""
    transport = scenario.transport.lower()
    return f"table:tier{scenario.tier}:{scenario.latency_class}:{transport}{suffix}"


def _cell_definition_query(payload: dict[str, Any]) -> list[tuple[str, str]]:
    allowed_keys = {
        "analysis_var",
        "measures",
        "poverty_line",
        "column",
        "row",
        "superColumn",
        "superRow",
        "filter_base",
        "ppp",
        "pop_share_threshold",
        "release",
    }
    filtered = {k: v for k, v in payload.items() if k in allowed_keys}
    return get_query(filtered)


class TableMakerUser(HttpUser):
    """Virtual user executing Page 1, Page 2, and Page 3 Table Maker API journeys."""

    host = SETTINGS.target_host
    wait_time = between(SETTINGS.think_time_min, SETTINGS.think_time_max)

    def on_start(self) -> None:
        worker_offset = int(os.environ.get("LOCUST_WORKER_INDEX", "0")) * 100_000
        seed = SETTINGS.random_seed + worker_offset + next(USER_SEQUENCE)
        random.seed(seed)
        self.rng = random.Random(seed)

    @task(SETTINGS.discovery_weight)
    def discover(self) -> None:
        if getattr(SETTINGS, "traffic_mode", "mixed") == "stampede":
            self._run_stampede()
            return
        path, name = self.rng.choice(DISCOVERY_ENDPOINTS)
        self._checked_get(path, name)

    @task(SETTINGS.tier_weights[0])
    def tier1_light(self) -> None:
        self._run_tier(1)

    @task(SETTINGS.tier_weights[1])
    def tier2_typical(self) -> None:
        self._run_tier(2)

    @task(SETTINGS.tier_weights[2])
    def tier3_heavy(self) -> None:
        self._run_tier(3)

    def _select_stampede_scenario(
        self,
        scenarios: tuple[Scenario, ...] | None = None,
        settings: Any | None = None,
    ) -> Scenario:
        active_scenarios = (
            scenarios
            if scenarios is not None
            else _get_scenarios(getattr(self, "environment", None))
        )
        active_settings = settings if settings is not None else SETTINGS
        pinned_id = getattr(active_settings, "stampede_scenario_id", None)
        if pinned_id:
            for item in active_scenarios:
                if item.scenario_id == pinned_id:
                    if item.expected_status != 200:
                        raise CatalogError(
                            f"stampede scenario '{pinned_id}' must expect HTTP 200"
                        )
                    return item
            raise CatalogError(f"unknown stampede_scenario_id '{pinned_id}'")
        stampede_tier = int(getattr(active_settings, "stampede_tier", 3))
        eligible = [
            item
            for item in active_scenarios
            if item.tier == stampede_tier and item.expected_status == 200
        ]
        if not eligible:
            raise CatalogError(f"no HTTP 200 scenarios available for stampede tier {stampede_tier}")
        return eligible[0]

    def _run_stampede(self) -> None:
        scenario = self._select_stampede_scenario()
        surveys = tuple(scenario.survey_pool[: scenario.survey_count])
        self._execute_scenario(scenario, surveys, include_preview=False)

    def _run_tier(self, tier: int) -> None:
        if getattr(SETTINGS, "traffic_mode", "mixed") == "stampede":
            self._run_stampede()
            return
        scenarios = _get_scenarios(getattr(self, "environment", None))
        scenario = choose_scenario(scenarios, tier, self.rng)
        surveys = tuple(self.rng.sample(scenario.survey_pool, k=scenario.survey_count))
        self._execute_scenario(scenario, surveys, include_preview=scenario.expected_status == 200)

    def _execute_scenario(
        self,
        scenario: Scenario,
        surveys: tuple[str, ...],
        include_preview: bool,
    ) -> None:
        payload = scenario.payload(surveys)
        if include_preview:
            self._run_cell_definition_preview(payload)
        name = table_request_name(scenario)
        request_kwargs: dict[str, Any] = {
            "name": name,
            "catch_response": True,
            "timeout": SETTINGS.request_timeout,
        }
        if scenario.transport == "GET":
            ctx = self.client.get("/table", params=get_query(payload), **request_kwargs)
        else:
            ctx = self.client.post("/table", json=payload, **request_kwargs)
        with ctx as response:
            try:
                if scenario.expected_status == 422:
                    validate_expected_422(response)
                    response.success()
                    return
                envelope = validate_envelope(response)
                table_data = validate_table(envelope, surveys, scenario.measures)
                response.success()
            except SemanticError as exc:
                response.failure(str(exc))
                return
        self._run_downstream(scenario, payload, table_data)

    def _run_cell_definition_preview(self, payload: dict[str, Any]) -> None:
        with self.client.get(
            "/cell-definition",
            params=_cell_definition_query(payload),
            name="preview:class-a:/cell-definition",
            catch_response=True,
            timeout=SETTINGS.request_timeout,
        ) as response:
            try:
                validate_cell_definition(response)
                response.success()
            except SemanticError as exc:
                response.failure(str(exc))

    def _run_downstream(
        self,
        scenario: Scenario,
        payload: dict[str, Any],
        table_data: list[dict[str, Any]],
    ) -> None:
        if scenario.downstream == "none":
            return
        if scenario.downstream in {"metadata_html", "metadata_markdown"}:
            fmt = "html" if scenario.downstream == "metadata_html" else "markdown"
            meta_payload = {**payload, "format": fmt}
            req_name = f"metadata:tier{scenario.tier}:class-a:{fmt}"
            if scenario.transport == "GET":
                ctx = self.client.get(
                    "/metadata",
                    params=get_query(meta_payload),
                    name=req_name,
                    catch_response=True,
                    timeout=SETTINGS.request_timeout,
                )
            else:
                ctx = self.client.post(
                    "/metadata",
                    json=meta_payload,
                    name=req_name,
                    catch_response=True,
                    timeout=SETTINGS.request_timeout,
                )
            with ctx as response:
                try:
                    validate_metadata(response, fmt)
                    response.success()
                except SemanticError as exc:
                    response.failure(str(exc))
            return
        fmt = "csv" if scenario.downstream == "download_csv" else "rds"
        body = {"data": table_data, "format": fmt}
        with self.client.post(
            "/table/download",
            json=body,
            name=f"download:tier{scenario.tier}:{scenario.latency_class}:{fmt}",
            catch_response=True,
            timeout=SETTINGS.request_timeout,
        ) as response:
            try:
                validate_download(response, fmt)
                response.success()
            except SemanticError as exc:
                response.failure(str(exc))

    def _checked_get(self, path: str, name: str) -> None:
        with self.client.get(
            path,
            name=name,
            catch_response=True,
            timeout=SETTINGS.request_timeout,
        ) as response:
            try:
                validate_envelope(response)
                response.success()
            except SemanticError as exc:
                response.failure(str(exc))
