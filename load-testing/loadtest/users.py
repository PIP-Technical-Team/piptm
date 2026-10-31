"""Locust virtual-user journeys for the Table Maker API."""

from __future__ import annotations

import json
import random
from itertools import count
from typing import Any

from locust import HttpUser, between, task

try:
    from .api import (
        extract_description_metadata,
        mark_response,
        validate_description,
        validate_download,
        validate_envelope,
        validate_table,
    )
    from .bootstrap import DISCOVERY_ENDPOINTS
    from .config import Settings
    from .scenarios import Scenario, choose_scenario, load_catalog
except ImportError:
    from api import (
        extract_description_metadata,
        mark_response,
        validate_description,
        validate_download,
        validate_envelope,
        validate_table,
    )
    from bootstrap import DISCOVERY_ENDPOINTS
    from config import Settings
    from scenarios import Scenario, choose_scenario, load_catalog


SETTINGS = Settings.from_env()
SCENARIOS = load_catalog(SETTINGS.scenario_catalog)
USER_SEQUENCE = count()


def get_query(payload: dict[str, Any]) -> list[tuple[str, str]]:
    query: list[tuple[str, str]] = []
    for key, value in payload.items():
        values = value if isinstance(value, list) else [value]
        for item in values:
            encoded = (
                json.dumps(item, separators=(",", ":")) if isinstance(item, dict) else str(item)
            )
            query.append((key, encoded))
    return query


class TableMakerUser(HttpUser):
    host = SETTINGS.target_host
    wait_time = between(SETTINGS.think_time_min, SETTINGS.think_time_max)

    def on_start(self) -> None:
        runner = getattr(self.environment, "runner", None)
        worker_id = str(getattr(runner, "client_id", "local"))
        worker_offset = sum((index + 1) * ord(char) for index, char in enumerate(worker_id))
        self.rng = random.Random(SETTINGS.random_seed + worker_offset + next(USER_SEQUENCE))
        if getattr(self.environment, "piptm_bootstrap_failed", False):
            raise RuntimeError("scenario bootstrap failed; refusing to generate traffic")
        self.scenarios = getattr(self.environment, "piptm_scenarios", None)
        if not self.scenarios:
            raise RuntimeError("scenarios were not prepared before users spawned")
        self.discovery()

    @task(SETTINGS.discovery_weight)
    def discovery(self) -> None:
        path = self.rng.choice(DISCOVERY_ENDPOINTS)
        with self.client.get(
            path,
            name=f"discovery:{path}",
            timeout=SETTINGS.request_timeout,
            catch_response=True,
        ) as response:
            mark_response(response, validate_envelope)

    @task(SETTINGS.tier_weights[0])
    def tier1(self) -> None:
        self.run_scenario(choose_scenario(self.scenarios, 1, self.rng))

    @task(SETTINGS.tier_weights[1])
    def tier2(self) -> None:
        self.run_scenario(choose_scenario(self.scenarios, 2, self.rng))

    @task(SETTINGS.tier_weights[2])
    def tier3(self) -> None:
        self.run_scenario(choose_scenario(self.scenarios, 3, self.rng))

    def run_scenario(self, scenario: Scenario) -> None:
        pool = list(scenario.survey_pool)
        surveys = tuple(self.rng.sample(pool, scenario.survey_count))
        payload = scenario.payload(surveys)
        metadata_mode = scenario.downstream == "description_metadata"
        payload["include_metadata"] = "true" if metadata_mode else "false"
        name = f"table:tier{scenario.tier}:{scenario.transport.lower()}"
        kwargs = (
            {"params": get_query(payload)} if scenario.transport == "GET" else {"json": payload}
        )
        with self.client.request(
            scenario.transport,
            "/table",
            name=name,
            timeout=SETTINGS.request_timeout,
            catch_response=True,
            **kwargs,
        ) as response:
            envelope = mark_response(response, validate_envelope)
            if envelope is None:
                return
            table_data = mark_response(
                response,
                validate_table,
                surveys,
                scenario.measures,
                metadata_mode,
            )
            if table_data is None:
                return
            chain_data = (
                extract_description_metadata(envelope) if metadata_mode else table_data
            )
        if metadata_mode and chain_data:
            self.description_metadata(chain_data)
        elif scenario.downstream == "description_recompute":
            self.description_recompute(payload)
        elif scenario.downstream.startswith("download_") and chain_data:
            self.download(chain_data, scenario.downstream.removeprefix("download_"))

    def description_metadata(self, metadata: dict[str, Any]) -> None:
        with self.client.post(
            "/description", json={"description_metadata": metadata}, name="description:metadata",
            timeout=SETTINGS.request_timeout, catch_response=True,
        ) as response:
            mark_response(response, validate_description)

    def description_recompute(self, payload: dict[str, Any]) -> None:
        body = dict(payload)
        body.pop("include_metadata", None)
        with self.client.post(
            "/description", json=body, name="description:recompute",
            timeout=SETTINGS.request_timeout, catch_response=True,
        ) as response:
            mark_response(response, validate_description)

    def download(self, data: list[dict[str, Any]], file_format: str) -> None:
        with self.client.post(
            "/table/download", json={"format": file_format, "data": data},
            name=f"download:{file_format}", timeout=SETTINGS.request_timeout,
            catch_response=True,
        ) as response:
            mark_response(response, validate_download, file_format)
