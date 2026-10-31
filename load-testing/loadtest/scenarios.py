"""Scenario catalog parsing, validation, and deterministic selection."""

from __future__ import annotations

import json
import random
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

PIP_ID = re.compile(r"^[A-Z]{3}_[0-9]{4}_[A-Z0-9_-]{1,40}$")
SLOTS = ("column", "row", "superColumn", "superRow")
DOWNSTREAM = {
    "none", "description_metadata", "description_recompute", "download_csv", "download_rds"
}


class CatalogError(ValueError):
    """Raised when a workload catalog cannot generate valid primary traffic."""


@dataclass(frozen=True)
class Scenario:
    scenario_id: str
    tier: int
    weight: int
    transport: str
    analysis_var: str
    measures: tuple[str, ...]
    slots: dict[str, str]
    survey_pool: tuple[str, ...]
    survey_count: int
    required_covariates: tuple[str, ...]
    poverty_line: float | None = None
    filter_base: dict[str, list[Any]] | None = None
    ppp: int = 2021
    downstream: str = "none"

    def payload(self, surveys: tuple[str, ...] | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {
            "pip_id": list(surveys or self.survey_pool[: self.survey_count]),
            "analysis_var": self.analysis_var,
            "measures": list(self.measures),
            "ppp": self.ppp,
        }
        body.update(self.slots)
        if self.poverty_line is not None:
            body["poverty_line"] = self.poverty_line
        if self.filter_base is not None:
            body["filter_base"] = self.filter_base
        return body

    def with_pool(self, pool: tuple[str, ...]) -> Scenario:
        return replace(self, survey_pool=pool)


def _validate(raw: dict[str, Any]) -> Scenario:
    required = {
        "id", "tier", "weight", "transport", "analysis_var", "measures", "slots",
        "survey_pool", "survey_count", "required_covariates", "downstream",
    }
    missing = required - raw.keys()
    if missing:
        raise CatalogError(f"scenario missing fields: {sorted(missing)}")
    scenario_id = str(raw["id"])
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", scenario_id):
        raise CatalogError(f"invalid scenario id: {scenario_id}")
    tier = int(raw["tier"])
    survey_count = int(raw["survey_count"])
    if tier not in {1, 2, 3} or not 1 <= survey_count <= 15:
        raise CatalogError(f"{scenario_id}: tier must be 1-3 and survey_count 1-15")
    if tier == 1 and survey_count != 1:
        raise CatalogError(f"{scenario_id}: tier 1 requires one survey")
    if tier == 2 and not 3 <= survey_count <= 5:
        raise CatalogError(f"{scenario_id}: tier 2 requires 3-5 surveys")
    if tier == 3 and survey_count != 15:
        raise CatalogError(f"{scenario_id}: tier 3 requires 15 surveys")
    transport = str(raw["transport"]).upper()
    if transport not in {"GET", "POST"}:
        raise CatalogError(f"{scenario_id}: transport must be GET or POST")
    measures = tuple(str(value) for value in raw["measures"])
    if not measures:
        raise CatalogError(f"{scenario_id}: measures cannot be empty")
    pool = tuple(str(value) for value in raw["survey_pool"])
    if len(pool) < survey_count or any(not PIP_ID.fullmatch(value) for value in pool):
        raise CatalogError(f"{scenario_id}: survey_pool is missing or contains invalid identifiers")
    slots = {str(key): str(value) for key, value in raw["slots"].items()}
    if any(key not in SLOTS for key in slots):
        raise CatalogError(f"{scenario_id}: unknown layout slot")
    present = [name in slots for name in SLOTS]
    if any(present[index] and not all(present[:index]) for index in range(len(SLOTS))):
        raise CatalogError(f"{scenario_id}: layout slots must be contiguous")
    if len(set(slots.values())) != len(slots):
        raise CatalogError(f"{scenario_id}: layout variables must be unique")
    required_covariates = tuple(str(value) for value in raw["required_covariates"])
    needed = set(slots.values()) | set((raw.get("filter_base") or {}).keys())
    if not needed.issubset(required_covariates):
        raise CatalogError(f"{scenario_id}: required_covariates must cover slots and filters")
    poverty_line = raw.get("poverty_line")
    if (raw["analysis_var"] == "pov_status" or "pov_status" in slots.values()) and not poverty_line:
        raise CatalogError(f"{scenario_id}: poverty_line is required for poverty status")
    downstream = str(raw["downstream"])
    if downstream not in DOWNSTREAM:
        raise CatalogError(f"{scenario_id}: invalid downstream mode")
    if "/session" in json.dumps(raw).lower():
        raise CatalogError(f"{scenario_id}: session traffic is forbidden")
    weight = int(raw["weight"])
    if weight <= 0:
        raise CatalogError(f"{scenario_id}: weight must be positive")
    return Scenario(
        scenario_id=scenario_id,
        tier=tier,
        weight=weight,
        transport=transport,
        analysis_var=str(raw["analysis_var"]),
        measures=measures,
        slots=slots,
        survey_pool=pool,
        survey_count=survey_count,
        required_covariates=required_covariates,
        poverty_line=float(poverty_line) if poverty_line is not None else None,
        filter_base=raw.get("filter_base"),
        ppp=int(raw.get("ppp", 2021)),
        downstream=downstream,
    )


def load_catalog(path: Path) -> tuple[Scenario, ...]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CatalogError(f"cannot load scenario catalog {path}: {exc}") from exc
    if document.get("schema_version") != 1 or not isinstance(document.get("scenarios"), list):
        raise CatalogError("catalog must use schema_version 1 and contain scenarios")
    scenarios = tuple(_validate(item) for item in document["scenarios"])
    if not scenarios or len({item.scenario_id for item in scenarios}) != len(scenarios):
        raise CatalogError("catalog must contain scenarios with unique IDs")
    if {item.tier for item in scenarios} != {1, 2, 3}:
        raise CatalogError("catalog must cover tiers 1, 2, and 3")
    return scenarios


def choose_scenario(scenarios: tuple[Scenario, ...], tier: int, rng: random.Random) -> Scenario:
    eligible = [item for item in scenarios if item.tier == tier]
    if not eligible:
        raise CatalogError(f"no tier {tier} scenarios are available")
    return rng.choices(eligible, weights=[item.weight for item in eligible], k=1)[0]
