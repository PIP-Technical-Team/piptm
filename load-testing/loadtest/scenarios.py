"""Scenario catalog parsing, UI-rule validation, and deterministic selection."""

from __future__ import annotations

import json
import math
import random
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

PIP_ID = re.compile(r"^[A-Z]{3}_[0-9]{4}_[A-Z0-9_-]{1,40}$")
SLOTS = ("column", "row", "superColumn", "superRow")
DOWNSTREAM = {
    "none",
    "metadata_html",
    "metadata_markdown",
    "download_csv",
    "download_rds",
}
LATENCY_CLASSES = {"class-b", "class-c"}

STAT_GROUP_MEASURES: dict[str, frozenset[str]] = {
    "summary_statistics": frozenset(
        {"mean", "median", "sd", "var", "min", "max", "nobs", "p10", "p25", "p75", "p90", "sum"}
    ),
    "poverty": frozenset(
        {"headcount", "poverty_gap", "severity", "watts", "pop_poverty"}
    ),
    "inequality": frozenset({"gini", "mld"}),
    "shares": frozenset(
        {"pop_share", "target_within_group_share", "target_survey_share"}
    ),
}

ANALYSIS_VAR_GROUPS: dict[str, tuple[str, ...]] = {
    "welfare": ("summary_statistics", "inequality"),
    "pov_status": ("poverty",),
    "age": ("summary_statistics",),
    "hsize": ("summary_statistics",),
    "imp_wat_rec": ("shares",),
    "imp_san_rec": ("shares",),
    "electricity": ("shares",),
    "female": ("shares",),
    "male": ("shares",),
    "urban": ("shares",),
    "rural": ("shares",),
    "youth": ("shares",),
    "prime_work_age": ("shares",),
    "elderly": ("shares",),
    "employed": ("shares",),
    "unemployed": ("shares",),
    "in_labor_force": ("shares",),
    "self_emp": ("shares",),
    "paid_emp": ("shares",),
    "inf_emp": ("shares",),
    "employer": ("shares",),
    "emp_agri": ("shares",),
    "emp_indu": ("shares",),
    "emp_serv": ("shares",),
    "emp_others": ("shares",),
}

VALID_FILTER_VARS: frozenset[str] = frozenset(
    {
        "imp_wat_rec",
        "imp_san_rec",
        "electricity",
        "gender",
        "area",
        "age_group",
        "age_group2",
        "hsize_group",
        "educat4",
        "educat5",
        "educat7",
        "wquintile",
        "lstatus",
        "empstat",
        "industrycat4",
        "industrycat10",
    }
)
VALID_SLOT_COVARIATES: frozenset[str] = VALID_FILTER_VARS | {"pov_status"}


class CatalogError(ValueError):
    """Raised when a workload catalog cannot generate valid primary traffic."""


@dataclass(frozen=True)
class Scenario:
    """Immutable workload scenario definition for `/table` and downstream endpoints."""

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
    filter_base: dict[str, list[int]] | None = None
    ppp: int = 2021
    pop_share_threshold: float | None = None
    downstream: str = "none"
    latency_class: str = "class-b"
    expected_status: int = 200

    def payload(self, surveys: tuple[str, ...] | None = None) -> dict[str, Any]:
        """Build the request parameter dictionary for `/table` and `/metadata`.

        Args:
            surveys: Optional tuple of ``pip_id`` strings overriding the default slice.

        Returns:
            Dictionary of request parameters ready for JSON or query serialization.
        """
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
        if self.pop_share_threshold is not None:
            body["pop_share_threshold"] = self.pop_share_threshold
        return body

    def with_pool(self, pool: tuple[str, ...]) -> Scenario:
        """Return a copy of this scenario with an updated ``survey_pool``."""
        return replace(self, survey_pool=pool)


def _allowed_measures(analysis_var: str) -> frozenset[str]:
    groups = ANALYSIS_VAR_GROUPS.get(analysis_var)
    if not groups:
        return frozenset()
    allowed: set[str] = set()
    for group in groups:
        allowed |= STAT_GROUP_MEASURES[group]
    return frozenset(allowed)


def _validate(raw: dict[str, Any]) -> Scenario:
    required = {
        "id",
        "tier",
        "weight",
        "transport",
        "analysis_var",
        "measures",
        "slots",
        "survey_pool",
        "survey_count",
        "required_covariates",
        "downstream",
    }
    missing = required - raw.keys()
    if missing:
        raise CatalogError(f"scenario missing fields: {sorted(missing)}")
    scenario_id = str(raw["id"])
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", scenario_id):
        raise CatalogError(f"invalid scenario id: {scenario_id}")
    raw_dump = json.dumps(raw).lower()
    if "/session" in raw_dump or "/description" in raw_dump or "include_metadata" in raw_dump:
        raise CatalogError(
            f"{scenario_id}: removed session/description/include_metadata traffic is forbidden"
        )

    tier = int(raw["tier"])
    survey_count = int(raw["survey_count"])
    if tier not in {1, 2, 3} or not 1 <= survey_count <= 15:
        raise CatalogError(f"{scenario_id}: tier must be 1-3 and survey_count 1-15")
    if tier == 1 and not 1 <= survey_count <= 2:
        raise CatalogError(f"{scenario_id}: tier 1 requires 1-2 surveys")
    if tier == 2 and not 3 <= survey_count <= 5:
        raise CatalogError(f"{scenario_id}: tier 2 requires 3-5 surveys")
    if tier == 3 and not 5 <= survey_count <= 15:
        raise CatalogError(f"{scenario_id}: tier 3 requires 5-15 surveys")

    default_class = "class-c" if tier == 3 else "class-b"
    latency_class = str(raw.get("latency_class", default_class))
    if latency_class not in LATENCY_CLASSES or latency_class != default_class:
        raise CatalogError(f"{scenario_id}: latency_class must be {default_class} for tier {tier}")

    expected_status = int(raw.get("expected_status", 200))
    if expected_status not in {200, 422}:
        raise CatalogError(f"{scenario_id}: expected_status must be 200 or 422")

    transport = str(raw["transport"]).upper()
    if transport not in {"GET", "POST"}:
        raise CatalogError(f"{scenario_id}: transport must be GET or POST")

    analysis_var = str(raw["analysis_var"])
    if analysis_var not in ANALYSIS_VAR_GROUPS:
        raise CatalogError(f"{scenario_id}: invalid analysis_var '{analysis_var}'")

    measures = tuple(str(value) for value in raw["measures"])
    if not measures:
        raise CatalogError(f"{scenario_id}: measures cannot be empty")
    allowed = _allowed_measures(analysis_var)
    disallowed = [m for m in measures if m not in allowed]
    if disallowed:
        raise CatalogError(
            f"{scenario_id}: measures {disallowed} are incompatible with '{analysis_var}'"
        )

    pool = tuple(str(value) for value in raw["survey_pool"])
    if len(pool) < survey_count or any(not PIP_ID.fullmatch(value) for value in pool):
        raise CatalogError(f"{scenario_id}: survey_pool is missing or contains invalid identifiers")

    slots = {str(key): str(value) for key, value in raw["slots"].items()}
    if any(key not in SLOTS for key in slots):
        raise CatalogError(f"{scenario_id}: unknown layout slot")
    if "superColumn" in slots and "column" not in slots:
        raise CatalogError(f"{scenario_id}: superColumn requires column")
    if "superRow" in slots and "row" not in slots:
        raise CatalogError(f"{scenario_id}: superRow requires row")
    if len(set(slots.values())) != len(slots):
        raise CatalogError(f"{scenario_id}: layout variables must be unique")
    if any(val not in VALID_SLOT_COVARIATES for val in slots.values()):
        raise CatalogError(f"{scenario_id}: invalid layout slot covariate")
    if analysis_var in slots.values():
        raise CatalogError(
            f"{scenario_id}: analysis_var '{analysis_var}' cannot also be in a layout slot"
        )

    filter_base = raw.get("filter_base")
    if filter_base is not None:
        if not isinstance(filter_base, dict) or not filter_base:
            raise CatalogError(
                f"{scenario_id}: filter_base must be a non-empty object when provided"
            )
        for fkey, fvals in filter_base.items():
            if (
                fkey not in VALID_FILTER_VARS
                or not isinstance(fvals, list)
                or not fvals
                or any(isinstance(v, bool) or not isinstance(v, int) for v in fvals)
            ):
                raise CatalogError(f"{scenario_id}: invalid filter_base entry '{fkey}'")

    required_covariates = tuple(str(value) for value in raw["required_covariates"])
    needed = (set(slots.values()) - {"pov_status"}) | set((filter_base or {}).keys())
    if not needed.issubset(required_covariates):
        raise CatalogError(f"{scenario_id}: required_covariates must cover slots and filters")

    poverty_line = raw.get("poverty_line")
    if poverty_line is not None and (
        isinstance(poverty_line, bool)
        or not math.isfinite(float(poverty_line))
        or float(poverty_line) <= 0
    ):
        raise CatalogError(f"{scenario_id}: poverty_line must be finite and positive")
    if (analysis_var == "pov_status" or "pov_status" in slots.values()) and poverty_line is None:
        raise CatalogError(f"{scenario_id}: positive poverty_line is required for pov_status")

    raw_ppp = raw.get("ppp", 2021)
    if isinstance(raw_ppp, bool) or not isinstance(raw_ppp, int) or raw_ppp <= 0:
        raise CatalogError(f"{scenario_id}: ppp must be a positive integer")

    pop_share_threshold = raw.get("pop_share_threshold")
    if pop_share_threshold is not None and (
        isinstance(pop_share_threshold, bool)
        or not math.isfinite(float(pop_share_threshold))
        or not 0 < float(pop_share_threshold) < 1
    ):
        raise CatalogError(f"{scenario_id}: pop_share_threshold must be strictly between 0 and 1")

    downstream = str(raw["downstream"])
    if downstream not in DOWNSTREAM:
        raise CatalogError(f"{scenario_id}: invalid downstream mode '{downstream}'")
    if expected_status == 422 and downstream != "none":
        raise CatalogError(
            f"{scenario_id}: expected_status 422 scenarios cannot chain downstream requests"
        )

    weight = int(raw["weight"])
    if weight <= 0:
        raise CatalogError(f"{scenario_id}: weight must be positive")

    return Scenario(
        scenario_id=scenario_id,
        tier=tier,
        weight=weight,
        transport=transport,
        analysis_var=analysis_var,
        measures=measures,
        slots=slots,
        survey_pool=pool,
        survey_count=survey_count,
        required_covariates=required_covariates,
        poverty_line=float(poverty_line) if poverty_line is not None else None,
        filter_base=filter_base,
        ppp=raw_ppp,
        pop_share_threshold=float(pop_share_threshold) if pop_share_threshold is not None else None,
        downstream=downstream,
        latency_class=latency_class,
        expected_status=expected_status,
    )


def load_catalog(path: Path) -> tuple[Scenario, ...]:
    """Load and validate a scenario catalog JSON file.

    Args:
        path: Filesystem path to ``scenarios.json``.

    Returns:
        Tuple of validated ``Scenario`` objects covering tiers 1, 2, and 3.

    Raises:
        CatalogError: If the file cannot be read or any scenario violates UI/API rules.
    """
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
    """Select one scenario for ``tier`` weighted by ``Scenario.weight``.

    Args:
        scenarios: Tuple of candidate scenarios.
        tier: Target tier (``1``, ``2``, or ``3``).
        rng: Seeded ``random.Random`` instance.

    Returns:
        Selected ``Scenario``.

    Raises:
        CatalogError: If no scenarios exist for ``tier``.
    """
    eligible = [item for item in scenarios if item.tier == tier]
    if not eligible:
        raise CatalogError(f"no tier {tier} scenarios are available")
    return rng.choices(eligible, weights=[item.weight for item in eligible], k=1)[0]
