"""Validated environment configuration for local and Azure Locust runs."""

from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


class ConfigError(ValueError):
    """Raised when load-test configuration is unsafe or inconsistent."""


def _integer(env: dict[str, str], name: str, default: int, minimum: int = 0) -> int:
    try:
        value = int(env.get(name, str(default)))
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc
    if value < minimum:
        raise ConfigError(f"{name} must be >= {minimum}")
    return value


def _number(env: dict[str, str], name: str, default: float, minimum: float = 0) -> float:
    try:
        value = float(env.get(name, str(default)))
    except ValueError as exc:
        raise ConfigError(f"{name} must be numeric") from exc
    if not math.isfinite(value) or value < minimum:
        raise ConfigError(f"{name} must be >= {minimum}")
    return value


def parse_duration(value: str) -> int:
    """Convert a Locust-style duration to positive seconds."""
    match = re.fullmatch(r"\s*(\d+)\s*([smh]?)\s*", value.lower())
    if not match:
        raise ConfigError("RUN_TIME must be a positive duration such as 60s, 10m, or 1h")
    amount = int(match.group(1))
    if amount == 0:
        raise ConfigError("RUN_TIME must be greater than zero")
    return amount * {"": 1, "s": 1, "m": 60, "h": 3600}[match.group(2)]


def _weight(env: dict[str, str], name: str, default: int) -> int:
    return _integer(env, name, default, minimum=0)


@dataclass(frozen=True)
class Settings:
    target_host: str
    load_profile: str
    users: int
    spawn_rate: float
    run_time: str
    run_time_seconds: int
    ramp_steps: tuple[int, ...]
    spike_users: int
    spike_hold_seconds: int
    random_seed: int
    scenario_catalog: Path
    bootstrap_mode: str
    request_timeout: float
    think_time_min: float
    think_time_max: float
    tier_weights: tuple[int, int, int]
    discovery_weight: int
    slo_p95_ms: int
    slo_error_percent: float

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> Settings:
        env = dict(os.environ if environ is None else environ)
        host = env.get("TARGET_HOST", "https://tablemakerapiqa.worldbank.org").rstrip("/")
        parsed = urlsplit(host)
        allow_local = env.get("ALLOW_INSECURE_LOCAL", "false").lower() == "true"
        is_local = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
        if not parsed.hostname or parsed.scheme not in {"http", "https"}:
            raise ConfigError("TARGET_HOST must be an absolute HTTP(S) URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ConfigError("TARGET_HOST must not contain credentials, a query, or a fragment")
        if parsed.scheme != "https" and not (is_local and allow_local):
            raise ConfigError(
                "non-HTTPS TARGET_HOST is allowed only for local hosts with "
                "ALLOW_INSECURE_LOCAL=true"
            )

        profile = env.get("LOAD_PROFILE", "ramp").lower()
        if profile not in {"ramp", "spike"}:
            raise ConfigError("LOAD_PROFILE must be ramp or spike")
        bootstrap = env.get("BOOTSTRAP_MODE", "curated").lower()
        if bootstrap not in {"curated", "live", "live-with-curated-fallback"}:
            raise ConfigError(
                "BOOTSTRAP_MODE must be curated, live, or live-with-curated-fallback"
            )
        try:
            ramp_steps = tuple(
                int(part.strip())
                for part in env.get("RAMP_STEPS", "10,50,100").split(",")
            )
        except ValueError as exc:
            raise ConfigError("RAMP_STEPS must be comma-separated integers") from exc
        if not ramp_steps or any(step < 0 for step in ramp_steps):
            raise ConfigError("RAMP_STEPS must contain non-negative user counts")

        tier_weights = (
            _weight(env, "TIER1_WEIGHT", 60),
            _weight(env, "TIER2_WEIGHT", 30),
            _weight(env, "TIER3_WEIGHT", 10),
        )
        discovery_weight = _weight(env, "DISCOVERY_WEIGHT", 10)
        if sum(tier_weights) + discovery_weight <= 0:
            raise ConfigError("workload weights must sum to a positive value")
        think_min = _number(env, "THINK_TIME_MIN", 1.0)
        think_max = _number(env, "THINK_TIME_MAX", 3.0)
        if think_min > think_max:
            raise ConfigError("THINK_TIME_MIN must not exceed THINK_TIME_MAX")

        run_time = env.get("RUN_TIME", "15m")
        catalog = Path(env.get("SCENARIO_CATALOG", "scenarios.json"))
        if not catalog.is_absolute():
            module_dir = Path(__file__).resolve().parent
            runtime_dir = module_dir.parent if module_dir.name == "loadtest" else module_dir
            catalog = runtime_dir / catalog
        return cls(
            target_host=host,
            load_profile=profile,
            users=_integer(env, "USERS", 100, minimum=0),
            spawn_rate=_number(env, "SPAWN_RATE", 5.0, minimum=0.001),
            run_time=run_time,
            run_time_seconds=parse_duration(run_time),
            ramp_steps=ramp_steps,
            spike_users=_integer(env, "SPIKE_USERS", 100, minimum=1),
            spike_hold_seconds=_integer(env, "SPIKE_HOLD_SECONDS", 120, minimum=1),
            random_seed=_integer(env, "RANDOM_SEED", 20261030),
            scenario_catalog=catalog,
            bootstrap_mode=bootstrap,
            request_timeout=_number(env, "REQUEST_TIMEOUT", 120.0, minimum=0.1),
            think_time_min=think_min,
            think_time_max=think_max,
            tier_weights=tier_weights,
            discovery_weight=discovery_weight,
            slo_p95_ms=_integer(env, "SLO_P95_MS", 3000, minimum=1),
            slo_error_percent=_number(env, "SLO_ERROR_PERCENT", 1.0, minimum=0),
        )
