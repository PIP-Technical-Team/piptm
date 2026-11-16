from __future__ import annotations

import pytest

from loadtest.config import ConfigError, Settings, parse_duration


def test_defaults_are_calibrated_and_complete() -> None:
    settings = Settings.from_env({})
    assert settings.target_host.startswith("https://")
    assert settings.load_profile == "ramp"
    assert settings.ramp_steps == (2, 5, 15, 30)
    assert settings.users == 30
    assert settings.spike_users == 40
    assert settings.traffic_mode == "mixed"
    assert settings.stampede_tier == 3
    assert settings.stampede_scenario_id is None
    assert settings.slo_class_a_p95_ms == 1000
    assert settings.slo_class_b_p95_ms == 3000
    assert settings.slo_class_c_p95_ms == 5000
    assert settings.run_time_seconds == 900
    assert settings.tier_weights == (60, 30, 10)


def test_spike_and_stampede_overrides_and_seed_are_deterministic() -> None:
    settings = Settings.from_env(
        {
            "LOAD_PROFILE": "spike",
            "USERS": "20",
            "SPAWN_RATE": "2.5",
            "RUN_TIME": "90s",
            "SPIKE_USERS": "45",
            "RANDOM_SEED": "42",
            "TRAFFIC_MODE": "stampede",
            "STAMPEDE_TIER": "2",
            "STAMPEDE_SCENARIO_ID": "tier2-download-rds",
        }
    )
    assert settings.load_profile == "spike"
    assert settings.users == 20
    assert settings.spawn_rate == 2.5
    assert settings.spike_users == 45
    assert settings.random_seed == 42
    assert settings.traffic_mode == "stampede"
    assert settings.stampede_tier == 2
    assert settings.stampede_scenario_id == "tier2-download-rds"


@pytest.mark.parametrize("value, seconds", [("60", 60), ("2m", 120), ("1h", 3600)])
def test_parse_duration(value: str, seconds: int) -> None:
    assert parse_duration(value) == seconds


@pytest.mark.parametrize(
    "env",
    [
        {"TARGET_HOST": "http://example.com"},
        {"TARGET_HOST": "https://user:secret@example.com"},
        {"LOAD_PROFILE": "invalid"},
        {"TRAFFIC_MODE": "invalid"},
        {"STAMPEDE_TIER": "4"},
        {"USERS": "-1"},
        {"RUN_TIME": "0s"},
        {"SPAWN_RATE": "0"},
        {"SPAWN_RATE": "NaN"},
        {"REQUEST_TIMEOUT": "Infinity"},
        {
            "TIER1_WEIGHT": "0",
            "TIER2_WEIGHT": "0",
            "TIER3_WEIGHT": "0",
            "DISCOVERY_WEIGHT": "0",
        },
    ],
)
def test_invalid_configuration_is_rejected(env: dict[str, str]) -> None:
    with pytest.raises(ConfigError):
        Settings.from_env(env)


def test_local_http_requires_explicit_opt_in() -> None:
    settings = Settings.from_env(
        {"TARGET_HOST": "http://127.0.0.1:8000", "ALLOW_INSECURE_LOCAL": "true"}
    )
    assert settings.target_host == "http://127.0.0.1:8000"
