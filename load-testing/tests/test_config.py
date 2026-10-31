from __future__ import annotations

import pytest

from loadtest.config import ConfigError, Settings, parse_duration


def test_defaults_are_safe_and_complete() -> None:
    settings = Settings.from_env({})
    assert settings.target_host.startswith("https://")
    assert settings.load_profile == "ramp"
    assert settings.ramp_steps == (10, 50, 100)
    assert settings.run_time_seconds == 900
    assert settings.tier_weights == (60, 30, 10)


def test_spike_overrides_and_seed_are_deterministic() -> None:
    settings = Settings.from_env({
        "LOAD_PROFILE": "spike", "USERS": "20", "SPAWN_RATE": "2.5",
        "RUN_TIME": "90s", "SPIKE_USERS": "75", "RANDOM_SEED": "42",
    })
    assert settings.load_profile == "spike"
    assert settings.users == 20
    assert settings.spawn_rate == 2.5
    assert settings.spike_users == 75
    assert settings.random_seed == 42


@pytest.mark.parametrize("value, seconds", [("60", 60), ("2m", 120), ("1h", 3600)])
def test_parse_duration(value: str, seconds: int) -> None:
    assert parse_duration(value) == seconds


@pytest.mark.parametrize(
    "env",
    [
        {"TARGET_HOST": "http://example.com"},
        {"TARGET_HOST": "https://user:secret@example.com"},
        {"LOAD_PROFILE": "invalid"},
        {"USERS": "-1"},
        {"RUN_TIME": "0s"},
        {"SPAWN_RATE": "0"},
        {"SPAWN_RATE": "NaN"},
        {"REQUEST_TIMEOUT": "Infinity"},
        {"TIER1_WEIGHT": "0", "TIER2_WEIGHT": "0", "TIER3_WEIGHT": "0", "DISCOVERY_WEIGHT": "0"},
    ],
)
def test_invalid_configuration_is_rejected(env: dict[str, str]) -> None:
    with pytest.raises(ConfigError):
        Settings.from_env(env)


def test_local_http_requires_explicit_opt_in() -> None:
    settings = Settings.from_env({
        "TARGET_HOST": "http://127.0.0.1:8000", "ALLOW_INSECURE_LOCAL": "true"
    })
    assert settings.target_host == "http://127.0.0.1:8000"
