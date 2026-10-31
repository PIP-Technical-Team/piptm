from __future__ import annotations

from loadtest.config import Settings
from loadtest.shapes import ProfileSchedule


def test_ramp_boundaries_and_expiry() -> None:
    schedule = ProfileSchedule(Settings.from_env({"RUN_TIME": "90s", "RAMP_STEPS": "10,50,100"}))
    assert schedule.tick(0) == (10, 5.0)
    assert schedule.tick(30) == (50, 5.0)
    assert schedule.tick(60) == (100, 5.0)
    assert schedule.tick(90) is None


def test_users_caps_every_ramp_step() -> None:
    settings = Settings.from_env({
        "USERS": "20", "RUN_TIME": "90s", "RAMP_STEPS": "10,50,100"
    })
    schedule = ProfileSchedule(settings)
    assert schedule.tick(0)[0] == 10
    assert schedule.tick(30)[0] == 20
    assert schedule.tick(60)[0] == 20


def test_spike_warmup_hold_recovery() -> None:
    settings = Settings.from_env({
        "LOAD_PROFILE": "spike", "RUN_TIME": "5m", "RAMP_STEPS": "10,50,100",
        "SPIKE_USERS": "80", "SPIKE_HOLD_SECONDS": "120",
    })
    schedule = ProfileSchedule(settings)
    assert schedule.tick(0)[0] == 10
    assert schedule.tick(60)[0] == 80
    assert schedule.tick(181)[0] == 10
    assert schedule.tick(300) is None
