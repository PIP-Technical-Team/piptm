"""Deterministic ramp and spike load profiles."""

from __future__ import annotations

try:
    from .config import Settings
except ImportError:
    from config import Settings


class ProfileSchedule:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def ramp(self, elapsed: float) -> tuple[int, float] | None:
        step_seconds = self.settings.run_time_seconds / len(self.settings.ramp_steps)
        index = int(elapsed // step_seconds)
        if index >= len(self.settings.ramp_steps):
            return None
        return min(self.settings.ramp_steps[index], self.settings.users), self.settings.spawn_rate

    def spike(self, elapsed: float) -> tuple[int, float] | None:
        if elapsed >= self.settings.run_time_seconds:
            return None
        baseline = min(self.settings.ramp_steps[0], self.settings.users)
        warmup = min(60, max(1, self.settings.run_time_seconds // 5))
        if elapsed < warmup:
            return baseline, self.settings.spawn_rate
        if elapsed < warmup + self.settings.spike_hold_seconds:
            return self.settings.spike_users, self.settings.spawn_rate
        return baseline, self.settings.spawn_rate

    def tick(self, elapsed: float) -> tuple[int, float] | None:
        return self.ramp(elapsed) if self.settings.load_profile == "ramp" else self.spike(elapsed)
