"""Deterministic ramp and spike load profiles."""

try:
    from .config import Settings
except ImportError:
    from config import Settings


class ProfileSchedule:
    """Time-sliced ramp or spike schedule. Always returns at least 1 user."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def tick(
        self,
        elapsed_seconds: float,
        total_seconds: int | None = None,
    ) -> tuple[int, float] | None:
        total = total_seconds or self.settings.run_time_seconds
        if elapsed_seconds >= total:
            return None

        if self.settings.load_profile == "spike":
            baseline = max(1, min(self.settings.users, self.settings.ramp_steps[0]))
            warmup = min(60, max(1, total // 5))
            hold_end = min(total, warmup + self.settings.spike_hold_seconds)
            users = self.settings.spike_users if warmup <= elapsed_seconds < hold_end else baseline
            return (max(1, users), self.settings.spawn_rate)

        steps = self.settings.ramp_steps
        step_length = total / len(steps)
        index = min(len(steps) - 1, int(elapsed_seconds // step_length))
        users = max(1, min(self.settings.users, steps[index]))
        return (users, self.settings.spawn_rate)
