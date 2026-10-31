"""Azure and local Locust entry point; importing this file performs no I/O."""

from locust import LoadTestShape, events

try:
    from loadtest.bootstrap import bootstrap_catalog
    from loadtest.config import Settings
    from loadtest.scenarios import load_catalog
    from loadtest.shapes import ProfileSchedule
    from loadtest.users import TableMakerUser
except ImportError:  # Azure places uploaded files in a single directory.
    from bootstrap import bootstrap_catalog
    from config import Settings
    from scenarios import load_catalog
    from shapes import ProfileSchedule
    from users import TableMakerUser


SETTINGS = Settings.from_env()
SCHEDULE = ProfileSchedule(SETTINGS)


@events.test_start.add_listener
def prepare_scenarios(environment, **_kwargs) -> None:
    environment.piptm_bootstrap_failed = False
    try:
        scenarios = load_catalog(SETTINGS.scenario_catalog)
        if SETTINGS.bootstrap_mode == "curated":
            environment.piptm_scenarios = scenarios
            return
        client = TableMakerUser(environment).client
        environment.piptm_scenarios = bootstrap_catalog(
            client, scenarios, SETTINGS.bootstrap_mode, SETTINGS.request_timeout
        )
    except Exception:
        environment.piptm_bootstrap_failed = True
        environment.piptm_scenarios = ()
        if environment.runner is not None:
            environment.runner.quit()
        raise


class ConfiguredLoadShape(LoadTestShape):
    def tick(self):
        return SCHEDULE.tick(self.get_run_time())


__all__ = ["ConfiguredLoadShape", "TableMakerUser"]
