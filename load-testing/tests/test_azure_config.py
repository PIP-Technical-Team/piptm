from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_azure_yaml_uses_locust_v01_and_complete_upload_set() -> None:
    document = yaml.safe_load((ROOT / "azure-load-test.yaml").read_text(encoding="utf-8"))
    assert document["version"] == "v0.1"
    assert document["testType"] == "Locust"
    assert re.fullmatch(r"[a-zA-Z0-9-_]+", document["testId"])
    paths = [
        document["testPlan"],
        document["properties"]["userPropertyFile"],
        *document["configurationFiles"],
    ]
    assert all((ROOT / path).is_file() for path in paths)
    basenames = [Path(path).name for path in paths]
    assert len(basenames) == len(set(basenames))
    assert all(".." not in Path(path).parts and not Path(path).is_absolute() for path in paths)


def test_failure_criteria_and_environment_match_runtime_config() -> None:
    text = (ROOT / "azure-load-test.yaml").read_text(encoding="utf-8")
    document = yaml.safe_load(text)
    criteria = document["failureCriteria"]["clientMetrics"]
    assert "p95(response_time_ms) > 3000" in criteria
    assert "percentage(error) > 1" in criteria
    env_names = {item["name"] for item in document["env"]}
    assert {"TARGET_HOST", "LOAD_PROFILE", "RUN_TIME", "BOOTSTRAP_MODE"} <= env_names
    assert not re.search(r"(?i)(password|token|secret)\s*[:=]\s*[^<\s]", text)
