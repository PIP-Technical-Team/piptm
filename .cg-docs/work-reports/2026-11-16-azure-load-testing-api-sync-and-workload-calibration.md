# Execution Report: Azure Load Testing API Sync and Workload Calibration

- **Plan reference**: `.cg-docs/plans/2026-11-16-azure-load-testing-api-sync-and-workload-calibration.md`
- **Active deviation policy**: `ask` (stored: `ask`, runtime override: none)
- **Started**: 2026-11-16
- **Completed**: 2026-11-16

## Completed Steps / Phases

- **Phase 1: Contract and catalog** — Completed 2026-11-16
  - Step 1: Confirmed the 13-scenario matrix with the user.
  - Step 2: Rewrote `load-testing/loadtest/scenarios.py`, `load-testing/scenarios.json`, and `load-testing/tests/test_scenarios.py` (18/18 scenario tests passing, ruff clean).
- **Phase 2: Journeys, gates, and handoff** — Completed 2026-11-16
  - Step 3: Aligned `loadtest/users.py`, `loadtest/api.py`, and `loadtest/bootstrap.py` with the 13 active system/UI endpoints (`/health`, `/releases`, `/regions`, `/countries`, `/surveys-ui`, `/categories`, `/analysis-variables`, `/statistics`, `/covariates`, `/cell-definition`, `/table`, `/metadata`, `/table/download`), removed `/description` and `include_metadata`, added partial survey exclusion acceptance (`1 <= returned <= requested`), expected-422 validation (`tier1-expected-422-missing-dim`), and `TRAFFIC_MODE=mixed|stampede`.
  - Step 4: Recalibrated `loadtest/config.py`, `azure-load-test.yaml`, and `.env.example` to `RAMP_STEPS=2,5,15,30`, `USERS=30`, `SPIKE_USERS=40`, `engineInstances: 1`, and Class A/B/C p95 targets (`1000ms` / `3000ms` / `5000ms`) with request-name latency-class tagging.
  - Step 5: Updated `load-testing/README.md` and verified the complete offline suite (`53 passed in 1.03s`) and `ruff check load-testing` (`All checks passed!`).

## Deviations

- None.

## Accepted Exceptions

- None.

## Evidence Table

| ID | Evidence Required | Command/Artifact | Required | Status |
|----|-------------------|------------------|----------|--------|
| V1 | Catalog rejects invalid `analysis_var`/`measures` pairs and the old `gender` scenario is gone | `load-testing\.venv\Scripts\pytest.exe load-testing/tests/test_scenarios.py` | yes | passed (18/18) |
| V2 | Journeys call the 13 agreed endpoints and never call removed or excluded endpoints | `load-testing\.venv\Scripts\pytest.exe load-testing/tests/test_users.py` | yes | passed (4/4) |
| V3 | `row`-only slots are allowed; parent-slot rules still hold | `load-testing\.venv\Scripts\pytest.exe load-testing/tests/test_scenarios.py` | yes | passed (18/18) |
| V4 | Partial exclusion passes and the dedicated 422 is an expected pass | `load-testing\.venv\Scripts\pytest.exe load-testing/tests/test_api.py` | yes | passed (7/7) |
| V5 | YAML uses one engine, ramp `2,5,15,30`, and classed SLO gates or a documented fallback | `load-testing\.venv\Scripts\pytest.exe load-testing/tests/test_azure_config.py` | yes | passed (2/2) |
| V6 | Offline suite and lint pass | `load-testing\.venv\Scripts\pytest.exe load-testing/tests`; `load-testing\.venv\Scripts\ruff.exe check load-testing` | yes | passed (53/53, 0 lint errors) |

## Constraints Check

| ID | Constraint | Check | Status |
|----|------------|-------|--------|
| C1 | No R package, Plumber, or API behavior changes | Diff limited to `load-testing/` and `.cg-docs/` / `roadmap.json` | passed |
| C2 | No secrets, real subnet IDs, or credentials committed | YAML and README scan (`test_azure_config.py`) | passed |
| C3 | Azure flat-import fallback and deterministic seed stay intact | `locustfile.py`, `test_users.py`, `test_config.py` | passed |
| C4 | Offline tests do not claim live capacity | `load-testing/README.md` distinguishes offline tests vs Azure runs | passed |

## Remaining Uncertainty

- None for offline implementation; authoritative capacity and latency measurements require an ITS-managed Azure Load Testing run with VNet injection against the QA environment, and `2,5,15,30` user ramp steps should be revisited once post-launch Table Maker telemetry is available.

## Final Status

- `completed`
