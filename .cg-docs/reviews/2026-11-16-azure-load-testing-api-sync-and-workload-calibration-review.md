---
date: 2026-11-16
depth: architecture
type: standard
plan: .cg-docs/plans/2026-11-16-azure-load-testing-api-sync-and-workload-calibration.md
findings:
  P1.1: fixed
  P1.2: fixed
  P1.3: fixed
  P1.4: fixed
  P1.5: fixed
  P1.6: fixed
  P1.7: fixed
  P1.8: fixed
  P1.9: fixed
  P2.1: fixed
  P2.2: fixed
  P2.3: fixed
  P2.4: fixed
  P2.5: fixed
  P2.6: fixed
  P2.7: fixed
  P2.8: fixed
  P2.9: fixed
  P2.10: fixed
  P2.11: fixed
  P3.1: fixed
  P3.2: fixed
  P3.3: fixed
---

## Review Report

**Review mode**: architecture
**Files reviewed**: 15
**Findings**: 23 (P0: 0, P1: 9, P2: 11, P3: 3)

### ⚠️ Incomplete Reviews
- `@cg-code-quality` did not produce usable output. Consider re-running `/cg-review` with a higher model tier, or invoke `@cg-code-quality` directly.

### P0 — BLOCKING (immediate remediation required)
None.

### P1 — CRITICAL (must fix before merge)

- **[P1.1]** [cg-architecture / cg-reproducibility] [safe_auto] `load-testing/azure-load-test.yaml:12` — `configurationFiles` omits the `loadtest/*.py` runtime modules required by `locustfile.py`
  **Why**: Azure Load Testing only uploads `testPlan` (`locustfile.py`), `properties.userPropertyFile` (`locust.conf`), and the files listed in `configurationFiles`. Omitting `loadtest/api.py`, `loadtest/bootstrap.py`, `loadtest/config.py`, `loadtest/scenarios.py`, `loadtest/shapes.py`, and `loadtest/users.py` causes `ModuleNotFoundError` on Azure engines.
  **Fix**: Add all six `loadtest/*.py` modules to `configurationFiles` in `load-testing/azure-load-test.yaml` and assert their presence in `load-testing/tests/test_azure_config.py`.

- **[P1.2]** [cg-architecture / cg-reproducibility] [safe_auto] `load-testing/loadtest/users.py:14` & `load-testing/loadtest/bootstrap.py:8` — Unconditional relative imports fail when Azure flattens uploaded files
  **Why**: Azure Load Testing flattens uploaded `configurationFiles` into a single directory alongside `locustfile.py`. Unlike `locustfile.py` and `shapes.py`, `users.py` and `bootstrap.py` lack `try ... except ImportError` flat-directory fallbacks.
  **Fix**: Wrap relative imports in `loadtest/users.py` and `loadtest/bootstrap.py` with `try: from .<mod> import ... except ImportError: from <mod> import ...` fallbacks.

- **[P1.3]** [cg-data-quality] [manual] `load-testing/scenarios.json:113` — `tier2-filtered-female-shares` uses non-integer `filter_base` code `"25-64"` for `age_group`, which the API rejects with HTTP 422
  **Why**: Parquet stores `age_group` as integer codes (`1`–`4`), and `.validate_and_normalize_table_params()` (`R/table_params.R:234-258`) rejects non-integer `filter_base` values (`as.numeric("25-64")` is `NA`).
  **Fix**: Change `"filter_base": {"age_group": ["25-64"]}` to `"filter_base": {"age_group": [3]}` in `load-testing/scenarios.json`.

- **[P1.4]** [cg-data-quality] [safe_auto] `load-testing/loadtest/scenarios.py:242` — `_validate()` does not enforce integer category codes in `filter_base`
  **Why**: `_validate()` allows non-integer strings or booleans in `filter_base` lists even though the API requires integer category codes.
  **Fix**: Reject non-integer or boolean values in `filter_base` lists during catalog validation.

- **[P1.5]** [cg-data-quality] [manual] `load-testing/scenarios.json:75` & `load-testing/loadtest/scenarios.py:36` — `tier1-expected-422-missing-dim` uses `industrycat10_2_year`, which is not in `piptm.optional_vars` (`R/zzz.R:39-58`) and returns HTTP 400 instead of HTTP 422
  **Why**: `validate_table_input()` checks slots against `piptm::pip_valid_dimensions()` (`piptm.optional_vars`) before calling `table_maker()` and returns HTTP 400 (`Unknown dimension(s): industrycat10_2_year`).
  **Fix**: Restrict `VALID_FILTER_VARS` and `ANALYSIS_VAR_GROUPS` in `scenarios.py` to variables enabled in `piptm.optional_vars` (`R/zzz.R`), and use an enabled optional dimension (e.g., `imp_wat_rec` or `industrycat10`) that triggers a 422 exclusion when missing from the target survey.

- **[P1.6]** [cg-testing] [safe_auto] `load-testing/tests/test_users.py:1` — `TableMakerUser` execution paths (`mixed` vs `stampede` mode, `_execute_scenario`, `_run_cell_definition_preview`, `_run_downstream`, `_select_stampede_scenario`) lack behavioral unit test coverage
  **Why**: `test_users.py` only tests `get_query`, `table_request_name`, and static strings.
  **Fix**: Add behavioral unit tests in `load-testing/tests/test_users.py` with a mock Locust client exercising mixed, stampede, preview, downstream (`metadata_html`, `metadata_markdown`, `download_csv`, `download_rds`), and 422 paths.

- **[P1.7]** [cg-version-control] [safe_auto] `load-testing/.gitignore:1` — `.env` files are not ignored in `load-testing/.gitignore`
  **Why**: Developers copying `.env.example` to `.env` could accidentally stage local overrides or internal URLs.
  **Fix**: Add `.env`, `.env.*`, and `!.env.example` to `load-testing/.gitignore`.

- **[P1.8]** [cg-performance] [manual] `load-testing/loadtest/users.py:130` — `discover` task executes all 9 discovery endpoints sequentially in a single tick, inflating Class A request share to ~38% of total HTTP requests
  **Why**: Firing 9 Class A requests per discovery tick dilutes the aggregate `p95(response_time_ms) > 3000` gate in `azure-load-test.yaml` and blocks the virtual user across 9 serial round-trips.
  **Fix**: Sample one discovery endpoint (or page group) per `discover` tick via `self.rng.choice(DISCOVERY_ENDPOINTS)`, or run the full sweep once in `on_start()`.

- **[P1.9]** [cg-architecture / cg-reproducibility / cg-performance] [manual] `load-testing/locustfile.py:23` & `load-testing/loadtest/users.py:30` — Duplicate `@events.test_start` bootstrap listeners and disconnected scenario state between `locustfile.py` and `users.py`
  **Why**: Both `locustfile.py` and `users.py` register `@events.test_start` listeners; `locustfile.py` writes `environment.piptm_scenarios` while `TableMakerUser` reads `users.SCENARIOS`.
  **Fix**: Consolidate into a single `test_start` bootstrap listener and shared scenario accessor.

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-testing] [safe_auto] `load-testing/loadtest/users.py:150` — `_select_stampede_scenario` silently falls back when `STAMPEDE_SCENARIO_ID` is unknown or points to a 422 scenario
  **Why**: A typo in `STAMPEDE_SCENARIO_ID` silently runs a different scenario instead of raising `CatalogError`.
  **Fix**: Raise `CatalogError` when `STAMPEDE_SCENARIO_ID` is not found in `SCENARIOS` or has `expected_status != 200`, and add tests.

- **[P2.2]** [cg-testing] [safe_auto] `load-testing/tests/test_api.py:116` — Missing negative and RDS format test cases for `validate_cell_definition`, `validate_metadata`, and `validate_download`
  **Why**: Only happy paths for HTML/Markdown metadata and CSV download are tested.
  **Fix**: Add tests for `validate_download(..., "rds")` and `SemanticError` on malformed cell-definition, metadata, and download responses.

- **[P2.3]** [cg-testing] [safe_auto] `load-testing/tests/test_scenarios.py:65` — Missing unit test cases for `latency_class`, `expected_status` with downstream, `pop_share_threshold` bounds, and `required_covariates` coverage
  **Why**: New validation branches in `scenarios.py` are not covered by parametrized negative tests.
  **Fix**: Add parametrized mutation cases for those branches in `load-testing/tests/test_scenarios.py`.

- **[P2.4]** [cg-data-quality] [safe_auto] `load-testing/loadtest/api.py:89` — `validate_table()` and `extract_approach_c()` do not verify per-cell requested measures or `isinstance(item.get("layout"), dict)`
  **Why**: A cell with an empty `measures: {}` dict passes `validate_table()`.
  **Fix**: Check `isinstance(item.get("layout"), dict)` and `set(expected_measures).issubset(cell["measures"].keys())`.

- **[P2.5]** [cg-data-quality] [safe_auto] `load-testing/loadtest/api.py:115` — `validate_metadata()` uses `and` instead of `or` for Markdown validation and skips HTML `Content-Disposition` check
  **Why**: Using `and` allows responses missing either the `table-metadata.md` filename or `#` heading to pass.
  **Fix**: Check `"table-metadata.html" not in disposition` for HTML and `"table-metadata.md" not in disposition or "#" not in body` for Markdown.

- **[P2.6]** [cg-data-quality] [safe_auto] `load-testing/loadtest/scenarios.py:251` — `_validate()` does not validate `ppp > 0`, non-bool `ppp`, or finite `poverty_line` / `pop_share_threshold`
  **Why**: Invalid `ppp` or `NaN`/`inf` float values could pass catalog validation.
  **Fix**: Validate `ppp` is a positive non-bool integer and `poverty_line` / `pop_share_threshold` are finite when provided.

- **[P2.7]** [cg-version-control] [safe_auto] `load-testing/.gitignore:6` — Downloaded `*.rds` test artifacts are not gitignored
  **Why**: Local RDS exports from `/table/download` could be accidentally committed.
  **Fix**: Add `*.rds` to `load-testing/.gitignore`.

- **[P2.8]** [cg-reproducibility] [safe_auto] `load-testing/loadtest/users.py:124` — Global `random` module used by Locust task scheduling and `between()` wait times is unseeded
  **Why**: Seeding `random.seed(SETTINGS.random_seed + worker_offset)` in `on_start` improves run-to-run task-scheduling determinism.
  **Fix**: Call `random.seed(SETTINGS.random_seed + worker_offset)` in `TableMakerUser.on_start`.

- **[P2.9]** [cg-performance] [safe_auto] `load-testing/loadtest/users.py:261` — Duplicate `extract_approach_c` pass when chaining `/table` to `/table/download`
  **Why**: `validate_table` already calls `extract_approach_c`; returning the validated `data` list avoids a second pass in `_run_downstream`.
  **Fix**: Return `data` from `validate_table` and pass it into `_run_downstream`.

- **[P2.10]** [cg-documentation] [safe_auto] `load-testing/loadtest/*.py` — Public functions and classes (`validate_*`, `Scenario`, `load_catalog`, `choose_scenario`, `bootstrap_catalog`, `get_query`, `table_request_name`, `TableMakerUser`, `Settings`) lack Google-style docstrings
  **Why**: Public interfaces should document `Args`, `Returns`, and `Raises`.
  **Fix**: Add concise Google-style docstrings to public classes and functions across `loadtest/*.py`.

- **[P2.11]** [cg-performance / cg-architecture / cg-reproducibility] [manual] `load-testing/loadtest/users.py:236`, `load-testing/loadtest/bootstrap.py:22`, `load-testing/requirements.txt:1` — Downstream request naming by scenario tier, per-survey live covariate check on fallback pools, and dependency pinning
  **Why**: Tagging `/table/download` with `scenario.latency_class` separates Tier 3 downloads from Tier 1/2 downloads; live bootstrap fallback surveys can verify `/covariates?pip_id=...`; and pinning `requests` in `requirements.txt` improves engine reproducibility.
  **Fix**: Include `scenario.latency_class` in `download` request names, pass `pip_id` when checking fallback survey pools in `bootstrap.py`, and list `requests` in `requirements.txt` / `pyproject.toml`.

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-documentation] [safe_auto] `load-testing/README.md:72` — `SCENARIO_CATALOG` environment variable is missing from the README configuration table
  **Why**: `SCENARIO_CATALOG` is in `.env.example` and `config.py` but omitted from the README table.
  **Fix**: Add `SCENARIO_CATALOG` to the Configuration Reference table in `load-testing/README.md`.

- **[P3.2]** [cg-reproducibility] [safe_auto] `load-testing/loadtest/config.py:142` — Relative `SCENARIO_CATALOG` path fails when specified relative to repo root CWD (`load-testing/scenarios.json`)
  **Why**: `config.py` unconditionally prefixes relative paths with `runtime_dir`.
  **Fix**: Only rebase `catalog` onto `runtime_dir` when `not catalog.is_file()`.

- **[P3.3]** [cg-testing / cg-architecture] [advisory] `load-testing/tests/test_bootstrap.py:51` & `load-testing/loadtest/users.py:28` — Add explicit assertion for 422 passthrough in `test_bootstrap.py` and remove unused `ROOT` in `users.py`
  **Why**: Improves test specificity and removes a dead constant.
  **Fix**: Assert 422 scenario retention in `test_bootstrap.py` and remove `ROOT` from `users.py`.

### ✅ Passed
- `@cg-version-control`: No secrets, credentials, or hardcoded Azure resource/subnet IDs found.
