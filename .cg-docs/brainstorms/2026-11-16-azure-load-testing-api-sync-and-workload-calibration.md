---
date: 2026-11-16
title: "Azure Load Testing Suite: API Sync, Scaling Model, and Workload Parameter Calibration"
status: decided
scope: "Standard"
artifact-schema-version: 1
chosen-approach: "Approach 1 - Full Endpoint & Parameter-Matrix Rewrite with Tiered SLOs"
tags: [load-testing, stress-test, azure-load-testing, locust, api, plumber, performance, slo, metadata, cell-definition]
---
<!-- Valid status values: decided, in-progress, abandoned -->

# Azure Load Testing Suite: API Sync, Scaling Model, and Workload Parameter Calibration

## Context

This brainstorm follows up on `.cg-docs/brainstorms/2026-10-30-azure-load-testing-stress-suite.md`, which designed and implemented the original `load-testing/` Python Locust + Azure Load Testing bundle for the Table Maker API. Since that work was merged, the Plumber API (`inst/plumber/plumber.R`) underwent several contract changes on the current branch (`feat/azure-load-testing-stress-script`), and the team needs to (1) resynchronize the load-testing script with those changes, (2) clarify how Azure Load Testing scaling actually works and what is defined locally vs. owned by ITS, and (3) jointly decide realistic concurrency levels and `/table` parameter combinations instead of guessing.

### API Changes Discovered (Breaking the Current `load-testing/` Script)

1. **`/description` endpoint removed, replaced by `GET|POST /metadata`.** The old script posts `{"description_metadata": ...}` to `POST /description`, which no longer exists. `/metadata` now accepts the same table parameters as `/table` directly (`pip_id`, `analysis_var`, `measures`, `poverty_line`, `column`, `row`, `superColumn`, `superRow`, `filter_base`, `ppp`, `pop_share_threshold`, `release`) plus `format` (`"html"` or `"markdown"`), runs with zero Arrow microdata I/O (`< 5 ms`, manifest/registry-only), and explicitly returns HTTP 400 if sent a legacy `description_metadata` body.
2. **`include_metadata` removed from `GET|POST /table`.** `/table` now always returns the Approach C `{layout, cells}` structure; the old script's `metadata_mode` branch (flat rows + `meta.description_metadata`) is dead code against the current API.
3. **New `GET /cell-definition` endpoint** added for the Page 2 live "what each cell means" preview, not yet covered by any user journey.
4. **Layout slot rule relaxed**: `column` and `row` are independent (`row` does not require `column`); only `superColumn → column` and `superRow → row` are required. `loadtest/scenarios.py` currently rejects non-contiguous slot combinations, which is stricter than the API.
5. **Live-bootstrap bug**: `loadtest/bootstrap.py` calls `GET /surveys-ui` and expects a `dimensions` field that endpoint does not return (only `GET /surveys`, or the intersection logic behind `GET /categories?pip_id=...` / `GET /covariates?pip_id=...`, expose per-survey dimension coverage).
6. **Existing catalog bug found during this brainstorm**: `scenarios.json`'s `tier2-filtered-binary-shares` scenario uses `"analysis_var": "gender"`, which per `inst/extdata/tm_variable_spec.yaml` is a `filter`/`covariate`-only variable, never a valid `analysis_var`. The UI only ever lets a user pick **one** `analysis_var` at a time, with `measures` constrained to that variable's allowed `stat_groups`.

## Requirements

### 1. Endpoint Coverage — Match the Real Page 1 → 2 → 3 UI Flow
Cover all 13 active UI/system endpoints (excluding optional developer endpoints `/surveys`, `/lookup`, `/dimensions`, and the removed `/session/*` and `/description`):
- **System**: `GET /health`, `GET /releases`.
- **Page 1 (Survey Selection)**: `GET /regions`, `GET /countries`, `GET /surveys-ui`.
- **Page 2 (Table Configuration)**: `GET /categories` (and `?pip_id=...` intersection), `GET /analysis-variables`, `GET /statistics`, `GET /covariates` (and `?pip_id=...` intersection), `GET /cell-definition`.
- **Page 3 (Compute & Export)**: `GET|POST /table`, `GET|POST /metadata`, `POST /table/download`.

No dedicated "bootstrap" call to `GET /surveys` is needed: the real UI never calls it. Virtual users instead discover valid filter/layout variables the same way real users do — via `GET /categories?pip_id=...` and `GET /covariates?pip_id=...` after selecting surveys on Page 1.

### 2. Valid `analysis_var` / `measures` Combinations (Enforced, Not Guessed)
Per `inst/extdata/tm_variable_spec.yaml` and `tm_measure_spec.yaml`, a user selects exactly **one** `analysis_var`, and `measures` must come only from that variable's allowed `stat_groups`:
- `welfare` → `summary_statistics` and/or `inequality` (`mean`, `median`, `sd`, `var`, `min`, `max`, `nobs`, `p10`, `p25`, `p75`, `p90`, `sum`, `gini`, `mld`).
- `pov_status` → `poverty` only (`headcount`, `poverty_gap`, `severity`, `watts`, `pop_poverty`); requires `poverty_line`.
- Continuous non-welfare (`age`, `educy`, `hsize`) → `summary_statistics` only.
- Binary variables (`electricity`, `imp_wat_rec`, `imp_san_rec`, `female`, `male`, `urban`, `rural`, `youth`, `prime_work_age`, `elderly`, `employed`, `unemployed`, `in_labor_force`, `self_emp`, `paid_emp`, `inf_emp`, `employer`, `emp_agri`, `emp_indu`, `emp_serv`, `emp_others`) → `shares` only (`pop_share`, `target_within_group_share`, `target_survey_share`).
- Categorical/filter/layout variables (`gender`, `area`, `age_group`, `age_group2`, `hsize_group`, `educat4`, `educat5`, `educat7`, `wquintile`, `lstatus`, `lstatus_year`, `empstat*`, `industrycat4*`, `industrycat10*`, plus `imp_wat_rec`/`imp_san_rec`/`electricity`/`pov_status` which double as layout slots) are valid only in `filter_base` or layout slots (`column`, `row`, `superColumn`, `superRow`), never as `analysis_var` for those that lack the `analysis_var` role.

The existing `scenarios.py` validator and `scenarios.json` catalog must be corrected and expanded to enforce and exercise this matrix, replacing the invalid `gender`-as-`analysis_var` scenario.

### 3. Realistic Concurrency Calibration
Anchored on ~2M requests/month platform-wide (~0.8 RPS average), the default `RAMP_STEPS` become **`2,5,15,30`** concurrent users (normal peak → busy hour → launch/workshop burst → extreme stress), with `SPIKE_USERS` in the `30-50` range for the spike profile. `engineInstances: 1` remains sufficient — Azure Load Testing engine capacity (client-side request generation) is independent of and far exceeds this concurrency range; it is not the bottleneck being tested. The bottleneck under test is the single-threaded-per-replica R Plumber container, whose horizontal/vertical scaling is an ITS-owned Container Apps configuration, informed by (not decided within) this test suite.

### 4. Tiered Latency SLOs (Replacing the Single Global `p95 <= 3000ms`)
Because Page 1/2 calls and `/metadata` are in-memory (no Arrow I/O, 5-50ms) while `/table` cost scales with the combination of survey count, active layout slots, filter complexity, and measure cost (fast linear-scan stats vs. heavy sorting/quantile stats like `median`/`p10`-`p90`/`gini`/`mld`), define 3 latency classes instead of one:
- **Class A — UI/Discovery/Preview/Metadata** (`/health`, `/releases`, `/regions`, `/countries`, `/surveys-ui`, `/categories`, `/analysis-variables`, `/statistics`, `/covariates`, `/cell-definition`, `/metadata`): `p95 <= 1000 ms`.
- **Class B — Light & Typical `/table` + `/table/download`** (Tier 1: 1-2 surveys, 0-1 slot, 1-2 fast measures; Tier 2: 3-5 surveys, 1-2 slots, 1-2 active filters, 2-4 mixed measures): `p95 <= 3000 ms`.
- **Class C — Heavy `/table`** (Tier 3: triggered by either many surveys (8-15) across 2-4 slots, OR parameter-heavy combinations — fewer surveys with 3-4 slots + `filter_base` + quantile/inequality measures): `p95 <= 5000 ms`.
- Global error rate gate remains `< 1%` (HTTP failures + semantic validation failures).
- Separate tracking prevents fast Class A calls from diluting/hiding a slow `/table` regression in a single blended `p95`.

### 5. Concurrency Modeling
Support **both**:
- **Mixed realistic traffic** (current default): each virtual user randomly follows the weighted Page 1→2→3 journey with 1-3s think time between actions.
- **"Same-query stampede" mode**: all concurrent users run a pinned scenario (configurable target tier, defaulting to Tier 2 or Tier 3) simultaneously, to directly answer "what happens if N users all request the same heavy table at once" (head-of-line blocking stress).

### 6. Edge Cases — Partial Survey Exclusion & Missing-Dimension 422s
Matches real UI behavior: if 1+ (but not all) selected surveys lack a requested filter/covariate dimension, `/table` returns `200 OK` with the remaining surveys and a warning — this is accepted as success as long as `1 <= returned surveys <= requested surveys`. A small dedicated scenario where **all** selected surveys lack a required dimension is included and validated as an **expected** `HTTP 422` (counted as a pass when that exact error occurs, not a failure).

## Approaches Considered

### Approach 1: Full Endpoint & Parameter-Matrix Rewrite with Tiered SLOs (Chosen)
Rewrite `loadtest/scenarios.py`, `scenarios.json`, `loadtest/bootstrap.py`, `loadtest/users.py`, and `azure-load-test.yaml` together: follow the real Page 1→2→3 flow with all 13 endpoints, replace `/description`/`include_metadata` with `GET|POST /metadata`, add `GET /cell-definition`, expand the scenario catalog to cover the validated `analysis_var` x `measures` x slot x `filter_base` matrix, replace the single global SLO with the 3 latency classes, and recalibrate `RAMP_STEPS` to `2,5,15,30` (spike ceiling `30-50`) on `engineInstances: 1`.
- **Pros**: Fixes all breaking API mismatches at once; scenario catalog is self-documenting proof of UI-valid combinations only; tiered SLOs separate "UI frozen" signal from "heavy compute slow" signal; realistic concurrency avoids both wasted ITS review time and false breaking-point alarms.
- **Cons**: Larger diff across ~6 files + tests; requires scenario-by-scenario validation against the variable/measure spec during implementation (user requested confirmation checkpoints).
- **Effort**: Medium (1.5-2 days).

### Approach 2: Minimal Patch (Fix Only What's Broken)
Only fix the 3 breaking calls (`/table` `include_metadata`, `/description` -> `/metadata`, `/surveys-ui` dimensions bug); leave scenario coverage, concurrency levels, and SLOs unchanged.
- **Pros**: Smallest diff, fastest to unblock running the suite again.
- **Cons**: Leaves the unrealistic `10,50,100` ramp, the misleading single global SLO, the invalid `gender`-as-`analysis_var` scenario, and no `/cell-definition`/Page-2-filtered discovery; does not address the scaling/parameter understanding gaps that motivated this brainstorm.
- **Effort**: Small (2-4 hours).

### Approach 3: Full Rewrite + Dynamic Runtime Scenario Generation
Same endpoint/SLO scope as Approach 1, but replace the static `scenarios.json` catalog with a generator that builds valid random parameter combinations at runtime from live `/analysis-variables`, `/statistics`, and `/covariates` responses, rather than curated static scenarios.
- **Pros**: Automatically adapts if the variable/measure spec changes again; no catalog to keep manually in sync.
- **Cons**: Harder to reproduce and reason about failures; loses deterministic-seed repeatability; substantially more engineering effort to encode all UI validity rules (single-`analysis_var` constraint, slot contiguity rules, `poverty_line` triggers) as generation logic instead of reviewable static data.
- **Effort**: Large (3+ days).

## Decision

**Chosen Approach**: **Approach 1 - Full Endpoint & Parameter-Matrix Rewrite with Tiered SLOs**, confirmed by the user.

**Why**:
- Resolves all 5 identified API-contract breakages plus the newly discovered `gender`-as-`analysis_var` catalog bug in one coordinated pass instead of a second disruptive rewrite later.
- Directly answers all three original questions: endpoint correctness, how Azure scaling/engines relate to API-side container scaling, and what realistic concurrency/parameter values should be used — all jointly decided with the user rather than guessed.
- Tiered latency classes and the dual mixed/stampede concurrency model give actionable, non-misleading signal to both the team and ITS.
- Effort (1.5-2 days) is proportional: the riskiest part is UI-rule correctness in the scenario catalog, which Approach 2 would leave broken and Approach 3 would solve with disproportionate engineering cost.

**Flagged risk (not a blocker)**: The `2,5,15,30` ramp calibration is anchored on platform-wide (not Table-Maker-specific) usage (~2M requests/month across the broader PIP platform), used only as an order-of-magnitude estimate. It should be revisited once Table-Maker-specific usage data exists post-launch.

## Next Steps

1. Run `/cg-plan` to design the implementation plan for `load-testing/`, covering:
   - **Catalog rewrite** (`scenarios.json`, `loadtest/scenarios.py`): correct the invalid `gender`-as-`analysis_var` scenario; expand to ~12-15 scenarios spanning 0/1/2/3/4 layout slots (including `row`-only and `pov_status`-in-slot), single/multi-variable `filter_base`, all four `analysis_var` classes (`welfare`, `pov_status`, continuous non-welfare, binary), fast vs. quantile/inequality measure bundles, `ppp` variants, and `pop_share_threshold`; relax the slot-contiguity validator to match the API's actual `column`/`row` independence rule; add a dedicated expected-422 missing-dimension scenario.
   - **Endpoint/journey rewrite** (`loadtest/users.py`, `loadtest/bootstrap.py`): remove `include_metadata`/`/description` usage; add `GET|POST /metadata` (`html`/`markdown`) chain from `/table`; add `GET /cell-definition` to the Page 2 flow; add `/regions`, `/countries` to Page 1 discovery; call `/categories?pip_id=...` and `/covariates?pip_id=...` with the user's selected surveys (matching real Page 2 behavior) instead of unfiltered-only; fix or remove the `/surveys-ui` dimensions bootstrap assumption.
   - **Tiered SLOs** (`azure-load-test.yaml`, `loadtest/config.py`): implement the 3 latency-class `failureCriteria`/reporting (Class A `<= 1000ms`, Class B `<= 3000ms`, Class C `<= 5000ms`, global error rate `< 1%`), with request-name tagging that lets Azure/Locust attribute each request to its class.
   - **Concurrency recalibration**: default `RAMP_STEPS=2,5,15,30`, `SPIKE_USERS` in `30-50`, `engineInstances: 1`; add the "same-query stampede" mode (pinned-scenario concurrent mode) as a configurable alternative to the existing mixed-traffic default.
   - **Edge-case handling**: partial-exclusion acceptance logic (`1 <= returned <= requested`) and expected-422 validation for the missing-dimension scenario in `loadtest/api.py`.
   - **Validation checkpoints**: per the user's request, confirm each new/changed scenario's `analysis_var`/`measures`/slot/`filter_base` combination against `tm_variable_spec.yaml`/`tm_measure_spec.yaml` during implementation rather than only at the end.
   - Update `README.md` (configuration table, SLO section, Azure handoff notes) and the `tests/` suite (`test_scenarios.py`, `test_users.py`, `test_bootstrap.py`, `test_azure_config.py`, `test_api.py`) to reflect all of the above.
