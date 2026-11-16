---
date: 2026-11-16
title: "Azure Load Testing API Sync and Workload Calibration"
status: completed
completed-date: 2026-11-16
scope: "Standard"
brainstorm: ".cg-docs/brainstorms/2026-11-16-azure-load-testing-api-sync-and-workload-calibration.md"
language: "Python"
estimated-effort: "medium"
deviation-policy: "ask"
artifact-schema-version: 1
phases: 2
completed-phases: [1, 2]
execution-report: ".cg-docs/work-reports/2026-11-16-azure-load-testing-api-sync-and-workload-calibration.md"
tags: [load-testing, azure-load-testing, locust, api, slo, metadata, cell-definition]
---

# Plan: Azure Load Testing API Sync and Workload Calibration

## Objective

Update the existing `load-testing/` Locust bundle so it matches the current Table Maker API, exercises only UI-valid parameter combinations, and reports latency by workload class under a realistic 2-to-30 user ramp.

## Context

Follow-up to `.cg-docs/plans/2026-10-30-azure-load-testing-stress-suite.md`. The original suite still calls removed `/description` and `include_metadata`, validates layout slots more strictly than the API, and uses a single blended p95 plus a 10/50/100 user ramp. Decision source: `.cg-docs/brainstorms/2026-11-16-azure-load-testing-api-sync-and-workload-calibration.md`.

The real UI flow is Page 1 (`/regions`, `/countries`, `/surveys-ui`) then Page 2 (`/categories`, `/analysis-variables`, `/statistics`, `/covariates`, `/cell-definition`) then Page 3 (`/table`, `/metadata`, `/table/download`). A user picks one `analysis_var` and only measures from that variable's `stat_groups` in `inst/extdata/tm_variable_spec.yaml` and `tm_measure_spec.yaml`. Missing dimensions may drop some surveys with a warning, or return HTTP 422 when every selected survey is excluded.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | Cover the 13 agreed system/UI endpoints and never call `/description`, `/session/*`, `/surveys`, `/lookup`, or `/dimensions`. | Brainstorm requirements 1 |
| R2 | Remove `include_metadata` and always validate Approach C `{layout, cells}` from `/table`. | `inst/plumber/plumber.R` |
| R3 | Chain `/table` to `GET|POST /metadata` with `format=html|markdown`, not `description_metadata`. | Brainstorm requirement 1 |
| R4 | Call `GET /cell-definition` in the Page 2 journey and validate its four explanation sections. | Brainstorm requirement 1 |
| R5 | Enforce one `analysis_var` and only its allowed `stat_groups`; remove the invalid `gender` analysis scenario. | `tm_variable_spec.yaml`, `tm_measure_spec.yaml` |
| R6 | Allow `row` without `column`; keep `superColumn` requiring `column` and `superRow` requiring `row`. | Brainstorm requirement 2 |
| R7 | Expand the catalog to about 12-15 scenarios covering slots, filters, analysis classes, fast vs quantile measures, `ppp`, and `pop_share_threshold`. | Brainstorm requirement 2 |
| R8 | Accept partial survey exclusion (`1 <= returned <= requested`) and treat one dedicated all-missing-dimension 422 as an expected pass. | Brainstorm requirement 6 |
| R9 | Default ramp `2,5,15,30`, spike ceiling `30-50`, `engineInstances: 1`. | Brainstorm requirement 3 |
| R10 | Gate or report Class A `p95 <= 1000ms`, Class B `p95 <= 3000ms`, Class C `p95 <= 5000ms`, and error rate `< 1%`. | Brainstorm requirement 4 |
| R11 | Keep mixed traffic and add a configurable same-query stampede mode. | Brainstorm requirement 5 |
| R12 | Confirm the scenario matrix with the user before writing the final catalog. | User request during brainstorm |

## Implementation Steps

## Phase 1: Contract and catalog

### 1. Confirm the scenario matrix

- **Requirements**: R5, R6, R7, R12
- **Files**: draft table in the implementation conversation; no file write until approved
- **Details**: Present each proposed scenario with `analysis_var`, allowed measures, slots, `filter_base`, survey count, downstream, and latency class. Include `row`-only, `pov_status` as analysis and as a slot, continuous and binary analysis variables, fast vs quantile/inequality bundles, and one expected-422 case. Wait for approval.
- **Test Scenarios**: happy path is an approved matrix; edge case is a rejected combination; error path is a spec conflict that stops implementation.
- **Tests**: none until the matrix is approved
- **Acceptance criteria**: User approves the matrix or names the rows to change.

### 2. Rewrite catalog validation and scenarios

- **Requirements**: R5, R6, R7, R8
- **Files**: `load-testing/loadtest/scenarios.py`, `load-testing/scenarios.json`, `load-testing/tests/test_scenarios.py`
- **Details**: Replace contiguous-slot validation with the API parent-slot rule. Validate `analysis_var` against an embedded allow-list copied from the YAML specs. Reject mixed stat groups. Tag each scenario with `latency_class` and `expected_status` (`200` or `422`). Replace the invalid `gender` analysis scenario. Keep deterministic weighted selection and the seeded survey sample.
- **Test Scenarios**: happy path loads the approved catalog; edge case accepts `row`-only and rejects `superRow` without `row`; error path rejects `gender` as `analysis_var` and mixed poverty plus inequality measures.
- **Tests**: `uv run --project load-testing pytest load-testing/tests/test_scenarios.py`
- **Acceptance criteria**: Catalog loads, covers tiers 1-3, and every scenario maps to one latency class.

## Phase 2: Journeys, gates, and handoff

### 3. Align user journeys with the current API

- **Requirements**: R1, R2, R3, R4, R8, R11
- **Files**: `load-testing/loadtest/users.py`, `load-testing/loadtest/api.py`, `load-testing/loadtest/bootstrap.py`, `load-testing/locustfile.py`, `load-testing/tests/test_users.py`, `load-testing/tests/test_api.py`, `load-testing/tests/test_bootstrap.py`
- **Details**: Page 1 calls `/regions`, `/countries`, and `/surveys-ui`. Page 2 calls unfiltered and `pip_id`-filtered `/categories` and `/covariates`, plus `/analysis-variables`, `/statistics`, and `/cell-definition`. Page 3 calls `/table` without `include_metadata`, then `/metadata` or `/table/download`. Remove `/description`. Accept partial exclusions. Mark the dedicated 422 scenario successful only when the error envelope is the expected missing-dimension failure. Add `TRAFFIC_MODE=mixed|stampede` and `STAMPEDE_TIER`.
- **Test Scenarios**: happy path chains table to metadata and download; edge case accepts fewer returned surveys than requested; error path fails a 200 with empty data and does not call removed endpoints.
- **Tests**: `uv run --project load-testing pytest load-testing/tests/test_users.py load-testing/tests/test_api.py load-testing/tests/test_bootstrap.py`
- **Acceptance criteria**: Request names stay stable and include the latency class, for example `table:tier2:class-b:post`.

### 4. Recalibrate load and SLO configuration

- **Requirements**: R9, R10, R11
- **Files**: `load-testing/loadtest/config.py`, `load-testing/azure-load-test.yaml`, `load-testing/.env.example`, `load-testing/tests/test_config.py`, `load-testing/tests/test_azure_config.py`
- **Details**: Default `RAMP_STEPS=2,5,15,30`, `USERS=30`, `SPIKE_USERS=40`, `engineInstances: 1`. Add stampede settings. If Azure Load Testing `v0.1` supports request-scoped criteria, add Class A/B/C p95 gates; otherwise keep the global error gate, record the class thresholds in README, and test that request names are stable enough for a manual Azure chart filter.
- **Test Scenarios**: happy path parses the new defaults; edge case rejects an unknown traffic mode; error path rejects a non-HTTPS remote host.
- **Tests**: `uv run --project load-testing pytest load-testing/tests/test_config.py load-testing/tests/test_azure_config.py`
- **Acceptance criteria**: YAML upload set still has unique basenames and no secrets.

### 5. Update tests, README, and offline verification

- **Requirements**: R1, R10, R12
- **Files**: `load-testing/README.md`, `load-testing/tests/`
- **Details**: Document the 13-endpoint journey, latency classes, ramp math, engine vs replica distinction, stampede mode, and expected-422 case. State that offline tests do not prove capacity. Run the full pytest and ruff commands.
- **Test Scenarios**: happy path README lists every agreed endpoint; edge case documents partial exclusion; error path tells operators to stop on unexpected 5xx.
- **Tests**: `uv run --project load-testing pytest load-testing/tests`; `uv run --project load-testing ruff check load-testing`
- **Acceptance criteria**: Both commands pass and README matches the approved matrix.

## Testing Strategy

Keep tests offline with mocked HTTP responses. Do not run a live load test during implementation. A later 1-user smoke test remains an ITS-approved manual step, not a completion requirement.

## Documentation Checklist

- [ ] Update the configuration table in `load-testing/README.md`.
- [ ] Document Class A/B/C thresholds and the Azure schema fallback if needed.
- [ ] Document `engineInstances: 1` versus Container App replica scaling.
- [ ] Document mixed vs stampede mode.
- [ ] State that `2,5,15,30` is an order-of-magnitude starting point, not measured Table Maker demand.

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| A scenario looks valid in Python but violates the UI spec | Step 1 confirmation checkpoint before writing the catalog |
| Azure `v0.1` cannot express per-request p95 gates | Keep stable request names and document manual class filters |
| Partial exclusion hides a bad survey pool | Require at least one returned survey and a separate expected-422 case |
| Fast metadata calls still hide slow `/table` calls | Class-tagged request names, not one blended success metric |
| Stampede mode overloads QA | Default it off; require an explicit env var and the existing auto-stop |

## Out of Scope

API or Plumber changes, Azure resource provisioning, CI/CD pipelines, production traffic, and optional developer endpoints.

## Completion Contract

### Outcome

The `load-testing/` bundle matches the current Page 1-3 API, sends only UI-valid parameter combinations, and reports latency by class under a default 2-to-30 user ramp on one Azure engine, with a separate stampede mode.

### Verification Surface

| ID | Evidence Required | Command/Artifact | Required |
|----|-------------------|------------------|----------|
| V1 | Catalog rejects invalid `analysis_var`/`measures` pairs and the old `gender` scenario is gone | `uv run --project load-testing pytest load-testing/tests/test_scenarios.py` | yes |
| V2 | Journeys call the 13 agreed endpoints and never call removed or excluded endpoints | `load-testing/tests/test_users.py` | yes |
| V3 | `row`-only slots are allowed; parent-slot rules still hold | `load-testing/tests/test_scenarios.py` | yes |
| V4 | Partial exclusion passes and the dedicated 422 is an expected pass | `load-testing/tests/test_api.py` | yes |
| V5 | YAML uses one engine, ramp `2,5,15,30`, and classed SLO gates or a documented fallback | `load-testing/tests/test_azure_config.py` | yes |
| V6 | Offline suite and lint pass | `uv run --project load-testing pytest load-testing/tests`; `uv run --project load-testing ruff check load-testing` | yes |

### Constraints

| ID | Constraint | Check |
|----|------------|-------|
| C1 | No R package, Plumber, or API behavior changes | Diff limited to `load-testing/` |
| C2 | No secrets, real subnet IDs, or credentials committed | YAML and README scan |
| C3 | Azure flat-import fallback and deterministic seed stay intact | `locustfile.py` and config tests |
| C4 | Offline tests do not claim live capacity | README distinguishes smoke vs Azure runs |

### Boundaries

- Allowed: `load-testing/` Python, catalog, YAML, tests, README, and `.env.example`.
- Out of scope: API changes, Azure resource provisioning, CI/CD pipelines, production runs, `/lookup`, `/dimensions`, and `/surveys`.

### Iteration Policy

1. Confirm the scenario matrix before writing `scenarios.json`.
2. Implement the contract and keep request names stable.
3. If Azure's documented schema cannot express per-request criteria, keep named requests plus the global error gate and document the fallback.
4. Stop rather than guess when an endpoint or UI rule is ambiguous.

### Blocked-Stop Conditions

- Current `plumber.R` disagrees with the agreed endpoint list.
- A scenario cannot be made valid against the variable or measure spec.
- The scenario-matrix checkpoint is rejected and no replacement is approved.
