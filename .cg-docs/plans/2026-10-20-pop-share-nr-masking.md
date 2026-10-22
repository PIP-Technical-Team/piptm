---
date: 2026-10-20
title: "Population share threshold: NR masking and API value formatting"
status: active
scope: "Standard"
brainstorm: ".cg-docs/brainstorms/2026-10-20-pop-share-nr-masking.md"
language: "R"
estimated-effort: "medium"
deviation-policy: "ask"
artifact-schema-version: 1
phases: 2
completed-phases: [1]
current-phase: 2
execution-report: .cg-docs/work-reports/2026-10-22-pop-share-nr-masking.md
tags: [table_maker, pop_share_threshold, suppression, api, formatting]
---

# Plan: Population share threshold: NR masking and API value formatting

## Objective
Replace row-dropping suppression in `table_maker()` with cell-level NR masking
at a 0.1% default threshold, remove the `population` column, and format the
main `/table` API values as strings (x100 for proportion/Gini measures, 2
decimals, whole numbers for counts, `"NR"` for masked cells). Download and
description changes are deferred to a follow-up plan.

## Context
See brainstorm decisions D1-D15. Current suppression lives in
`R/table_maker.R:877-934` (drops non-share rows). Share computed by
`.shares_setup()` (`R/compute_shares.R`) after `filter_base`. API endpoint
`inst/plumber/plumber.R:145` (default 0.01) and validation in
`inst/plumber/helpers.R:478-512`. `/table/download` coerces `value` to numeric
at `plumber.R:288`.

## Requirements
| ID | Requirement | Source |
|----|-------------|--------|
| R1 | Default `pop_share_threshold = 1e-3` in `table_maker()` and `/table` | D1 |
| R2 | Denominator unchanged (post-`filter_base` survey total) | D2 |
| R3 | No rows dropped; below-threshold cells masked: `value = NA_real_`, `flag = "NR"` | D3, D15 |
| R4 | All measures masked, including share measures | D4 |
| R5 | Masked values never present in API output | D5 |
| R6 | API `value` is character: formatted number or `"NR"`; genuine NA -> `null` (UI confirmed string is acceptable) | D6 |
| R7 | Masking uses unrounded share | D7 |
| R8 | `population` removed from `table_maker()` output | D8 |
| R9 | Warning kept with per-cell pop_share; reworded to "Reporting N cells as NR" | D9 |
| R10 | `suppressed_cells` metadata kept unchanged (description wording deferred) | D10 |
| R11 | x100: gini, headcount, poverty_gap, severity, pop_share, target_within_group_share, target_survey_share | D11 |
| R12 | 2 decimals; `pop_poverty`, `nobs` as whole numbers | D12 |
| R13 | `mean` never rescaled | D13 |
| R14 | Formatting only in API layer via `format_table_output()` | D14 |
| R16 | Formatter defines behavior for every registered measure and for `NA`, `NaN`, `Inf`, and `-Inf` | Plan review |
| R17 | Formatter/schema failures in `/table` use the existing HTTP 422 error envelope | Plan review |
| R18 | Missing `by` dimension values are matched and masked consistently | Plan review |
| R19 | Zero-denominator (`pop_share = NA`) cells are treated as NR | Plan review |
| R20 | Formatter is exported with roxygen and verified after documentation regeneration | Plan review |

phases: 2  # convenience hint -- may be stale; always recount from ## Phase headers

## Implementation Steps

## Phase 1: Masking in table_maker()

### 1. Masking logic and output shape
- **Requirements**: R1, R2, R3, R4, R7, R8, R9, R10, R18, R19
- **Files**: `R/table_maker.R`
- **Details**: Default `pop_share_threshold = 1e-3`. In the suppression block,
  replace `.suppress`/row filter with: join suppressed cell keys, set
  `value := NA_real_` and `flag := "NR"` for all matched rows (no share
  exception). Treat `pop_share = NA` from a zero weighted denominator as NR.
  Preserve data.table matching semantics for `NA` dimension keys and test them
  explicitly. Always add `flag` (character, `NA` default) after `value`, also
  when threshold is NULL or `by` is NULL. Remove `population` from result and
  from `tail_cols`. Reword `cli_warn` to "Reporting {n} cell{?s} as NR
  (population share < {threshold}):" keeping per-cell labels with pop_share.
  Update roxygen (`@param pop_share_threshold`, `@return`: drop `population`,
  add `flag`).
- **Test Scenarios**: happy (low cell -> all rows NR, row count unchanged);
  edge (threshold NULL -> no NR, flag all NA; by NULL -> no masking; genuine
  NA value stays flag NA; an `NA` by-key cell is masked; zero denominator is
  NR); error (invalid threshold aborts).
- **Tests**: `tests/testthat/test-table-maker-suppression.R`
- **Acceptance criteria**: V1 passes; `value` is double; no `population` column.

### 2. Update existing R tests
- **Requirements**: R3, R8
- **Files**: `tests/testthat/test-table-maker-v2.R`, `test-phase4-table-and-endpoint.R`, `test-table-maker-metadata.R`
- **Details**: Replace `population` expectations with `flag`; replace row-drop
  expectations with NR-mask expectations; adjust for new default threshold.
- **Test Scenarios**: as above.
- **Tests**: `devtools::test(filter = "table-maker|phase4")`
- **Acceptance criteria**: green.

## Phase 2: API formatting and contract

### 3. format_table_output()
- **Requirements**: R5, R6, R11, R12, R13, R14, R16, R20
- **Files**: `R/format_table_output.R` (new, exported), `NAMESPACE`, `man/`
- **Details**: Input: `table_maker()` data.table. Copy (do not modify input).
  Enumerate all registered measures in a formatting contract: percentage-scale
  measures are x100; `mld`, `watts`, and amount/index measures remain unchanged;
  `pop_poverty` and `nobs` are whole numbers; all other finite values use two
  decimals. Convert `NA`, `NaN`, `Inf`, and `-Inf` to genuine missing output
  (`NA_character_`/JSON null), except NR rows which always become `"NR"`.
  Vectorize the finite-value formatting and guard against `formatC` emitting
  literal `"NA"`. Define `.PCT_MEASURES` next to `.MEASURE_REGISTRY`, add
  roxygen `@export`, regenerate documentation, and verify the function loads
  from a freshly documented package.
- **Test Scenarios**: happy (every registered measure, each scale group, 2 dp
  incl. trailing zeros `"6.70"`); edge (NR -> `"NR"`, genuine NA/NaN/Inf/
  -Inf -> NA, negative values, large numbers no scientific notation, empty
  table); error (missing `flag`/`value` column aborts).
- **Tests**: `tests/testthat/test-format-table-output.R`; 1M-row timing snippet.
- **Acceptance criteria**: V2 passes; V7 < 2 s (non-blocking).

### 4. Plumber wiring
- **Requirements**: R1, R5, R6, R17
- **Files**: `inst/plumber/plumber.R`, `inst/plumber/helpers.R`
- **Details**: `/table` default `pop_share_threshold = 1e-3`; apply
  `piptm::format_table_output()` to `result_data` before `api_response()`.
  Ensure formatter/schema failures are caught by the endpoint's existing error
  path and return HTTP 422 with the standard error envelope. Download behavior
  is deferred and must not be changed in this plan. Check helpers.R
  default/messages.
- **Test Scenarios**: happy (JSON value strings, no population, no flag);
  edge (masked and genuine missing values remain distinguishable); error
  (invalid threshold 400; formatter/schema error returns 422 envelope).
- **Tests**: `test-api-endpoints.R`, `test-api-helpers.R`
- **Acceptance criteria**: V3 and V4 pass.

### 5. Main endpoint contract documentation
- **Requirements**: R6, R8, R11, R12
- **Files**: `docs/api-contract.md`, `SWAGGER_GUIDE.md`, `NEWS.md`
- **Details**: document the main `/table` value type as string; NR semantics
  versus null; scales; population removed; default 0.1%. Do not modify
  download or description documentation beyond avoiding contradictions.
- **Tests**: `devtools::check(document = TRUE)`
- **Acceptance criteria**: API contract documents match `/table` behavior.

## Testing Strategy
Unit tests per step with synthetic data.tables (data.table fixtures); API tests
via existing plumber router harness; full suite and R CMD check at end.

## Documentation Checklist
- [ ] roxygen for `table_maker()` and `format_table_output()`
- [ ] `docs/api-contract.md`
- [ ] `SWAGGER_GUIDE.md`
- [ ] `NEWS.md`

## Risks & Mitigations
| Risk | Mitigation |
|------|------------|
| UI breaks on string `value` | UI team confirmed (2026-10-22) string `value` is acceptable; formatter stays isolated so a later switch to numeric+flag is local |
| `formatC` emits "NA" or scientific notation | Explicit NA handling; `format = "f"`; tests |
| Hidden consumers of `population` | Grep before removal; blocked-stop if found |
| Download silently numeric-coerces "NR" (deferred; tracked for follow-up) | Documented as out of scope in this plan |
| Many tests assume drop behaviour | Step 2 dedicated update |

## Out of Scope
UI changes; `/table/download` behavior and validation; `/description` wording,
rendering, and verification; denominator changes; rescaling mean for binary
vars; per-measure thresholds; numeric+flag JSON contract.

## Completion Contract

### Outcome
`table_maker()` returns all cells, with below-0.1% (post-filter) cells carrying
`value = NA`, `flag = "NR"` and no `population` column; `/table` returns string
values (x100 proportions, 2 dp, whole-number counts, `"NR"` for masked cells).

### Verification Surface
| ID | Evidence Required | Command/Artifact | Required |
|----|-------------------|------------------|----------|
| V1 | Masking tests pass | `devtools::test(filter = "table-maker-suppression")` | yes |
| V2 | Formatter unit tests pass | `devtools::test(filter = "format-table-output")` | yes |
| V3 | Main `/table` endpoint returns strings, NR, no population/flag, and no masked-value leaks | `devtools::test(filter = "api")` | yes |
| V4 | Formatter/schema failures return HTTP 422 envelope | API regression test | yes |
| V5 | Full suite green | `devtools::test()` | yes |
| V6 | R CMD check clean (no new errors/warnings) | `devtools::check(document = TRUE)` | yes |
| V7 | Formatter on 1M rows < 2 s | benchmark snippet | no |

### Constraints
| ID | Constraint | Check |
|----|------------|-------|
| C1 | Masking uses raw share, post-`filter_base` denominator | review + test |
| C2 | No masked measure value in `/table` data | V1, V3 |
| C3 | `table_maker()` `value` stays numeric | type test |
| C4 | data.table/collapse style; no new dependencies | DESCRIPTION unchanged |
| C5 | `.shares_setup()` unchanged | diff review |
| C6 | Download and description behavior remain unchanged | targeted diff/tests |

### Boundaries
- Allowed: `R/table_maker.R`, `R/format_table_output.R`, `R/measures.R` (constant only), `inst/plumber/plumber.R`, `inst/plumber/helpers.R`, `tests/testthat/*`, `docs/api-contract.md`, `SWAGGER_GUIDE.md`, `NEWS.md`, `man/`, `NAMESPACE`.
- Out of scope: UI, download endpoint behavior, description files, denominator, binary-mean rescale, per-measure thresholds, numeric+flag JSON contract.

### Iteration Policy
1. Fix failing tests within allowed files first.
2. Existing tests asserting old drop/`population` behaviour are updated to brainstorm decisions.
3. Max 2 fix rounds per verification item, then stop and report.

### Blocked-Stop Conditions
- Another main `/table` code path depends on `population`.
- Formatter cannot distinguish NR from genuine missing values or cannot map errors to 422.
- R CMD check fails for reasons unrelated to this change.
