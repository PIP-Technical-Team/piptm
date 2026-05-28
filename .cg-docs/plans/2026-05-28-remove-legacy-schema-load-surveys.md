---
date: 2026-05-28
title: "Remove legacy schema from load_surveys()"
status: completed
completed-date: 2026-05-28
scope: Lightweight
estimated-effort: small
tags: [inline]
---

# Remove Legacy Schema from load_surveys()

## Objective

Refactor `load_surveys()` to assume all entries carry `welfare_vars` (new schema only).
Remove all backward-compatibility handling for the retired single-`welfare` column schema.

## Steps

### 1. Refactor load_surveys() in R/load_data.R

- Add `"welfare_vars"` and `"ppp_sort"` to the `stopifnot` required columns.
- Remove `has_welfare_vars` / `new_schema_mask` conditionals.
- Remove `target_col <- "welfare"` and `all_welfare_vars <- character(0L)` legacy defaults.
- Remove the `if (any(new_schema_mask))` outer wrapper — make PPP resolution unconditional.
- Remove the mixed-batch guard (`target_col != "welfare" && "welfare" %in% names(dt)`).
- Drop the outer `if (target_col != "welfare")` check — always true; make rename unconditional.
- Update `@param entries_dt` doc to remove legacy mentions.

Acceptance: `load_surveys()` errors if `welfare_vars`/`ppp_sort` absent from `entries_dt`.
PPP resolution is unconditional. No legacy code paths remain.

### 2. Update tests/testthat/test-load-data.R

- Update `write_fixture_parquet` to write `welfare_ppp_2017_01_02` column instead of `welfare`.
- Update `make_fixtures()` manifest entries to include `welfare_vars` and `ppp_sort = 2017L`.
- Update Cartesian test and partial-path test `entries_list` to include `welfare_vars` / `ppp_sort`.
- Remove "P2.4: mixed legacy + new-schema batch" test.
- Update the `load_survey_microdata()` legacy test to use its own inline fixture (decoupled from `make_fixtures()`).

Acceptance: All existing tests pass. No tests reference the mixed-batch guard.
