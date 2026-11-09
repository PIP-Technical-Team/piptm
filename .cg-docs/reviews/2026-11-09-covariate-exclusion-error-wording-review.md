---
date: 2026-11-09
depth: standard
type: standard
plan: .cg-docs/plans/2026-11-09-covariate-exclusion-error-wording.md
findings:
  P1.1: fixed
  P1.2: fixed
  P2.1: fixed
  P2.2: fixed
  P2.3: fixed
  P2.4: fixed
  P2.5: fixed
  P3.1: fixed
  P3.2: fixed
  P3.3: fixed
  P3.4: fixed
---

## Review Report

**Review mode**: standard
**Files reviewed**: 5 (`R/errors.R`, `R/table_maker.R`, `docs/api-contract.md`, `tests/testthat/test-errors.R`, `tests/testthat/test-api-endpoints.R`)
**Findings**: 11 (P0: 0, P1: 2, P2: 5, P3: 4)

### P0 — BLOCKING (immediate remediation required)
- None.

### P1 — CRITICAL (must fix before merge)
- **[P1.1]** [cg-code-quality / cg-testing / cg-data-quality] `R/errors.R:211-218` — Unhandled empty-list `ui_label` and factor/numeric inputs crash `.piptm_covariate_labels()` instead of falling back cleanly `[safe_auto]`
  **Why**: (1) If a registry entry has `ui_label = list()`, `is.list(label)` is `TRUE` and `label[[1L]]` throws `subscript out of bounds`; (2) If `varnames` is a factor or non-character, `reg[[v]]` performs integer positional indexing, returning the wrong variable's label or crashing if the level is out of bounds. This violates the documented contract that `.piptm_covariate_labels()` *"never raises"*.
  **Fix**: Defensively coerce `varnames <- as.character(varnames)` (stripping NAs), check `is.list(label) && length(label) >= 1L`, and verify `v %in% names(reg)` before indexing.

- **[P1.2]** [cg-testing / cg-reproducibility] `tests/testthat/test-errors.R:3-14` — Unit test relies on ambient live release and skips when run in isolation `[safe_auto]`
  **Why**: The new unit test calls `piptm_current_release()` and skips if `NULL`. When `test-errors.R` runs in isolation or on a worker without an ambient release, this test is silently skipped, leaving zero unit test coverage for the primary label-resolution path. Unit tests must be self-contained and deterministic per `cg-skill-r-testing`.
  **Fix**: Wrap the test with `testthat::with_mocked_bindings()` to mock `piptm_variable_registry()` with a self-contained fixture registry (`list(area = list(ui_label = "Area"), gender = list(ui_label = "Gender"))`).

### P2 — IMPORTANT (should fix)
- **[P2.1]** [cg-code-quality / cg-architecture] `R/table_maker.R:728-744, 806-823`, `R/errors.R` — Duplicated message templating logic between `filter_base` and `by` exclusion paths (DRY violation) `[safe_auto]`
  **Why**: The 11-line singular/plural template formatting block (`if (length(labels) == 1L) ... else ...`) is duplicated verbatim across both exclusion sites. `{piptm}`'s architecture centralizes user-facing error message builders in `R/errors.R` (like `.piptm_unknown_release_user_error()`).
  **Fix**: Extract `.piptm_covariate_exclusion_user_error(varnames, release)` in `R/errors.R` that handles label resolution, deduplication, and singular/plural formatting in one place, and call it from both sites in `R/table_maker.R`.

- **[P2.2]** [cg-code-quality / cg-architecture / cg-data-quality] `R/table_maker.R:733-744, 812-823` — Missing label deduplication and zero-length input guard allows repeated labels or malformed sentences `[safe_auto]`
  **Why**: Duplicate input varnames (e.g. `by = c("gender", "gender")`) format as `"covariates: Gender, Gender"` and trigger plural phrasing; a zero-length label vector falls through to `else` and formats `"covariates: . Remove..."`.
  **Fix**: Apply `unique()` to varnames/labels and add a defensive fallback when `length(labels) == 0L`.

- **[P2.3]** [cg-testing] `tests/testthat/test-api-endpoints.R:1010-1035` — Missing test coverage for plural (2+ covariates) exclusion on `filter_base` `[safe_auto]`
  **Why**: `test-api-endpoints.R` covers both singular and plural cases for `by` dimensions (`# 3` and `# 4b`), but only tests single covariate exclusion for `filter_base` (`# 4`). The plural `filter_base` branch is untested.
  **Fix**: Add test case `# 4c` in `test-api-endpoints.R` testing `filter_base = '{"gender":1,"area":1}'` against `BOL_2000_ECH_INC_ALL` to assert the plural message.

- **[P2.4]** [cg-documentation] `docs/api-contract.md:68-69` — Truncated text and informal shorthand in API contract table `[safe_auto]`
  **Why**: Row 68 truncates the plural variant with an ellipsis (`"...selected covariates: Area, Gender. Remove one or more of these covariates..."`), and row 69 uses narrative shorthand (`"Same wording style as above..."`) rather than providing exact quoted sentences like every other row.
  **Fix**: Provide complete quoted example strings for both singular and plural variants in both rows.

- **[P2.5]** [cg-version-control] `.cg-docs/active-state/current.json:7` — Untracked companion plan and report files referenced by active-state `[safe_auto]`
  **Why**: `.cg-docs/plans/2026-11-09-covariate-exclusion-error-wording.md` and `.cg-docs/work-reports/2026-11-09-covariate-exclusion-error-wording.md` are currently untracked, while `current.json` references them.
  **Fix**: Stage and commit the plan and report files together with the code changes.

### P3 — MINOR (nice to have)
- **[P3.1]** [cg-documentation] `R/table_maker.R:749-753` — Stale inline comment contradicts active dimension filtering logic `[safe_auto]`
  **Why**: The comment says "Partial: keep but warn — missing dims filled with NA in per-survey loop", but the code below it enforces strict all-or-nothing filtering (`full_idx <- overlap == length(by_check)`).
  **Fix**: Update the comment to describe the strict all-or-nothing matching logic.

- **[P3.2]** [cg-documentation] `docs/api-contract.md:54` — Incomplete terminology standardization in HTTP 422 summary row `[safe_auto]`
  **Why**: Line 54 still says "unsupported breakdown dimensions", while the rest of the contract was updated to "covariates".
  **Fix**: Update line 54 to "unsupported breakdown covariates".

- **[P3.3]** [cg-testing] `tests/testthat/test-errors.R:1-22` — Untested defensive branches in `.piptm_covariate_labels()` `[safe_auto]`
  **Why**: Empty string labels, `NA` labels, and missing `ui_label` fields are handled defensively in code but not exercised by unit tests.
  **Fix**: Add a test verifying fallback on `""`, `NA`, and missing `ui_label` entries.

- **[P3.4]** [cg-documentation] `R/errors.R:191-193` — Incomplete type specification in roxygen `@param release` tag `[safe_auto]`
  **Why**: The tag documents the parameter without specifying the scalar type (`Character scalar or NULL`).
  **Fix**: Update to `@param release Optional character scalar release ID (or NULL)`.

### ✅ Passed
- `@cg-performance`: No issues found (hot-path overhead is zero — helper only runs on terminal error paths).
- `@cg-reproducibility`: Deterministic outputs, no unmanaged dependencies or seeds.
