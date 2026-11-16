---
date: 2026-11-03
depth: architecture
type: standard
plan: .cg-docs/plans/2026-11-02-page2-cell-definition-endpoint.md
findings:
  P1.1: fixed
  P1.2: fixed
  P1.3: fixed
  P1.4: fixed
  P1.5: fixed
  P2.1: fixed
  P2.2: fixed
  P2.3: fixed
  P2.4: fixed
  P2.5: skipped
  P2.6: fixed
  P3.1: fixed
  P3.2: fixed
  P3.3: fixed
  P3.4: fixed
---

## Review Report

**Review mode**: architecture
**Files reviewed**: 14
**Findings**: 15 (P0: 0, P1: 5, P2: 6, P3: 4)

### P1 — CRITICAL (must fix before merge)

- **[P1.1]** [cg-architecture / cg-code-quality / cg-testing / cg-data-quality / cg-performance] [safe_auto] `inst/plumber/plumber.R`:900 — Multi-line `raw_body` fallback in `POST /cell-definition` does not collapse multi-element character vectors before `nzchar()`, re-parses empty `{}` JSON objects, and does not guard against top-level JSON arrays (`data.frame`).
  **Why**: When `req$rook.input$read_lines()` or `req$postBody` returns a multi-element character vector (multi-line JSON body), evaluating `!nzchar(trimws(raw_body))` inside `||` throws `'length = N' in coercion to 'logical(1)'` (HTTP 500) on R >= 4.2. Also, checking `length(body) == 0L` before the fallback causes `{}` to be parsed twice, and a JSON array (`[...]`) parsed into a `data.frame` bypasses `is.list(body)` if `is.data.frame(body)` / `is.null(names(body))` is not checked.
  **Fix**: Only enter the raw-body fallback when `is.null(body)`, collapse multi-element character vectors with `paste(raw_body, collapse = "\n")` before `nzchar()`, and reject `is.data.frame(body)` and `is.null(names(body))` in both `POST /cell-definition` and `validate_cell_definition_input()`.

- **[P1.2]** [cg-testing / cg-data-quality] [safe_auto] `inst/plumber/helpers.R`:620, 815 & `R/cell_explanation.R`:43 — Vector/non-finite `pop_share_threshold`, `NA`/blank `filter_base` category codes, and non-scalar `body$release` can trigger unhandled coercion or subscript errors instead of HTTP 400.
  **Why**: `.validate_table_shape()` coerces `pop_share_threshold` without a scalar/type guard (unlike `ppp`), so `"pop_share_threshold": [0.001, 0.005]` throws a length > 1 coercion error. In `validate_cell_definition_input()`, `filter_base` allows `NA`, blank strings, or non-filter continuous variables (`"age"`, `"hsize"`), and `body$release` is not checked for scalar type/length before `[[1L]]`. In `.fallback_cat_label()`, `NA` causes `if (grepl(...))` to error with `missing value where TRUE/FALSE needed`.
  **Fix**: Add scalar/type/finite guards on `pop_share_threshold` and `body$release`, reject `NA`/blank/non-filter variables in `filter_base`, and guard `.fallback_cat_label()` against `NULL`/`NA`/empty inputs.

- **[P1.3]** [cg-architecture / cg-testing / cg-data-quality] [safe_auto] `inst/plumber/helpers.R`:757 — `validate_cell_definition_input()` only rejects `"welfare"` and `"pov_status"` for target share measures (`target_within_group_share`, `target_survey_share`), allowing continuous or categorical variables (`"age"`, `"educy"`, `"hsize"`, `"educat4"`) to pass validation and emit broken prose (`"have age (in years)"`).
  **Why**: Target share measures require a binary target `analysis_var` (present in `tm_cell_prose.yaml$targets`). Allowing non-binary variables produces nonsensical sentences and mismatches `compute_shares.R`.
  **Fix**: Require `trimws(body$analysis_var) %in% names(piptm:::.get_cell_prose()$targets)` whenever `target_within_group_share` or `target_survey_share` is requested.

- **[P1.4]** [cg-testing] [safe_auto] `R/cell_explanation.R`:476, 673 — Long filter combined with share measures duplicates `"everyone in the table"` in `out_of` (`"everyone in the table (the filtered group above) (everyone in the table)"`).
  **Why**: When `has_filter && !is_short_filter`, `table_pop_denom` is set to `"everyone in the table (the filtered group above)"`, and the share measure builders then wrap it in `sprintf("%s (everyone in the table)", table_pop_denom)`.
  **Fix**: Avoid appending `" (everyone in the table)"` when `table_pop_denom` already starts with `"everyone in the table"`, and add unit tests for long filters with share measures.

- **[P1.5]** [cg-architecture] [safe_auto] `inst/plumber/helpers.R`:1 — Unqualified `%||%` used in Plumber scripts without a local definition in `helpers.R` while `DESCRIPTION` declares `Depends: R (>= 4.2)`.
  **Why**: Base R only added `%||%` in R 4.4.0, and `helpers.R` / `plumber.R` run outside the `piptm` namespace.
  **Fix**: Define `` `%||%` <- function(x, y) if (!is.null(x)) x else y `` at the top of `inst/plumber/helpers.R`.

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-testing / cg-data-quality] [safe_auto] `R/cell_explanation.R`:345, 438, 551 — Direct R callers of `build_cell_explanation()`: missing entry-point argument validation, duplicate/empty `filter_base` codes not deduplicated, and doubled words (`"age group group"`, `"The the poverty line line"`) on 1-slot `age_group` or `poverty_line = NULL`.
  **Why**: Exported `build_cell_explanation()` can be called directly from R; duplicate filter codes repeat phrases (`"aged 25–64 or 25–64"`), `slot_noun = "age group"` produces `"one age group group"`, and `.format_pov_line(NULL)` produces `"The the poverty line line"`.
  **Fix**: Validate `analysis_var`, `measures`, `poverty_line`, and `ppp` at entry; deduplicate `filter_base` codes and drop empty vectors; omit trailing `" group"` when `slot_noun` already contains `"group"`; and avoid doubling `"the"`/`"line"` in `.format_pov_line(NULL)`.

- **[P2.2]** [cg-documentation / cg-code-quality] [safe_auto] `inst/plumber/helpers.R`:225 & `R/cell_explanation.R`:344 — `.validate_table_shape()` was inserted between `validate_table_input()`'s roxygen block and its function definition; `build_cell_explanation()` is missing `@examples` in `R/cell_explanation.R` and `man/build_cell_explanation.Rd`.
  **Why**: Re-attaching `validate_table_input()`'s roxygen block to `validate_table_input()` and adding `@examples` to `build_cell_explanation()` keeps documentation accurate and CRAN-ready.
  **Fix**: Move `.validate_table_shape()` above `validate_table_input()`'s roxygen block and add `@examples` to `R/cell_explanation.R` and `man/build_cell_explanation.Rd`.

- **[P2.3]** [cg-code-quality / cg-performance] [safe_auto] `R/cell_explanation.R`:198, 578, 789 — Duplicate fallback list construction in `.lookup_cat_prose()`, repeated `vapply()` inside `precedence_order` loop, and hardcoded `poverty_measures` vector instead of deriving from `.MEASURE_REGISTRY`.
  **Why**: Deduplicating fallback construction, hoisting `resolved_slots`, and deriving poverty measure keys from `.MEASURE_REGISTRY` (plus aliases) improves maintainability and avoids redundant work.
  **Fix**: Refactor `.lookup_cat_prose()` to share a single fallback return helper, hoist `resolved_slots` and `active_varnames`, and derive poverty measures from `.MEASURE_REGISTRY`.

- **[P2.4]** [cg-testing / cg-reproducibility] [safe_auto] `tests/testthat/test-cell-explanation.R`, `test-api-cell-definition.R`, `test-cell-prose-sync.R` — Add `skip_if_not_installed("yaml")`, validate `adjectives` codes in `test-cell-prose-sync.R`, clear/restore `PIPTM_ARROW_ROOT` in the API fallback test, remove top-level `library()` calls, and add unit tests for `modifier: null` share targets, 4-slot layout (`superRow`), 1-var/3-cat long filter, long-filter shares, and all 23 canonical measures.
  **Why**: Ensures `_R_CHECK_DEPENDS_ONLY_=true` compatibility and complete branch coverage across `build_cell_explanation()`.
  **Fix**: Update the three test files with these guards and test cases.

- **[P2.5]** [cg-architecture] [manual] `DESCRIPTION`:31 — `yaml` is declared in `Suggests` rather than `Imports`.
  **Why**: Kept in `Suggests` per Plan Constraint C1/C4 (`requireNamespace("yaml")` guard in `.get_cell_prose()`). If production Plumber containers install `piptm` without `Suggests`, `yaml` would need to move to `Imports`.
  **Fix**: Manual decision — keep in `Suggests` per plan C1/C4 or move to `Imports` in a follow-up.

- **[P2.6]** [cg-architecture / cg-code-quality] [manual] `R/cell_explanation.R`:345 — `R/cell_explanation.R` is ~650 lines and could be split into smaller internal helpers or share a package-level validator with `inst/plumber/helpers.R`.
  **Why**: Advisory architectural refactoring beyond the current file list.
  **Fix**: Consider splitting into section helpers during Phase 2 metadata rework.

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-architecture / cg-documentation] [safe_auto] `SWAGGER_GUIDE.md`:71, 357 — Update `/table` parameter table (`column`, `row`, `superColumn`, `superRow`) and Key Point 3 (`POST /cell-definition` accepts both native JSON object and JSON-encoded string `filter_base`, returning HTTP 400 on invalid JSON).
  **Why**: Keeps `SWAGGER_GUIDE.md` aligned with `docs/api-contract.md` and `POST /cell-definition`.
  **Fix**: Update lines 70-71 and Key Point 3 in `SWAGGER_GUIDE.md`.

- **[P3.2]** [cg-data-quality] [safe_auto] `inst/extdata/tm_cell_prose.yaml`:268 — Align `prime_work_age` age range in `tm_cell_prose.yaml` (`25–54`) with `tm_variable_spec.yaml`:173 (`"Prime working age (25-54)"`).
  **Why**: Prevents UI dropdown label (`25-54`) and cell explanation prose (`25–64`) from stating different age ranges.
  **Fix**: Update `prime_work_age` predicate in `inst/extdata/tm_cell_prose.yaml` to `"are of prime working age (25\u201354)"`.

- **[P3.3]** [cg-architecture] [advisory] `inst/extdata/tm_cell_prose.yaml`:30 — `tm_cell_prose.yaml` includes prose entries for variables/measures in `tm_variable_spec.yaml` not yet enabled in `piptm.optional_vars` or `.MEASURE_REGISTRY`.
  **Why**: Required by `test-cell-prose-sync.R` so all spec entries have prose ready when enabled.
  **Fix**: No action needed; keep as forward-compatible spec coverage.

- **[P3.4]** [cg-code-quality] [advisory] `inst/plumber/helpers.R`:748 — `poverty_line` required error deduplication in `validate_cell_definition_input()`.
  **Why**: Cosmetic check to avoid duplicate `poverty_line` required messages when `analysis_var == "pov_status"` and `measures` includes a poverty measure.
  **Fix**: Check `!(identical(body$analysis_var, "pov_status") || "pov_status" %in% shape$by)` directly.

### ✅ Passed
- `cg-version-control`: No issues found
