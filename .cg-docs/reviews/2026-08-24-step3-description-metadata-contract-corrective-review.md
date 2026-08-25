---
date: 2026-08-24
depth: architecture
type: standard
plan: .cg-docs/plans/2026-08-24-step3-description-metadata-contract-corrective.md
findings:
  P0.1: fixed
  P0.2: fixed
  P0.3: fixed
  P1.1: fixed
  P1.2: fixed
  P1.3: fixed
  P1.4: fixed
  P1.5: fixed
  P1.6: fixed
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
  P2.12: fixed
  P2.13: fixed
  P2.14: fixed
  P2.15: fixed
  P2.16: fixed
  P3.1: open
  P3.2: open
  P3.3: open
  P3.4: open
  P3.5: open
  P3.6: open
  P3.7: open
  P3.8: open
  P3.9: open
  P3.10: open
  P3.11: open
  P3.12: open
  P3.13: open
---

# Architecture Review Report

**Review mode**: architecture  
**Files reviewed**: 4 (R/table_maker.R, R/description.R, tests/testthat/test-table-maker-with-meta.R, tests/testthat/helper-description.R)  
**Commit**: 454950f  
**Findings**: 45 (P0: 3, P1: 6, P2: 16, P3: 20)

**Auto-routing applied**: User explicitly requested `architecture` mode. No high-risk auto signals detected. Mandatory emphasis on `@cg-architecture`, `@cg-performance`, and `@cg-testing`.

---

## P0 — BLOCKING (immediate remediation required)

### **[P0.1]** `[manual]` [cg-data-quality] `R/description.R:62` — `ppp_column_used` consumed without null check

**Why**: `build_description_model()` reads `exec$ppp_column_used` directly into metadata without validating it exists or is non-NA. If schema population fails, this field could be `NULL` or `NA_character_`, producing silently incorrect descriptions. Statistical documentation would be corrupted with missing execution truth.

**Fix**: Add validation in `build_description_model()` after line 54:
```r
# Validate execution metadata fields
if (is.null(exec$resolved_release) || is.null(exec$resolved_ppp) || is.null(exec$ppp_column_used)) {
  cli::cli_abort(
    c(
      "{.arg table_result} execution metadata incomplete",
      "i" = "resolved_release, resolved_ppp, and ppp_column_used must all be populated"
    )
  )
}

if (is.na(exec$ppp_column_used)) {
  cli::cli_warn(
    c(
      "Physical welfare column could not be determined",
      "i" = "PPP resolution may have failed"
    )
  )
}
```

### **[P0.2]** `[manual]` [cg-data-quality] `R/table_maker.R:664` — Silent NA assignment when welfare column match fails

**Why**: When `.find_welfare_col()` returns empty, `ppp_column_used` is set to `NA_character_` without raising an error. This masks configuration or data schema errors. The true error (PPP mismatch) is hidden from users.

**Fix**: Replace NA assignment with error at line 664:
```r
if (length(matched_col) > 0L) {
  .meta_state[["ppp_column_used"]] <- matched_col[[1L]]
} else {
  cli::cli_abort(
    c(
      "PPP column resolution failed",
      "i" = "No welfare column found for PPP year {ppp} in manifest",
      "i" = "Available columns: {.val {welfare_vars_sample}}"
    )
  )
}
```

### **[P0.3]** `[manual]` [cg-architecture] `R/description.R:206-222` — Edge case violates schema contract

**Why**: When `n_loaded == 0L` (all surveys excluded), the code shows "No surveys contributed data" but then renders an empty table skeleton. This is awkward — we say "no surveys" but show table headers.

**Fix**: Wrap table rendering in an `else` block:
```r
if (n_loaded == 0L) {
  idx <- idx + 1L; parts[[idx]] <- "No surveys contributed data to this table. All requested surveys were excluded."
  idx <- idx + 1L; parts[[idx]] <- ""
} else {
  idx <- idx + 1L; parts[[idx]] <- paste0(n_loaded, " survey", if (n_loaded != 1) "s" else "", " contributed data to this table.")
  idx <- idx + 1L; parts[[idx]] <- ""

  # Survey table
  idx <- idx + 1L; parts[[idx]] <- "| Country | Year | Welfare type | Survey ID |"
  idx <- idx + 1L; parts[[idx]] := "|---------|------|--------------|-----------|"
  for (i in seq_len(nrow(model$surveys$included))) {
    row <- model$surveys$included[i]
    wt <- if (row$welfare_type == "INC") "Income" else "Consumption"
    idx <- idx + 1L; parts[[idx]] <- paste0("| ", row$country_code, " | ", row$surveyid_year, " | ", wt, " | ", row$pip_id, " |")
  }
  idx <- idx + 1L; parts[[idx]] <- ""
}
```

---

## P1 — CRITICAL (must fix before merge)

### **[P1.1]** `[safe_auto]` [cg-architecture] `R/table_maker.R:95-181` — `.build_specification` lacks error resilience for registry lookups

**Why**: Registry failures silently produce `NULL` or empty lists. Inconsistent error handling: some produce empty lists, others `NULL`. Downstream consumers cannot distinguish between "registry unavailable" and "label not found". The specification block may contain mix of labeled and unlabeled entities.

**Fix**: Standardize error handling. Recommended: fail-fast since specification is opt-in metadata:
```r
# Remove try-catch wrappers and let registry failures propagate
stat_groups <- piptm_stat_groups(release = release)
layout_vars <- piptm_layout_covariates(release = release)
filter_cats <- piptm_filter_categories(release = release)
```

### **[P1.2]** `[manual]` [cg-architecture] `R/description.R:71-73` — Excluded surveys field check fragile

**Why**: Checks `if (nrow(exec$excluded_surveys) > 0L)` but assumes `exec$excluded_surveys` is always a data.table. If NULL, fails with unclear error.

**Fix**: Add defensive check:
```r
if (!is.null(exec$excluded_surveys) && 
    data.table::is.data.table(exec$excluded_surveys) && 
    nrow(exec$excluded_surveys) > 0L) {
  surveys$excluded_surveys <- exec$excluded_surveys
}
```

### **[P1.3]** `[manual]` [cg-testing] Missing test for all-excluded scenario

**Why**: No test verifies behavior when all surveys are excluded. This is a critical edge case.

**Fix**: Add test:
```r
test_that("all surveys excluded returns appropriate response", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())
  
  # Request dimension that no survey has
  res <- piptm::table_maker(
    pip_id = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
    analysis_var = "welfare",
    measures = "mean",
    by = "nonexistent_dimension",
    with_meta = TRUE
  )
  
  exec <- res$execution
  expect_equal(nrow(exec$loaded_surveys), 0L)
  expect_equal(nrow(exec$excluded_surveys), 2L)
  expect_true(all(exec$excluded_surveys$stage == "dimension_pre"))
}
```

### **[P1.4]** `[manual]` [cg-testing] No test for filter_pre stage exclusions

**Why**: Tests cover `stage="manifest"` and `stage="dimension_pre"` but not `stage="filter_pre"`.

**Fix**: Add test capturing filter_pre stage exclusion.

### **[P1.5]** `[safe_auto]` [cg-data-quality] `R/description.R:68` — `loaded_surveys` not validated for schema integrity

**Why**: Assumes `exec$loaded_surveys` has expected columns but doesn't verify. Could crash on invalid schema.

**Fix**: Add schema validation:
```r
req_survey_cols <- c("pip_id", "country_code", "surveyid_year", "welfare_type")
if (!data.table::is.data.table(exec$loaded_surveys)) {
  cli::cli_abort("{.field execution$loaded_surveys} must be a data.table")
}
missing_cols <- setdiff(req_survey_cols, names(exec$loaded_surveys))
if (length(missing_cols) > 0L) {
  cli::cli_abort(
    c(
      "{.field execution$loaded_surveys} missing column{?s}: {.val {missing_cols}}",
      "i" = "Required columns: {.val {req_survey_cols}}"
    )
  )
}
```

### **[P1.6]** `[manual]` [cg-reproducibility] `R/table_maker.R:101-103` — Registry lookups wrapped in tryCatch may return inconsistent defaults

**Why**: Falls back to `list()` on error without logging. Different error states produce identical empty list outputs, masking real issues.

**Fix**: Add explicit warning:
```r
stat_groups <- tryCatch(
  piptm_stat_groups(release = release),
  error = function(e) {
    cli::cli_warn("Failed to load stat_groups for release {release}: {conditionMessage(e)}")
    list()
  }
)
```

---

## P2 — IMPORTANT (should fix)

### **[P2.1]** `[manual]` [cg-architecture] `R/table_maker.R:659-666` — Accessing internal helper via `:::` operator

**Why**: Uses `piptm:::.find_welfare_col()` to access internal helper from within same package. Should call directly.

**Fix**: Call `.find_welfare_col()` directly without namespace prefix.

### **[P2.2]** `[manual]` [cg-architecture] `R/table_maker.R:856-860` — Specification receives resolved_release instead of original release

**Why**: Semantic inconsistency. Specification should store *requested* release (may be NULL), while execution stores *resolved* release.

**Fix**: Thread original `release` to `.build_specification()`, add `release` field to specification schema, use `resolved_release` internally for registry calls but store original `release`.

### **[P2.3]** `[safe_auto]` [cg-architecture] `R/table_maker.R:588-600` — Duplicated exclusion tracking logic

**Why**: rbind pattern repeated three times (manifest, filter_pre, dimension_pre). DRY violation.

**Fix**: Extract `.record_exclusions()` helper function.

### **[P2.4]** `[manual]` [cg-code-quality] `R/table_maker.R:469-477` — DRY violation: repeated rbind pattern

**Why**: Code duplication across 3 exclusion stages.

**Fix**: Extract helper function `.record_excluded_survey(meta_state, pip_id, reason, stage)`.

### **[P2.5]** `[manual]` [cg-code-quality] `R/table_maker.R:494-502, 572-580` — DRY violation: duplicated dropped survey info

**Why**: `dropped_info` construction duplicated with only variable name differences.

**Fix**: Extract `.format_dropped_info()` helper.

### **[P2.6]** `[manual]` [cg-documentation] `R/table_maker.R:98` — `.build_specification()` missing @param release

**Why**: Signature added `release` parameter but no roxygen documentation.

**Fix**: Add `@param release Character scalar release ID used for registry lookups.`

### **[P2.7]** `[manual]` [cg-documentation] `R/description.R:399` — `.generate_cell_definition()` @param incomplete

**Why**: `@param exec` doesn't explain which fields are used or why.

**Fix**: Document that it uses `resolved_ppp` for execution truth.

### **[P2.8]** `[manual]` [cg-documentation] Missing README documentation of with_meta mode

**Why**: No user-facing docs for `with_meta = TRUE` or new schema fields.

**Fix**: Add section to README with example code and field descriptions.

###  **[P2.9]** `[manual]` [cg-version-control] `.gitignore` missing R package patterns

**Why**: No `.Rhistory`, `.RData`, `.Renviron`, `*.Rproj`. Risk of leaking sensitive env vars.

**Fix**: Add R development artifacts to `.gitignore`.

### **[P2.10]** `[manual]` [cg-version-control] `.gitignore` no data file patterns

**Why**: No exclusions for `.csv`, `.xlsx`, `.dta`, `.parquet`. Risk of committing large datasets.

**Fix**: Add data exclusion patterns.

### **[P2.11]** `[safe_auto]` [cg-testing] `test-table-maker-with-meta.R:76-78` — Insufficient validation of loaded_surveys structure

**Why**: Checks column count but not data types or non-NA constraints.

**Fix**: Add type and completeness assertions.

### **[P2.12]** `[manual]` [cg-testing] No test for ppp=NULL resolution

**Why**: Tests explicit `ppp=2021L` but not default `ppp=NULL`.

**Fix**: Add test verifying fallback to manifest `ppp_sort`.

### **[P2.13]** `[safe_auto]` [cg-testing] Missing test for aggregate mode (by=NULL)

**Why**: No test for by=NULL with valid metadata.

**Fix**: Add aggregate mode test.

### **[P2.14]** `[safe_auto]` [cg-reproducibility] `R/table_maker.R:649-655` — `.find_welfare_col()` lookup depends on manifest order

**Why**: Tracks first survey's column without verifying uniformity.

**Fix**: Add assertion verifying all surveys have identical welfare column structure.

### **[P2.15]** `[advisory]` [cg-data-quality] `R/table_maker.R:310-314` — `excluded_surveys` initialized without type constraints

**Why**: No runtime enforcement that later `rbind()` calls preserve type consistency.

**Fix**: Add type checks or use helper function with stopifnot().

### **[P2.16]** `[safe_auto]` [cg-data-quality] `R/description.R:190` — `metadata$ppp_column` rendered without null/NA guard

**Why**: Would render "**Column:** NA" if P0 issue occurs.

**Fix**: Conditional rendering to skip column field if NA.

---

## P3 — MINOR (nice to have)

### **[P3.1]** `[advisory]` [cg-architecture] `R/table_maker.R:306-325` — `.meta_state` has no namespace isolation

**Why**: Mutation via `<<-` in warning handler creates implicit coupling. Harder to test in isolation.

**Fix**: Advisory only. Consider R6 object for future extensibility.

### **[P3.2]** `[advisory]` [cg-architecture] `R/description.R:58-63` — Metadata section duplicates release from provenance

**Why**: Both `metadata$release` and `provenance$release` exist. If they diverge, unclear which is authoritative.

**Fix**: Document intended relationship.

### **[P3.3]** `[manual]` [cg-architecture] `R/description.R:439` — Cell definition uses exec$resolved_ppp (correct but needs comment)

**Why**: Uses execution truth (correct) but lacks explanation.

**Fix**: Add comment explaining why `exec$resolved_ppp` instead of `spec$ppp`.

### **[P3.4]** `[safe_auto]` [cg-code-quality] `tests/testthat/helper-description.R:79` — Mock fixture has stale comment

**Why**: `# NEW: stage column` marker outdated after implementation complete.

**Fix**: Remove `# NEW` markers.

### **[P3.5]** `[safe_auto]` [cg-code-quality] `R/description.R:292` — Variable shadowing in lapply

**Why**: Uses `function(cat)` shadowing base R `cat()`.

**Fix**: Rename to `function(category)`.

### **[P3.6]** `[advisory]` [cg-code-quality] `R/table_maker.R:804` — Inconsistent use of `4L` as rounding precision

**Why**: Uses `4L` integer literal instead of plain `4`.

**Fix**: Use `round(..., 4)` for consistency.

### **[P3.7]** `[advisory]` [cg-documentation] `R/table_maker.R:295-305` — Schema comment uses undefined "Step N" references

**Why**: References "Step 2", "Step 3" but steps not documented.

**Fix**: Remove step numbers, use descriptive phrases.

### **[P3.8]** `[advisory]` [cg-documentation] `R/table_maker.R:306-322` — `.meta_state` initialization lacks inline comments

**Why**: Cryptic step markers. Field names would benefit from clarifications.

**Fix**: Enhance comments with brief explanations.

### **[P3.9]** `[manual]` [cg-documentation] `R/description.R:34` — `build_description_model()` has no @examples

**Why**: Exported function lacks usage example.

**Fix**: Add `\dontrun{}` example.

### **[P3.10]** `[manual]` [cg-documentation] `R/description.R:163` — `render_description_markdown()` has no @examples

**Why**: Exported function lacks usage example.

**Fix**: Add `\dontrun{}` example.

### **[P3.11]** `[advisory]` [cg-testing] No test verifies determinism of repeated calls

**Why**: Missing test for identical output from identical inputs.

**Fix**: Add determinism regression test.

### **[P3.12]** `[safe_auto]` [cg-testing] `test-table-maker-with-meta.R:257-283` — Field presence checked but not types

**Why**: Verifies fields exist but not their types.

**Fix**: Add type checks for all execution fields.

### **[P3.13]** `[advisory]` [cg-data-quality] `R/description.R:165` — Missing input validation could allow partial model

**Why**: Doesn't validate structure of required fields. Poor error messages if NULL.

**Fix**: Add structure validation after field presence check.

---

## ✅ Passed

- **cg-performance**: Registry hoisting optimization (beneficial)  
- **cg-performance**: Metadata state initialization overhead (negligible)  
- **cg-performance**: Resolved release capture (performance win)  
- **cg-performance**: List accumulation pattern in renderer (optimal)  
All performance findings show the redesign introduces acceptable overhead (<10% for typical workloads) with three net performance improvements.

- **cg-reproducibility**: Test fixture determinism achieved (runif() → seq())  
The P0 blocking reproducibility issue was resolved in this commit.

- **cg-version-control**: Commit message follows conventional commits  
- **cg-version-control**: No secrets or credentials detected  
- **cg-version-control**: Comprehensive test coverage

---

## Summary

**Architecture Assessment**: **B+**

The metadata schema redesign successfully achieves "execution truth" through clean separation of `specification` (requested) and `execution` (actual) blocks. The implementation correctly threads new fields through the call chain and maintains backward compatibility.

**Key Strengths**:
- ✅ Clean module boundaries between `table_maker`, `build_description_model`, and `render_description_markdown`
- ✅ Excellent backward compatibility (`with_meta = FALSE` unchanged)
- ✅ Performance optimizations included (registry hoisting, release caching, list accumulation)
- ✅ Comprehensive test coverage (16 new tests)
- ✅ Deterministic test fixtures (runif → seq)

**Key Concerns**:
- ❌ **3 P0 issues**: Missing validation for metadata fields could produce incorrect statistical documentation
- ❌ **6 P1 issues**: Schema validation gaps and error resilience
- ⚠️ **16 P2 issues**: DRY violations, documentation gaps, .gitignore missing patterns
- ℹ️ **13 P3 issues**: Advisory improvements for maintainability

**Immediate Actions Required**:
1. **P0**: Add null/NA validation for `ppp_column_used` (data quality)
2. **P0**: Fail loudly on PPP resolution failure instead of silent NA
3. **P0**: Fix empty survey table rendering edge case
4. **P1**: Add schema validation for `loaded_surveys` and `excluded_surveys`
5. **P1**: Add tests for all-excluded scenario and filter_pre stage
6. **P1**: Standardize registry error handling

**Recommended Next Steps**:
1. Fix all P0 and P1 findings before merge
2. Update .gitignore with R package patterns (P2.9, P2.10)
3. Extract exclusion-tracking helper to eliminate DRY violations (P2.3, P2.4, P2.5)
4. Add README documentation for `with_meta` mode (P2.8)

---

Parsed 45 finding IDs. Use `/cg-fix-triage P1` to apply all P1 findings, or `/cg-fix-triage P0.1 P0.2` to apply specific findings by ID.
