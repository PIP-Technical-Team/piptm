---
date: 2026-08-24
title: "Metadata Schema: Execution Truth Pattern"
category: "data-quality"
language: "R"
tags: [metadata, reproducibility, execution-truth, specification-vs-execution, architecture-review]
root-cause: "Metadata schema conflated user requests with actual execution results, making descriptions inaccurate when surveys were excluded or parameters resolved differently"
severity: "P1"
---

# Metadata Schema: Execution Truth Pattern

## Problem

The `table_maker(with_meta=TRUE)` metadata contract mixed **requested** parameters with **actual** execution results, producing descriptions that didn't accurately reflect what happened during computation:

1. **Specification used resolved values**: Used `resolved_release` instead of the original `release` parameter (which might be NULL)
2. **Execution lacked key fields**: Missing `requested_pip_id`, stage tracking for exclusions, physical PPP column name
3. **No schema validation**: Consumers could receive NULL or malformed metadata without errors
4. **DRY violations**: Exclusion tracking duplicated across 3 stages (manifest, filter_pre, dimension_pre)

**Symptoms**:
- Description said "using release X" when user requested NULL (defaulted to current)
- Couldn't distinguish between "user requested these surveys" vs "these surveys actually loaded"
- No visibility into *why* surveys were excluded or *when* (manifest vs pre-filter vs dimension check)
- Silent failures when PPP column resolution failed (NA assignment instead of error)

## Root Cause

The original schema didn't separate **specification** (what the user requested) from **execution** (what actually happened). This conflation made it impossible to write accurate, truthful descriptions of computation provenance.

### Core Design Flaw

```r
# BEFORE: Conflated schema
metadata <- list(
  release = resolved_release,  # ❌ Execution result, not request
  ppp = ppp,                    # ❌ Could be NULL (request) or resolved value
  surveys = loaded_surveys      # ❌ Missing excluded surveys and reasons
)
```

The schema also lacked defensive validation at boundaries, allowing NULL/NA values to propagate into rendered descriptions.

## Solution

### 1. Separate Specification from Execution

**Specification** = what the user requested (may contain NULL, unresolved parameters):

```r
specification <- .build_specification(
  pip_id = pip_id,              # All requested IDs
  release = requested_release,  # May be NULL
  ppp = ppp,                    # May be NULL
  # ... other parameters as requested
)
```

**Execution** = what actually happened (all values resolved, surveys tracked):

```r
execution <- list(
  requested_pip_id  = pip_id,           # Original request
  loaded_surveys    = dt_surveys,       # Surveys that contributed data
  excluded_surveys  = dt_excluded,      # With stage: manifest|filter_pre|dimension_pre
  resolved_release  = current_release,  # Single authority value
  resolved_ppp      = 2021L,            # After resolution
  ppp_column_used   = "welfare_ppp_2021"  # Physical column name
)
```

### 2. Add Schema Validation at Boundaries

```r
# Validate execution metadata on read
if (is.null(exec$resolved_release) || is.null(exec$resolved_ppp) || 
    is.null(exec$ppp_column_used)) {
  cli::cli_abort(
    c(
      "{.arg table_result} execution metadata incomplete",
      "i" = "resolved_release, resolved_ppp, and ppp_column_used must all be populated"
    )
  )
}

# Validate loaded_surveys schema
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

### 3. Fail Loudly on Resolution Errors

```r
# BEFORE: Silent NA assignment
if (length(matched_col) > 0L) {
  .meta_state[["ppp_column_used"]] <- matched_col[[1L]]
} else {
  .meta_state[["ppp_column_used"]] <- NA_character_  # ❌ Silent failure
}

# AFTER: Explicit error
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

### 4. Extract Helpers to Eliminate DRY Violations

```r
# Helper for exclusion tracking (used 3x)
.record_exclusions <- function(meta_state, pip_ids, reasons, stage) {
  if (is.null(meta_state)) return(meta_state)
  
  exc <- data.table::data.table(
    pip_id = pip_ids,
    reason = reasons,
    stage  = stage
  )
  
  meta_state[["excluded_surveys"]] <- data.table::rbindlist(list(
    meta_state[["excluded_surveys"]], exc
  ))
  
  meta_state
}

# Usage:
.meta_state <- .record_exclusions(
  .meta_state,
  pip_ids = missing_ids,
  reasons = rep("Not found in manifest", length(missing_ids)),
  stage = "manifest"
)
```

```r
# Helper for dropped survey info (used 2x)
.format_dropped_info <- function(dropped_entries, required_vars, var_type = "dimensions") {
  vapply(seq_len(nrow(dropped_entries)), function(i) {
    have <- dropped_entries$dimensions[[i]]
    miss <- setdiff(required_vars, have)
    if (length(miss) == length(required_vars)) {
      paste0(dropped_entries$pip_id[[i]], ": no ", var_type)
    } else {
      paste0(dropped_entries$pip_id[[i]], ": missing ", paste(miss, collapse = ", "))
    }
  }, character(1L))
}
```

### 5. Document the Schema in README

```markdown
#### Metadata mode (`with_meta = TRUE`)

result <- piptm::table_maker(..., with_meta = TRUE)

# Result structure:
# $ data         : data.table with computed statistics
# $ specification: list of requested parameters
# $ execution    : list of what actually happened
# $ provenance   : list of package version, timestamp
# $ warnings     : list of captured warnings

result$execution$loaded_surveys   # Surveys that contributed
result$execution$excluded_surveys # Surveys excluded (with stage & reason)
```

## Prevention

### Architectural Patterns

1. **Always separate request from result**
   - Specification = user input (preserve NULLs, unresolved params)
   - Execution = runtime truth (all values resolved, actual outcomes)

2. **Validate at schema boundaries**
   - Check for NULL/NA before consuming metadata fields
   - Fail fast with informative errors, not silent degradation
   - Use data.table `is.data.table()` checks before `nrow()`

3. **Track provenance with stages**
   - Exclusions: record `stage` (manifest | filter_pre | dimension_pre)
   - Resolutions: store both requested and resolved values
   - Physical column names: track the actual column used, not just PPP year

4. **Extract helpers for repeated patterns**
   - If logic appears 2+ times, extract a function
   - Parameterize differences (stage name, var_type label)
   - Use vectorized operations over loops when possible

### Code Smells to Watch For

- **Silent fallbacks**: `x <- if (...) value else NA_character_`  
  → Should be: `if (...) value else cli::cli_abort(...)`

- **Assumption without validation**: `nrow(df)` without checking `is.null(df)`  
  → Should be: defensive checks before use

- **Duplicated rbind patterns**: Same structure in multiple places  
  → Extract helper function

- **Conflated naming**: Using `release` for both request and resolution  
  → Use `requested_release` vs `resolved_release`

### Testing Requirements

When adding execution metadata:
- Test all-excluded scenario (0 loaded surveys)
- Test each exclusion stage independently
- Test NULL parameter resolution
- Test aggregate mode (by=NULL) vs disaggregated
- Add type/completeness assertions for data.table schemas

## Related

- **Architecture review**: `.cg-docs/reviews/2026-08-24-step3-description-metadata-contract-corrective-review.md`
- **Implementation plan**: `.cg-docs/plans/2026-08-24-step3-description-metadata-contract-corrective.md`
- **Schema validation pattern**: `.cg-docs/solutions/data-quality/2026-06-11-arrow-schema-expansion-propagation-pattern.md` — defensive validation at schema boundaries
- **Testing edge cases**: `.cg-docs/solutions/testing-patterns/2026-04-14-partial-miss-regression-test-batch-loaders.md` — pattern for testing all-excluded scenarios
- **R testing patterns**: `cg-skill-r-testing` (testthat 3, fixtures, mocking)
- **data.table Reference**: `cg-skill-r-datatable` (schema validation, rbindlist)

### External References

- **Defensive programming in R**: Use `cli::cli_abort()` for user-facing errors with formatting
- **data.table best practices**: Always check `is.data.table()` before `nrow()`
- **Metadata standards**: DCAT, PROV-O (provenance ontology) for inspiration on specification vs execution separation
