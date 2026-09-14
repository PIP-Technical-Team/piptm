---
date: 2026-08-27
title: "JSON Round-Trip Defensive Coding: Schema Validation + Test Coverage Strategy"
category: "data-quality"
language: "R"
tags: [json-deserialization, schema-validation, data.table, defensive-programming, test-coverage, api-safety]
root-cause: "JSON round-trip from jsonlite::fromJSON() converts data.tables to data.frames, empty tables to empty lists, and can change column types — causing crashes when code assumes strict structure"
severity: "P0"
---

# JSON Round-Trip Defensive Coding: Schema Validation + Test Coverage Strategy

## Problem

An API endpoint accepting `description_metadata` from JSON crashed with malformed inputs because:
1. Code assumed `data.table` structures but `jsonlite::fromJSON()` returns `data.frame`
2. Empty data.tables became `list()` (not `NULL` or empty `data.frame`)
3. List-column elements could be `NULL`, integers, or wrong types after deserialization
4. Missing columns weren't validated before access

**Symptoms**:
- Silent crashes: `loaded$country_code` on `NULL` → error
- Type errors: `vapply()` over `NULL` elements in list-column → `character(0)` violates contract
- Malformed data passed schema checks → downstream crashes

**Impact**: P0 data corruption / crash risk when JSON metadata bypasses validation.

---

## Root Cause

**JSON round-trip type conversions**:
```r
# Before JSON                    # After fromJSON(toJSON(...))
data.table(x = 1:3)         →    data.frame(x = 1:3)  # loses data.table class
data.table()                →    list()               # empty table → empty list
list(NULL, c("a"))          →    list(NULL, "a")      # unchanged
list(c(1L, 2L))             →    list(c(1, 2))        # integer → numeric
```

**Code assumptions broken**:
1. `is.data.frame(loaded)` passed, but `loaded` was `NULL` → later `loaded$col` crashed
2. `nrow(list())` returns `NULL` → comparisons with `0L` failed
3. `selected_labels` list-column had `NULL` elements → `paste(NULL, collapse=", ")` returned `character(0)` in `vapply()` expecting `character(1)`

---

## Solution

### 1. **Defensive NULL/Type Checks Before Column Access**

**Pattern**: Always check `is.null() || !is.data.frame() || nrow() == 0` before accessing columns.

```r
# R/description_builder.R:223-224
# Before (WRONG):
n_loaded_rows <- if (is.data.frame(loaded)) nrow(loaded) else 0L

# After (CORRECT):
n_loaded_rows <- if (is.null(loaded)) {
  0L
} else if (is.data.frame(loaded)) {
  nrow(loaded)
} else {
  0L
}
```

**Why**: `is.data.frame(NULL)` returns `FALSE`, but subsequent code must handle the NULL case explicitly.

---

### 2. **Schema Validation Before Column Access**

**Pattern**: Validate required columns exist before accessing them.

```r
# R/description_builder.R:269-274
filters_dt <- data.table::as.data.table(filters_dt)
required_cols <- c("ui_label", "selected_labels")
if (!all(required_cols %in% names(filters_dt))) {
  cli::cli_abort("filters_dt missing required columns: {setdiff(required_cols, names(filters_dt))}")
}
```

**Why**: JSON deserialization can produce data.frames with incomplete schemas. Check before accessing.

---

### 3. **Normalize List-Column Types Before vapply()**

**Pattern**: Coerce list-column elements to expected type before `vapply()`.

```r
# R/description_builder.R:276-280
# Normalize selected_labels list-column BEFORE vapply
filters_dt[, selected_labels := lapply(selected_labels, function(x) {
  if (is.null(x) || length(x) == 0) return(character(0))
  as.character(x)  # Coerce integers/factors from JSON
})]

# THEN use vapply safely
vapply(filters_dt$selected_labels, 
       function(x) if (length(x) == 0) "(unspecified)" else paste(x, collapse = ", "),
       character(1))
```

**Why**: JSON can convert integer codes to numeric, or leave `NULL` elements. Normalize types first.

---

### 4. **Type Coercion Before fifelse()**

**Pattern**: Coerce columns to expected types before using `fifelse()` to avoid type mismatch errors.

```r
# R/description_builder.R:356-369
# Coerce FIRST
covariates_formatted[, ':='(
  varname = as.character(varname),      # JSON could produce factors
  ui_label = as.character(ui_label),
  n_categories = as.integer(n_categories)  # JSON could produce numeric
)]

# THEN use fifelse safely
covariates_formatted[, ':='(
  varname = fifelse(is.na(varname), "", varname),
  ui_label = fifelse(is.na(ui_label), "(None)", ui_label),
  n_categories = fifelse(is.na(n_categories), 0L, n_categories)
)]
```

**Why**: `fifelse()` requires `yes` and `no` branches to have compatible types. Coercing `""` to numeric fails silently.

---

### 5. **Test Coverage Strategy: Test the Defensive Code Paths**

**Problem**: The defensive checks were added but **not tested** — schema validation error paths were never exercised.

**Solution**: Write tests that **directly exercise the defensive code** with malformed inputs.

```r
# tests/testthat/test-description-builder-schema-validation.R

# P0-1: Test NULL handling
test_that(".build_surveys_content() handles NULL loaded surveys", {
  meta <- list(
    surveys = list(loaded = NULL, excluded = NULL),
    execution = list(n_surveys_loaded = 0L, n_surveys_excluded = 0L)
  )
  result <- piptm:::.build_surveys_content(meta)
  expect_equal(result$n_loaded, 0L)
  expect_equal(nrow(result$loaded_list), 0L)
})

# P0-2: Test schema validation abort
test_that(".build_filters_content() aborts when filters_dt missing ui_label", {
  meta <- list(
    resolved_labels = list(
      filters = data.table(varname = "age_group", selected_labels = list(c("0-14")))
      # missing ui_label column
    )
  )
  expect_error(
    piptm:::.build_filters_content(meta),
    class = "rlang_error",
    regexp = "missing required columns"
  )
})

# P0-3: Test NULL normalization in list-column
test_that(".build_filters_content() normalizes NULL elements in selected_labels", {
  meta <- list(
    resolved_labels = list(
      filters = data.table(
        varname = "gender",
        ui_label = "Gender",
        selected_labels = list(NULL)  # JSON round-trip can produce this
      )
    )
  )
  result <- piptm:::.build_filters_content(meta)
  expect_equal(result$filters$selected_categories, "(unspecified)")
})
```

**Key insight**: Don't just test happy paths — **test the error paths your defensive code protects against**.

---

## Prevention

### Coding Standards

**When accepting JSON-deserialized data in R**:

1. ✅ **Always check `is.null()` before `is.data.frame()`** — `NULL` is a valid JSON round-trip result
2. ✅ **Validate schema (required columns) before column access** — JSON can produce incomplete data.frames
3. ✅ **Normalize list-column types before `vapply()`** — JSON doesn't preserve `NULL` vs `character(0)` distinctions
4. ✅ **Coerce to expected types before `fifelse()`** — JSON converts integers to numeric, characters to factors
5. ✅ **Use `data.table::as.data.table()` defensively** — don't assume incoming structure is already data.table

### Anti-Patterns to Avoid

❌ **Don't assume `is.data.frame()` handles `NULL`**:
```r
n_rows <- if (is.data.frame(x)) nrow(x) else 0L  # WRONG if x is NULL
```

❌ **Don't access columns without schema validation**:
```r
filters_dt$ui_label  # WRONG if ui_label column missing after JSON
```

❌ **Don't use `vapply()` on unvalidated list-columns**:
```r
vapply(x$col, paste, character(1))  # WRONG if col has NULL elements
```

❌ **Don't use `fifelse()` on uncoerced types**:
```r
fifelse(is.na(x), "", x)  # WRONG if x is numeric (coerces "" to 0)
```

### Test Coverage Rules

**For every defensive check you add, write a test that exercises it**:

| Defensive Code | Required Test |
|----------------|---------------|
| `if (is.null(x)) return(...)` | Test with `x = NULL` |
| `if (!all(cols %in% names(dt))) abort(...)` | Test with missing column |
| `lapply(x, function(e) if (is.null(e)) ...)` | Test with `list(NULL)` |
| `as.character(x)` before `fifelse()` | Test with factor/numeric input |

**Verification strategy**: After adding defensive code, run `/cg-review mode:verify` to catch untested error paths.

---

## Related

### Internal References
- `.cg-docs/reviews/2026-08-27-phase5-standard-review.md` — Initial P0 findings
- `.cg-docs/reviews/2026-08-27-phase5-standard-verify-review.md` — Test coverage gaps identified
- `.cg-docs/reviews/2026-08-27-phase5-standard-verify-review-2.md` — Coverage verification

### R Packages
- `jsonlite::fromJSON()` — JSON deserialization behavior
- `data.table::fifelse()` — Type-safe if-else (requires compatible types)
- `cli::cli_abort()` — User-facing error messages

### External Resources
- [jsonlite vignette](https://cran.r-project.org/web/packages/jsonlite/vignettes/json-aaquickstart.html) — JSON mapping to R types
- [data.table wiki: Special symbols](https://github.com/Rdatatable/data.table/wiki/Getting-started#special-symbols) — `:=`, `fifelse()`

### Similar Patterns
- API input validation in plumber endpoints
- Arrow parquet round-trip type safety
- Database query result type coercion
