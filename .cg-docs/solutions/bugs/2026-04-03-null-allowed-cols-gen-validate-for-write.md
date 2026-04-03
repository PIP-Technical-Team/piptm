---
date: 2026-04-03
title: "NULL .ALLOWED_COLS_GEN causes .validate_for_write() to reject all schema-valid columns"
category: "bugs"
type: "bug"
language: "R"
tags: [arrow, pipdata, piptm, onLoad, schema, validation, load_all, devtools]
root-cause: ".onLoad() in aaa.R assigned schema globals from piptm:: without error handling; when piptm is not installed/loaded (load_all() dev session), the assignment failed silently, leaving .ALLOWED_COLS_GEN as NULL. setdiff(names(dt), NULL) returns all column names, so every schema-valid column was flagged as 'extra'."
severity: "P1"
test-written: "yes"
fix-confirmed: "yes"
---

# NULL .ALLOWED_COLS_GEN causes .validate_for_write() to reject all schema-valid columns

## Symptom

`generate_arrow_dataset()` returned an `"error"` row for every survey with this
message:

```
Input contains column(s) not in the Arrow schema: "welfare", "weight", "area",
"educat4", "educat5", "age", "gender", "country_code", "surveyid_year",
"survey_acronym", "welfare_type", "pip_id", and "version".
Run `prepare_for_arrow()` first, or drop these columns manually.
```

The listed columns are the *correct* schema columns produced by
`prepare_for_arrow()`. The data was fully prepared; the validation was broken.

## Root Cause

`aaa.R`'s `.onLoad()` initialised five package-level globals by calling
`piptm::pip_arrow_schema()` et al.:

```r
.onLoad <- function(libname, pkgname) {
  .SCHEMA_GEN        <<- piptm::pip_arrow_schema()
  .REQUIRED_COLS_GEN <<- piptm::pip_required_cols()
  .ALLOWED_COLS_GEN  <<- piptm::pip_allowed_cols()
  ...
}
```

During a `devtools::load_all("pipdata")` dev session, `piptm` is *not*
automatically put on the search path (only `pipdata` is loaded). The `piptm::`
calls therefore failed, but the failure was **silent** — `<<-` to an already-
declared `NULL` global does not propagate the error out of `.onLoad()` by
default. All five globals stayed `NULL`.

Inside `.validate_for_write()`:

```r
extra_cols <- setdiff(names(dt), .ALLOWED_COLS_GEN)  # setdiff(x, NULL) == x
```

`setdiff(x, NULL)` returns its left argument unchanged, so *every* column in
the data was listed as "extra" and the function aborted.

The same pattern affected `.REQUIRED_COLS_GEN` (returns all names →
"required columns missing") and `.SCHEMA_GEN` / `.GENDER_LEVELS_GEN` /
`.AREA_LEVELS_GEN` downstream.

## Reproduction Test

Added to `pipdata/tests/testthat/test-arrow-generation.R`:

```r
test_that(".validate_for_write succeeds for valid data when .ALLOWED_COLS_GEN is NULL", {
  dt <- make_arrow_dt()

  allowed <- piptm::pip_allowed_cols()
  expect_false(is.null(allowed))
  expect_true("pip_id"   %in% allowed)
  expect_true("welfare"  %in% allowed)
  expect_true("version"  %in% allowed)

  expect_true(pipdata:::.validate_for_write(dt))
})
```

The test failed (10 failures across the file) before the fix because `.ALLOWED_COLS_GEN`
was `NULL` in the session — the installed `piptm` had a stale schema
(`survey_id`, `education`) that did not match the dev schema. All `write_survey_parquet()`
and `.validate_for_write()` tests also failed.

## Fix

### 1. `aaa.R` — harden `.onLoad()` with `tryCatch`

```r
.onLoad <- function(libname, pkgname) {
  tryCatch({
    .SCHEMA_GEN        <<- piptm::pip_arrow_schema()
    .REQUIRED_COLS_GEN <<- piptm::pip_required_cols()
    .ALLOWED_COLS_GEN  <<- piptm::pip_allowed_cols()
    .GENDER_LEVELS_GEN <<- .SCHEMA_GEN$levels$gender
    .AREA_LEVELS_GEN   <<- .SCHEMA_GEN$levels$area
  }, error = function(e) {
    packageStartupMessage(
      "[pipdata] Could not initialise Arrow schema globals from {piptm}: ",
      conditionMessage(e),
      "\n  Arrow validation will call piptm:: lazily at runtime."
    )
  })
}
```

Failures in `.onLoad()` now surface as a startup message rather than being
swallowed.

### 2. `arrow_generation.R` — add lazy accessor functions

Five accessor helpers replace direct global references. Each falls back to
calling `piptm::` directly when the global is `NULL`:

```r
.get_schema         <- function() if (is.null(.SCHEMA_GEN))        piptm::pip_arrow_schema()  else .SCHEMA_GEN
.get_required_cols  <- function() if (is.null(.REQUIRED_COLS_GEN))  piptm::pip_required_cols() else .REQUIRED_COLS_GEN
.get_allowed_cols   <- function() if (is.null(.ALLOWED_COLS_GEN))   piptm::pip_allowed_cols()  else .ALLOWED_COLS_GEN
.get_gender_levels  <- function() if (is.null(.GENDER_LEVELS_GEN))  piptm::pip_arrow_schema()$levels$gender else .GENDER_LEVELS_GEN
.get_area_levels    <- function() if (is.null(.AREA_LEVELS_GEN))    piptm::pip_arrow_schema()$levels$area   else .AREA_LEVELS_GEN
```

All five direct global references in `.validate_for_write()` and
`.build_arrow_schema()` were replaced with calls to the corresponding accessor.

## Lessons Learned

1. **Never use bare `<<-` in `.onLoad()` for cross-package calls.** If the
   dependency is unavailable, the assignment fails silently and leaves
   `NULL` globals that corrupt every downstream call. Always wrap in
   `tryCatch`.

2. **`setdiff(x, NULL)` is identity, not empty-set.** A `NULL` "allowed list"
   effectively allows nothing — the opposite of the intent. Defensive code
   must treat `NULL` as "not initialised" rather than "no columns allowed".

3. **Package-level globals populated from a Suggests/Imports dependency at
   load time need lazy fallbacks.** In a two-package development workflow
   (`load_all(A)` where A depends on B), B may not be on the search path.
   The lazy-accessor pattern (`if (is.null(GLOBAL)) call_dep() else GLOBAL`)
   is the correct idiom here.

4. **Install both packages from source when their schemas co-evolve.**
   The installed `piptm` had a stale schema (`survey_id`, `education`) that
   didn't match the dev schema (`pip_id`, `educat4`/`educat5`/`educat7`,
   `version`). Always `devtools::load_all()` or `devtools::install()` both
   packages after a schema-breaking change.

## Related

None.
