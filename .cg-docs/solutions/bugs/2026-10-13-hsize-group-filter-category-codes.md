---
date: 2026-10-13
title: "hsize_group/age_group filter metadata crash from stale inline category codes shadowing recode_spec mapping"
category: "bugs"
type: "bug"
language: "R"
tags: [registry, tm_variable_spec, recode_spec, filter_base, table_maker, metadata, hsize_group, age_group, subscript-out-of-bounds]
root-cause: "`.build_registry_entry()` applied hand-authored inline `categories:` from tm_variable_spec.yaml before checking the pipdata recode_spec mapping, so stale range-string codes (\"2-3\", \"4-6\", \"7+\") silently shadowed the correct integer codes (\"2\",\"3\",\"4\") that pipdata actually assigns to hsize_group/age_group."
severity: "P2"
test-written: "yes"
fix-confirmed: "yes"
red-phase-confirmed: "yes"
expected-behavior-source: "documentation"
test-gap: "edge-case-gap"
---

# hsize_group/age_group filter metadata crash from stale inline category codes shadowing recode_spec mapping

## Symptom

`GET /table` with `filter_base={"hsize_group":[2]}` and `include_metadata=true` returned correct
data rows but failed metadata generation with:

```
Description metadata assembly failed; returning data without metadata.
ℹ subscript out of bounds
```

The crash originated in `.build_description_metadata()` (`R/table_maker.R`), in the filter
label-resolution step, when trying to look up the integer code `"2"` against a catalog whose
`hsize_group` subcategories only had the string keys `"1"`, `"2-3"`, `"4-6"`, `"7+"`.

## Expected Behavior Source

**Documentation** — `{pipdata}`'s authoritative `recode_spec.yml` (external package, confirmed
on disk at `R_packages/pipdata/inst/extdata/recode_spec.yml:335-353`) documents `hsize_group` as
`recode_type: binned_from_continuous` with an explicit integer `mapping`:

```yaml
hsize_group:
  recode_type: binned_from_continuous
  source_column: hsize
  mapping:
    1: "1 person"
    2: "2–3 persons"
    3: "4–6 persons"
    4: "7+ persons"
```

The same file documents `age_group` (lines 286-304) with an identical pattern: integer codes
`1-4` mapped to range-text labels. This is the authoritative contract for what the real
materialized data column contains. Per piptm's own architecture comment in
`R/registry.R` ("merges pipdata recode spec (automated type + mapping metadata) + UI YAML
specification (labels, roles, stat groups)"), category **codes** for pipdata-backed variables
must come from this recode_spec mapping, not from independently hand-typed YAML values.

## Root Cause

`tm_variable_spec.yaml` (`inst/extdata/tm_variable_spec.yaml:115-129`) carries a manually
hand-authored inline `categories:` block for `hsize_group` that duplicates the range text into
the `code` field itself:

```yaml
hsize_group:
  categories:
    - code: "1"
    - code: "2-3"    # should resolve to "2" via recode_spec
    - code: "4-6"    # should resolve to "3" via recode_spec
    - code: "7+"      # should resolve to "4" via recode_spec
```

`.build_registry_entry()` in `R/registry.R` checked for this inline block **first** (prior to
consulting `pip_vars`/recode_spec) and returned immediately when present, completely bypassing
the `.mapping_to_categories(pip_var$mapping)` path (lines 102-152) that would otherwise derive
the correct integer codes from recode_spec. `age_group` has the identical defect.

Because the stale codes were published by `piptm_filter_categories()` / `GET /categories`, and
`load_surveys()`/`table_maker()` independently enforce an integer-only contract for
`filter_base` values, any client correctly sending an integer code (e.g. `2`) for `hsize_group`
could never find a matching key in the stale catalog. The label-resolution step indexed an
**atomic character vector** (`subcat_map`, built with `stats::setNames`) using unsafe `[[`,
which throws `Error: subscript out of bounds` on a missing name (unlike a list, which would
return `NULL`). This uncaught error was converted by the outer `tryCatch` in `table_maker()`
into the user-visible "Description metadata assembly failed" warning.

## Reproduction Test

`tests/testthat/test-registry-binned-categories-source.R`:

```r
test_that(".build_registry_entry prefers recode_spec mapping for binned variables", {
  spec_var <- list(
    ui_label = "Household size",
    roles = c("filter", "covariate"),
    stat_groups = list(),
    categories = list(
      list(code = "1", label = "1 person"),
      list(code = "2-3", label = "2 to 3 persons"),
      list(code = "4-6", label = "4 to 6 persons"),
      list(code = "7+", label = "7 or more persons")
    )
  )

  pip_vars <- list(
    hsize_group = list(
      type = "factor", recode_type = "binned_from_continuous",
      mapping = list("1" = "1 person", "2" = "2-3 persons",
                     "3" = "4-6 persons", "4" = "7+ persons")
    )
  )

  entry <- piptm:::.build_registry_entry("hsize_group", spec_var, pip_vars)

  expect_identical(entry$tm_type, "categorical")
  expect_identical(entry$n_categories, 4L)
  expect_identical(
    vapply(entry$categories, `[[`, character(1), "code"),
    c("1", "2", "3", "4")
  )
})
```

Confirmed failing before the fix with:

```
Expected `vapply(entry$categories, `[[`, character(1), "code")` to be identical to `c("1", "2", "3", "4")`.
Differences:
`actual`:   "1" "2-3" "4-6" "7+"
`expected`: "1" "2"   "3"   "4"
```

## Test Gap

**edge-case-gap** — existing registry/category tests (`test-categories-current.R`,
`test-registry.R`, `test-registry-structure.R`) covered general structure, role filtering, and
the happy-path case where a variable has only inline categories (no pipdata entry). None of
them exercised the specific interaction where a variable has **both** an inline `categories:`
block *and* a `pip_vars` entry with a conflicting `mapping` — the exact boundary condition that
exposed the precedence bug. The gap was in fixture coverage, not in assertion strength.

## Fix

Two changes, both required:

**1. Registry precedence fix (`R/registry.R`, `.build_registry_entry()`)** — pipdata recode-spec
mapping now takes precedence; inline YAML `categories:` is used only as a fallback when the
variable has no corresponding `pip_vars` entry:

```r
pip_var <- pip_vars[[varname]]
inline_categories <- spec_var$categories
if (is.null(pip_var)) {
  if (!is.null(inline_categories) && length(inline_categories) > 0L) {
    categories <- lapply(inline_categories, function(entry) {
      list(code = as.character(entry$code), label = as.character(entry$label))
    })
    return(list(
      varname = varname, ui_label = as.character(spec_var$ui_label),
      tm_type = "categorical",
      roles = as.character(unlist(spec_var$roles, use.names = FALSE)),
      stat_groups = stat_groups,
      n_categories = as.integer(length(categories)),
      categories = categories
    ))
  }
  cli::cli_warn(...)  # unchanged: warn-and-skip when neither source has the variable
  return(NULL)
}
# ... falls through to pip_vars-driven .mapping_to_categories() path when pip_var exists
```

This causes `hsize_group`/`age_group` to now derive `categories` via
`.mapping_to_categories(pip_var$mapping)` whenever a pipdata recode-spec entry exists, yielding
the correct integer codes (`1,2,3,4`) automatically — no change needed to
`tm_variable_spec.yaml` itself, since the inline block remains a valid fallback for genuinely
pipdata-absent variables.

**2. Defensive lookup hardening (`R/table_maker.R`, `.build_description_metadata()`)** —
replaced the unsafe atomic-vector `[[` lookup with a safe `[`-based lookup plus explicit
fallback, so any future code/catalog mismatch degrades to displaying the raw code instead of
throwing:

```r
selected_labels <- vapply(selected_codes, function(code) {
  code_char <- as.character(code)
  resolved <- subcat_map[code_char]
  if (length(resolved) == 0L || is.na(resolved[[1L]]) || !nzchar(resolved[[1L]])) {
    code_char
  } else {
    unname(resolved[[1L]])
  }
}, character(1))
```

### Verification performed

- `devtools::test(filter='registry-binned-categories-source')` — new test passes (green).
- `devtools::test(filter='(categories-current|registry|description-metadata)')` —
  605 passing, 0 failing.
- `devtools::test()` (full suite) — 1140 passing, 0 failing, 12 pre-existing environment-config
  skips (unrelated to this change).
- Manual simulation: `.build_registry_entry("hsize_group", ...)` with a pipdata mapping present
  now returns `1,2,3,4`; with pipdata mapping absent, correctly falls back to inline categories
  (`1,2-3,4-6,7+`) — confirming no regression to the fallback path.
- Manual simulation: `.build_description_metadata()` fed the exact original crash input (stale
  catalog, integer code `2`) no longer throws; it degrades gracefully to the raw code as the
  label instead of crashing.
- Diff isolation confirmed: only `R/registry.R`, `R/table_maker.R`, and the new test file were
  modified; no other repository changes are part of this fix.

## Lessons Learned

When a registry/spec layer merges two sources of truth for the same field (here: a hand-authored
UI YAML spec and an externally-owned, machine-generated recode spec), the **precedence order
must favor the externally-owned source** for any variable the external source actually covers.
Inline/manual overrides should only ever be a *fallback* for variables the external source does
not cover — never a silent shadow for variables it does cover. This is an `edge-case-gap`
pattern: structural tests that assert general shape (count of filter variables, basic subcategory
structure) will not catch a precedence bug unless a fixture explicitly constructs the "both
sources present with conflicting values" scenario. Any future variable added with an inline
`categories:` block in `tm_variable_spec.yaml` should have a companion test asserting that, if
pipdata gains a matching recode_spec entry later, the pipdata-derived codes win — not the inline
ones.

## Related

None.
