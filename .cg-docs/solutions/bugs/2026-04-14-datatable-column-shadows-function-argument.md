---
date: 2026-04-14
title: "data.table column name shadows function argument in filter expression"
category: "bugs"
type: "bug"
language: "R"
tags: [data.table, NSE, scoping, filter, column-shadowing, piptm, load_data]
root-cause: "Inside [.data.table, a column named 'country_code' shadowed the function argument of the same name, making 'country_code == country_code' always TRUE and returning all rows"
severity: "P2"
test-written: "yes"
fix-confirmed: "yes"
---

# `data.table` Column Name Shadows Function Argument in Filter Expression

## Symptom

`load_survey_microdata("COL", 2010L, "INC")` returned all rows from the
manifest instead of the single matching row. The filter appeared to work (it
returned *a* row) but was semantically wrong: it was comparing the column to
itself rather than to the caller's argument, so it would have returned all rows
if the manifest contained multiple entries.

## Root Cause

`data.table`'s `[` operator evaluates filter expressions in the environment of
the `data.table` itself first, so column names take precedence over symbols in
the enclosing function scope. Given:

```r
load_survey_microdata <- function(country_code, year, welfare_type, ...) {
  entry <- mf[country_code == country_code &
               year         == year         &
               welfare_type == welfare_type]
  ...
}
```

When `country_code` is both the name of a column in `mf` **and** a function
argument, the expression `country_code == country_code` resolves to
`mf$country_code == mf$country_code` — always `TRUE`. All rows pass the filter.

This is a well-known `data.table` scoping gotcha. It does not affect `dplyr`
(which resolves `.data$col` vs `env$arg` unambiguously) or base R `subset()`
(which uses `parent.frame()`).

## Fix

Rename the local scalars to names that cannot collide with column names by
adding a leading dot:

```r
load_survey_microdata <- function(country_code, year, welfare_type, ...) {
  # Rename scalars before filtering to avoid data.table column-shadowing:
  # inside [.data.table, bare names resolve to columns first.
  .cc <- country_code
  .yr <- year
  .wt <- welfare_type

  entry <- mf[
    country_code == .cc &
    year         == .yr &
    welfare_type == .wt
  ]
  ...
}
```

## Prevention

**Rule**: In `data.table` filter expressions (`DT[expr]`), never use a bare
local variable whose name matches a column in `DT`. Always assign to a
distinctly-named scalar first.

**Recommended naming conventions** (pick one consistently):

| Convention | Example |
|---|---|
| Leading dot | `.cc`, `.yr`, `.wt` |
| `_val` suffix | `cc_val`, `yr_val` |
| Explicit `.env` pronoun (data.table ≥ 1.14.3) | `country_code == .env$country_code` |

The `.env` pronoun is the most self-documenting and explicit option if you are
targeting `data.table >= 1.14.3`:

```r
entry <- mf[country_code == .env$country_code &
             year         == .env$year         &
             welfare_type == .env$welfare_type]
```

**Anti-pattern**:
```r
# WRONG — country_code == country_code is always TRUE
entry <- mf[country_code == country_code]
```

**Lint/test check**: Always include a test that verifies the filtered result
has exactly 1 row when the manifest contains multiple countries. If shadowing
is present, the test returns all rows and fails the `nrow == 1` assertion.

## Related

- `.cg-docs/solutions/bugs/2026-04-14-load-surveys-silent-partial-miss.md` — companion
  bug in `load_surveys()` path construction
- `.cg-docs/solutions/bugs/2026-04-28-cli-dot-prefix-glue-expression-rejected.md` —
  tension: dot-prefix local variables (required here) are rejected by cli glue
  expressions; use parentheses `{(.ids)}` to resolve
- `.cg-docs/solutions/bugs/2026-04-28-fsetdiff-wrong-namespace-collapse-vs-datatable.md` —
  related namespace confusion between data.table and collapse
- data.table FAQ: [https://rdatatable.gitlab.io/data.table/articles/datatable-faq.html](https://rdatatable.gitlab.io/data.table/articles/datatable-faq.html)
  (search "column names shadows")
