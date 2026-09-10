---
date: 2026-05-20
title: "validate_parquet() unified wrapper"
status: completed
completed-date: 2026-05-20
scope: Lightweight
estimated-effort: small
tags: [inline]
---

# validate_parquet() unified wrapper

## Objective

Add a single-entry-point `validate_parquet()` function to `R/validate_parquet.R`
that dispatches to the three existing validators based on a `check` argument.
Eliminates the need for callers to pick the right low-level function.

## Steps

### 1. Implement `validate_parquet()` in `R/validate_parquet.R`

- `path` — file path (for `"schema"` / `"data"`) or directory path (for `"consistency"`).
- `check` — one or more of `"schema"`, `"data"`, `"consistency"`. Default `"schema"`.
- `"consistency"` cannot be combined with the others (different path type) — stop with a clear message.
- When multiple checks are requested (`c("schema", "data")`), run both and merge results:
  - `valid` = AND of all individual `valid` flags
  - `errors` / `warnings` = combined character vectors
  - `file` = `path`
- Full roxygen2 docs, `@family parquet-validation`, `@export`.

**Acceptance criteria**: function is exported; `validate_parquet("f.parquet")` is equivalent to `validate_parquet_schema("f.parquet")`; `validate_parquet("f.parquet", check = c("schema", "data"))` returns merged result; `validate_parquet(dir, check = "consistency")` delegates to `validate_partition_consistency()`.

### 2. Add tests in `tests/testthat/test-validate-parquet.R`

- `validate_parquet()` default dispatches to schema check.
- `check = "data"` dispatches to data check.
- `check = "consistency"` dispatches to consistency check.
- `check = c("schema", "data")` merges — errors from both appear, `valid = FALSE` if either fails.
- Mixing `"consistency"` with others raises an error.

**Acceptance criteria**: all new tests pass; no existing tests broken.
