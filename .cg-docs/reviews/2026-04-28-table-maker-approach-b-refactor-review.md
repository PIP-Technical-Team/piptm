---
plan: .cg-docs/plans/2026-04-28-table-maker-approach-b-refactor.md
findings:
  P1.1: fixed
  P2.1: fixed
  P2.2: fixed
  P2.3: fixed
  P3.1: open
  P3.2: open
  P3.3: open
  P3.4: open
---

## Review Report

**Review depth**: Thorough (from `compound-gpid.local.md`)
**Files reviewed**: 10 (`R/compute_measures.R`, `R/table_maker.R`, `R/zzz.R`,
`tests/testthat/test-compute-measures.R`, `tests/testthat/test-table-maker.R`,
`NAMESPACE`, `compound-gpid.md`, `man/compute_measures.Rd`, `man/table_maker.Rd`,
`.cg-docs/plans/2026-04-28-table-maker-approach-b-refactor.md`)
**Findings**: 8 (P0: 0, P1: 1, P2: 3, P3: 4)
**Skill used**: `cg-skill-r-technical`, `cg-skill-r-analytical`, `cg-skill-r-testing`

**Auto-escalation applied**: Files call statistical functions (`GRP`, `fmean`,
`fgini`, etc.) → `@cg-data-quality` + `@cg-reproducibility` added.

---

### P1 — CRITICAL (must fix before merge)

- **[P1.1]** [cg-adversarial / cg-code-quality] `R/zzz.R:~50` — `collapse::set_collapse(nthreads = n)` is unreachable when `PIPTM_MANIFEST_DIR` is not set.
  **Why**: `.onLoad()` has an early `return(invisible(NULL))` in the dev/CI branch
  (`if (!nzchar(manifest_dir_opt))`). The `nthreads` block is placed *after* that
  return, so it is never executed in CI runners, fresh developer environments, or
  any test session that hasn't set the env var. This defeats requirement R8
  entirely for the most common development context.
  **Fix**: Move the threading block **before** the early return:
  ```r
  # --- collapse threading ---
  tryCatch({
    n <- min(4L, max(1L, parallel::detectCores(logical = FALSE), na.rm = TRUE))
    collapse::set_collapse(nthreads = n)
  }, error = function(e) NULL)

  if (!nzchar(manifest_dir_opt)) {
    return(invisible(NULL))
  }
  # ... rest of manifest loading
  ```

---

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-reproducibility / cg-adversarial] `R/zzz.R` — `parallel::detectCores(logical = FALSE)` returns `NA` on some virtualized/CRAN platforms. `max(1L, NA)` → `NA`; `min(4L, NA)` → `NA`; `set_collapse(nthreads = NA)` errors and the `tryCatch` swallows it silently — nthreads ends up unset with no indication.
  **Why**: This is a known CRAN policy: `detectCores()` is not guaranteed to return
  a non-NA integer. Silent failure violates the "fail loudly" charter rule here
  (no fallback message, no thread count set).
  **Fix**: Note this is also fixed by moving the block per P1.1. Add `na.rm = TRUE`:
  ```r
  n <- min(4L, max(1L, parallel::detectCores(logical = FALSE), na.rm = TRUE))
  ```

- **[P2.2]** [cg-data-quality] `R/compute_measures.R:~68` — No validation that
  columns listed in `by` exist in `dt` before `collapse::GRP(dt, by = batch_by)`.
  **Why**: `table_maker()` NA-fills missing `by` columns in Step 6, so the full
  pipeline is safe. But `compute_measures()` is called directly in tests and
  scripts. A missing `by` column produces a cryptic collapse/C-level error.
  **Fix**: After the existing required-columns guard:
  ```r
  if (!is.null(by)) {
    missing_by <- setdiff(by, names(dt))
    if (length(missing_by)) {
      cli_abort(
        c("Column{?s} listed in {.arg by} not found in {.arg dt}: {.col {missing_by}}.",
          "i" = "Use {.fn table_maker} (which NA-fills missing dimension columns automatically) or add the columns before calling {.fn compute_measures} directly."),
        call = NULL
      )
    }
  }
  ```

- **[P2.3]** [cg-data-quality / cg-adversarial] `R/table_maker.R:Step 8` —
  `unique(dt[, .(pip_id, country_code, surveyid_year, welfare_type)])` does not
  guard against a `pip_id` with multiple distinct metadata values. If
  `load_surveys()` returns rows where the same `pip_id` has two `country_code`
  values, `meta[result, on = "pip_id"]` silently multiplies output rows.
  **Why**: Silent row multiplication is a data corruption vector. Violates the
  "fail loudly" charter rule.
  **Fix**:
  ```r
  meta <- unique(dt[, .(pip_id, country_code, surveyid_year, welfare_type)])
  if (uniqueN(meta, by = "pip_id") != nrow(meta)) {
    dups <- meta[duplicated(meta, by = "pip_id"), pip_id]
    cli_abort(
      c("{.fn load_surveys} returned inconsistent metadata for {length(dups)} pip_id{?s}.",
        "i" = "Each pip_id must map to exactly one country_code/year/welfare_type.",
        "i" = "Affected: {.val {dups}}"),
      call = NULL
    )
  }
  ```

---

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-documentation] `R/compute_measures.R:@return` — The `\describe{}`
  block does not document the `pip_id` column, which is *always* present in the
  output (it is included in `batch_by` unconditionally).
  **Why**: Any caller iterating `names(result)` or pattern-matching the return
  schema will be surprised. Currently documents `poverty_line`, `[by cols]`,
  `measure`, `value`, `population` but omits `pip_id`.
  **Fix**: Add to `@return \describe{}`:
  `\item{\`pip_id\`}{(character) Source survey identifier; always present.}`

- **[P3.2]** [cg-testing / cg-learnings-researcher] `tests/testthat/test-compute-measures.R` — The multi-survey regression test uses `weight = rep(1.0, n)` (uniform weights). Per `.cg-docs/solutions/testing-patterns/2026-04-14-partial-miss-regression-test-batch-loaders.md`, regression tests should exercise non-trivial inputs.
  **Why**: Uniform weights make `fmean(x, w)` identical to `fmean(x)` and
  `fsum(w)` == n, so any accidental loss of the `weight` argument in the compound
  GRP path would still pass. The new multi-survey GRP path is the change being
  tested.
  **Fix**: Add a variant of `make_multi_dt()` with `weight = seq_len(n_rows)` to
  the regression test and verify `value` and `population` against the `lapply`
  reference.

- **[P3.3]** [cg-testing / cg-learnings-researcher] `tests/testthat/test-table-maker.R:"fills missing dim with NA for partial match survey"` — Verifies structural NA in `area` for PER rows but does not assert numeric correctness of `value` for those rows.
  **Why**: A bug in the batch NA-fill (e.g., filling `0` instead of `NA`) would
  still pass the structural check. The 2026-04-14 partial-miss solution pattern
  recommends end-to-end value validation.
  **Fix**: Add `expect_equal(per_rows[measure == "mean"]$value, 5.5, tolerance = 1e-9)` (PER fixture also uses `welfare = 1:10, weight = 1 each`).

- **[P3.4]** [cg-architecture] `R/table_maker.R:7` — `%||%` null-coalescing
  operator defined inline in `table_maker.R`.
  **Why**: If any future R file needs it, there will be a duplicate definition
  conflict. Package convention is to put helpers in `R/aaa.R`.
  **Fix**: Move to `R/aaa.R`, or replace the two usages with explicit
  `if (is.null(...))` checks.

---

### ✅ Passed

- **cg-code-quality**: Style consistent; `<-`, `snake_case`, alignment, comment
  density conform to project conventions. `rbindlist` import correctly migrated
  from `table_maker.R` to `compute_measures.R`. `funique` correctly removed from
  NAMESPACE. No DRY violations.
- **cg-documentation**: `compute_measures.Rd` accurately reflects new multi-survey
  contract; old warning correctly removed. `table_maker.Rd` population field
  expanded. `compound-gpid.md` Current Focus updated accurately.
- **cg-version-control**: No secrets or credentials in code. `.Rbuildignore` already
  has `^\.cg-docs$`. Env vars via `Sys.getenv()` is correct practice.
- **cg-architecture**: GRP ownership correctly centralised in `compute_measures.R`.
  `compute_poverty()` correctly excluded from shared GRP (constraint documented
  and respected). Metadata join replaces scalar assignment cleanly.
- **cg-performance**: Single GRP construction for inequality/welfare confirmed.
  Memory profile better than Approach A (123 MB vs 164 MB). `unique()` on metadata
  is negligible for typical workloads.
- **cg-learnings-researcher**: Benchmark results in `.cg-docs/solutions/performance-issues/`
  consistent with implementation. Partial-miss test pattern from 2026-04-14
  partially applied (zero-overlap surveys tested); gap noted in P3.3.
