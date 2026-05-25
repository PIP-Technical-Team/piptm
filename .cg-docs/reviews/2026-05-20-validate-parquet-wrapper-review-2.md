---
plan: .cg-docs/plans/2026-05-20-validate-parquet-wrapper.md
findings:
  P1.1: fixed
  P2.1: fixed
  P2.2: fixed
  P2.3: skipped
  P2.4: fixed
  P2.5: fixed
  P3.1: fixed
  P3.2: fixed
  P3.3: open
---

## Review Report

**Review depth**: thorough  
**Files reviewed**: `R/load_data.R`, `tests/testthat/test-load-data.R`  
**Findings**: 9 (P0: 0, P1: 1, P2: 5, P3: 3)  
**Focus**: `load_surveys()` — `cols` argument implementation

---

### P0 — BLOCKING

*None.*

---

### P1 — CRITICAL (must fix before merge)

- **[P1.1]** [cg-data-quality] `R/load_data.R:556–574` — **Mixed legacy + new-schema batches crash with a `data.table::setnames` error**  
  **Why**: When `entries_dt` contains both legacy surveys (single `welfare` column in file) and new-schema surveys (`welfare_ppp_*` columns), `open_dataset()` produces a unified Arrow schema containing *both* `welfare` (NA for new-schema rows) *and* `welfare_ppp_2017_01_02` (NA for legacy rows). The post-collect rename `setnames(dt, "welfare_ppp_2017_01_02", "welfare")` then fails because `dt` already has a `welfare` column. No guard exists for this case.  
  **Fix**: Add a check immediately before the rename block:
  ```r
  if (target_col != "welfare" && "welfare" %in% names(dt)) {
    cli::cli_abort(
      c(
        "Mixed legacy/new-schema surveys in the same batch: cannot safely rename {.val {target_col}} to {.val \"welfare\"}.",
        "i" = "Legacy surveys (no {.field welfare_vars}) have a {.val \"welfare\"} column; new-schema surveys do not.",
        "i" = "Load legacy and new-schema surveys separately, or pass a batch containing only one schema type."
      )
    )
  }
  ```

---

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-testing] `tests/testthat/test-load-data.R` — **No test pins the `weight` auto-inclusion invariant when `weight` is absent from `cols`**  
  **Why**: The docstring guarantees `"welfare"`, `"weight"`, and `"pip_id"` are always included even if not listed in `cols`. Every existing `cols` test already includes `weight` explicitly, so the auto-inclusion path (`union(physical_cols, c("pip_id", target_col, "weight"))`) is never exercised by omission. A refactor removing `weight` from that union would pass all current tests.  
  **Fix**:
  ```r
  test_that("load_surveys() cols always includes weight even when not requested", {
    fx <- make_fixtures()
    piptm::set_manifest_dir(fx$tmp_manifest)
    piptm::set_arrow_root(fx$tmp_arrow)
    withr::defer(reset_load_env())

    mf  <- piptm::piptm_manifest()
    col <- mf[mf$country_code == "COL" & mf$year == 2010L]
    # Deliberately omit "weight" and "welfare" from cols
    dt  <- piptm::load_surveys(col, cols = c("pip_id", "gender"))

    expect_true("weight"  %in% names(dt))
    expect_true("welfare" %in% names(dt))
    expect_true("pip_id"  %in% names(dt))
  })
  ```

- **[P2.2]** [cg-code-quality] `R/load_data.R:519–527` — **`welfare_family_set` variable name is misleading and its inclusion of `"weight"` is undocumented**  
  **Why**: This variable suppresses warnings for columns absent from the unified Arrow schema. Including `"weight"` in it means a schema-missing `weight` is silently skipped, even though `weight` is not a welfare-family column. The name signals to future maintainers that the set contains only welfare variants.  
  **Fix**: Rename and annotate:
  ```r
  # Columns that are auto-fetched unconditionally (pip_id, weight) or belong
  # to the welfare variant family are never surfaced in the "absent from schema"
  # warning — they are either guaranteed present or managed by PPP selection.
  auto_or_welfare_cols <- unique(c(all_welfare_vars, target_col, "welfare",
                                   "weight", "pip_id"))
  dropped_requested <- setdiff(setdiff(physical_cols, safe_cols), auto_or_welfare_cols)
  ```

- **[P2.3]** [cg-code-quality] `.Rbuildignore` — **`.cg-docs/` not excluded from package build**  
  **Why**: `.cg-docs/` contains review and brainstorm files that should not be bundled with the installed R package.  
  **Fix**: Add `^\.cg-docs$` to `.Rbuildignore`.

- **[P2.4]** [cg-testing] `tests/testthat/test-load-data.R` — **No test covers the mixed legacy + new-schema crash (P1.1 scenario)**  
  **Why**: Once P1.1 is fixed to raise an informative error, there must be a regression test pinning the abort so a future "fix" does not silently reintroduce corrupt output.  
  **Fix**: After the P1.1 fix, add a test that constructs a manifest with one legacy entry (no `welfare_vars`) and one new-schema entry (`welfare_vars` non-empty) and asserts:
  ```r
  expect_error(piptm::load_surveys(mf, ppp = 2017L),
               regexp = "[Mm]ixed|legacy.*new-schema")
  ```

- **[P2.5]** [cg-documentation] `R/load_data.R:322–337` — **`@param cols` does not warn against passing physical welfare column names**  
  **Why**: The docstring says to use logical `"welfare"`, but does not say *not* to pass physical names like `"welfare_ppp_2017_01_02"`. A caller who inspects the Parquet schema and passes the physical name triggers the P1.9 warning path and gets the column silently dropped — confusing without a doc pointer.  
  **Fix**: Append to `@param cols`:
  > Do **not** pass physical welfare column names (e.g. `"welfare_ppp_2017_01_02"`). Those are treated as explicitly-requested welfare-family columns, trigger a warning, and are dropped. Always use `"welfare"` as the logical name.

---

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-code-quality] `R/load_data.R:504–510` — **`cols` input type is not validated**  
  **Why**: Only the empty-length case is guarded. A caller passing `cols = 1:3` (integer) proceeds without error and will fail downstream in `intersect()` or `select()` with a non-obvious message.  
  **Fix**:
  ```r
  if (!is.null(cols) && (!is.character(cols) || length(cols) == 0L))
    cli::cli_abort("{.arg cols} must be NULL or a non-empty character vector.")
  ```

- **[P3.2]** [cg-documentation] `R/load_data.R:498–503` — **`@param cols` omits the reason `pip_id` is auto-included**  
  **Why**: Callers who see an unexpected `pip_id` in output have no doc explaining why.  
  **Fix**: Append: "(`pip_id` is required for the loaded-vs-requested integrity check and cannot be excluded.)"

- **[P3.3]** [cg-architecture] `R/load_data.R` — **`load_survey_microdata()` has no `cols` parameter**  
  **Why**: Its batch sibling gained column-pruning for the 68% I/O improvement, but the single-survey function still loads all 14 columns unconditionally.  
  **Fix**: Advisory — add `cols = NULL` to `load_survey_microdata()` in a follow-up, sharing the logical-to-physical translation logic. Not urgent (not called in hot loops), but worth a roadmap entry.

---

### ✅ Passed

- **cg-performance**: `select()` correctly precedes `collect()` on all code paths. `target_col` is resolved before `open_dataset()`. L1 from benchmark lessons honoured throughout.
- **cg-reproducibility**: `release` attribute attached on every output path; manifest is authoritative source for `version` and `welfare_vars`.
- **cg-version-control**: No secrets or credentials in changed files.
- **cg-data-quality** (cols path): `safe_cols = intersect(physical_cols, ds$schema$names)` prevents Arrow errors on partial-schema surveys; `pip_id` integrity check catches partition contamination; `.build_parquet_paths()` multi-file guard protects the sort contract.
- **cg-learnings-researcher**: Implementation matches L1 from `2026-04-30-arrow-io-and-compute-lessons.md` (select-before-collect). Multi-file guard matches `2026-05-21-arrow-multifile-partition-sort-violation.md`.
- **cg-adversarial**: Welfare rename is safe for single-schema batches. `explicitly_requested` warning path correctly surfaces physical welfare column names passed by callers. The `unexpected pip_id` integrity check cannot be bypassed by column pruning (pip_id is unconditionally included).

---

*Parsed 9 finding IDs.*
