---
plan: ".cg-docs/plans/2026-04-03-manifest-generation-version-partition.md"
findings:
  P1.1: fixed
  P1.2: fixed
  P2.1: fixed
  P2.2: fixed
  P2.3: fixed
  P2.4: fixed
  P2.5: fixed
  P2.6: fixed
  P2.7: fixed
  P3.1: fixed
  P3.2: fixed
  P3.3: fixed
  P3.4: skipped
  P3.5: fixed
  P3.6: fixed
---

## Review Report

**Review depth**: standard (+ auto-escalation: `load_data.R` matches `**/load*.R` → `@cg-data-quality` always included)
**Files reviewed**: `R/load_data.R`, `R/manifest.R`, `R/measures.R`, `R/zzz.R`, `R/schema.R`, `tests/testthat/test-load-data.R`, `tests/testthat/test-manifest.R`
**Findings**: 2 P1 · 7 P2 · 6 P3

---

### P1 — CRITICAL (must fix before merge)

- **[P1.1]** [cg-architecture / cg-data-quality] `R/load_data.R` — **architectural asymmetry**: `load_survey_microdata()` uses `open_dataset(arrow_root) |> filter(...)` (global repo scan + partition pushdown), while `load_surveys()` uses the faster and more predictable path-based `list.files()` approach. Same conceptual operation, two different backends.
  **Why**: In a large production Arrow repository with hundreds of surveys, `load_survey_microdata()` must open and introspect the entire dataset metadata graph before pruning to the target partition. The path-based approach used by `load_surveys()` is already proven ~19× faster (benchmarked in session). Having two different backends for the same operation also complicates maintenance — a fix to one approach won't propagate to the other.
  **Fix**: Extract a shared internal helper `.build_parquet_paths(arrow_root, country_code, year, welfare_type, version)` that constructs the leaf path and calls `list.files()`. Rewrite `load_survey_microdata()` to use this helper instead of `open_dataset(arrow_root) |> filter(...)`.

- **[P1.2]** [cg-architecture] `R/zzz.R` — hardcoded `Y:/PIP_ingestion_pipeline_v2/...` paths are baked into package source. The env-var graceful-degradation branch (`if (!nzchar(manifest_dir_opt))`) is **dead code** because `manifest_dir_opt` is always a non-empty hardcoded string — `Sys.getenv()` is never called.
  **Why**: The current design assumes all team members share the same mapped network drive at `Y:`. If that drive is unmounted or unavailable (another machine, CI, a team member with a different drive letter), `.onLoad()` silently fails and the package starts in a broken state. The `if (!nzchar(...))` guard was clearly intended for an env-var pattern that was never wired up.
  **Decision required**: Choose one of two valid approaches:
  - **Option A — env vars** (portable, recommended): Replace the hardcoded strings with `Sys.getenv("PIPTM_ARROW_ROOT", unset = "")` and `Sys.getenv("PIPTM_MANIFEST_DIR", unset = "")`. The graceful-degradation branch then works correctly for machines where they are unset. Each team member sets these two vars in `~/.Renviron` (via `usethis::edit_r_environ()`):
    ```
    PIPTM_ARROW_ROOT=Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/arrow
    PIPTM_MANIFEST_DIR=Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/manifests
    ```
    The `Y:` path lives in each user's env file, not in committed source. Add setup instructions to `README.md`.
  - **Option B — hardcoded with existence check** (simpler): Keep the hardcoded paths but only assign if the path exists: `if (dir.exists(arrow_root_opt)) .piptm_env$arrow_root <- arrow_root_opt`. Remove the dead-code branch. No env file required, but the paths remain baked into the codebase.

---

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-data-quality] `R/load_data.R:load_surveys()` — `list.files(..., recursive = TRUE)` walks the entire subtree under each partition leaf. Files in unexpected subdirectories (aborted writes, nested versioning artefacts) will be silently included in the dataset.
  **Why**: The canonical schema places Parquet files directly at the leaf level (`version=*/data.parquet`). A `recursive = TRUE` discovery can pull in wrong files without any warning, leading to silent data contamination.
  **Fix**: Use `recursive = FALSE`. If the schema ever places files in sub-leaves, update this explicitly. Add a test with a subdirectory containing a stray `.parquet` file.

- **[P2.2]** [cg-data-quality] `R/load_data.R:load_surveys()` — after loading, there is no cross-check that the returned rows belong only to the requested surveys. A path-construction bug or partition contamination would return wrong data without triggering any error.
  **Why**: The `pip_id` column is present in every Parquet file and encodes the exact survey identity. A lightweight integrity check costs one `uniqueN()` call.
  **Fix**: After `dt <- ... |> collect() |> as.data.table()`, add:
  ```r
  loaded_ids <- unique(dt$pip_id)
  unexpected <- setdiff(loaded_ids, entries_dt$pip_id)
  if (length(unexpected) > 0L) {
    cli::cli_abort(
      c("Loaded unexpected survey(s): {.val {unexpected}}.",
        "i" = "This may indicate partition path contamination.")
    )
  }
  ```

- **[P2.3]** [cg-testing] `tests/testthat/test-load-data.R` — no test for **partial path failure** in `load_surveys()`: when some (but not all) partition paths in `entries_dt` do not exist on disk, the missing ones are silently skipped by `list.files()`, and the function succeeds but returns fewer rows than expected.
  **Why**: A manifest pointing to a survey whose Parquet files were not yet written (or were deleted) would silently return a partial result instead of erroring. This violates the "no silent data corruption" rule.
  **Fix**: After `P2.2`'s integrity check is added, this becomes partially covered. Additionally, add an explicit test: write 2 surveys to fixture, put 3 entries in the manifest, call `load_surveys(mf)`, expect an error or a specific warning about the missing survey.

- **[P2.4]** [cg-testing] `tests/testthat/test-manifest.R:test_that(".load_manifests() warns and skips unreadable JSON")` — asserts a warning is fired but does not assert `length(piptm_manifests()) == 1L`.
  **Why**: If a regression silently loaded the corrupted manifest (or corrupted both), the test would still pass. The assertion is incomplete.
  **Fix**: Add `expect_equal(length(piptm_manifests()), 1L)` after the `expect_warning()` call.

- **[P2.5]** [cg-data-quality] `R/validate_parquet.R:validate_partition_consistency()` — check 4 reads `pip_id` via `arrow::read_parquet(f, col_select = "pip_id")[[1L]][[1L]]`, extracting only the **first value**. A write-corrupted file with mixed `pip_id` values would pass.
  **Why**: The constraint is "one unique `pip_id` per file" (schema rule: `unique_per_file: true`). Reading only the first row doesn't enforce this.
  **Fix**: Use `data.table::uniqueN(arrow::read_parquet(f, col_select = "pip_id")[[1L]])` and error if `> 1`.

- **[P2.6]** [cg-documentation] `R/load_data.R` — file-level header comment (lines 7–11) still describes the **old** architecture ("using partition pushdown via `open_dataset() |> filter()`"). The function bodies were updated but the module-level docstring was not.
  **Why**: Future developers reading the header will get a misleading description of `load_surveys()`.
  **Fix**: Update the header to describe both functions accurately:
  - `load_survey_microdata()`: uses `open_dataset(arrow_root) |> filter(...)` with 4 partition-key filters
  - `load_surveys()`: constructs exact leaf paths from the 4 partition keys, uses `list.files()` + `open_dataset(files)`

- **[P2.7]** [cg-architecture] `R/zzz.R` — `set_arrow_root()` validates path existence at call time. But in `.onLoad()`, `arrow_root` is set directly to the hardcoded constant with no existence check. If the network drive `Y:` is unmounted, `arrow_root` is silently set to a broken path, and the first `load_survey_microdata()` call will fail with an opaque Arrow error rather than the helpful "Arrow root is not configured" message.
  **Why**: The existence check in `set_arrow_root()` was designed to provide a useful error. Bypassing it in `.onLoad()` means the validation is only run on manual override calls.
  **Fix**: After setting `arrow_root` in `.onLoad()`, wrap in `if (dir.exists(arrow_root_opt))` — only assign if the path exists; otherwise leave `NULL` for the accessor to report as unconfigured.

---

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-code-quality] `R/measures.R::.bin_age()` — `age <- NULL` is declared to suppress CMD check NOTE for NSE column reference, but `age_group <- NULL` is missing. The `age_group :=` assignment on the left side of `:=` is also an NSE reference and will generate a NOTE.
  **Fix**: Add `age_group <- NULL` at the top of `.bin_age()`, alongside `age <- NULL`.

- **[P3.2]** [cg-code-quality] `R/load_data.R` — stray double blank line at ~line 121 (between the `entry` subsetting block and the `if (nrow(entry) == 0L)` guard). Minor style inconsistency.
  **Fix**: Remove the extra blank line.

- **[P3.3]** [cg-code-quality] `R/manifest.R:piptm_manifest()` — error message for an unknown release uses:
  ```r
  "i" = "Available releases: {available}"
  ```
  where `available` is a plain comma-joined string. Using `{.val {names(manifests)}}` would provide cli-styled inline code highlighting and be consistent with the rest of the file.
  **Fix**:
  ```r
  "i" = "Available releases: {.val {sort(names(manifests))}}"
  ```
  (remove the manual `paste(sort(...))` and let cli format it).

- **[P3.4]** [cg-testing] `tests/testthat/test-load-data.R` — the `withr::defer` cleanup block is duplicated verbatim in 8 of 9 single-survey tests. This is repetitive and fragile (if the env slot names change, 8 blocks need updating).
  **Fix**: Extract into a local helper or use `withr::local_()` binding at the start of each test.

- **[P3.5]** [cg-documentation] `R/zzz.R` — the comment block at the top describes `PIPTM_MANIFEST_DIR` / `PIPTM_ARROW_ROOT` env vars as the configuration mechanism, but this is not currently implemented (see P1.2). The comment is misleading until P1.2 is resolved. After applying P1.2 Option A, update the comment to document the env-var names, their expected values, and the fallback behaviour.

- **[P3.6]** [cg-documentation] `R/zzz.R` / `piptm_current_release()` — the `pipfun::get_wrk_release()` override of the manifest-derived `current_release` is intentional (each user tracks their own working release), but this design decision is invisible to API callers. A user who reads the manifest's `current_release.json` pointer and then checks `piptm_current_release()` may see a different value with no explanation.
  **Fix**: Add a note to the `piptm_current_release()` roxygen doc:
  > *"At package load time, this value is overridden by `pipfun::get_wrk_release()` to reflect the user's active working release, taking precedence over the `current_release.json` pointer file."*

---

### ✅ Passed

- **cg-code-quality**: `.Rbuildignore` correctly excludes `^\.cg-docs$` — no package build pollution.
- **cg-code-quality**: All exported functions have roxygen2 documentation with `@param`, `@return`, and `@examples`.
- **cg-code-quality**: `data.table` scoping collision in `load_survey_microdata()` (`.cc`/`.yr`/`.wt` aliasing) is correctly handled and documented inline.
- **cg-testing**: 76/76 tests pass in `test-measures.R`. 33/33 in `test-load-data.R`. Manifest tests all pass.
- **cg-testing**: Regression tests for both the old `pip_id` filter (Cartesian over-fetch) and the `data.table` scoping collision are present and clearly documented.
- **cg-data-quality**: `validate_parquet_data()` checks welfare ≥ 0, weight > 0, partition key consistency, ISO3 format, and factor level conformance — comprehensive.
- **cg-performance**: `load_surveys()` path-based approach correctly avoids the global repository scan. The 19× speedup (59 sec → 3 sec for 11 surveys) is real and sustained.
- **cg-performance**: Manifest loading (`.load_manifests()`) runs once at `.onLoad()` and is cached in `.piptm_env` — no repeated I/O on repeated `piptm_manifest()` calls.
- **cg-version-control**: No secrets, credentials, or PII detected in the diff. Conventional commit structure is appropriate for the changes.
- **cg-reproducibility**: `pipfun::get_wrk_release()` override in `.onLoad()` is intentional — each user's default release tracks their own active working release, consistent with the team's workflow design.
- **cg-architecture**: The manifest-first architecture (load manifest on startup, all data loading on-demand) is sound. The 4-level Hive partition structure is correctly modelled throughout.
- **cg-reproducibility**: The `version` partition key ensures that manifest-pinned loads always retrieve the same data snapshot for a given release, regardless of subsequent writes.

---

## Review Summary
- **Fixed**: 0
- **Skipped**: 0
- **Remaining**: 15 findings (2 P1, 7 P2, 6 P3)

**What would you like to do next?**
1. **`/cg-fix-triage P1`** — Apply the two critical fixes
2. **`/cg-fix-triage P1.2`** — Decide env vars (Option A) vs hardcoded-with-guard (Option B) for `zzz.R` first
3. **`/cg-compound`** — Capture the `recursive = TRUE` data contamination pattern as a solution note
4. **Ready to merge** — Accept findings as known issues, address in follow-up
