---
plan: .cg-docs/plans/2026-05-20-validate-parquet-wrapper.md
findings:
  P1.1: fixed
  P1.2: fixed
  P1.3: fixed
  P1.4: fixed
  P1.5: skipped
  P1.6: fixed
  P1.7: fixed
  P1.8: fixed
  P1.9: fixed
  P2.1: fixed
  P2.2: skipped
  P2.3: fixed
  P2.4: fixed
  P2.5: skipped
  P2.6: skipped
  P2.7: fixed
  P3.1: fixed
  P3.2: fixed
  P3.3: skipped
  P3.4: skipped
  P3.5: skipped
  P3.6: skipped
  P3.7: fixed
  P3.8: skipped
  P3.9: skipped
---

## Review Report

**Review depth**: thorough
**Files reviewed**: `R/compute_inequality.R`, `R/load_data.R`, `R/table_maker.R`, `R/schema.R`, `R/validate_parquet.R`, `man/` (Rd files), `tests/testthat/test-load-data.R`, `tests/testthat/test-table-maker.R`
**Findings**: 25 (P0: 0, P1: 9, P2: 7, P3: 9)
**Context**: Two changes reviewed together — (1) removal of redundant welfare sort in `compute_inequality()`, relying on upstream pre-sort contract; (2) `cols` parameter added to `load_surveys()` for Arrow column pruning.

---

### P0 — BLOCKING

None.

---

### P1 — CRITICAL (must fix before merge)

- **[P1.1]** [cg-code-quality + cg-version-control] `NAMESPACE:54,38` — `importFrom(data.table,setorder)` is stale (removed from `@importFrom` in `compute_inequality.R`) and `importFrom(dplyr,any_of)` may be stale (no `@importFrom dplyr any_of` tag found; code uses `dplyr::all_of()`, not `any_of()`). The NAMESPACE says "do not edit by hand" — it was partially hand-patched (new exports added) but not fully regenerated.
  **Why**: Stale NAMESPACE causes `R CMD check` NOTE/WARNING; stale importFrom entries can shadow other functions in edge cases.
  **Fix**: Run `devtools::document()` and commit the regenerated NAMESPACE and all man/ files.

- **[P1.2]** [cg-documentation + cg-version-control] `man/load_surveys.Rd:7` — `\usage{}` block shows `load_surveys(entries_dt, ppp = NULL, release = NULL)`, missing the new `cols = NULL` parameter. The source signature is correct; the Rd was not regenerated.
  **Why**: Rendered docs and `R CMD check` will flag the signature mismatch. Users reading `?load_surveys` see the wrong API.
  **Fix**: `devtools::document()` (same run as P1.1) regenerates this Rd correctly.

- **[P1.3]** [cg-testing] `tests/testthat/test-compute-inequality.R` — No test verifies that unsorted input to `compute_inequality()` produces wrong results. After removing the internal sort, the function is silently wrong on unsorted input (confirmed empirically: errors up to 0.39 Gini). No test will catch a future upstream regression that breaks the pre-sort contract.
  **Why**: A contract with no test enforcement is documentation only. This is the highest-risk gap from the sort-removal change.
  **Fix**: Add:
  ```r
  test_that("gini: unsorted input produces wrong result (pre-sort contract)", {
    set.seed(1)
    dt_sorted   <- data.table(welfare = as.numeric(1:10), weight = rep(1, 10))
    dt_unsorted <- data.table(welfare = sample(1:10), weight = rep(1, 10))
    res_sorted   <- compute_inequality(dt_sorted,   measures = "gini")$value
    res_unsorted <- compute_inequality(dt_unsorted, measures = "gini")$value
    # sorted result is the correct Gini; unsorted should differ
    expect_false(isTRUE(all.equal(res_sorted, res_unsorted)),
      label = "unsorted input must not accidentally equal sorted result")
  })
  ```

- **[P1.4]** [cg-documentation] `R/compute_inequality.R:54–57` — The function-level description still reads: *"Gini is computed via a fully vectorised data.table sort + cumulative-sum approach … within each group the data are sorted by welfare"*. The sort was removed; this is now stale and contradicts the `@param dt` pre-sort contract.
  **Why**: Readers, including future maintainers, will be misled about where sorting occurs.
  **Fix**: Update to: *"Gini is computed via a fully vectorised collapse C-level cumulative-sum approach. The grouping GRP assigns group ids; `.gini_sorted()` applies the Brown trapezoid formula — rows must already be sorted ascending by welfare (pre-sort contract on `dt`)."*

- **[P1.5]** [cg-documentation] `man/validate_parquet_data.Rd:21` — Check item 1 reads *"`welfare` — all finite, all ≥ 0"* but the new deflated-data schema uses `welfare_lcu` and `welfare_ppp_*` columns. If the source `validate_parquet_data()` has been updated to check the new column names, the Rd is stale. If source has not been updated, validation logic is misaligned with the actual Parquet schema.
  **Why**: Validation against a non-existent bare `welfare` column would silently pass any new-schema file without checking its actual welfare columns.
  **Fix**: Confirm whether `validate_parquet_data()` source checks new column names; if so, regenerate Rd. If not, update the source to check `welfare_lcu`/`welfare_ppp_*` per the `welfare_vars` manifest attribute.

- **[P1.6]** [cg-reproducibility + cg-adversarial] `R/load_data.R` (`.build_parquet_paths()`) — No guard for `length(files) > 1L`. If a partition directory ever contains two Parquet files (retried write, manual copy, upstream shard), `open_dataset()` concatenates them in file order. Within the group for that `pip_id`, the welfare vector becomes two sorted segments concatenated (not globally sorted). The Gini sort-removal change means this produces silently wrong results.
  **Why**: The current convention is one file per partition, but it is not enforced. The integrity check only catches unexpected `pip_id` values, not duplicate files.
  **Fix**:
  ```r
  if (length(files) > 1L) {
    cli::cli_abort(c(
      "Multiple Parquet files in partition for {.val {country_code}} / {year}: {.val {files}}.",
      "i" = "Expected exactly one file per partition."
    ))
  }
  ```

- **[P1.7]** [cg-data-quality] `R/compute_inequality.R:34–44` (`.gini_sorted()`) — NA welfare values produce a silently biased Gini. `collapse::fsum` uses `na.rm = TRUE` by default, so `fsum(ww)` silently excludes NA products. `fcumsum()` propagates NAs forward from the first NA position, but `fsum(v)` again silently drops them. The result is a Gini over a biased subset with no error or warning.
  **Why**: Silent wrong results on NA input violate the "fail loudly" project rule and are worse than an error.
  **Fix**: Add to `.gini_sorted()` or `compute_inequality()` before the Gini block:
  ```r
  if (anyNA(welfare_v) || anyNA(w))
    cli::cli_abort("Gini requires non-NA welfare and weight. Found {sum(is.na(welfare_v))} NA welfare row(s).")
  ```

- **[P1.8]** [cg-adversarial] `R/compute_inequality.R:145` (`by = NULL` path) — `gini_vals <- .gini_sorted(welfare_v, w)` has no runtime sort check. Direct callers with user-assembled (unsorted) `dt` and `by = NULL` get a wrong Gini with no error. `compute_inequality()` is exported.
  **Why**: The pipeline always uses `by` with `pip_id`, so the internal path is safe. But an exported function with a silent correctness precondition is fragile for direct users.
  **Fix**:
  ```r
  if (is.unsorted(welfare_v)) {
    cli::cli_abort(c(
      "Pre-sort contract violated: {.arg dt}$welfare is not sorted ascending.",
      "i" = "Sort by welfare before calling {.fn compute_inequality}."
    ))
  }
  ```
  `is.unsorted()` terminates on the first out-of-order pair — negligible cost on already-sorted data.

- **[P1.9]** [cg-adversarial] `R/load_data.R` (post-collect welfare cleanup) — If `cols` explicitly contains a physical welfare column name (e.g. `"welfare_lcu"`), it is silently dropped. The translate-and-drop logic identifies `welfare_lcu` as part of `all_welfare_vars` and includes it in `drop_cols`. The caller receives no column, no error, no warning, yet the bytes were transferred.
  **Why**: The `@param cols` documentation says columns *absent from the schema* are silently omitted; it does not document that *explicitly requested welfare family columns* are silently dropped. Undocumented silent data loss.
  **Fix**: Before dropping, warn if any explicitly-requested column is about to be removed:
  ```r
  explicitly_requested_welfare <- intersect(drop_cols, cols)
  if (length(explicitly_requested_welfare) > 0L) {
    cli::cli_warn(c(
      "Column(s) {.val {explicitly_requested_welfare}} are in the welfare family and were dropped.",
      "i" = "Use {.arg ppp} to select a welfare column; do not pass physical welfare names in {.arg cols}."
    ))
  }
  ```

---

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-code-quality] `R/load_data.R` (`safe_cols` block) — When a column in `cols` is absent from the Arrow schema it is silently omitted. This violates the project rule *"Fail loudly, never silently"* for non-welfare columns (e.g. a typo in a `by` dimension name like `"gneder"` instead of `"gender"`).
  **Why**: Silent omission of a requested dimension column will cause confusing downstream errors far from the source.
  **Fix**: After computing `safe_cols`, emit a warning for any non-welfare column in `physical_cols` that was dropped:
  ```r
  welfare_family <- c(all_welfare_vars, target_col, "welfare")
  dropped_non_welfare <- setdiff(physical_cols, c(safe_cols, welfare_family))
  if (length(dropped_non_welfare) > 0L)
    cli::cli_warn("Requested column(s) not in Arrow schema and skipped: {.val {dropped_non_welfare}}")
  ```

- **[P2.2]** [cg-reproducibility] Cross-package pre-sort contract — The sort invariant has no write-side enforcement. `prepare_for_arrow()` in `{pipdata}` performs no `setorder()` and no assertion that input rows are welfare-sorted. The contract exists only in prose.
  **Why**: If `pipload::load_pip_deflated_data()` ever changes its sort guarantee, `prepare_for_arrow()` will silently write unsorted Parquet files, and downstream Gini results will be silently wrong. This is a `{pipdata}` task, not `{piptm}`.
  **Fix**: In `{pipdata}` `prepare_for_arrow()` or `write_survey_parquet()`, add either a `setorder(dt, welfare)` (making the write side own the invariant) or a `stopifnot(all(diff(dt$welfare) >= 0))` guard.

- **[P2.3]** [cg-testing] `tests/testthat/test-load-data.R` (partial-match test) — The *"cols with partial-match survey omits absent dimension silently"* test asserts only row count and presence of `pip_id`/`welfare`/`weight`. It never checks whether `gender`/`area` appear in `dt` at all, or whether BOL rows carry `NA` for those columns.
  **Why**: Arrow's unified-schema `open_dataset` fills absent columns with `NA`. This is the correct expected behaviour — but it is completely unasserted. A future change that drops columns entirely would leave this test green.
  **Fix**: Add:
  ```r
  expect_true("gender" %in% names(dt))
  bol_rows <- dt[pip_id == "BOL_2015_EH_CON_ALL"]
  expect_true(all(is.na(bol_rows$gender)))
  ```

- **[P2.4]** [cg-testing] `R/load_data.R` — `cols = character(0L)` (empty vector) is unguarded. `union(character(0), "pip_id")` → `"pip_id"` only; `load_surveys()` returns a single-column table with no `welfare`. No error is raised; downstream callers fail with a confusing message.
  **Why**: Even if callers are not expected to pass `character(0)`, the current silent behaviour should be either guarded with an explicit error or locked by a test.
  **Fix**:
  ```r
  if (!is.null(cols) && length(cols) == 0L)
    cli::cli_abort("{.arg cols} must be NULL or a non-empty character vector.")
  ```

- **[P2.5]** [cg-architecture] `R/load_data.R:117` (`load_survey_microdata`) — `load_survey_microdata()` does not support `cols`. Both loading functions share `.build_parquet_paths()` and the Arrow open/collect path, but only `load_surveys()` benefits from column pruning.
  **Why**: Direct users of `load_survey_microdata()` cannot prune columns. Asymmetric API surfaces are a maintenance burden.
  **Fix**: Add `cols = NULL` to `load_survey_microdata()` and share the select-before-collect logic via a private helper (`.resolve_select_cols(ds, cols, target_col)`).

- **[P2.6]** [cg-version-control] Branch mismatch — The repo context shows branch `tm_input_rt`; the `piptm` terminal confirms the most recent push was `git push origin api-endpoints`.
  **Why**: If work was committed to `api-endpoints` rather than `tm_input_rt`, a PR opened from the wrong branch would not include these changes.
  **Fix**: Clarify which branch these commits landed on; cherry-pick or rebase to `tm_input_rt` if `api-endpoints` was unintentional.

- **[P2.7]** [cg-testing] `tests/testthat/test-table-maker.R:1–100` — `write_fixture_parquet_tm` / `write_fixture_manifest_tm` are inline copies of the helpers from `test-load-data.R`. If the manifest schema gains a new required field (e.g. `ppp_sort`), only one copy may get updated.
  **Why**: Fixture drift is a realistic risk given the active schema migration trajectory of this package.
  **Fix**: Move shared fixture builders to `tests/testthat/helper-fixtures.R` (auto-loaded by testthat).

---

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-code-quality] `R/compute_inequality.R` — Pre-sort contract is stated only in `@param dt` prose; it does not appear at the top of the rendered help page. The contract produces silent wrong results if violated — it deserves a `@section Pre-sort contract:` that appears prominently.

- **[P3.2]** [cg-code-quality] `R/load_data.R:106` — `@importFrom dplyr collect` tag is unnecessary since `dplyr::collect()` is always called with `::`. Remove on the next `devtools::document()` pass.

- **[P3.3]** [cg-documentation] `man/load_surveys.Rd` (`\value{}`) — Clarify that the returned `welfare` column is the renamed `welfare_ppp_*` column, not a raw column from the Parquet file, to avoid confusion for users who know the underlying schema.

- **[P3.4]** [cg-testing] `tests/testthat/test-table-maker.R` — Column-pruning test only covers `by = "gender"`. A second assertion with `by = NULL` in the same fixture would confirm that the `by = NULL` code path also prunes correctly.

- **[P3.5]** [cg-performance] `R/table_maker.R:248–254` — `needed_cols` includes `country_code`, `surveyid_year`, `welfare_type` for the Step 8 metadata join, but these are already in `entries`. Building `meta` from `entries` instead of from `dt` would eliminate three repeated scalar columns from the full microdata working set (~24 bytes × n_rows per survey).
  ```r
  meta <- entries[, .(pip_id, country_code, year, welfare_type)]
  data.table::setnames(meta, "year", "surveyid_year")
  ```

- **[P3.6]** [cg-architecture] `R/table_maker.R` — `needed_cols` hardcodes knowledge of what all downstream compute functions require. If a new measure family needs an extra column, `table_maker()` must be updated in lockstep. Consider a simple named-list constant per compute family (e.g. `.INEQUALITY_REQUIRED_COLS <- c("welfare", "weight")`) that `table_maker()` unions declaratively.

- **[P3.7]** [cg-version-control] `compound-gpid.context.md` — Internal UNC hostname `//w1wbgencifs01/pip/…` is committed in plain text. While not a credential, it leaks internal network topology. Consider replacing with a placeholder in committed docs.

- **[P3.8]** [cg-learnings-researcher] — The pre-sort contract removal creates a standing risk: if the sort contract is ever broken, the natural debugging instinct is "add a sort" — but adding `arrange(welfare)` inside the Arrow query (L5) carries 537% overhead on a network share. Document at the `load_surveys()` boundary: `# CONTRACT: returned rows are welfare-sorted within each survey. Do NOT add Arrow-side arrange() — see 2026-04-30-arrow-io-and-compute-lessons.md §L5`.

- **[P3.9]** [cg-learnings-researcher] — The `cols` parameter now passes `by`-dimension columns (`gender`, `area`, etc.) as first-class Arrow select inputs. These are dict-encoded in the PIP schema (L4). If Arrow push-down aggregation is pursued in a future cycle (L6 shows headcount/mean/MLD are feasible), `mutate(across(all_of(dict_cols), as.character))` must be applied before `group_by` to avoid `NotImplemented: Unifying differing dictionaries`. Flag this in a `.cg-docs/` note before that work begins.

---

### ✅ Passed

- **cg-data-quality**: No issues found with `cols` duplicate/named-vector handling (`union()` deduplicates; `intersect()` strips names).
- **cg-data-quality**: No issues with `validate_parquet.R` change (doc-only).
- **cg-reproducibility**: `is.unsorted()` / sorted-vs-copy numeric stability — no difference.
- **cg-reproducibility**: `needed_cols` construction is deterministic; `.validate_by()` guards unknown `by` dimension names before `intersect()`.
- **cg-adversarial**: Multi-survey batch ordering (across-file) — mitigated by `GRP(c("pip_id", by))` grouping.
- **cg-adversarial**: Tied welfare values — Gini invariant to permutation of ties.
- **cg-adversarial**: All-same welfare — correctly returns 0.
- **cg-adversarial**: Single-row group — `length == 1L` guard returns 0.
- **cg-adversarial**: `cols` with absent dimension column — Arrow unified schema fills with NULL/NA correctly.
- **cg-adversarial**: `cols` without `pip_id` — `union()` always adds it.
- **cg-adversarial**: `NULL` element in `cols` — R's `c()` drops `NULL` silently.
- **cg-performance**: `vapply` loops for PPP resolution — negligible at realistic scales.
- **cg-performance**: `work_g` copy in Gini `by != NULL` path — minimal (2-column copy).
