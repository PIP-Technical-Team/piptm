---
date: 2026-05-25
depth: light
parent-review: .cg-docs/reviews/2026-05-20-validate-parquet-wrapper-review-2.md
type: verification
findings:
  P2.1: fixed
  P3.1: fixed
---

## Review Report

**Review depth**: light (verify mode)  
**Files reviewed**: `R/load_data.R`, `tests/testthat/test-load-data.R`  
**Findings**: 2 (P0: 0, P1: 0, P2: 1, P3: 1)

---

### P0 — BLOCKING

*None.*

### P1 — CRITICAL

*None.*

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-code-quality] `R/load_data.R:~245–275` — **PPP-column resolution logic duplicated between step 4a and step 5 of `load_survey_microdata()`**  
  **Why**: Step 4a defines `find_ppp_col_lsm()` locally to resolve `target_col_for_prune` before `open_dataset()`. Step 5 defines a separate `find_ppp_col()` with identical logic (`prefix <- paste0(...)`, same `startsWith` match) to resolve `target_col` for the rename. The two functions are character-for-character equivalent. A future change to the matching logic (e.g. handling a new column naming convention) must be applied in both places — a maintainability hazard. This is a **new issue** introduced by P3.3 (adding `cols` to `load_survey_microdata()`), not in scope of any prior fixed finding.  
  **Why**: `load_surveys()` also defines `find_ppp_col_wv` with the same logic — three copies total.  
  **Fix**: Extract a single package-internal helper `.find_welfare_col(wv, year_val)` placed near `.build_parquet_paths()` and used by both functions:
  ```r
  .find_welfare_col <- function(wv, year_val) {
    prefix <- paste0("welfare_ppp_", year_val)
    wv[wv == prefix | startsWith(wv, paste0(prefix, "_"))]
  }
  ```
  Then replace `find_ppp_col_lsm`, `find_ppp_col`, and `find_ppp_col_wv` calls throughout `R/load_data.R` with `.find_welfare_col(...)`.

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-testing] `tests/testthat/test-load-data.R` — **`cols` + `ppp=NULL` path in `load_survey_microdata()` is untested**  
  **Why**: All five new `load_survey_microdata()` cols tests use explicit `ppp = 2017L` or the legacy fixture. The code path where `cols != NULL` and `ppp = NULL` (so `ppp_year_for_prune <- ppp_sort_val`) runs but is not exercised by any assertion. If the `ppp_sort_val` lookup is broken in step 4a, no test catches it.  
  **Fix**:
  ```r
  test_that("load_survey_microdata() cols with ppp=NULL uses ppp_sort for translation", {
    fx <- make_ppp_fixtures()
    piptm::set_manifest_dir(fx$tmp_manifest)
    piptm::set_arrow_root(fx$tmp_arrow)
    withr::defer(reset_load_env())

    dt <- piptm::load_survey_microdata("COL", 2010L, "INC", ppp = NULL,
                                       cols = c("welfare", "weight", "pip_id"))

    expect_true("welfare" %in% names(dt))
    expect_setequal(names(dt), c("welfare", "weight", "pip_id"))
    expect_equal(dt$welfare, c(1.5, 2.0, 2.5, 3.0, 3.5))  # ppp_sort = 2017
  })
  ```

### ✅ Passed

- **cg-code-quality**: Mixed-schema guard (P1.1), `auto_or_welfare_cols` rename (P2.2), `@param cols` documentation (P2.5), type validation, `pip_id` doc — all correctly implemented, no regressions.
- **cg-testing**: Weight auto-inclusion test (P2.1), mixed-schema regression test (P2.4), type-check tests (P3.1 original), five `load_survey_microdata()` cols tests (P3.3) — all present and passing. 98 tests pass total.

---

*Parsed 2 finding IDs.*
