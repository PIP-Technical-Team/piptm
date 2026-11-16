---
date: 2026-11-13
depth: standard
type: standard
plan: .cg-docs/plans/2026-11-12-metadata-rework-and-table-maker-decoupling.md
findings:
  P1.1: fixed
  P2.1: fixed
  P2.2: fixed
  P2.3: fixed
  P2.4: fixed
  P2.5: fixed
  P2.6: skipped
  P3.1: fixed
  P3.2: fixed
  P3.3: fixed
  P3.4: fixed
  P3.5: fixed
  P3.6: fixed
  P3.7: fixed
  P3.8: fixed
  P3.9: skipped
  P3.10: skipped
---

## Review Report

**Review mode**: standard
**Files reviewed**: 18
**Findings**: 17 (P0: 0, P1: 1, P2: 6, P3: 10)

### P0 — BLOCKING (immediate remediation required)
None.

### P1 — CRITICAL (must fix before merge)
- **[P1.1]** [cg-testing] `[safe_auto]` `tests/testthat/test-description-renderer.R:139` — Failing test assertion for legacy `"Version"` table header after `vermast`/`veralt` split
  **Why**: `.make_render_model()` replaced the legacy `version` column in `surveys_selected$content$loaded_list` with `vermast` and `veralt`, which `.display_table_header_markdown()` maps to `"Master version"` and `"Alternative version"`. The string `"Version"` (capital `V`) no longer appears in the rendered Markdown for `.make_render_model()`, causing `expect_match(out, "Version", fixed = TRUE)` at line 139 to fail. Similarly, the override fixture at line 165 still used `version = "v01_v01"`.
  **Fix**: Replace the stale `"Version"` assertion with `"Master version"` and `"Alternative version"`, and update the override fixture at line 165 to use `vermast` and `veralt`.

### P2 — IMPORTANT (should fix)
- **[P2.1]** [cg-documentation] `[safe_auto]` `R/description_builder.R:221-223` — Missing `@param` and `@return` tags on `.build_notes_content()` produce an `.Rd` file with undocumented arguments
  **Why**: `.build_notes_content()` uses `@keywords internal` without `@noRd`, generating `man/dot-build_notes_content.Rd` with `\usage{}` but no `\arguments{}` section, which triggers an `R CMD check` warning.
  **Fix**: Add `@param` and `@return` roxygen tags to `.build_notes_content()` and regenerate `man/dot-build_notes_content.Rd`.
- **[P2.2]** [cg-testing] `[safe_auto]` `tests/testthat/test-description-builder.R:220-238` — `filters_applied` test does not exercise category label resolution
  **Why**: `activate_test_registry()` defines `area` category codes as `"urban"` and `"rural"` rather than `"1"` and `"2"`, so `filter_base = list(area = 1L)` falls back to raw code `"1"` and `expect_true(nzchar(...))` passes without testing label lookup.
  **Fix**: Seed integer-string category codes (`"1"`, `"2"`) on `area` in the test and assert exact label resolution (`"Urban, 99"`) with `expect_identical()`.
- **[P2.3]** [cg-testing] `[safe_auto]` `tests/testthat/test-description-builder.R:435` — Tautological assertion `nchar(...) >= 0L` in missing-registry-category test
  **Why**: `nchar()` on any non-`NA` string is always `>= 0L`; `.format_category_count_display()` specifically returns `"0: Unspecified"` when both `categories` and `n_categories` are `NULL`.
  **Fix**: Assert `expect_identical(layout$n_categories[[1]], "0: Unspecified")`.
- **[P2.4]** [cg-version-control] `[manual]` `man/dot-build_notes_content.Rd:1` — Untracked `roxygen2` `.Rd` files and unstaged `.Rd` deletion in working tree
  **Why**: Replacing `.build_overview_content()` with `.build_notes_content()` and documenting internal helpers deleted `man/dot-build_overview_content.Rd` and created untracked `man/dot-*.Rd` files. Using `git commit -a` without `git add -A` would leave `man/` out of sync.
  **Fix**: Stage all `man/` additions and deletions with `git add -A` when committing.
- **[P2.5]** [cg-architecture] `[manual]` `R/description_renderer.R:75-82` & `R/description_renderer_html.R:100-119` — Duplicated renderer dispatch, section special-casing, and label/header/value mappings across Markdown and HTML renderers
  **Why**: Both renderer files maintain parallel copies of section/key display-label resolvers, table header mappings, and `notes` unlisting logic.
  **Fix**: Extract shared format-agnostic label/header/value helpers into a common internal module in a future refactor.
- **[P2.6]** [cg-architecture] `[advisory]` `R/description_builder.R:1-715` — Metadata builder and renderer files exceed the 300-line modularity threshold
  **Why**: `R/description_builder.R` combines top-level model assembly, section builders, registry role-slice helpers, and display string formatters in one file.
  **Fix**: Consider splitting registry role-slice and display formatting helpers into `R/description_helpers.R`.

### P3 — MINOR (nice to have)
- **[P3.1]** [cg-code-quality] `[safe_auto]` `R/description_builder.R:115-126` — Duplicated list-unwrapping and fallback coercion logic for `analysis_var_label` and `analysis_var_type`
  **Why**: Both fields repeat the same 6-line `unlist(..., use.names = FALSE)` + non-empty scalar character fallback check.
  **Fix**: Extract `.coerce_scalar_chr(x, default)` and reuse it for both fields.
- **[P3.2]** [cg-performance] `[safe_auto]` `R/description_builder.R:278-282` — Replace `strsplit()` + dual `vapply()` loops with vectorized string operations for `vermast`/`veralt` parsing
  **Why**: Vectorized `sub()` and `data.table::fifelse()` avoid intermediate list allocation and per-row R callbacks.
  **Fix**: Use `sub("_.*$", "", raw_ver)` and `data.table::fifelse(grepl("_", raw_ver, fixed = TRUE), sub("^[^_]*_", "", raw_ver), "")`.
- **[P3.3]** [cg-architecture] `[safe_auto]` `R/description_builder.R:165,265` — Unused `release` parameter in `.build_surveys_content()`
  **Why**: `release` is passed into `.build_surveys_content()` but never referenced in its body.
  **Fix**: Make `release = NULL` optional in `.build_surveys_content()` and omit passing `resolved_release` from `build_description_model()`.
- **[P3.4]** [cg-documentation] `[safe_auto]` `R/description_renderer.R:59-70` — `.render_section_content()` roxygen block omits `@param section_name` and `"notes"` dispatch description
  **Fix**: Document `@param section_name` and the `"notes"` unlabeled-bullet branch in `.render_section_content()` and regenerate `.Rd`.
- **[P3.5]** [cg-testing] `[safe_auto]` `tests/testthat/test-description-renderer.R:150` — Missing Markdown renderer test for `statistics_selected$content$poverty_line` when `applicable = TRUE`
  **Fix**: Add a test in `tests/testthat/test-description-renderer.R` asserting `- Poverty line:\n  - Value: 2.15\n  - PPP year: 2021` when `applicable = TRUE`.
- **[P3.6]** [cg-code-quality] `[advisory]` `R/description_renderer.R:348` & `R/description_renderer_html.R:399` — Legacy `version = "Version"` entry retained in table header mappings for backward compatibility.
- **[P3.7]** [cg-testing] `[advisory]` `R/description_renderer.R:385` — Minor Markdown vs. HTML fallback divergence for unmapped `stat_group` values.
- **[P3.8]** [cg-documentation] `[advisory]` `R/description_builder.R:16-19` — `@description` references public registry wrapper names alongside single-load registry slicing.
- **[P3.9]** [cg-performance] `[advisory]` `R/description_renderer.R:114-120` & `R/description_renderer_html.R:165-176` — Row-by-row nested `vapply()` in table renderers.
- **[P3.10]** [cg-version-control] `[advisory]` `R/description_builder.R:1` — Branch name `step3_metadata` does not use `<type>/<short-description>` prefix.

### ⚠️ Incomplete Reviews
- `@cg-data-quality` did not produce usable output (connection reset by server). Consider re-running `/cg-review` with a higher model tier, or invoke `@cg-data-quality` directly.

### ✅ Passed
- `cg-reproducibility`: No issues found
