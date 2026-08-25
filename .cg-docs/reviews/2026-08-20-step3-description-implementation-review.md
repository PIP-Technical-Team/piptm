---
date: 2026-08-21
depth: standard
type: standard
plan: .cg-docs/plans/2026-08-20-step3-description-implementation.md
findings:
  P0.1: fixed
  P0.2: fixed
  P0.3: fixed
  P1.1: fixed
  P1.2: fixed
  P1.3: fixed
  P2.1: fixed
  P2.2: fixed
  P2.3: fixed
  P2.4: fixed
  P2.5: fixed
  P2.6: fixed
  P2.7: fixed
  P2.8: fixed
  P2.9: fixed
  P2.10: fixed
  P2.11: fixed
  P3.1: open
  P3.2: open
  P3.3: open
  P3.4: open
---

## Review Report

**Review mode**: standard
**Files reviewed**: 10 code files + 5 test files + 1 helper
**Findings**: 22 (P0: 3, P1: 3, P2: 11, P3: 4)

### P0 — BLOCKING (immediate remediation required)

- **[P0.1]** [cg-code-quality, cg-data-quality, cg-performance, cg-reproducibility] `R/table_maker.R:305-313` — Warning handler defined but never registered with `withCallingHandlers()`
  **Why**: `.warn_handler` is assigned a closure but never wired into any `withCallingHandlers()` call. Warnings emitted by `cli_warn()` inside `table_maker()` (dimension exclusion, suppression, missing pip_id) are silently lost from the `with_meta = TRUE` path. The `$warnings` field always returns `list()`.
  **Fix**: Wrap the main computation body in `withCallingHandlers({...}, warning = .warn_handler)` when `with_meta = TRUE`.

- **[P0.2]** [cg-code-quality, cg-data-quality, cg-performance, cg-reproducibility] `R/table_maker.R:307` — Warning handler appends to wrong `.meta_state` position
  **Why**: Even if registered, `.meta_state[[length(.meta_state) + 1L]] <<-` appends warnings as unnamed elements at position 8+ of the list, not into `.meta_state[["warnings"]]`. Line 741 reads `.meta_state[["warnings"]]` which remains the original empty `list()`.
  **Fix**: Replace with `.meta_state[["warnings"]][[length(.meta_state[["warnings"]]) + 1L]] <<- conditionMessage(w)`.

- **[P0.3]** [cg-code-quality] `R/description.R:153` — Unicode escape `\\u00b7` double-escaped, renders literal `\u00b7` instead of `·`
  **Why**: The source file contains `" \\u00b7"` (double backslash). In R string literals, `\\` produces a literal backslash, so the markdown output contains the text `\u00b7` instead of the middle-dot character `·`. This affects every `/description` response.
  **Fix**: Change to `"\u00b7 "` (single backslash) or use literal `·`.

### P1 — CRITICAL (must fix before merge)

- **[P1.1]** [cg-documentation, cg-architecture] `R/table_maker.R:182-274` — Missing `@param with_meta` in roxygen block
  **Why**: `with_meta = FALSE` is in the function signature (line 284) and man page usage, but the roxygen block has no `@param with_meta` entry. `R CMD check` will emit `Undocumented arguments in documentation object 'table_maker'`.
  **Fix**: Add `@param with_meta Logical. When TRUE, returns a list with data, specification, execution, provenance, and warnings instead of a plain data.table. Default FALSE.`

- **[P1.2]** [cg-documentation, cg-code-quality] `R/description.R:28-30` — `@return` inaccurately lists `excluded_surveys` as a top-level model section
  **Why**: The docs say the returned list optionally contains `excluded_surveys` at the top level. In code, it is nested under `surveys$excluded_surveys` (line 115).
  **Fix**: Update `@return` to: "...and optionally `poverty_line`, `layout`, `warnings`. Excluded surveys are in `surveys$excluded_surveys`."

- **[P1.3]** [cg-architecture, cg-version-control] `DESCRIPTION:26` — Trailing comma after last Imports entry
  **Why**: Line 26 reads `jsonlite,` followed immediately by `Suggests:` on line 27. The last entry in a DESCRIPTION field must not have a trailing comma. This causes `R CMD check` NOTEs and can break `pak::pkg_install()`.
  **Fix**: Change `jsonlite,` to `jsonlite`.

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-code-quality, cg-data-quality] `R/description.R:52-54 + 114-116` — Duplicate `excluded_surveys` assignment
  **Why**: The same condition `nrow(exec$excluded_surveys) > 0L` is checked and assigned twice. Lines 52-54 write into the local `surveys` variable; lines 114-116 overwrite `model$surveys` with the same value.
  **Fix**: Remove lines 114-116 (keep only lines 52-54).

- **[P2.2]** [cg-code-quality] `inst/plumber/plumber.R:255-256` — `piptm:::` used for exported functions
  **Why**: Both `build_description_model` and `render_description_markdown` are exported in NAMESPACE. Using `:::` bypasses the namespace contract and is inconsistent with all other endpoints using `piptm::`.
  **Fix**: Change to `piptm::build_description_model()` and `piptm::render_description_markdown()`.

- **[P2.3]** [cg-code-quality] `R/description.R:239` — Parameter `c` shadows built-in `c()` function
  **Why**: Inside `render_description_markdown`, `function(c) c$label` uses `c` as a parameter name, shadowing R's concatenate function.
  **Fix**: Rename to `function(cat) cat$label` or `function(entry) entry$label`.

- **[P2.4]** [cg-code-quality] `R/table_maker.R:98-180` — `.build_specification()` calls registry functions without `release` parameter
  **Why**: `piptm_analysis_variables()`, `piptm_stat_groups()`, `piptm_layout_covariates()`, and `piptm_filter_categories()` all use the default `release = NULL` regardless of the `release` passed to `table_maker()`.
  **Fix**: Thread the `release` parameter through `.build_specification(..., release)` and pass it to each registry call.

- **[P2.5]** [cg-data-quality] `R/table_maker.R:290-302` — `.meta_state` fields `filters_applied`, `measures_computed`, `suppression`, `ppp_used` never populated
  **Why**: Four fields are initialized but never written during execution. The `execution` block returned always has these as NULL/empty. The suppression info is always reported as "Suppression is disabled" in descriptions.
  **Fix**: Populate each field at the appropriate execution point (after filter normalization, after classification, after suppression, after load_surveys).

- **[P2.6]** [cg-data-quality] `R/table_maker.R:436-466` — Filter-base excluded surveys not harvested into metadata
  **Why**: Surveys excluded for lacking required `filter_base` dimensions are not added to `.meta_state[["excluded_surveys"]]`. Only dimension pre-filter exclusions are harvested.
  **Fix**: Add a harvest block after the filter-base exclusion.

- **[P2.7]** [cg-data-quality] `R/description.R:34,143` — No input validation on `build_description_model()` and `render_description_markdown()`
  **Why**: Both functions access nested fields without checking that the input has the expected structure. Malformed inputs produce cryptic errors.
  **Fix**: Add structural validation at function entry.

- **[P2.8]** [cg-performance] `R/description.R:147-282` — O(n²) string growth via repeated `c(parts, ...)` in `render_description_markdown()`
  **Why**: ~30 sequential `c(parts, ...)` calls each copy the entire accumulated vector.
  **Fix**: Use list-based accumulation with index assignment, then `paste(unlist(...), collapse = "\n")`.

- **[P2.9]** [cg-performance] `R/table_maker.R:109-124` — `piptm_stat_groups()` called inside per-measure `lapply` loop
  **Why**: The registry is re-read on every iteration (up to 19× per call). Same issue with `piptm_filter_categories()` per `by` variable.
  **Fix**: Hoist registry lookups before the loop.

- **[P2.10]** [cg-code-quality, cg-version-control] `tests/testthat/test-api-description-endpoint.R:6-44` — DRY violation: request/response helpers duplicated from `test-api-endpoints.R`
  **Why**: Three copies of `make_api_req`/`parse_api_res` across test files.
  **Fix**: Extract into shared `helper-api.R`.

- **[P2.11]** [cg-version-control] `R/table_maker.R:1` — UTF-8 BOM introduced
  **Why**: File starts with `﻿` (BOM character EF BB BF). Can cause `R CMD check` NOTEs and encoding issues.
  **Fix**: Remove BOM; save as UTF-8 without BOM.

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-testing] `tests/testthat/helper-description.R:134` — `runif()` in mock fixture produces non-deterministic test data
  **Why**: `make_mock_table_result()` uses `runif()` with no seed. All current tests pass but this is fragile.
  **Fix**: Replace with deterministic sequence.

- **[P3.2]** [cg-testing] Multiple test files — Missing branch coverage (warnings in model/renderer, suppression disabled, 1-var layout, 3+ var layout, consumption welfare, singular grammar, category display, poverty_line pass-through, error paths)
  **Why**: 15 edge-case branches are untested. See testing agent details.
  **Fix**: Add targeted tests per testing agent recommendations.

- **[P3.3]** [cg-code-quality] `R/description.R:1` — File header says "Description model and markdown renderer" but file also contains `build_table_description()`
  **Why**: Minor documentation inconsistency.
  **Fix**: Update header to "Description model, markdown renderer, and convenience wrapper".

- **[P3.4]** [cg-code-quality] `tests/testthat/test-description-renderer.R:25` — Weak assertion `grepl("Colombia", md) || grepl("COL", md)`
  **Why**: The `||` means the test passes if either string appears anywhere, which is too loose.
  **Fix**: Use more targeted assertion like `grepl("COL_2010_ECH_INC_ALL.*INC", md)`.

### Passed

- **cg-version-control**: No sensitive data, credentials, or secrets found. Branching follows conventions.
- **cg-reproducibility**: No `renv.lock` (project-level decision), but all test fixtures use consistent patterns.
- **cg-architecture**: Endpoint design is consistent with existing patterns. Sidecar pattern is well-isolated from computation path.
- **cg-documentation**: `README.md` endpoint table is correct. `@family api` grouping is appropriate.

### Summary

The 3 P0 findings (broken warning capture + Unicode escape) are the highest priority — the warning capture mechanism is entirely non-functional and will silently lose metadata, and the Unicode issue affects every API response. P1 findings include a missing `@param` doc, inaccurate `@return`, and a trailing comma in DESCRIPTION that breaks `R CMD check`.
