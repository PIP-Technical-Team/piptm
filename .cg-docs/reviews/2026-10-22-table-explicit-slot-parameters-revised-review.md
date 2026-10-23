---
date: 2026-10-23
depth: standard
type: standard
plan: .cg-docs/plans/2026-10-22-table-explicit-slot-parameters-revised.md
findings:
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
  P2.9: skipped
  P2.10: skipped
  P3.1: fixed
  P3.2: fixed
  P3.3: fixed
  P3.4: fixed
  P3.5: fixed
  P3.6: fixed
---

## Review Report

**Review mode**: standard
**Files reviewed**: 8 (`inst/plumber/plumber.R`, `inst/plumber/helpers.R`, `docs/api-contract.md`, `tests/testthat/test-api-helpers.R`, `tests/testthat/test-api-endpoints.R`, `tests/testthat/test-api-description.R`, `tests/testthat/test-phase2-analysis-var-and-poverty-line.R`, `tests/testthat/fixtures/table-slot-baseline/*.json`)
**Findings**: 19 (P0: 0, P1: 3, P2: 10, P3: 6)

### P1 — CRITICAL (must fix before merge)

- **[P1.1]** [cg-data-quality] [safe_auto] `inst/plumber/helpers.R:382` — Missing type check and list-unboxing on layout slot inputs (`column`, `row`, `superColumn`, `superRow`) allows nested JSON arrays/objects to bypass arity and type validation.
  **Why**: When a POST JSON payload passes a nested array or object (e.g., `{"column": [["gender", "area"]]}` or `{"column": []}`), `jsonlite::fromJSON()` parses `value` as a `list`. `length(value)` is `1L`, `is.na(list(character(0)))` evaluates to `logical(0)` and crashes with `Error in if (is.na(value)) : argument is of length zero` (HTTP 500), and non-character scalars like `123` are coerced via `as.character()` rather than rejected.
  **Fix**: Unlist `list` values and reject non-character slot inputs before checking arity and `NA`s. *(Fixed via autofix)*

- **[P1.2]** [cg-testing / cg-reproducibility] [safe_auto] `tests/testthat/test-api-endpoints.R:1461` — Baseline equivalence loop stacks `withr::defer()` / `local_tempdir()` across iterations and mis-scopes `.local_envir = parent.frame()`.
  **Why**: Inside `for (fx_file in fx_files)`, `withr::local_tempdir()`, `withr::defer()`, and `activate_test_registry(..., .local_envir = parent.frame())` attach cleanup handlers to the enclosing frame rather than each iteration, leaving `.piptm_env$registries` mutated across iterations.
  **Fix**: Wrap the loop body in `local({ ... })`, sort `fx_files`, and omit `.local_envir = parent.frame()`. *(Fixed via autofix)*

- **[P1.3]** [cg-version-control] [manual] `check_out.txt:1` — Pre-existing tracked temporary check outputs, scratch reproduction scripts, and cached `.rds` binaries in repository.
  **Why**: Diagnostic and temporary artifacts (`check_out.txt`, `check_out2.txt`, `.kilo_tmp_repro.R`, `.cg-docs/tmp/check_chin_fix.R`, `example_description_output*.html`, `benchmarks/data-cache.rds`) are tracked in git, bloating git history and causing `R CMD check` top-level notes.
  **Fix**: Untrack with `git rm --cached` when ready to clean up repository history. *(Manual — requires user approval)*

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-code-quality / cg-documentation / cg-architecture / cg-data-quality] [safe_auto] `inst/plumber/helpers.R:226` — Outdated roxygen2 docstring in `validate_table_input()` still documents `@param by` and omits the new slot parameters and return fields.
  **Why**: `validate_table_input()` replaced `by` with `column`, `row`, `superColumn`, and `superRow` and returns `pop_share_threshold`, `by`, and `slots`, but the roxygen comment block still documented `@param by`.
  **Fix**: Update the roxygen block above `validate_table_input()` to document `column`, `row`, `superColumn`, `superRow`, `pop_share_threshold`, and the 7-field return structure. *(Fixed via autofix)*

- **[P2.2]** [cg-testing] [safe_auto] `tests/testthat/test-api-helpers.R:441` — Missing unit test coverage for 3-slot (`superColumn`) and 4-slot (`superRow`) assembly, slot-order preservation, and non-adjacent slot gaps/duplicates.
  **Why**: Only 0, 1, and 2 filled slots were tested; 3-slot and 4-slot order preservation, a gap at `superColumn`, and non-adjacent duplicates were untested.
  **Fix**: Add unit tests in `tests/testthat/test-api-helpers.R` for 3-slot, 4-slot, `superColumn` gap, and non-adjacent duplicate slots. *(Fixed via autofix)*

- **[P2.3]** [cg-testing] [safe_auto] `tests/testthat/test-api-description.R:422` — Missing slot-validation error tests for `POST /description` fallback path and redundant regex assertion.
  **Why**: `test-api-description.R` only tested the happy path (`column = "gender"`) and did not test legacy `by`-only rejection, gapped slots, or duplicate slots on `/description`.
  **Fix**: Add fallback error tests for legacy `by`-only body (400), gapped slots (422), and duplicate slots (422), and simplify the regex check to `expect_match(body, "gender", ignore.case = TRUE)`. *(Fixed via autofix)*

- **[P2.4]** [cg-testing] [safe_auto] `tests/testthat/test-api-endpoints.R:29` — `make_api_req()` injects `analysis_var=welfare` into `QUERY_STRING` on `POST /table` JSON-body requests.
  **Why**: When `method == "POST"` and `body` is supplied, `query$analysis_var` was still populated, sending `analysis_var` in both query string and JSON body.
  **Fix**: Guard query injection with `is.null(body)`. *(Fixed via autofix)*

- **[P2.5]** [cg-reproducibility] [safe_auto] `tests/testthat/test-api-endpoints.R:1454` — `rprojroot::find_package_root_file()` used instead of `testthat::test_path()` to locate test fixtures.
  **Why**: `testthat::test_path()` is portable across `devtools::test()` and `R CMD check` without walking parent directories for `DESCRIPTION`.
  **Fix**: Use `testthat::test_path("fixtures", "table-slot-baseline")`. *(Fixed via autofix)*

- **[P2.6]** [cg-version-control] [safe_auto] `.Rbuildignore:1` — Non-standard top-level files and directories missing from `.Rbuildignore`.
  **Why**: Top-level project/adapter directories (`docs`, `wiki`, `roadmap.json`, `.kilo`, `.claude`, `.agents`, etc.) were not in `.Rbuildignore`.
  **Fix**: Add exclusions for non-package top-level files and directories to `.Rbuildignore`. *(Fixed via autofix)*

- **[P2.7]** [cg-architecture / cg-data-quality] [manual] `inst/plumber/plumber.R:428` — `/description` fallback input validation occurs inside `capture_with_warnings()`, returning HTTP 422 instead of HTTP 400 on parameter validation failure; `pop_share_threshold` default fixed to `0.001`.
  **Why**: Defaulting `pop_share_threshold` to `0.001` when omitted from `body` was applied via autofix (`plumber.R:428`). However, existing test `test-api-description.R:409` ("POST /description returns 422 for fallback path when pip_id not found in manifest") and `plumber.R:350` explicitly codify 422 for `/description` fallback errors; changing 422 -> 400 would be a breaking behavior change to `/description`.
  **Fix**: Keep 422 or confirm whether `/description` fallback validation errors should change to 400. *(Manual — requires user decision)*

- **[P2.8]** [cg-data-quality] [manual] `inst/plumber/helpers.R:447` — `validate_table_input()` does not check whether `analysis_var` is duplicated in a layout slot (`by`).
  **Why**: Passing the same variable as `analysis_var` and a disaggregation slot (e.g. `analysis_var = "pov_status"` and `column = "pov_status"`) is not rejected at the 400 validator boundary.
  **Fix**: Confirm whether `analysis_var %in% by` should be rejected with HTTP 400 in `validate_table_input()`. *(Manual — requires user decision)*

- **[P2.9]** [cg-version-control] [advisory] `.git/HEAD:1` — Active feature development performed directly on `DEV` branch, with case-insensitive `dev/` directory name collision on Windows.
  **Why**: Working on a dedicated feature branch avoids ambiguity with the `dev/` folder and keeps `DEV` clean for PR merges.
  **Fix**: Consider branching `feat/table-explicit-slot-params` before committing.

- **[P2.10]** [cg-version-control] [advisory] `.git/COMMIT_EDITMSG:1` — Ensure untracked fixture files under `tests/testthat/fixtures/table-slot-baseline/*.json` are staged when committing.
  **Why**: `git commit -a` omits untracked files, which would leave the baseline fixtures out of the commit.
  **Fix**: Stage `tests/testthat/fixtures/table-slot-baseline/*.json` explicitly with `git add`.

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-code-quality] [safe_auto] `inst/plumber/helpers.R:369` — Extract repeated layout slot name vector to module-level constant `.SLOT_NAMES`. *(Fixed via autofix)*
- **[P3.2]** [cg-testing] [safe_auto] `tests/testthat/test-api-helpers.R:23` — Remove unused `poverty_lines` compatibility shim in `.vti()`. *(Fixed via autofix)*
- **[P3.3]** [cg-architecture] [safe_auto] `docs/api-contract.md:271` — Replace stale `by` property in `TableRow` schema documentation with `"<dimension>"` top-level column description. *(Fixed via autofix)*
- **[P3.4]** [cg-version-control] [safe_auto] `.gitignore:1` — Add `*.Rcheck/`, `*.tar.gz`, `.Ruserdata`, `.vscode/`, and `.idea/` exclusions. *(Fixed via autofix)*
- **[P3.5]** [cg-version-control] [safe_auto] `.gitattributes:1` — Add `*.yaml text eol=lf` and `*.json text eol=lf`. *(Fixed via autofix)*
- **[P3.6]** [cg-code-quality] [advisory] `tests/testthat/test-api-description.R:428` — Consider extracting inline single-survey Arrow/manifest fixture setup into `helper-fixtures.R` in a future cleanup.

### ✅ Passed
- `cg-performance`: No issues found ($O(1)$ 4-element slot validation with no extra allocations or copies).
