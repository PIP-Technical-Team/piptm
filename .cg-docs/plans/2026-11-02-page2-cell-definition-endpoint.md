---
date: 2026-11-02
title: "Page 2 cell definition endpoint (POST /cell-definition)"
status: active
scope: "Deep"
brainstorm: ".cg-docs/brainstorms/2026-11-02-page2-cell-definition-endpoint.md"
language: "R"
estimated-effort: "medium"
deviation-policy: "ask"
phases: 2
completed-phases: [1]
current-phase: 2
execution-report: ".cg-docs/work-reports/2026-11-02-page2-cell-definition-endpoint.md"
artifact-schema-version: 1
tags: [cell-definition, page-2, api, endpoint, shares, prose, table-maker]
---

# Plan: Page 2 Cell Definition Endpoint (`POST /cell-definition`)

## Objective

Implement a result-free Page 2 cell explanation feature consisting of a dedicated curated prose specification (`inst/extdata/tm_cell_prose.yaml`), an exported pure R builder (`build_cell_explanation()`), and a new Plumber endpoint (`POST /cell-definition`) that returns a clear, three-section JSON explanation (*Who is in the table*, *What a cell is*, *What is shown in that cell*) of what each cell in the configured `/table` output represents.

## Context

On Page 2 of the Table Maker UI, users choose a sample-base filter (`filter_base`), an analysis variable (`analysis_var`), up to four statistics (`measures`), and up to four layout covariates (`column`, `row`, `superColumn`, `superRow`). Before running `table_maker()`, the UI needs to explain in plain, natural prose what the table covers, what a single cell identifies, and what each statistic inside that cell represents — especially for the three share measures (`pop_share`, `target_within_group_share`, `target_survey_share`), whose numerators ($C$ vs $C \cap Y=1$) and 100% denominators ($T$ vs $C$) are easily confused.

Per the approved brainstorm (`.cg-docs/brainstorms/2026-11-02-page2-cell-definition-endpoint.md`):
- All prose fragments live in a standalone file `inst/extdata/tm_cell_prose.yaml` so `tm_variable_spec.yaml`, `tm_measure_spec.yaml`, and `build_variable_registry()` remain untouched.
- The explanation is organized into three fixed sections (`who_is_in_table`, `what_a_cell_is`, `what_is_shown`) plus optional `notes`, with head-noun promotion (*"women with no schooling in urban areas"*), explicit `counts` / `out_of` / `total_note` fields on share measures, and a short-vs-long filter length rule so multi-filter configurations never produce run-on sentences.
- The metadata rework of `/description` and `POST /table/download/metadata` is deferred to a separate follow-up plan; existing `build_cell_definition()` and `/description` remain untouched here to avoid regressions in their 1,075-line test suite before the metadata rework removes the overlapping section.

### Revision note (`/cg-plan-review` pass, 2026-11-02)

`@cg-plan-critic` found 3 P1 and 2 P2 issues in the first version of this plan, all approved for revision by the user. This version resolves them:

1. **Registry fallback was unimplementable.** Most filter/covariate variables (gender, area, age_group, educat4, etc.) have no inline `categories:` block in `tm_variable_spec.yaml` — real category codes/labels only exist in pipdata's `recode_spec`, materialized per release by `build_variable_registry()`. The original plan's "fall back to reading the spec files directly" path could not reconstruct categories for most variables. **Fixed**: `tm_cell_prose.yaml` category authoring, and the sync test that guards it, are now scoped against a new checked-in test fixture (`tests/testthat/fixtures/registry-cell-prose-sample.json`) that mirrors the shape `piptm_variable_registry()` produces — not against `tm_variable_spec.yaml` inline entries, which are incomplete by design.
2. **Undefined behavior when `piptm_variable_registry()` aborts.** That function `cli_abort()`s rather than returning `NULL` when no release/registry is resolvable — exactly the condition a bare unit-test environment (no `PIPTM_REGISTRY_DIR`) hits. **Fixed**: `build_cell_explanation()` wraps the lookup in `tryCatch(piptm_variable_registry(release), error = function(e) NULL)`, reusing the existing codebase pattern (`R/table_maker.R:178`, `R/manifest.R:363`). When `NULL`, category/slot labels degrade to the raw code (title-cased) and a `warnings[]` entry is added — a defined, tested contract instead of an undefined path.
3. **Validator "reuse" was not actually callable.** `validate_table_input()` has `pip_id` as a required (no-default) argument and no `filter_base` parameter at all, so the described reuse was not a direct function call. **Fixed**: extract a new shared internal helper `.validate_table_shape()` (no `pip_id`, no `filter_base`) in `inst/plumber/helpers.R`; `validate_table_input()` wraps it plus `pip_id` checks, `validate_cell_definition_input()` wraps it plus `filter_base`/`welfare_type` checks. A regression test asserts `validate_table_input()`'s errors and returned values are byte-for-byte unchanged after the extraction.
4. **No precedence rule for head-noun promotion** when two active slots both produce candidate head nouns. **Fixed**: explicit precedence order **column > row > superColumn > superRow** (innermost-to-outermost, matching the existing Approach C layout convention in `docs/ui-description.md`). Only the first active slot in that order supplies the head noun; all other active slots become trailing descriptive phrases. Covered by a new test with two simultaneously active noun-producing slots.
5. **R1 overclaimed "100% coverage of `tm_variable_spec.yaml`"** when most category data isn't in that file. **Fixed**: R1 is rescoped to variable-level coverage of `tm_variable_spec.yaml` plus category-level coverage of the new fixture registry snapshot; the runtime fallback is the documented gap-filler for anything the fixture doesn't cover.

Both P3 observations are also folded in: `yaml` stays in `Suggests` (guarded by `requireNamespace()`, consistent with the charter's CRAN-readiness priority) rather than moving to `Imports`; and the "NR" suppression note's trigger condition is cross-checked against `R/table_response.R` with a parity test rather than re-derived independently.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | Create `inst/extdata/tm_cell_prose.yaml` with natural-English prose metadata for every **variable name** in `tm_variable_spec.yaml` (not every category — categories are release-dependent, see R2), every binary target variable (`predicate` + `rate_noun`), and all 23 measures in `tm_measure_spec.yaml`, without modifying `tm_variable_spec.yaml`, `tm_measure_spec.yaml`, or `build_variable_registry()`. | Brainstorm Decision §1; revised per P2.2 |
| R2 | Create `tests/testthat/fixtures/registry-cell-prose-sample.json`, a hand-authored fixture mirroring the shape `piptm_variable_registry()` produces for a representative release, covering at least one admitted category set per `filter`/`covariate` variable. Author `tm_cell_prose.yaml` category-level entries against this fixture's codes. | New, resolves P1.1/P2.2 |
| R3 | Guard `tm_cell_prose.yaml` against drift with (a) a `testthat` sync test verifying every variable name and every binary target in `tm_variable_spec.yaml`/`tm_measure_spec.yaml` is covered, and every category code in the R2 fixture is covered, and (b) a runtime fallback to the raw category code (title-cased) when a category entry is absent at runtime. | Brainstorm Decision §1; revised per P1.1 |
| R4 | Implement exported pure function `build_cell_explanation()` in `R/cell_explanation.R` taking `analysis_var`, `measures`, `poverty_line`, `column`, `row`, `superColumn`, `superRow`, `filter_base`, `ppp`, `pop_share_threshold`, `welfare_type`, and `release`, returning `{schema_version, who_is_in_table, what_a_cell_is, what_is_shown, notes, warnings}` without calling `table_maker()` or accessing Arrow microdata. | Brainstorm Requirements & Decision §2–3 |
| R5 | Resolve the variable registry via `tryCatch(piptm_variable_registry(release), error = function(e) NULL)`. When `NULL`, every category/slot label degrades to the raw code (title-cased, e.g. `"25-64"` stays `"25-64"`, `"1"` becomes `"Category 1"`), and `warnings` gains `"Variable registry unavailable for release <release>; using fallback labels."`. The function never errors due to a missing registry. | New, resolves P1.2 |
| R6 | Implement head-noun promotion with an explicit precedence order **column > row > superColumn > superRow**: only the first active slot in that order supplies the head noun (e.g. `gender` → `"women"` / `"men"`); every other active slot (noun-producing or not) is appended as a trailing phrase in display order. Implement the short-vs-long filter length rule (short filter = 1 variable with $\le 2$ categories woven into the cell group phrase; long filter = 2+ variables or 3+ categories kept in `who_is_in_table` and referenced as `"everyone in the table (the filtered group above)"`). | Brainstorm Decision §1; precedence rule new, resolves P2.1 |
| R7 | For share measures (`pop_share`, `target_within_group_share`, `target_survey_share`), populate `counts` ($C$ or $C \cap Y=1$), `out_of` ($T$ or $C$), `text`, and `total_note` (what all cells sum to, including the single-cell `by = NULL` edge case). For non-share measures, set `counts = NULL`, `out_of = NULL`, `total_note = NULL`. | Brainstorm Decision §2–3 |
| R8 | Handle special cases in `build_cell_explanation()`: (a) `pov_status` in a layout slot with a non-poverty measure adds the explanatory note that the poverty line only splits the rows/columns; (b) filter variable also used as a layout slot adds a note that excluded categories will be empty; (c) active layout slots with non-NULL `pop_share_threshold` adds the `"NR"` suppression note, with the trigger condition cross-checked against `R/table_response.R`'s actual suppression logic (not re-derived independently); (d) PPP units (`(2021 PPP dollars)`) appear only on monetary welfare measures, never on `age`, `educy`, `hsize`, or shares. | Brainstorm Decision-to-text map & Examples 1–5; (c) parity requirement new, resolves P3.2 |
| R9 | Extract a shared internal helper `.validate_table_shape(analysis_var, measures, poverty_line, column, row, superColumn, superRow, ppp, pop_share_threshold)` (no `pip_id`, no `filter_base`) in `inst/plumber/helpers.R`. Reimplement `validate_table_input()` to wrap it plus existing `pip_id` checks, with a regression test proving its errors/values are byte-for-byte unchanged. Implement `validate_cell_definition_input()` wrapping the same helper plus `filter_base` validation (nested JSON object or JSON string), optional `welfare_type` (`NULL`/`"INC"`/`"CON"`, case-insensitive), and a rule that `target_within_group_share`/`target_survey_share` require a binary target `analysis_var` (not `"welfare"` or `"pov_status"`). | Brainstorm Decision §3; extraction design new, resolves P1.3 |
| R10 | Add `POST /cell-definition` in `inst/plumber/plumber.R` returning the standard JSON envelope (`status`, `data`, `warnings`, `errors`, `meta`), HTTP 400 on validation errors, and HTTP 422 on unknown release or domain errors. | Brainstorm Decision §3 |
| R11 | Document `POST /cell-definition` in `docs/api-contract.md` (Step 2 UI mapping + endpoint reference + response schema) and `SWAGGER_GUIDE.md`. | Brainstorm Constraints & Next Steps |

## Phase 1: Prose Specification, Fixture Registry, and Pure Builder

### 1. Create `tests/testthat/fixtures/registry-cell-prose-sample.json`
- **Requirements**: R2
- **Files**: `tests/testthat/fixtures/registry-cell-prose-sample.json` (new)
- **Details**:
  - Hand-author a fixture JSON mirroring the per-variable shape `piptm_variable_registry()` produces (`varname`, `ui_label`, `tm_type`, `roles`, `stat_groups`, `n_categories`, `categories: [{code, label}]`), covering every `filter`/`covariate` variable named in `tm_variable_spec.yaml` with a representative, documented category set (e.g. `gender`: codes `"0"`/`"1"` → `Female`/`Male`; `area`: `"0"`/`"1"` → `Urban`/`Rural`; `educat4`: `"1"`–`"4"`; `age_group`: the inline `"0-14"`/`"15-24"`/`"25-64"`/`"65+"` codes already in the spec; `wquintile`: `"1"`–`"5"`; `hsize_group`: the inline codes already in the spec; `pov_status`: `"0"`/`"1"` → `Non-poor`/`Poor`).
  - Add a code comment at the top of the fixture stating: "This fixture is a hand-authored snapshot of the category-coding scheme `build_variable_registry()` currently produces via pipdata's `recode_spec` for admitted `piptm.optional_vars`. If pipdata changes a category coding scheme for an admitted variable, update this fixture and `tm_cell_prose.yaml` together — see Risk 5 in this plan."
- **Test Scenarios**:
  - Happy path: fixture JSON parses with `jsonlite::read_json()` and contains `categories` for every `filter`/`covariate` variable name in `tm_variable_spec.yaml`.
- **Tests**: covered by `tests/testthat/test-cell-prose-sync.R` (Step 2)
- **Acceptance criteria**: fixture parses and the Step 2 sync test's fixture-coverage assertion passes.

### 2. Create `inst/extdata/tm_cell_prose.yaml` and the package-level cache loader
- **Requirements**: R1, R3
- **Files**:
  - `inst/extdata/tm_cell_prose.yaml` (new)
  - `R/zzz.R` (initialize `.piptm_env$cell_prose <- NULL`; no `DESCRIPTION` change — `yaml` stays in `Suggests`)
  - `R/cell_explanation.R` (new — `.get_cell_prose()` helper)
- **Details**:
  - `yaml` remains in `Suggests`. `.get_cell_prose()` guards with `requireNamespace("yaml", quietly = TRUE)` and `cli::cli_abort()` with an install hint if missing, matching the existing `build_variable_registry()` pattern (`R/registry.R:285-292`).
  - Author `inst/extdata/tm_cell_prose.yaml` with four top-level keys:
    1. `schema_version: "1.0"`
    2. `analysis_vars`: noun phrases and monetary flags for continuous/special analysis variables (`welfare` → `"daily income"` / `"daily consumption"` / `"daily welfare"` depending on `welfare_type`, `monetary: true`; `age` → `"age in years"`, `monetary: false`; `educy` → `"years of education"`, `monetary: false`; `hsize` → `"household size"`, `monetary: false`; `pov_status` → `"poverty status"`, `monetary: true`).
    3. `variables`: every `filter`/`covariate` variable name in `tm_variable_spec.yaml`, with `slot_noun` (e.g. `"education level"`, `"gender"`, `"area"`, `"age group"`, `"welfare quintile"`, `"household size"`, `"labor status"`, `"employment status"`, `"industry"`) and per-category entries keyed by the codes present in `registry-cell-prose-sample.json` (Step 1): `noun` (optional head noun, e.g. `"women"`, `"men"`, `"poor people"`, `"non-poor people"`) and `phrase` (trailing phrase, e.g. `"in rural areas"`, `"with no schooling"`, `"aged 25–64"`, `"in the poorest 20% (Q1)"`). Category keys are matched case-insensitively by code first, then by normalized label.
    4. `targets`: all 21 binary `shares`-only analysis variables (`female`, `male`, `urban`, `rural`, `youth`, `prime_work_age`, `elderly`, `employed`, `unemployed`, `in_labor_force`, `self_emp`, `paid_emp`, `inf_emp`, `employer`, `emp_agri`, `emp_indu`, `emp_serv`, `emp_others`, `imp_wat_rec`, `imp_san_rec`, `electricity`), each with `predicate` (e.g. `"are employed"`) and `rate_noun` (e.g. `"employment rate"`).
    5. `measures`: templates for all 23 measures in `tm_measure_spec.yaml`.
  - Implement `.get_cell_prose()` to lazily load and cache `inst/extdata/tm_cell_prose.yaml` in `.piptm_env$cell_prose`, re-parsing only when the cache is `NULL`.
- **Test Scenarios**:
  - Happy path: `.get_cell_prose()` parses and returns non-empty `analysis_vars`, `variables`, `targets`, `measures`; a second call returns the cached object (no re-parse — verify via a call counter mock on `yaml::read_yaml`).
  - Sync check (variable-level): every `filter`/`covariate` variable name in `tm_variable_spec.yaml`, every binary target (`stat_groups: [shares]`), and every measure in `tm_measure_spec.yaml` has a corresponding entry in `tm_cell_prose.yaml`.
  - Sync check (category-level, fixture-scoped): every category `code` present in `registry-cell-prose-sample.json` for each `filter`/`covariate` variable has a corresponding category entry in `tm_cell_prose.yaml`.
- **Tests**: `tests/testthat/test-cell-prose-sync.R`
- **Acceptance criteria**: `test-cell-prose-sync.R` passes with 0 failures; `git diff inst/extdata/tm_variable_spec.yaml inst/extdata/tm_measure_spec.yaml R/registry.R DESCRIPTION` is empty.

### 3. Implement `build_cell_explanation()` in `R/cell_explanation.R`
- **Requirements**: R3, R4, R5, R6, R7, R8
- **Files**:
  - `R/cell_explanation.R`
  - `NAMESPACE` (export `build_cell_explanation`)
  - `man/build_cell_explanation.Rd` (via `roxygen2::roxygenise()` or a targeted Rd file)
- **Details**:
  - Signature:
    ```r
    build_cell_explanation <- function(analysis_var,
                                       measures,
                                       poverty_line = NULL,
                                       column = NULL,
                                       row = NULL,
                                       superColumn = NULL,
                                       superRow = NULL,
                                       filter_base = NULL,
                                       ppp = 2021L,
                                       pop_share_threshold = 0.001,
                                       welfare_type = NULL,
                                       release = NULL)
    ```
  - Resolve `release` via `piptm_current_release()` when `NULL`. Resolve the registry via `reg <- tryCatch(piptm_variable_registry(release), error = function(e) NULL)` (R5) — **never** attempt to read `tm_variable_spec.yaml`/`tm_measure_spec.yaml` directly as a category source; those files do not carry category codes for most variables. When `reg` is `NULL`, every category/slot label lookup degrades to the raw code (title-cased via `.fallback_label(code)`, e.g. `"urban"` → `"Urban"`, `"25-64"` stays `"25-64"`), and the return value's `warnings` gains `"Variable registry unavailable for release <release or 'unresolved'>; using fallback labels."`.
  - Construct **Section 1 (`who_is_in_table`)**:
    - `title = "Who is in the table"`, `filtered = length(filter_base) > 0L`.
    - When unfiltered: `text = "Everyone in the survey. No filter is applied."`, with `" That group is the 100% for the table."` appended when any requested measure is a share.
    - When filtered: compose the full natural-English filter description from `tm_cell_prose.yaml` category phrases (falling back to `.fallback_label()` per category when `reg` is `NULL` or a code is unmapped). Append `"Anyone outside this group is excluded before the table is built."`, or the share-specific sentence above when all requested measures are shares.
  - Apply the **filter length rule (R4)**:
    - `is_short_filter <- length(filter_base) == 1L && length(filter_base[[1L]]) <= 2L`.
    - Short: weave the filter phrase into the example cell phrase ($C$) and the short table-population phrase ($T$, e.g. `"all adults aged 20–65 in the survey"`).
    - Long: example cell phrase uses only active slot categories; `what_a_cell_is$example_text` appends `"— within the filtered group above"`; share denominators ($T$) use `"everyone in the table (the filtered group above)"`.
  - Construct **Section 2 (`what_a_cell_is`)**:
    - Collect active slots with human role labels: `column` → `"column"`, `row` → `"row"`, `superColumn` → `"column group"`, `superRow` → `"row group"`.
    - **Head-noun precedence (R4)**: iterate active slots in the order `column, row, superColumn, superRow`; the **first** active slot that has a `noun` entry for its first category (deterministically, the fixture/registry's first listed category) supplies the head noun; every other active slot (regardless of whether it also has a `noun` entry) is appended as a trailing `phrase`. If no active slot has a `noun` entry, the head noun defaults to `"people"`.
    - When no slots are active (`by = NULL`): `dimensions = list()`, `structure_text = sprintf("No row or column variables are selected, so the table has a single cell covering %s.", table_pop_short)`, `example_cell_label = table_pop_short`, `example_text = NULL`.
  - Construct **Section 3 (`what_is_shown`)**:
    - One entry per measure in `measures` order: `{key, label, counts, out_of, text, total_note}`.
    - Non-share measures: `counts = NULL`, `out_of = NULL`, `total_note = NULL`; `text` renders the measure template with the concrete cell group, poverty line (`$2.15 a day (2021 PPP)`), and PPP units (`(2021 PPP dollars)`) only when `analysis_var`'s `monetary` flag in `tm_cell_prose.yaml$analysis_vars` is `true`.
    - Share measures: populate `counts`, `out_of`, `text` (`"Out of <out_of>, the percentage who <counts_clause>."`), and `total_note` per the $T$/$C$/$C \cap Y=1$ table in the brainstorm, including the `by = NULL` single-cell collapse case.
    - Set `intro` to the two-100%-totals sentence when 2+ share measures are requested; `NULL` otherwise.
  - Construct **`notes`** (in this order when applicable):
    - `pov_status` in a layout slot with a non-poverty measure present: note that the line only splits the slot and does not affect that measure's calculation.
    - Overlap between `names(filter_base)` and active slot variables: note that excluded categories will be empty.
    - Suppression note, gated on **the same condition `R/table_response.R` uses to apply `"NR"` masking** (cross-checked in Step 4, not re-derived here): at least 1 active slot and non-NULL `pop_share_threshold`.
- **Test Scenarios**:
  - Happy paths: all 5 brainstorm examples produce the expected section titles, structure text, example cell labels, measure texts, `counts`/`out_of`/`total_note`, and notes — using the R2 fixture registry loaded into `.piptm_env$registries` for the test's duration.
  - Head-noun precedence: a configuration with **two simultaneously active noun-producing slots** (e.g. `column = "gender"`, `row = "pov_status"`) asserts `gender`'s noun wins and `pov_status`'s category appears only as a trailing phrase.
  - No-registry fallback: with `.piptm_env$registries` empty/unset, `build_cell_explanation()` returns a structurally valid response (no error), category labels are the raw codes title-cased, and `warnings` contains the fallback message.
  - Short-vs-long filter length rule; non-monetary analysis variable (`age`, `educy`, `hsize`) omits PPP dollars; `welfare_type = "INC"` vs `"CON"` vs `NULL`; zero Arrow/microdata access verified by running with `PIPTM_ARROW_ROOT` unset.
- **Tests**: `tests/testthat/test-cell-explanation.R`
- **Acceptance criteria**: all unit tests in `tests/testthat/test-cell-explanation.R` pass.

## Phase 2: Validator Extraction, Plumber Endpoint, Documentation, and API Tests

### 4. Extract `.validate_table_shape()` and add `validate_cell_definition_input()`
- **Requirements**: R8, R9
- **Files**: `inst/plumber/helpers.R`
- **Details**:
  - Extract `.validate_table_shape(analysis_var, measures, poverty_line, column, row, superColumn, superRow, ppp, pop_share_threshold)` from the body of `validate_table_input()` (`inst/plumber/helpers.R:290` onward), covering every check except `pip_id` and returning `{valid, errors, poverty_line, ppp, pop_share_threshold, by, slots}` — identical return shape to today's `validate_table_input()` minus the `pip_id`-specific error branch.
  - Reimplement `validate_table_input()` to call `.validate_table_shape()` then prepend/append the existing `pip_id` validation block (`inst/plumber/helpers.R:316-345`), merging errors in the same order as today so error message ordering is unchanged.
  - Implement `validate_cell_definition_input(body)` calling `.validate_table_shape()` with the body's fields, then adding: `filter_base` validation (accepts a parsed JSON list/object in the POST body, or a JSON string parsed with `jsonlite::fromJSON()`; names must be in `piptm::pip_valid_dimensions()` or `piptm_filter_categories()`'s varname set, matching `/table`'s existing filter semantics); optional `welfare_type` (`NULL`/`"INC"`/`"CON"`, case-insensitive, trimmed); and a check that if `measures` includes `target_within_group_share` or `target_survey_share`, `analysis_var` is not `"welfare"` or `"pov_status"` (HTTP 400 otherwise, consistent with `R/compute_shares.R`'s requirement that target shares need a binary target variable).
- **Test Scenarios**:
  - Regression: for a representative set of existing `/table` request fixtures (valid and invalid), `validate_table_input()`'s `errors`/`valid`/coerced-value output is identical before and after the extraction (byte-for-byte string comparison).
  - Happy path: `validate_cell_definition_input()` accepts a config with no `pip_id` and a nested-object `filter_base`.
  - Error paths: `filter_base` as malformed JSON string; unknown `welfare_type` value; `target_survey_share` requested with `analysis_var = "welfare"`.
- **Tests**: `tests/testthat/test-api-helpers.R` (regression additions), new cases in `tests/testthat/test-api-cell-definition.R`
- **Acceptance criteria**: all `test-api-helpers.R` tests (existing + new regression cases) pass; `validate_cell_definition_input()` unit tests pass.

### 5. Add `POST /cell-definition`
- **Requirements**: R10
- **Files**: `inst/plumber/plumber.R`
- **Details**:
  - Add `#* @post /cell-definition` with `@parser json` and `@serializer json list(na = "null")` (matching the router's existing JSON envelope convention):
    - Parse body (with the same raw `req$postBody` / `req$rook.input` fallback used by `/description`).
    - Run `validate_cell_definition_input(body)` → `api_error(check$errors, 400L, res)` on failure.
    - Inside `capture_with_warnings()`, resolve release (`resolve_release(body$release)` when provided; otherwise `NULL`, letting `build_cell_explanation()` resolve the current release or degrade per R5) and call `piptm::build_cell_explanation(...)`.
    - Return `api_error(out$error$message, 422L, res)` if `out$error` is non-NULL; otherwise `api_response(out$result, warnings = out$warnings, meta = list(release = rel))`.
- **Test Scenarios**:
  - Happy path: `POST /cell-definition` with a JSON body for welfare summary stats, poverty measures, and share measures returns HTTP 200 with `status = "success"` and `data` containing `who_is_in_table`, `what_a_cell_is`, `what_is_shown`, and `notes`.
  - Edge cases: `filter_base` as a nested JSON object vs. JSON-encoded string; `by = NULL` (no slots); `welfare_type = "INC"` vs omitted; no release loaded (degraded-label response, HTTP 200, not an error).
  - Error paths: empty body (400); missing `analysis_var`/`measures` (400); slot gap (400); duplicate variable across slots (400); `analysis_var` also used in a slot (400); missing `poverty_line` when `headcount`/`pov_status` requested (400); target share with `analysis_var = "welfare"` (400); unknown `release` (422).
- **Tests**: `tests/testthat/test-api-cell-definition.R`
- **Acceptance criteria**: all tests pass using the socket-free `pr$call()` test harness.

### 6. Cross-check suppression-note parity and update API contract + Swagger documentation
- **Requirements**: R8, R11
- **Files**: `docs/api-contract.md`, `SWAGGER_GUIDE.md`
- **Details**:
  - Read `R/table_response.R`'s `"NR"` masking condition and confirm (or correct) that `build_cell_explanation()`'s suppression-note trigger (≥1 active slot + non-NULL `pop_share_threshold`) matches it exactly; add a unit test asserting this parity directly rather than only visually comparing code.
  - In `docs/api-contract.md`: add `/cell-definition` (POST) to the **Step 2 — Configure measures & cuts** row of the `UI Step Mapping` table; add a `### /cell-definition — POST (Step 2)` section documenting purpose, request body parameters, validation rules (400/422), and the full JSON response schema (`who_is_in_table`, `what_a_cell_is`, `what_is_shown`, `notes`, `warnings`).
  - In `SWAGGER_GUIDE.md`: document `POST /cell-definition` with a copy-pasteable request body example.
- **Test Scenarios**: suppression parity test; verification that `docs/api-contract.md`/`SWAGGER_GUIDE.md` contain the route documentation and schema fields; existing `/description` and `/table` test suites pass without regression.
- **Tests**: new suppression parity test in `tests/testthat/test-cell-explanation.R`; `tests/testthat/test-api-cell-definition.R`, `tests/testthat/test-cell-definition.R`, `tests/testthat/test-api-description.R`
- **Acceptance criteria**: V3, V4, and V5 pass.

## Testing Strategy

- **Fixture registry (`tests/testthat/fixtures/registry-cell-prose-sample.json`)**: single source of truth for category-level sync testing and for `build_cell_explanation()`'s "real labels" test paths.
- **Spec sync test (`tests/testthat/test-cell-prose-sync.R`)**: variable/target/measure-name coverage against `tm_variable_spec.yaml`/`tm_measure_spec.yaml`; category-code coverage against the fixture registry.
- **Builder unit tests (`tests/testthat/test-cell-explanation.R`)**: 5 approved examples, head-noun precedence with 2 active noun-producing slots, explicit no-registry fallback, short-vs-long filter rule, `counts`/`out_of`/`total_note` on all three share measures, `pov_status`-in-slot note, filter-slot overlap note, suppression note with parity check against `R/table_response.R`, non-monetary PPP omission, zero Arrow/microdata access.
- **Validator regression + unit tests (`tests/testthat/test-api-helpers.R`, `tests/testthat/test-api-cell-definition.R`)**: `.validate_table_shape()` extraction proven non-regressive for `validate_table_input()`; `validate_cell_definition_input()` happy/error paths.
- **Plumber route tests (`tests/testthat/test-api-cell-definition.R`)**: socket-free `pr$call()` tests covering HTTP 200/400/422.
- **Regression suite**: `test-cell-definition.R` and `test-api-description.R` confirm zero regressions in existing endpoints.

## Documentation Checklist

- [ ] `R/cell_explanation.R` roxygen2 documentation + `NAMESPACE` export + `man/build_cell_explanation.Rd`
- [ ] `inst/plumber/plumber.R` roxygen/plumber annotations for `POST /cell-definition`
- [ ] `docs/api-contract.md` Step 2 mapping and endpoint specification
- [ ] `SWAGGER_GUIDE.md` example request payload for `POST /cell-definition`
- [ ] Fixture registry header comment explaining its maintenance obligation (Step 1)

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Drift between `tm_variable_spec.yaml`/`tm_measure_spec.yaml` and `tm_cell_prose.yaml` when variables or measures are added later | Missing prose entry at runtime | Automated `test-cell-prose-sync.R` fails `devtools::test()` on any missing key; runtime fallback to raw-code labels ensures production never errors |
| `registry-cell-prose-sample.json` drifts from the real `recode_spec`-derived category coding scheme over time | Stale fixture produces stale sync-test confidence while real registries use different codes | Header comment states the maintenance obligation explicitly; this is accepted as a known, documented limitation rather than solved automatically — flagged as a roadmap follow-up idea (fixture-vs-live-registry drift detection) |
| No release registry loaded when `build_cell_explanation()` runs (unit tests, missing `PIPTM_REGISTRY_DIR`) | Category/code lookups could fail or abort | `tryCatch()` around `piptm_variable_registry()` per the existing codebase pattern; explicit degrade-to-raw-code contract, tested directly (R5) |
| Category codes in live release registries may be integer strings, range strings, or labels depending on `pipdata` recode type | Category prose lookup misses and falls back to generic phrasing | Dual lookup by code and normalized label in `tm_cell_prose.yaml`; tested against the fixture in both forms |
| Multi-variable or multi-category `filter_base` produces run-on sentences | Poor readability on complex configurations | Short-vs-long filter length rule with dedicated unit tests |
| `.validate_table_shape()` extraction silently changes `/table`'s existing validation behavior | Regression in a production endpoint with an existing large test suite | Byte-for-byte regression test on `validate_table_input()` output before merging the extraction |
| Two active slots both produce head-noun candidates | Grammatically inconsistent or ambiguous example-cell prose | Explicit column > row > superColumn > superRow precedence rule, tested directly |
| Modifying `build_cell_definition()` prematurely would break 1,075 lines of existing `/description` tests before the Phase 2 metadata rework | Unnecessary churn and test breakage | `build_cell_definition()` and `/description` remain untouched in this plan; the overlapping section is removed in the follow-up metadata rework plan |

## Out of Scope

- Reworking `/description` into a downloadable metadata document and adding `POST /table/download/metadata` (deferred to the follow-up Phase 2 metadata plan)
- Adding row-percentage or column-percentage share measures to `R/compute_shares.R`
- Modifying `/table`, `/table/download`, or the Approach C response format
- Internationalization (i18n) beyond English
- Automated drift detection between `registry-cell-prose-sample.json` and live `recode_spec`-derived registries (tracked as a roadmap idea, not built here)

## Completion Contract

### Outcome
A new pure function `build_cell_explanation()` backed by `inst/extdata/tm_cell_prose.yaml` and a fixture registry, exposed at `POST /cell-definition`, returns the three-section cell explanation (`who_is_in_table`, `what_a_cell_is`, `what_is_shown`, `notes`) for any valid Page 2 configuration — with or without a loaded release registry — without calling `table_maker()` or touching Arrow microdata, with all unit, sync, fixture, and route tests passing, and with `validate_table_input()`'s existing behavior provably unchanged.

### Verification Surface
| ID | Phase | Evidence Required | Command/Artifact | Required |
|----|-------|-------------------|------------------|----------|
| V1 | 1 | `inst/extdata/tm_cell_prose.yaml` + `tests/testthat/fixtures/registry-cell-prose-sample.json` exist; sync test passes variable-level coverage against `tm_variable_spec.yaml`/`tm_measure_spec.yaml` and category-level coverage against the fixture | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-cell-prose-sync.R')"` | yes |
| V2 | 1 | `build_cell_explanation()` unit tests pass: 5 approved examples, head-noun precedence (2 active noun-producing slots), explicit no-registry fallback, short-vs-long filter rule, fixture-backed full-label path | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-cell-explanation.R')"` | yes |
| V3 | 2 | `POST /cell-definition` route tests pass (200/400/422); `.validate_table_shape()` extraction regression test confirms `validate_table_input()` output byte-for-byte unchanged | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-api-cell-definition.R'); testthat::test_file('tests/testthat/test-api-helpers.R')"` | yes |
| V4 | final | Existing `/description` and `/table` tests continue to pass without regression | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-cell-definition.R'); testthat::test_file('tests/testthat/test-api-description.R')"` | yes |
| V5 | final | `docs/api-contract.md`/`SWAGGER_GUIDE.md` document the route; suppression-note trigger condition parity with `R/table_response.R` is asserted by a test, not only inspected | `docs/api-contract.md`, `SWAGGER_GUIDE.md`, parity test | yes |

### Constraints
| ID | Phase | Constraint | Check |
|----|-------|------------|-------|
| C1 | 1 | `tm_variable_spec.yaml`, `tm_measure_spec.yaml`, `build_variable_registry()`, and `DESCRIPTION` must not be modified | `git diff inst/extdata/tm_variable_spec.yaml inst/extdata/tm_measure_spec.yaml R/registry.R DESCRIPTION` is empty |
| C2 | 1 | `build_cell_explanation()` must not call `table_maker()`, `load_surveys()`, or any Arrow/microdata reader, must emit no computed data values or sample counts, and must never error due to a missing/absent variable registry | Unit test running with `PIPTM_ARROW_ROOT` unset and `.piptm_env$registries` empty |
| C3 | 2 | Existing `/table`, `/table/download`, and `/description` endpoints and their response shapes must not regress; `validate_table_input()`'s output is byte-for-byte unchanged after the `.validate_table_shape()` extraction | V3 and V4 test runs pass |
| C4 | final | No new external packages added; `yaml` remains in `Suggests`, guarded by `requireNamespace()` | Inspect `DESCRIPTION` diff (expected empty) |

### Boundaries
- **Allowed**: Creating `inst/extdata/tm_cell_prose.yaml`, `tests/testthat/fixtures/registry-cell-prose-sample.json`, `R/cell_explanation.R`, `man/build_cell_explanation.Rd`, `tests/testthat/test-cell-prose-sync.R`, `tests/testthat/test-cell-explanation.R`, `tests/testthat/test-api-cell-definition.R`; updating `NAMESPACE`, `R/zzz.R`, `inst/plumber/helpers.R` (adding `.validate_table_shape()` and `validate_cell_definition_input()`, reimplementing `validate_table_input()` non-regressively), `inst/plumber/plumber.R`, `docs/api-contract.md`, `SWAGGER_GUIDE.md`, and `tests/testthat/test-api-helpers.R` (regression additions only).
- **Out of scope**: Reworking `/description` into a downloadable metadata document, adding `POST /table/download/metadata`, adding row/column share measures, modifying `/table` and `/table/download`, moving `yaml` to `Imports`, automated fixture-vs-live-registry drift detection.

### Iteration Policy
1. Implement Phase 1 (fixture registry → `tm_cell_prose.yaml` → `build_cell_explanation()` → unit/sync tests) and verify V1 and V2 before starting Phase 2.
2. Implement Phase 2 (`.validate_table_shape()` extraction → `POST /cell-definition` → suppression parity → docs → API tests) and verify V3, V4, and V5.
3. Follow `deviation-policy: ask` if any requirement or file boundary needs adjustment, especially if the `.validate_table_shape()` extraction reveals it cannot be made byte-for-byte non-regressive without a larger refactor than scoped here.

### Blocked-Stop Conditions
- Required verification cannot be run through `Rscript -e "testthat::test_file(...)"`.
- Any required evidence item (V1–V5) fails after allowed recovery attempts.
- The `.validate_table_shape()` extraction cannot be made byte-for-byte non-regressive for `validate_table_input()` and no revised plan is approved.
- A required deviation is discovered under policy `ask` and user approval is unavailable.
- `/cg-work` cannot durably create or update the execution report.
