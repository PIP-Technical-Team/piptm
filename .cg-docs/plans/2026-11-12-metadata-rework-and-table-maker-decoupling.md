---
date: 2026-11-12
title: "Table Metadata Rework and Decoupling from table_maker()"
status: completed
completed-date: 2026-11-12
scope: "Deep"
brainstorm: ".cg-docs/brainstorms/2026-11-12-metadata-rework-and-table-maker-decoupling.md"
language: "R"
estimated-effort: "medium"
deviation-policy: "ask"
phases: 3
completed-phases: [1, 2, 3]
execution-report: ".cg-docs/work-reports/2026-11-12-metadata-rework-and-table-maker-decoupling.md"
artifact-schema-version: 1
tags: [metadata, table-maker, api, endpoint, description-builder, registry, manifest, decoupling, breaking-change]
---

# Plan: Table Metadata Rework and Decoupling from `table_maker()`

## Objective

Make `table_maker()` compute tables and nothing else, and make `GET|POST /metadata`
a pure, fast (`< 5 ms`), zero-microdata document generator driven entirely by the
in-memory release manifest and variable registry. Remove the `include_metadata`
parameter from both the engine and `/table`, replace the legacy 9-section
description model with a streamlined 6-section table metadata document, and
deliver that document as a downloadable HTML or Markdown companion to
`POST /table/download`.

## Context

Table metadata is currently assembled *inside* the computation engine. Passing
`include_metadata = TRUE` to `table_maker()` (`R/table_maker.R:519`) activates
`.build_description_metadata()` (`R/table_maker.R:137-384`), a
`withCallingHandlers()` warning interceptor (`R/table_maker.R:1018-1031`), and
`excluded_surveys` / `suppressed_cells` trackers (`R/table_maker.R:707-715`,
`:774-782`, `:969-971`). The result is passed to `build_description_model()`
(`R/description_builder.R:35`) and rendered by `render_description_markdown()` /
`render_description_html()`.

Three problems follow from that design:

1. **Engine bloat and a dual return shape.** `table_maker()` returns a
   `data.table` normally but `list(data, description_metadata)` when
   `include_metadata = TRUE`. `/table` branches on that
   (`inst/plumber/plumber.R:854-877`) and, in the metadata case, **skips
   `serialize_table_response()` entirely**, returning flat long-format rows
   instead of the Approach C `{layout, cells}` payload.
2. **`/metadata` reads microdata it throws away.** Without a pre-built
   `description_metadata` object, `POST /metadata` calls
   `table_maker(..., include_metadata = TRUE)` (`inst/plumber/plumber.R:1101-1112`),
   performing a full Arrow read and statistical computation, then discards
   `tm_out$data` and renders only the metadata.
3. **Redundant, partly dead, and partly wrong content.** The model still builds a
   `cell_definition` section that the route explicitly hides
   (`inst/plumber/plumber.R:1045`) because `GET /cell-definition`
   (`R/cell_explanation.R`) now owns Page-2 prose — roughly 400 lines of
   `build_cell_definition()` and helpers in `R/description_builder.R:600-1035` are
   unreachable in production. The layout section also hardcodes every covariate's
   slot to `"rows"` (`R/table_maker.R:319`, `:327`, `:332`) because
   `table_maker()` only ever receives a flat `by` vector, so the document
   misreports which variable is a row versus a column.

The enabling discovery from research: **everything the metadata document needs is
already in memory before any microdata is touched.** `table_maker()`'s
included/excluded survey decision is computed purely from
`piptm_manifest(release)$dimensions` (`R/table_maker.R:683-799`), and all
variable, category, and measure metadata comes from the release registry via
`piptm_variable_registry()`, `piptm_analysis_variables()`,
`piptm_filter_categories()`, `piptm_layout_covariates()`, and
`piptm_stat_groups()` (`R/registry.R:476-649`). The only field that ever required
real computation was the exact count of `"NR"`-suppressed cells; the document will
state the suppression *rule* (`pop_share_threshold`) instead of a count.

### Prior work this builds on

- `.cg-docs/brainstorms/2026-11-12-metadata-rework-and-table-maker-decoupling.md`
  (decided, Approach 1) — source of record for this plan.
- `.cg-docs/brainstorms/2026-11-02-page2-cell-definition-endpoint.md` — deferred
  exactly this work to "Phase 2" and established that `/cell-definition` owns
  Page-2 prose.
- `.cg-docs/plans/2026-11-11-rename-description-to-metadata.md` (completed) —
  moved the route to `/metadata`; Brain finding from
  `.cg-docs/solutions/bugs/2026-08-31-swagger-req-parameter-exposed.md` applies
  here too: **OpenAPI path entries are hand-patched after `getApiSpec()`**
  (`inst/plumber/plumber.R:467-622`), so removing a parameter from the `#* @param`
  annotation alone leaves it advertised in Swagger.
- `.cg-docs/solutions/data-quality/2026-08-27-json-roundtrip-defensive-coding.md`
  — the defensive `is.data.frame()` / list-unwrapping coercions throughout
  `R/description_builder.R` exist solely because `description_metadata` arrived
  over JSON. Retiring that input mode removes the need for them.
- `.cg-docs/solutions/bugs/2026-10-13-hsize-group-filter-category-codes.md` and
  `.cg-docs/solutions/bugs/2026-09-14-ansi-escape-codes-in-description-warnings.md`
  — both were failures of metadata enrichment bolted onto the compute path.

### Charter alignment

Aligned with `compound-gpid.md`: scalability (manifest/registry lookup replaces a
full Arrow scan), determinism and reproducibility (same parameters plus release
always yield the same document), interoperability (`/table` gains a single stable
response shape), and no new dependencies.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | One shared internal helper (`.partition_surveys_by_dimensions()`) computes the two-stage sequential survey partition (`filter_vars` pre-filter followed by `by_check` pre-filter, including partial `pip_id` manifest match handling and early abort when all surveys fail `filter_vars`) from `piptm_manifest(release)$dimensions`; both `table_maker()` and `build_description_model()` call it, so they never disagree. | Research; C2; P3.1 |
| R2 | `build_description_model()` is a pure function of table parameters plus release: `pip_id`, `analysis_var`, `measures`, `poverty_line`, the four named slots (`column`, `row`, `superColumn`, `superRow`), `filter_base`, `ppp`, `pop_share_threshold`, `release`. It performs no Arrow/microdata I/O and never calls `table_maker()`. | Brainstorm §3; C1 |
| R3 | Because `/metadata` no longer calls `table_maker()`, `build_description_model()` runs the shared parameter-level domain validations (`.classify_measures()`, `.validate_poverty_lines()`, `.validate_by(..., release = release)`, and the `filter_base` schema/variable/category-code validation from `R/table_maker.R:540-630`) using `piptm_abort(..., user_message = ...)` so invalid inputs produce the same user-facing errors as `table_maker()`. | Plan review P1.3 |
| R4 | The model has exactly 6 sections: `overview`, `provenance`, `surveys_selected`, `filters_applied` (conditional), `layout_configuration` (conditional), `statistics_selected`. | Brainstorm §2 |
| R5 | `overview` states the statistic count, included-survey count, active disaggregation roles (`structure_description`), the sampling-weight note (`weighting_note`), the PPP note when applicable (`ppp_note`), and the `"NR"` suppression rule derived from `pop_share_threshold` (`suppression_note`). No computed cell counts. | Brainstorm §2.1; P2.1 |
| R6 | `provenance` reports `release_id`, `ppp_year`, `poverty_line` (formatted string when applicable, omitted/NULL otherwise), and `generated_at` (UTC timestamp). | Brainstorm §2.2; P2.1 |
| R7 | `surveys_selected` lists included surveys with `pip_id`, `country_code`, `country_label` (`country_name` from manifest), `year`, `welfare_type_label` (`Income`/`Consumption`), and `version`. The excluded-survey table (`pip_id`, `reason` with the redundant `<pip_id>: ` prefix stripped) renders only when at least one requested survey is dropped. | Brainstorm §2.3; P3.1 |
| R8 | `filters_applied` resolves active `filter_base` variables through `piptm_filter_categories(release)` and reports `varname`, `variable` (`label`), and `selected_categories` (comma-separated **category labels** — never raw internal codes). | Brainstorm §2.4; user caveat 1 |
| R9 | `layout_configuration` reports each active slot in visual-hierarchy order (`superRow` → `row` → `superColumn` → `column`) by its true role (`Super-rows`, `Rows`, `Super-columns`, `Columns`) with `varname` matching the `table.csv` column header, `ui_label`, and `n_categories` (`"<count>: <label_1>, <label_2>, ..."`). `format_slot_label()` supports both singular camelCase and plural keys. | Brainstorm §2.5; P2.1 |
| R10 | `statistics_selected` reports the analysis variable (`analysis_var_name`, `analysis_var_label`, `analysis_var_type`) via `piptm_analysis_variables(release)`, `poverty_line`, and the selected measures table (`measure` key matching the `table.csv` column header, `measure_label`, `stat_group` / `group_label`) via `piptm_stat_groups(release)`, in request order. | Brainstorm §2.6; P2.1 |
| R11 | `build_cell_definition()` and its helpers, `.build_execution_content()`, and the `warnings` section are removed from `R/description_builder.R` and both renderers. | Brainstorm §1; research |
| R12 | Both renderers (`R/description_renderer.R`, `R/description_renderer_html.R`) emit all 6 sections in parity: the `statistics_selected` skip guard on `analysis_var_*` is removed, `.display_label_markdown()` / `.display_label_html()` and `.display_table_header_markdown()` / `.display_table_header_html()` map all new section keys and table columns, and HTML stays inline-styled with escaped dynamic content. | Brainstorm §Decision; P2.1 |
| R13 | `GET /metadata` and `POST /metadata` accept the same table parameters as `/table` plus `format` (`"html"` or `"markdown"`, default `"html"`), validated by `validate_table_input()` plus `format` validation. | Brainstorm §4 |
| R14 | The legacy `description_metadata` request mode, `validate_description_input()`, and `.PIPTM_DESCRIPTION_METADATA_USER_ERROR` (`R/errors.R`) are removed. No compatibility alias. | Brainstorm §4; C4; P3.1 |
| R15 | `/metadata` sets `Content-Disposition: attachment; filename="table-metadata.html"` (or `"table-metadata.md"` for Markdown) for file-download delivery, preserving `text/html; charset=utf-8` for HTML and `text/plain` for Markdown. | Brainstorm §4; user answer Q1 |
| R16 | `include_metadata` is removed from `/table`'s signature, `#* @param` annotation, parameter-coercion branch (`inst/plumber/plumber.R:242`), root-doc prose, and every post-`getApiSpec()` OpenAPI patch. `enrich_openapi_spec()` and `tests/testthat/test-api-endpoints.R` are updated for `GET|POST /metadata` (`required_by_path`, `enrich_param()`, `400`/`422` responses, `required_map`, `server_default_params`, non-WIP summary). `/table` always returns Approach C. | Brainstorm §3; P1.2 |
| R17 | `include_metadata`, `.build_description_metadata()`, `withCallingHandlers()` warning capture, and `excluded_surveys` / `suppressed_cells` metadata trackers are removed from `R/table_maker.R`. `table_maker()` always returns a `data.table`, and its exclusion warnings and abort messages are unchanged. | Brainstorm §3; C5 |
| R18 | Live documentation (`README.md`, `SWAGGER_GUIDE.md`, `docs/api-contract.md`, `docs/metadata-endpoint-spec.md`, `R/api.R`, `NEWS.md`, regenerated `man/`) no longer advertises `include_metadata` or the `description_metadata` request mode, and documents the new `GET|POST /metadata` contract. | Brainstorm §Next Steps |
| R19 | `GET /cell-definition`, `R/cell_explanation.R`, and `inst/extdata/tm_cell_prose.yaml` are untouched; the metadata document contains no Page-2 cell-reading prose. | Brainstorm §1; C3 |

## Implementation Steps

## Phase 1: Pure metadata builder and renderers

### 1. Extract shared domain-validation and two-stage survey-partition helpers

- **Requirements**: R1, R3, R17
- **Files**: `R/table_maker.R`, `tests/testthat/test-table-maker-v2.R`
- **Details**:
  - Extract a shared parameter-validation helper (e.g.
    `.validate_and_normalize_table_params(pip_id, analysis_var, measures, poverty_line, by, filter_base, ppp, pop_share_threshold, release)`)
     encapsulating `R/table_maker.R:526-637` (`.classify_measures()`,
    `.validate_poverty_lines()`, `.validate_by(..., release = release)`, and the
    `filter_base` schema/variable/category-code validation and normalization) using
    `piptm_abort(..., user_message = ...)` so both `table_maker()` and
    `build_description_model()` enforce identical domain rules (P1.3).
  - Extract `.partition_surveys_by_dimensions(release, pip_id, normalized_filter_base, filter_vars, by, warn = TRUE)`
    encapsulating `R/table_maker.R:641-803` (P3.1):
    1. Resolve `release` and match `pip_id` against `piptm_manifest(release)`
       (aborting when zero match; warning and continuing when a subset of
       requested `pip_id`s are absent from the manifest, matching `:653-672`).
    2. Stage 1 (`filter_vars` pre-filter, `:684-735`): drop surveys lacking all
       `filter_vars`, emit the existing `cli_warn()` with `<pip_id>: ...` bullets
       when `warn = TRUE`, and abort immediately via
       `.piptm_covariate_exclusion_user_error(filter_vars, release)` if zero
       surveys remain.
    3. Stage 2 (`by_check` pre-filter excluding `"pov_status"`, `:737-803`): on
       surviving entries, drop surveys lacking all `by_check` dimensions, emit the
       existing `cli_warn()` when `warn = TRUE`, and abort if zero surveys remain.
    4. Return `list(release = release, included = entries, excluded = data.table(pip_id, reason))`
       where `excluded$reason` stores the clean reason without the redundant
       `<pip_id>: ` prefix (e.g. `"missing educat4"` or `"no requested dimensions"`),
       while `cli_warn()` continues to format `paste0(pip_id, ": ", reason)` so
       existing warning strings stay byte-identical.
  - Do not yet remove `include_metadata` from `table_maker()` (that is Step 7).
- **Test Scenarios**:
  - Happy path: surveys carrying all requested `filter_base` and `by` dimensions
    are returned in `included` with an empty `excluded` table.
  - Edge case: `pov_status` in `by` does not cause exclusion; partial `pip_id`
    match warns and continues; Stage 1 exclusions precede Stage 2 exclusions;
    `excluded$reason` does not duplicate `pip_id`.
  - Error path: all surveys failing Stage 1 abort before Stage 2; all surveys
    failing Stage 2 abort with the unchanged user-facing message.
- **Tests**: `Rscript -e "pkgload::load_all(quiet=TRUE); testthat::test_file('tests/testthat/test-table-maker-v2.R')"`
- **Acceptance criteria**: `table_maker()`'s warnings, abort messages, and returned
  rows are unchanged; the domain-validation and survey-partition rules each exist
  in one shared helper.

### 2. Rewrite `build_description_model()` as a pure manifest + registry builder

- **Requirements**: R1, R2, R3, R4, R5, R6, R7, R8, R9, R10
- **Files**: `R/description_builder.R`, `tests/testthat/test-description-builder.R`,
  `tests/testthat/test-description-builder-schema-validation.R`,
  `tests/testthat/test-description-helpers.R`
- **Details**:
  - Replace the `(description_metadata, params)` signature of
    `build_description_model()` with table parameters:
    `build_description_model(pip_id, analysis_var = "welfare", measures = "mean", poverty_line = NULL, column = NULL, row = NULL, superColumn = NULL, superRow = NULL, filter_base = NULL, ppp = 2021L, pop_share_threshold = 0.001, release = NULL)`.
  - Derive `slots <- list(superRow = superRow, row = row, superColumn = superColumn, column = column)`
    (in visual-hierarchy order matching `flatten_table_response()`) and
    `by <- unname(unlist(slots))`.
  - Call the shared domain validator and `.partition_surveys_by_dimensions(..., warn = FALSE)`
    from Step 1 so invalid parameters abort with user-facing `piptm_abort()`
    messages (never raw `cli_abort()`, satisfying `test-error-coverage-audit.R` —
    P1.1, P1.3) and included/excluded surveys match `table_maker()` identically.
  - Update `format_slot_label()` (`R/description_builder.R:993-1001`) to map both
    singular parameter names (`superRow`, `row`, `superColumn`, `column`) and
    plural keys (`super_rows`/`superRows`, `rows`, `super_columns`/`superColumns`,
    `columns`) to `"Super-rows"`, `"Rows"`, `"Super-columns"`, `"Columns"` (P2.1).
  - Construct the 6 sections with exact keys:
    1. `overview`: `visible = TRUE`, `title = "Table Overview"`, `content = list(structure_description = ..., weighting_note = "All calculations use survey sampling weights.", ppp_note = <when monetary>, suppression_note = <when !is.null(pop_share_threshold) && !is.null(by)>)`.
    2. `provenance`: `visible = TRUE`, `title = "Data Provenance"`, `content = list(release_id = resolved_release, ppp_year = ppp, poverty_line = <formatted string when applicable>, generated_at = <UTC timestamp>)`.
    3. `surveys_selected`: `visible = TRUE`, `title = "Surveys Selected"`, `content = list(n_loaded = nrow(included), n_excluded = nrow(excluded), loaded_list = data.table(pip_id, country_code, country_label, surveyid_year = year, welfare_type_label, version), excluded_list = <data.table(pip_id, reason) when nrow(excluded) > 0 else NULL>)`.
    4. `filters_applied`: `visible = has_filters`, `title = "Sample Base Filters"`, `content = list(description = ..., filters = data.table(varname, variable = ui_label, selected_categories))` resolved via `piptm_filter_categories(resolved_release)` (labels only, no integer codes).
    5. `layout_configuration`: `visible = has_covariates`, `title = "Table Layout Configuration"`, `content = list(description = "Table dimensions are organized as follows:", layout = data.table(slot_label, varname, ui_label, n_categories))` resolved via `piptm_layout_covariates(resolved_release)` and `piptm_variable_registry(resolved_release)`.
    6. `statistics_selected`: `visible = TRUE`, `title = "Statistics Computed"`, `content = list(analysis_var_name = analysis_var, analysis_var_label = ..., analysis_var_type = ..., poverty_line = list(applicable = has_poverty, value = ..., ppp_year = ...), measures = data.table(measure, measure_label, stat_group))` resolved via `piptm_analysis_variables(resolved_release)` and `piptm_stat_groups(resolved_release)` in request order.
  - Rewrite `tests/testthat/test-description-builder-schema-validation.R` and
    `tests/testthat/test-description-helpers.R` to test the new pure builder
    helpers, registry fallbacks, category label formatting, and updated
    `format_slot_label()` mappings (P2.1, P2.2).
- **Test Scenarios**:
  - Happy path: full configuration (filter + all four slots + multiple measures)
    returns the 6 sections with truthful slot labels (`Super-rows`, `Rows`,
    `Super-columns`, `Columns`) and human-readable category labels.
  - Edge case: no `filter_base` hides `filters_applied`; no active slot hides
    `layout_configuration` and omits the suppression note; no excluded survey sets
    `excluded_list = NULL`; vector `poverty_line` formats cleanly; registry lookup
    failure degrades to raw keys.
  - Error path: unknown `pip_id`, invalid `filter_base` variable or category code,
    missing `poverty_line` for poverty measures, or all surveys excluded abort with
    user-facing `piptm_abort()` messages.
- **Tests**: `Rscript -e "pkgload::load_all(quiet=TRUE); testthat::test_file('tests/testthat/test-description-builder.R'); testthat::test_file('tests/testthat/test-description-builder-schema-validation.R'); testthat::test_file('tests/testthat/test-description-helpers.R')"`
- **Acceptance criteria**: V1 passes; all 3 test files pass; slot roles and
  included/excluded surveys match `table_maker()`.

### 3. Prove and enforce the zero-microdata property

- **Requirements**: R2, R19
- **Files**: `tests/testthat/test-description-builder.R`
- **Details**:
  - Add a guard test using `testthat::local_mocked_bindings()` to make
    `load_survey_microdata()` and `table_maker()` abort if reached, then assert
    `build_description_model()` completes successfully.
  - Add a static assertion scanning `R/description_builder.R` to verify it
    contains no `table_maker`, `load_survey_microdata`, or `arrow::` calls.
  - Assert that the returned model contains no `cell_definition`,
    `execution_summary`, or `warnings` keys.
- **Test Scenarios**:
  - Happy path: builder succeeds when data-loading functions are mocked to abort.
  - Edge case: static scan finds zero forbidden references.
  - Error path: N/A (guard test).
- **Tests**: `Rscript -e "pkgload::load_all(quiet=TRUE); testthat::test_file('tests/testthat/test-description-builder.R')"`
- **Acceptance criteria**: V2 passes.

### 4. Remove dead sections and update both renderers in parity

- **Requirements**: R11, R12
- **Files**: `R/description_builder.R`, `R/description_renderer.R`,
  `R/description_renderer_html.R`,
  `tests/testthat/test-description-renderer.R`,
  `tests/testthat/test-description-renderer-html.R`,
  `tests/testthat/test-cell-definition.R`,
  `tests/testthat/test-error-coverage-audit.R`
- **Details**:
  - Delete `build_cell_definition()` and its private helpers
    (`R/description_builder.R:600-1035`) and `.build_execution_content()` (`:545`).
    Run `devtools::document()` so `build_cell_definition` is removed from
    `NAMESPACE` and `man/build_cell_definition.Rd` is deleted.
  - In both `R/description_renderer.R` and `R/description_renderer_html.R` (P2.1):
    - Remove the `cell_definition` branches, `.render_shares_interpretation*()`,
      the `warnings` branch, and the execution-summary key labels.
    - Remove the `next` skip guard on `statistics_selected` for
      `analysis_var_label` and `analysis_var_type` (`R/description_renderer.R:154-160`,
      `R/description_renderer_html.R:269-275`) so `analysis_var_name`,
      `analysis_var_label`, and `analysis_var_type` render at the top of
      `statistics_selected`.
    - Update `.display_label_markdown()` and `.display_label_html()` to map
      `suppression_note` → `"Cell suppression"`, `poverty_line` (in `provenance`)
      → `"Poverty line"`, `analysis_var_name` → `"Analysis variable code"`,
      `analysis_var_label` → `"Analysis variable"`, and `analysis_var_type` →
      `"Variable type"`.
    - Update `.display_table_header_markdown()` and `.display_table_header_html()`
      to map `country_label` → `"Country name"`, `version` → `"Version"`,
      `reason` → `"Exclusion reason"`, `variable` → `"Label"`,
      `selected_categories` → `"Selected categories"`, and `measure` →
      `"Measure code"`.
  - Delete `tests/testthat/test-cell-definition.R`.
  - In `tests/testthat/test-error-coverage-audit.R`, remove **only** the
    `R/description_builder.R` entries (`build_cell_definition`,
    `.build_filters_content`, `.build_statistics_content` at `:131-133`) from
    `.MIXED_FILE_SITE_ALLOWLIST`. Leave the `R/table_maker.R` entries
    (`.build_description_metadata` and `include_metadata` at `:100-103`) in place
    until Step 7 when they are deleted from `R/table_maker.R` (P1.1).
- **Test Scenarios**:
  - Happy path: Markdown and HTML both render all 6 sections with matching headings,
    field labels (including `analysis_var_*`, `suppression_note`, `poverty_line`),
    and table headers.
  - Edge case: conditional sections omitted when invisible; HTML escapes dynamic
    strings.
  - Error path: `NULL`-content sections are skipped cleanly; AST error audit
    passes.
- **Tests**: `Rscript -e "pkgload::load_all(quiet=TRUE); testthat::test_file('tests/testthat/test-description-renderer.R'); testthat::test_file('tests/testthat/test-description-renderer-html.R'); testthat::test_file('tests/testthat/test-description-builder-schema-validation.R'); testthat::test_file('tests/testthat/test-description-helpers.R'); testthat::test_file('tests/testthat/test-error-coverage-audit.R')"`
- **Acceptance criteria**: V3 passes; Phase 1 is 100% green including
  `test-error-coverage-audit.R`.

## Phase 2: Endpoint contract and engine decoupling

### 5. Rebuild `GET|POST /metadata` on the unified parameter contract

- **Requirements**: R13, R14, R15, R16
- **Files**: `inst/plumber/plumber.R`, `inst/plumber/helpers.R`,
  `tests/testthat/test-api-metadata.R`
- **Details**:
  - Replace the `POST`-only `/metadata` route (`inst/plumber/plumber.R:993-1127`)
    with a combined `#* @get /metadata` + `#* @post /metadata` handler taking the
    same parameters as `/table` (`analysis_var`, `pip_id`, `measures`,
    `poverty_line`, `column`, `row`, `superColumn`, `superRow`, `ppp = 2021L`,
    `filter_base = NULL`, `pop_share_threshold = 0.001`, `release = NULL`) plus
    `format = "html"`, `req`, and `res`.
  - Support both `GET` query parameters and `POST` JSON bodies (rejecting any
    legacy `description_metadata`-only body with HTTP 400). Validate `format`
    (case-insensitive `"html"` or `"markdown"`, else 400) and validate table
    parameters via `validate_table_input()`. Parse `filter_base` via
    `parse_filter_base_input()`. Delete `validate_description_input()` from
    `inst/plumber/helpers.R:876-920`.
  - Call `piptm::build_description_model(...)` and render via
    `piptm::render_description_html()` or `piptm::render_description_markdown()`.
  - Set `Content-Disposition: attachment; filename="table-metadata.html"` (or
    `"table-metadata.md"` when `format == "markdown"`) alongside the verified
    `Content-Type` header (`text/html; charset=utf-8` for HTML; `text/plain` for
    Markdown).
  - Update `enrich_openapi_spec()` in `inst/plumber/plumber.R` (P1.2):
    - Remove `[WIP — Ignore for now / still being built]` from `spec$info$description`
      (`:119-120`), `summary_by_path[["/metadata"]]` (`:295`),
      `description_by_path[["/metadata"]]` (`:337-338`), and the route header
      (`:995`).
    - Add `"/metadata" = c("analysis_var", "pip_id", "measures")` to
      `required_by_path` (`:167-171`).
    - In `enrich_param()` (`:209`, `:242-268`), apply `pip_id` `minItems`/`maxItems`
      when `path %in% c("/table", "/metadata")` and add a branch for
      `identical(nm, "format")` (`type = "string"`, `default = "html"`,
      `enum = list("html", "markdown")`).
    - Add `"/metadata"` to the `400`/`422` response-enrichment loop (`:447-458`).
    - Update the `/metadata` OpenAPI block (`:467-622`) to strip any `req` query
      parameter on both `GET` and `POST`, and replace the two-mode `requestBody`
      with the single table-parameter + `format` schema and `200`/`400`/`422`
      responses.
  - Rewrite `tests/testthat/test-api-metadata.R` to cover `GET` and `POST`
    equivalence, HTML and Markdown output, `Content-Disposition` attachment
    headers, `filter_base` as query string vs nested JSON object, 400/422 error
    paths, rejection of `description_metadata` bodies, and the OpenAPI `req` guard.
- **Test Scenarios**:
  - Happy path: `GET /metadata` and `POST /metadata` return `200` with identical
    HTML (default) or Markdown (`format = "markdown"`) and the expected
    `Content-Disposition` filename.
  - Edge case: `filter_base` as a `GET` JSON string matches `POST` nested JSON
    object; OPTIONS preflight succeeds; OpenAPI spec exposes neither `req` nor
    `res`.
  - Error path: missing `pip_id` → 400; invalid `format` → 400; legacy
    `description_metadata` body → 400; invalid `filter_base` category or all
    surveys excluded → 422 with user-facing message.
- **Tests**: `Rscript -e "pkgload::load_all(quiet=TRUE); testthat::test_file('tests/testthat/test-api-metadata.R')"`
- **Acceptance criteria**: V4 passes.

### 6. Remove `include_metadata` from `/table` and update OpenAPI endpoint tests

- **Requirements**: R16
- **Files**: `inst/plumber/plumber.R`, `tests/testthat/test-api-endpoints.R`
- **Details**:
  - In `inst/plumber/plumber.R`:
    - Remove `#* @param include_metadata` (`:806`), `include_metadata = "false"`
      from the `/table` formals (`:813`), the `include_meta` parse (`:833-834`),
      `include_metadata = include_meta` in the `table_maker()` call (`:851`), the
      dual return-shape branch (`:854-877`), and `meta$description_metadata`
      (`:888-894`). `/table` now unconditionally runs `serialize_table_response()`.
    - Remove the `identical(nm, "include_metadata")` branch in `enrich_param()`
      (`:242`) and all `include_metadata` / `description_metadata` mentions in
      `description_by_path` and the `/table/download` OpenAPI descriptions
      (`:322`, `:329`, `:674`, `:693`).
  - In `tests/testthat/test-api-endpoints.R` (P1.2):
    - Replace the `include_metadata=false` vs `true` test (`:733-760`) with an
      assertion that `/table` returns Approach C and `meta` contains no
      `description_metadata`.
    - Add `"/metadata" = c("analysis_var", "pip_id", "measures")` to
      `required_map` (`:1807-1811`).
    - Replace `"include_metadata"` with `"format"` in `server_default_params`
      (`:1846`).
    - Update the `/metadata` OpenAPI test (`:1859-1875`) to assert the non-WIP
      summary and the presence of both `get` and `post` operations with table
      parameters.
- **Test Scenarios**:
  - Happy path: `/table` returns the unchanged Approach C response; OpenAPI audit
    passes across all routes and methods.
  - Edge case: `meta` has only `release` and `n_surveys`.
  - Error path: `/table` 400 and 422 responses are unchanged.
- **Tests**: `Rscript -e "pkgload::load_all(quiet=TRUE); testthat::test_file('tests/testthat/test-api-endpoints.R'); testthat::test_file('tests/testthat/test-phase4-table-and-endpoint.R')"`
- **Acceptance criteria**: V5 passes; zero occurrences of `include_metadata` or
  `description_metadata` remain in `inst/plumber/`.

### 7. Remove metadata assembly from `table_maker()` and clean up error constants

- **Requirements**: R14, R17
- **Files**: `R/table_maker.R`, `R/errors.R`, `man/table_maker.Rd`,
  `tests/testthat/test-table-maker-metadata.R`,
  `tests/testthat/test-description-metadata.R`,
  `tests/testthat/test-table-maker-suppression.R`,
  `tests/testthat/test-error-coverage-audit.R`
- **Details**:
  - In `R/table_maker.R`, delete `.build_description_metadata()` (`:121-384`), the
    `include_metadata` formal and its validation (`:519-524`), the metadata-only
    trackers (`suppressed_cells`, `captured_warnings`), the `withCallingHandlers()`
    wrapper (`:1018-1035`), and the `if (include_metadata)` return branch
    (`:1042-1086`). `table_maker()` always returns `result[]`.
  - Update `table_maker()` roxygen (`:433-519`) to remove `@param include_metadata`
    and the two-shape `@return`.
  - In `R/errors.R`, remove the now-unused `.PIPTM_DESCRIPTION_METADATA_USER_ERROR`
    constant (`:28-31`) (P3.1).
  - Delete `tests/testthat/test-table-maker-metadata.R` and
    `tests/testthat/test-description-metadata.R`.
  - In `tests/testthat/test-table-maker-suppression.R`, remove the
    `include_metadata = TRUE` ANSI-warning capture test (`:194-225`) while keeping
    all `"NR"` suppression tests intact.
  - In `tests/testthat/test-error-coverage-audit.R`, remove the
    `.build_description_metadata` and `include_metadata` allowlist entries for
    `R/table_maker.R` (`:100-103`) (P1.1).
- **Test Scenarios**:
  - Happy path: `table_maker()` returns a `data.table` for all valid calls.
  - Edge case: `"NR"` suppression and survey exclusion warnings fire normally.
  - Error path: passing `include_metadata = TRUE` fails as an unused argument;
    `test-error-coverage-audit.R` passes with zero stale allowlist entries.
- **Tests**: `Rscript -e "pkgload::load_all(quiet=TRUE); testthat::test_file('tests/testthat/test-table-maker-v2.R'); testthat::test_file('tests/testthat/test-table-maker-suppression.R'); testthat::test_file('tests/testthat/test-error-coverage-audit.R')"`
- **Acceptance criteria**: V6 passes; `include_metadata` and
  `.build_description_metadata` are absent from `R/`.

## Phase 3: Documentation and final verification

### 8. Update live documentation, regenerate `man/`, and run the full suite

- **Requirements**: R18, R19
- **Files**: `README.md`, `SWAGGER_GUIDE.md`, `docs/api-contract.md`,
  `docs/metadata-endpoint-spec.md`, `R/api.R`, `R/description_builder.R`,
  `R/description_renderer.R`, `R/description_renderer_html.R`, `NEWS.md`,
  regenerated `man/*.Rd`
- **Details**:
  - Update `@examples` in `R/description_builder.R`, `R/description_renderer.R`,
    and `R/description_renderer_html.R` so they call `build_description_model()`
    directly with table parameters instead of `table_maker(..., include_metadata = TRUE)`.
  - `docs/api-contract.md`: remove `include_metadata` from `/table`, remove the
    `description_metadata` fast-path mode and schema, remove
    `validate_description_input` and the malformed-metadata 422 row, and document
    `GET|POST /metadata` with table parameters, `format`, and the
    `Content-Disposition` download header.
  - `SWAGGER_GUIDE.md`, `README.md`, and `R/api.R`: update parameter and endpoint
    tables for `/table` and `GET|POST /metadata`.
  - `docs/metadata-endpoint-spec.md`: add a top-level status note marking the
    `include_metadata` / `description_metadata` sections as superseded by the pure
    manifest + registry `GET|POST /metadata` architecture.
  - `NEWS.md`: add a breaking-change entry covering the removal of
    `include_metadata`, the retirement of `description_metadata`, and the new
    `GET|POST /metadata` contract.
  - Run `devtools::document()` and `devtools::test()`.
- **Test Scenarios**: N/A (documentation + full regression suite).
- **Tests**: `Rscript -e "devtools::document(); devtools::test()"`
- **Acceptance criteria**: V7 and V8 pass; `R/cell_explanation.R` and
  `inst/extdata/tm_cell_prose.yaml` are untouched.

## Testing Strategy

- **Red phase per step**: update or write the failing test in the same step as the
  code change.
- **Survey-partition agreement**: Step 1 and Step 2 verify that
  `.partition_surveys_by_dimensions()` produces identical included and excluded
  survey sets (and identical exclusion aborts) for both `table_maker()` and
  `build_description_model()`.
- **Zero-microdata guard**: Step 3 mocks `load_survey_microdata()` and
  `table_maker()` to abort and verifies `build_description_model()` still succeeds,
  backed by a static source check on `R/description_builder.R`.
- **AST error-audit alignment**: `test-error-coverage-audit.R` is updated in two
  stages matching the code deletions (Step 4 for `R/description_builder.R`, Step 7
  for `R/table_maker.R`) so Phase 1 and Phase 2 each end green.
- **OpenAPI contract coverage**: Step 6 runs the full `test-api-endpoints.R`
  OpenAPI audit (`required_map`, `server_default_params`, `req` exclusion, and
  `/metadata` `GET`/`POST` operations).

## Documentation Checklist

- [ ] `README.md` endpoint table
- [ ] `SWAGGER_GUIDE.md` parameter table and `/metadata` section
- [ ] `docs/api-contract.md` `/table` parameters, `/metadata` contract, error table
- [ ] `docs/metadata-endpoint-spec.md` superseded-architecture note
- [ ] `R/api.R` route table and regenerated `man/run_api.Rd`
- [ ] `R/table_maker.R` roxygen and regenerated `man/table_maker.Rd`
- [ ] `R/description_builder.R` and `R/description_renderer*.R` roxygen examples
      and regenerated `man/build_description_model.Rd`,
      `man/render_description_*.Rd`
- [ ] `man/build_cell_definition.Rd` removed via `devtools::document()`
- [ ] `NEWS.md` breaking-change entry
- [ ] OpenAPI patch text inside `inst/plumber/plumber.R`

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Metadata and the actual table disagree on included/excluded surveys | **High** | R1/C2: single shared `.partition_surveys_by_dimensions()` helper preserving the exact two-stage (`filter_vars` → `by_check`) pre-filter and partial `pip_id` match rule |
| Decoupling `/metadata` from `table_maker()` drops domain validations (`filter_base` contents, `poverty_line` for poverty measures, release-scoped covariates) | **High** | R3/Step 1: extract shared parameter-level domain validation helper called by both `table_maker()` and `build_description_model()` |
| `test-error-coverage-audit.R` fails in Phase 1 if `.build_description_metadata` allowlist entries are removed before Step 7 | **High** | Step 4 removes only `R/description_builder.R` entries; `R/table_maker.R` entries are removed in Step 7 alongside `.build_description_metadata()` |
| OpenAPI contract tests (`test-api-endpoints.R`) fail when `GET /metadata` is added or `[WIP]` is removed | Medium | Step 5 & Step 6 update `required_by_path`, `enrich_param()`, `required_map`, `server_default_params`, and the `/metadata` OpenAPI assertions together |
| Renderer drift or silent omission of `statistics_selected` analysis-variable fields | Medium | Step 4 removes the `analysis_var_label`/`analysis_var_type` skip guard in both renderers and updates `.display_label_*()` and `.display_table_header_*()` in lockstep |
| External clients still pass `include_metadata=true` or `description_metadata` | Medium | Intended breaking change, no alias (C4); documented in `NEWS.md` and `docs/api-contract.md` |

## Out of Scope

- `GET /cell-definition`, `R/cell_explanation.R`, `inst/extdata/tm_cell_prose.yaml`,
  and Page-2 prose generally.
- The external variable/coverage catalogue website.
- A full-registry codebook dump in the metadata document.
- `POST /table/download` flattening logic and the Approach C serializer
  (`R/table_response.R`).
- Measure/compute functions, the registry build pipeline
  (`build_variable_registry()`), and manifest generation.
- Renaming `description_builder.R`, `description_renderer*.R`,
  `render_description_*()`, or the `/metadata` route.
- Compatibility aliases for `include_metadata` or `description_metadata`.
- Exact `"NR"` cell counts in the metadata document.
- JSON output format for `/metadata` (tracked separately on the roadmap as
  `structured-json-output-format-for-get-post-metadata`), i18n, caching, and the
  PIP platform UI client.
- Editing historical `.cg-docs/**` artifacts or `roadmap.json`.

## Completion Contract

### Outcome

`table_maker()` computes tables only and always returns a `data.table`;
`include_metadata` is gone from both the engine and `/table`, so `/table` always
returns the Approach C `{layout, cells}` payload. `GET|POST /metadata` renders a
6-section table metadata document in HTML or Markdown from query parameters
alone, resolved and domain-validated entirely from the in-memory manifest and
release registry with zero Arrow microdata reads, and delivers it with a
downloadable `Content-Disposition` attachment header.

### Verification Surface

| ID | Phase | Evidence Required | Command/Artifact | Required |
|----|-------|-------------------|------------------|----------|
| V1 | 1 | Pure builder enforces shared domain validations, returns the 6 sections with correct conditional visibility and truthful slot labels, and matches `table_maker()`'s two-stage survey partition | `Rscript -e "pkgload::load_all(quiet=TRUE); testthat::test_file('tests/testthat/test-description-builder.R'); testthat::test_file('tests/testthat/test-description-builder-schema-validation.R'); testthat::test_file('tests/testthat/test-description-helpers.R')"` | yes |
| V2 | 1 | Builder performs no Arrow/microdata access (mocked-binding guard + static source check on `R/description_builder.R`) | `Rscript -e "pkgload::load_all(quiet=TRUE); testthat::test_file('tests/testthat/test-description-builder.R')"` | yes |
| V3 | 1 | Markdown and HTML renderers emit all 6 sections in parity (including `analysis_var_*`, `suppression_note`, `poverty_line`), no `cell_definition`/`execution_summary`/`warnings` block remains, and Phase 1 error-coverage audit passes | `Rscript -e "pkgload::load_all(quiet=TRUE); testthat::test_file('tests/testthat/test-description-renderer.R'); testthat::test_file('tests/testthat/test-description-renderer-html.R'); testthat::test_file('tests/testthat/test-error-coverage-audit.R')"` | yes |
| V4 | 2 | `GET /metadata` and `POST /metadata` both return 200 with identical HTML/Markdown output and `Content-Disposition: attachment`; invalid params or `description_metadata` bodies return 400/422 | `Rscript -e "pkgload::load_all(quiet=TRUE); testthat::test_file('tests/testthat/test-api-metadata.R')"` | yes |
| V5 | 2 | `/table` exposes no `include_metadata` parameter and always returns Approach C; OpenAPI spec and `test-api-endpoints.R` pass for `GET|POST /metadata` and `/table` | `Rscript -e "pkgload::load_all(quiet=TRUE); testthat::test_file('tests/testthat/test-api-endpoints.R'); testthat::test_file('tests/testthat/test-phase4-table-and-endpoint.R')"` | yes |
| V6 | 2 | `table_maker()` has no `include_metadata` parameter, `.build_description_metadata()` and `.PIPTM_DESCRIPTION_METADATA_USER_ERROR` are removed, return is always a `data.table`, and `test-error-coverage-audit.R` passes | `Rscript -e "pkgload::load_all(quiet=TRUE); testthat::test_file('tests/testthat/test-table-maker-v2.R'); testthat::test_file('tests/testthat/test-table-maker-suppression.R'); testthat::test_file('tests/testthat/test-error-coverage-audit.R')"` | yes |
| V7 | 3 | No live source or doc references a callable `include_metadata` parameter or the `description_metadata` request mode | `Select-String` / grep check over `R/`, `inst/`, `tests/`, `README.md`, `SWAGGER_GUIDE.md`, `docs/api-contract.md` | yes |
| V8 | final | Full test suite passes; `devtools::document()` leaves no stale Rd files | `Rscript -e "devtools::document(); devtools::test()"` | yes |

### Constraints

| ID | Phase | Constraint | Check |
|----|-------|------------|-------|
| C1 | 1 | Metadata generation performs zero Arrow/microdata I/O and never calls `table_maker()` | V2; `R/description_builder.R` has no `table_maker`/`load_survey_microdata`/`arrow::` reference |
| C2 | 1 | Domain validation and the two-stage included/excluded survey partition are computed by shared helpers used by both `table_maker()` and `build_description_model()` | single definitions of `.validate_and_normalize_table_params()` and `.partition_surveys_by_dimensions()` called by both |
| C3 | 1 | The metadata document contains no Page-2 "how to read a cell" prose; `GET /cell-definition` remains the sole owner | no `cell_definition` section in the model; `R/cell_explanation.R` and `tm_cell_prose.yaml` untouched |
| C4 | 2 | No compatibility alias: no `include_metadata` parameter and no `description_metadata` request mode survive | V5, V7 |
| C5 | 2 | `/table` response shape for existing default calls is byte-equivalent to today's `include_metadata=false` output | existing Approach C tests pass unchanged |
| C6 | all | Registry/manifest accessor functions (`piptm_manifest`, `piptm_surveys_ui`, `piptm_analysis_variables`, `piptm_filter_categories`, `piptm_layout_covariates`, `piptm_stat_groups`) are consumed, not modified | `R/manifest.R` and `R/registry.R` absent from the diff |
| C7 | all | No new package dependencies | `DESCRIPTION` Imports/Suggests unchanged |
| C8 | 3 | Historical `.cg-docs/**` and `roadmap.json` are not rewritten during implementation | those paths absent from the implementation diff |

### Boundaries

- **Allowed**: `R/description_builder.R`, `R/description_renderer.R`,
  `R/description_renderer_html.R`, `R/table_maker.R`, `R/errors.R`,
  `inst/plumber/plumber.R`, `inst/plumber/helpers.R`, affected
  `tests/testthat/test-*.R`, `README.md`, `SWAGGER_GUIDE.md`,
  `docs/api-contract.md`, `docs/metadata-endpoint-spec.md`, `R/api.R` route
  tables, `NEWS.md`, `NAMESPACE`, regenerated `man/*.Rd`.
- **Out of scope**: `GET /cell-definition`, `R/cell_explanation.R`,
  `inst/extdata/tm_cell_prose.yaml`; the external variable/coverage catalogue
  website; `POST /table/download` flattening logic; the Approach C serializer
  (`R/table_response.R`); measure/compute functions; the registry build pipeline
  (`build_variable_registry()`); i18n; a dumped full-registry codebook section;
  JSON output format for `/metadata`; the PIP platform UI client; renaming
  `description_*` files or the `/metadata` route.

### Iteration Policy

1. Deviation policy is `ask` — stop and record before any extra rename, alias, or
   scope expansion.
2. Red-phase first: update or write the failing test in the same change as the
   behavior it covers.
3. Phase order is strict: the pure builder and renderers (Phase 1) must be green
   — including `test-error-coverage-audit.R` — before the endpoint and engine
   cutover (Phase 2), which must be green before docs (Phase 3).
4. If the shared survey-partition or domain-validation helpers cannot be extracted
   without changing `table_maker()`'s existing exclusion warnings or abort
   messages, stop and report rather than weakening those messages.
5. Do not weaken or delete an existing assertion except where it asserts a
   removed parameter or the retired `description_metadata` mode.

### Blocked-Stop Conditions

- The release manifest lacks the dimension data needed to reproduce
  `table_maker()`'s exclusion decisions, making metadata-vs-table agreement
  unverifiable without microdata.
- Extracting the shared partition or domain-validation helpers would change
  `/table`'s existing error or warning behavior.
- A consumer of `/table?include_metadata=true` or the `description_metadata` fast
  path is discovered that was not accounted for as an intended breaking change.
- Required test commands cannot be run in this environment.
- A required deviation is discovered under policy `ask` and user approval is
  unavailable.
