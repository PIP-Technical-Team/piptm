---
date: 2026-11-12
title: "Table Metadata Rework and Decoupling from table_maker()"
status: decided
scope: "Deep"
artifact-schema-version: 1
chosen-approach: "Approach 1 — Standalone Pure Metadata Builder in R/description_builder.R Backed by Manifest & Registry Functions, Decoupled from table_maker()"
tags: [metadata, table-maker, api, endpoint, description-builder, registry, manifest, download, decoupling]
---
<!-- Valid status values: decided, in-progress, abandoned -->

# Table Metadata Rework and Decoupling from `table_maker()`

## Context

The Table Maker project provides explanatory and reference information across distinct surfaces that must stay clearly separated and non-redundant:

1. **"How to read the table" (`GET /cell-definition`, `R/cell_explanation.R`)** — Shown interactively in the UI on Page 2 after the user selects sample-base filters, analysis variables/statistics, and layout slots. It explains in plain English *Who is in the table* ($T$), *What a cell is* ($C$), and *What is shown in that cell* (including share denominators).
2. **External Variable & Coverage Catalogue Website** *(external team, out of scope)* — A standalone resource listing all variables available in Table Maker (analysis variables, filters, covariates) and their survey coverage across the database.
3. **Downloadable Table Metadata (`GET|POST /metadata`)** *(target of this rework)* — A companion metadata document downloaded by the user via a separate download button alongside their table export (`POST /table/download`). It documents the exact table result the user receives.

Historically, table metadata was assembled inside `table_maker()` whenever `include_metadata = TRUE` was passed, via `.build_description_metadata()` (`R/table_maker.R:137-384`), and then formatted by `build_description_model()` (`R/description_builder.R`) and rendered by `render_description_html()` / `render_description_markdown()`. This design caused several issues:

- **Engine bloat & inefficiency**: `table_maker()` carried ~280 lines of metadata assembly, `withCallingHandlers()` warning interception, and exclusion tracking. On `/table?include_metadata=true`, the route skipped `serialize_table_response()` and returned flat rows instead of the Approach C `{layout, cells}` structure.
- **Unnecessary microdata computation on `/metadata`**: When called without a pre-computed `description_metadata` object, `POST /metadata` invoked `table_maker(..., include_metadata = TRUE)`, reading Arrow microdata and computing all statistics only to discard `tm_out$data`.
- **Redundant or broken content**: `build_description_model()` still built a legacy 9-section model including `cell_definition` (hidden at the route layer via `model$cell_definition$visible <- FALSE` once `GET /cell-definition` launched), runtime microdata warnings, and a layout table that hardcoded every covariate's slot to `"rows"` because `table_maker()` only received a flat `by` vector.

Key architectural insight from code inspection: **100% of the information needed for the downloadable metadata document — including which requested surveys are included vs. excluded and which required filter/layout dimensions they lack — is already available in memory from `piptm_manifest(release)` / `piptm_surveys_ui(release)` and the release registry functions (`piptm_analysis_variables()`, `piptm_filter_categories()`, `piptm_layout_covariates()`, `piptm_stat_groups()`, `piptm_variable_registry()`) before any Arrow microdata is ever loaded.**

## Requirements

### 1. Clear Division of Responsibilities (No Cross-Surface Redundancy)
- **Page 2 (`GET /cell-definition`)** owns pedagogical prose on how to read cells. The downloadable metadata document must **not** include the `cell_definition` section, and dead `build_cell_definition()` code in `R/description_builder.R` should be removed.
- **External Catalogue** owns the global dictionary of all Table Maker variables and cross-survey coverage. The downloadable metadata document must **not** dump the entire variable registry.
- **Downloadable Metadata (`GET|POST /metadata`)** documents **only the specific table requested by the user** — its provenance, included/excluded surveys, and the selected variables, roles, and statistics present in that result.
- Because both `/table` (Approach C JSON) and `POST /table/download` (`table.csv` / `table.rds`) already output human-readable category labels (`"Female"`, `"Male"`, `"Urban"`, `"Rural"`), the metadata document does **not** need to expose raw internal integer codes (`0 = Female, 1 = Male`). Instead, it maps the selected variables, their assigned table roles, their human-readable labels and category labels, and the selected statistics.

### 2. Streamlined 6-Section Metadata Content
Built exclusively from existing manifest and registry functions (`piptm_manifest()`, `piptm_surveys_ui()`, `piptm_analysis_variables()`, `piptm_filter_categories()`, `piptm_layout_covariates()`, `piptm_stat_groups()`, `piptm_variable_registry()`):

1. **Table Overview (`overview`, always visible)**:
   - Summary sentence stating the number of statistics, number of included surveys, and active disaggregation roles.
   - Methodological notes: survey sampling weights note, PPP international dollars note (when applicable), and the `"NR"` cell-suppression threshold rule (`pop_share_threshold`, e.g., cells representing less than `0.1%` of the survey sample population are suppressed as `"NR"`).
2. **Data Provenance (`provenance`, always visible)**:
   - Release ID (`release`), PPP year (`ppp`), poverty line (`poverty_line`, when applicable), and UTC generation timestamp (`generated_at`).
3. **Surveys (`surveys_selected`, always visible)**:
   - **Included surveys**: Table of surveys included in the result (`pip_id`, `country_code`, `country_name` / label, `year`, `welfare_type` mapped to `Income` / `Consumption`, `version`).
   - **Excluded surveys (conditional — shown only when $\ge 1$ requested survey is dropped by manifest pre-filtering)**: Table of excluded `pip_id`s and the human-readable reason (which required `filter_base` or layout covariate dimension is absent from that survey in `piptm_manifest(release)$dimensions`), so users know why a survey selected on Page 1 is absent from their Page 3 table/download.
4. **Sample Base Filters (`filters_applied`, conditional on `filter_base` non-empty)**:
   - Sourced via `piptm_filter_categories(release)`: table of active filter variables (`varname`, `label`, and selected category labels).
5. **Table Layout Configuration (`layout_configuration`, conditional on $\ge 1$ active layout slot)**:
   - Sourced via `piptm_layout_covariates(release)` and `piptm_variable_registry(release)`: table mapping each active slot (`Super-rows`, `Rows`, `Super-columns`, `Columns`) to its `varname` (matching the column header in `table.csv`), `label`, and category labels (`n: label_1, label_2, ...`).
6. **Analysis Variable & Statistics (`statistics_selected`, always visible)**:
   - Sourced via `piptm_analysis_variables(release)` and `piptm_stat_groups(release)`: the selected `analysis_var` (`varname`, `label`, `type`), poverty line details (when applicable), and a table of selected `measures` (`measure` key matching the column header in `table.csv`, `label`, and `group_label`).

### 3. Engine Decoupling (`table_maker()` and `/table`)
- Remove the `include_metadata` parameter and `.build_description_metadata()` from `table_maker()` (`R/table_maker.R`), along with `withCallingHandlers()` warning capture and metadata-only trackers.
- `table_maker()` always returns a `data.table`.
- Remove the `include_metadata` parameter from `GET|POST /table` (`inst/plumber/plumber.R`). `/table` always returns the structured Approach C `{layout, cells}` response.

### 4. `/metadata` Endpoint Contract & Delivery
- Support both `GET /metadata` (query parameters) and `POST /metadata` (JSON body), accepting the same table query parameters as `/table`: `pip_id`, `analysis_var`, `measures`, `poverty_line`, `column`, `row`, `superColumn`, `superRow`, `ppp`, `filter_base`, `pop_share_threshold`, `release`, plus `format` (`"html"` or `"markdown"`, default `"html"`).
- Retire the legacy `description_metadata` request-body mode on `/metadata` — there is now a single, fast (`< 5 ms`), zero-microdata parameter contract.
- Support downloadable file delivery (`Content-Disposition: attachment` with appropriate filename such as `table-metadata.html` or `table-metadata.md` when requested/downloaded, while preserving `text/html; charset=utf-8` and `text/markdown` / `text/plain` content types).

## Approaches Considered

### Approach 1: Standalone Pure Metadata Builder in `R/description_builder.R` Backed by Manifest & Registry Functions *(Chosen)*

Remove `include_metadata` and `.build_description_metadata()` from `R/table_maker.R` completely. Refactor `build_description_model()` in `R/description_builder.R` to accept validated table parameters directly (including the four named layout slots `column`, `row`, `superColumn`, `superRow`), resolve included/excluded surveys from `piptm_manifest(release)` / `piptm_surveys_ui(release)`, resolve variable/category/measure metadata from `piptm_analysis_variables()`, `piptm_filter_categories()`, `piptm_layout_covariates()`, `piptm_stat_groups()`, and `piptm_variable_registry()`, and return the 6-section model for `render_description_html()` and `render_description_markdown()`.

- **Pros**:
  - Zero Arrow microdata I/O (`< 5 ms` response time on `GET|POST /metadata`).
  - Removes ~280 lines of metadata complexity from `R/table_maker.R` and ~400 lines of dead `build_cell_definition()` code from `R/description_builder.R`.
  - `/table` has a single deterministic return shape (Approach C).
  - Preserves exact slot roles (`row`, `column`, `superRow`, `superColumn`) instead of losing them in a flat `by` vector.
  - Reuses the existing HTML and Markdown renderers with minimal adjustment.
- **Cons**:
  - Existing unit tests that call `table_maker(..., include_metadata = TRUE)` or pass mock `description_metadata` lists into `build_description_model()` must be updated to the new direct signature.
- **Effort**: Medium (2–3 days).

### Approach 2: Extract `assemble_description_metadata()` Out of `table_maker()`, Keep Two-Step `description_metadata -> build_description_model()` Pipeline

Move `.build_description_metadata()` out of `R/table_maker.R` into a standalone manifest+registry function that produces the legacy intermediate `description_metadata` list, then pass it to `build_description_model(description_metadata, params)`.

- **Pros**: Smaller diff in existing `build_description_model()` unit tests.
- **Cons**: Retains an unnecessary intermediate data structure and JSON-roundtrip coercion logic that only existed for the retired `/table?include_metadata=true` -> `POST /metadata` Mode A handoff.
- **Effort**: Small–Medium (1.5–2 days).

### Approach 3: New `R/table_metadata.R` Module + Delete `R/description_*.R`

Create a new module with renamed functions (`build_table_metadata()`, `render_metadata_html()`, `render_metadata_markdown()`) and delete the `R/description_*.R` files.

- **Pros**: File and function names match `/metadata` verbatim.
- **Cons**: High churn across `NAMESPACE`, `man/`, and renderer tests for cosmetic renaming when the existing renderers already work cleanly.
- **Effort**: Medium–Large (3–4 days).

## Decision

**Approach 1** was chosen because it cleanly decouples metadata generation from `table_maker()`, eliminates the obsolete two-step `description_metadata` handoff structure, deletes dead legacy cell-definition code from `R/description_builder.R`, fixes the layout slot reporting bug by taking named slots directly, and reuses the existing manifest/registry accessor functions and HTML/Markdown renderers with zero microdata I/O.

## Next Steps

1. **Decouple `table_maker()` and `/table`**:
   - Remove `include_metadata` and `.build_description_metadata()` from `R/table_maker.R` (and update roxygen docs / `man/table_maker.Rd`).
   - Remove `include_metadata` from `GET|POST /table` in `inst/plumber/plumber.R` and its OpenAPI spec annotations.
2. **Refactor `R/description_builder.R` (and renderers)**:
   - Implement the pure manifest + registry metadata builder taking the table parameters (`pip_id`, `analysis_var`, `measures`, `poverty_line`, `slots` / `column`, `row`, `superColumn`, `superRow`, `filter_base`, `ppp`, `pop_share_threshold`, `release`).
   - Perform manifest pre-filter checks on `piptm_manifest(release)` to partition requested `pip_id`s into included vs. excluded surveys (with missing dimension reasons) and error with the existing user-facing message when all requested surveys are excluded.
   - Build the 6 streamlined sections (`overview`, `provenance`, `surveys_selected`, `filters_applied`, `layout_configuration`, `statistics_selected`) using `piptm_surveys_ui()`, `piptm_analysis_variables()`, `piptm_filter_categories()`, `piptm_layout_covariates()`, `piptm_stat_groups()`, and `piptm_variable_registry()`.
   - Remove dead `build_cell_definition()` and legacy `execution_summary` / `warnings` section builders from `R/description_builder.R` and `R/description_renderer*.R`.
3. **Update `GET|POST /metadata` in `inst/plumber/plumber.R`**:
   - Expose both `GET /metadata` and `POST /metadata` using the unified table parameter validation (`validate_table_input()` + `format` validation for `"html"` / `"markdown"`).
   - Remove the legacy `description_metadata` input branch and `validate_description_input()`.
   - Add download headers (`Content-Disposition`) and update OpenAPI documentation, `docs/api-contract.md`, and `SWAGGER_GUIDE.md`.
4. **Update Test Suite**:
   - Update `tests/testthat/test-description-*.R`, `tests/testthat/test-api-metadata.R`, and `tests/testthat/test-table_maker.R` to reflect the removal of `include_metadata` and the new 6-section pure metadata builder.
