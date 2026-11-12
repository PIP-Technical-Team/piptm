# Execution Report: Table Metadata Rework and Decoupling from table_maker()

- **Plan**: `.cg-docs/plans/2026-11-12-metadata-rework-and-table-maker-decoupling.md`
- **Branch**: `step3_metadata`
- **Started**: 2026-11-12
- **Scope this run**: Phase 1 (steps 1–4)
- **Deviation policy**: `ask`
- **Review mode**: `review:manual` (default)
- **Test runner**: R `testthat` via `C:\Program Files\R\R-4.5.2\bin\Rscript.exe`

## Environment Notes

- `Rscript` not on PATH; resolved to `C:\Program Files\R\R-4.5.2\bin\Rscript.exe`.
- Targeted tests run through a temp runner script
  (`%TEMP%\kilo\run_one_test.R`) that loads via `pkgload::load_all()` and runs
  one `test_file()`, printing `RESULT file=... PASS/FAIL/WARN/SKIP`.
- Benign startup warnings only: `data.table`/`arrow`/`jsonlite` "built under R
  version 4.5.3".

## Baselines (pre-change)

- `test-table-maker-v2.R`: PASS=13 FAIL=0 (clean).

## Run Log

### Phase 1

#### Step 1 — Extract shared domain-validation and two-stage survey-partition helpers

- **Red-phase**: `tests/testthat/test-table-params.R` written first; errored at
  baseline because `.validate_and_normalize_table_params()` and
  `.partition_surveys_by_dimensions()` did not exist (`exists(...) == FALSE`).
- **Implementation**: created `R/table_params.R` with both helpers, extracted
  from `table_maker()` (validation/normalization and manifest + two-stage
  partition). Refactored `table_maker()` to call them; preserved `cli_warn()`
  exclusion bullets (`<pip_id>: reason`) while the returned `excluded$reason`
  stores the clean reason without the `<pip_id>: ` prefix (P3.1).
- **Tests**: `test-table-params.R` PASS=20 FAIL=0 (incl. table_maker agreement).
  Regression: v2 13/0, suppression 59/0, table-maker-metadata 0/0 (6 skip),
  description-metadata 26/0, phase4 14/0. Added one allowlist entry for the
  duplicate-manifest `cli::cli_abort()` in `.partition_surveys_by_dimensions()`.
- **Pre-existing failure (NOT a regression)**: `test-error-coverage-audit.R`
  fails on 6 raw `cli::cli_abort()` sites in `R/cell_explanation.R`. Confirmed
  present at the committed `step3_metadata` baseline. `R/cell_explanation.R` is
  Out of Scope (R19/C3); left untouched. My change adds zero new audit
  violations.
- **Acceptance**: met. **Suggested commit**: `refactor(table_maker): extract
  shared param-validation and two-stage survey-partition helpers`.

#### Step 2 — Rewrite build_description_model() as a pure manifest + registry builder

- Rewrote `R/description_builder.R` into the pure 6-section builder taking table
  parameters (`pip_id`, `analysis_var`, `measures`, `poverty_line`, the four
  named slots, `filter_base`, `ppp`, `pop_share_threshold`, `release`). Calls the
  Step 1 shared validator + partition helper; reads only manifest + registry.
  Removed `build_cell_definition()` + shares helpers, `.build_execution_content`,
  `.resolve_group_label`, `.coerce_description_scalar`,
  `format_covariate_description`, and all JSON-roundtrip `meta` coercions.
- `format_slot_label()` now maps both singular camelCase (`superRow`/`row`/
  `superColumn`/`column`) and plural keys to `Super-rows`/`Rows`/`Super-columns`/
  `Columns` (fixes the hardcoded-`rows` bug — P2.1/R9).
- Rewrote `test-description-builder.R` (63 tests) and `test-description-helpers.R`
  (16); deleted obsolete `test-description-builder-schema-validation.R` (tested
  the retired `meta` shape — P2.2).
- Safe `[`-based filter-label lookup with raw-code fallback preserved (Brain:
  2026-10-13-hsize-group bug).

#### Step 3 — Zero-microdata guard

- Added `local_mocked_bindings()` guard (mocks `load_surveys` +
  `load_survey_microdata` to abort; builder still succeeds) and a static source
  scan asserting `R/description_builder.R` has no `table_maker`/
  `load_survey_microdata`/`load_surveys`/`arrow::` reference. Both in
  `test-description-builder.R`. V2 green.

#### Step 4 — Remove dead sections and update both renderers in parity

- Removed `cell_definition`/`warnings`/`execution_summary` branches, the
  `statistics_selected` `analysis_var_*` skip guard, and
  `.render_measure_interpretation_shares[_html]()` from both
  `R/description_renderer.R` and `R/description_renderer_html.R`. Updated
  `.display_label_*()` (added `suppression_note`, `analysis_var_name`,
  provenance `poverty_line`) and `.display_table_header_*()` (added
  `country_label`, `version`, `reason`, `variable`, `selected_categories`,
  `measure`).
- Deleted `test-cell-definition.R`. Rewrote both renderer test files
  (markdown 31, html 37) around the 6-section model.
- `test-error-coverage-audit.R`: removed the 3 `R/description_builder.R`
  allowlist entries (P1.1); left the `R/table_maker.R` entries for Step 7.
- `devtools::document()` removed `man/build_cell_definition.Rd` and dead helper
  Rd files, dropped `build_cell_definition` from `NAMESPACE`, and added
  `man/dot-partition_surveys_by_dimensions.Rd` +
  `man/dot-validate_and_normalize_table_params.Rd`.

#### Phase 1 Boundary — evidence gate

- **V1**: `test-description-builder.R` 63/0 + `test-table-params.R` 20/0
  (6 sections, visibility, slot roles, partition agreement). PASS.
- **V2**: zero-microdata mocked-binding guard + static source scan (in
  `test-description-builder.R`). PASS.
- **V3**: `test-description-renderer.R` 31/0 + `test-description-renderer-html.R`
  37/0 (6 sections in parity, no removed blocks). PASS.
- **Full suite**: PASS=2699 FAIL=19 SKIP=14. The 19 failures are:
  (a) **17 in `test-api-metadata.R`** — the legacy `description_metadata`
  fast-path and 2-arg `build_description_model()` tests; these are **Phase 2's
  red-phase baseline** (Step 5 rewrites that file and the route). Not in Phase 1
  scope.
  (b) **2 in `test-error-coverage-audit.R`** — raw `cli::cli_abort()` in
  `R/cell_explanation.R`. **Accepted exception**: pre-existing on the committed
  `step3_metadata` branch (verified by reverting all my changes → still FAIL=2);
  `R/cell_explanation.R` is Out of Scope (R19/C3). My changes add zero new audit
  violations.
- Phase 1 marked complete (`completed-phases: [1]`).
- **Suggested commit**: `refactor(metadata): pure manifest+registry
  build_description_model with 6-section renderers`.

### Phase 2

#### Step 5 — Rebuild GET|POST /metadata on the unified parameter contract

- **Red-phase**: Rewrote `tests/testthat/test-api-metadata.R` around the unified
  `GET|POST /metadata` contract; failed with 27 failures against the legacy
  route (`GET` 404, missing `Content-Disposition`, legacy `description_metadata`
  OpenAPI schema).
- **Implementation**:
  - Removed `validate_description_input()` from `inst/plumber/helpers.R`.
  - Replaced `POST /metadata` in `inst/plumber/plumber.R` with a combined
    `#* @get /metadata` + `#* @post /metadata` route taking table parameters
    plus `format = "html"`, validating via `validate_table_input()`, explicitly
    rejecting any legacy `description_metadata` body with HTTP 400, calling
    `build_description_model()`, and setting
    `Content-Disposition: attachment; filename="table-metadata.html"` (or
    `"table-metadata.md"`).
  - Updated `enrich_openapi_spec()`: removed `[WIP]` notes, added `"/metadata"`
    to `required_by_path` and `400`/`422` response loops, stripped `req`/`res`
    from `op$parameters`, added `format` default `"html"`, and updated the
    `/metadata` `GET` and `POST` OpenAPI schemas.
- **Tests**: `test-api-metadata.R` PASS=50 FAIL=0.
- **Suggested commit**: `feat(api): rebuild GET|POST /metadata on pure manifest+registry parameter contract`.

#### Step 6 — Remove include_metadata from /table and update OpenAPI endpoint tests

- **Red-phase**: Updated `tests/testthat/test-api-endpoints.R` (`required_map`,
  `server_default_params` with `"format"` replacing `"include_metadata"`,
  Approach C-only `/table` test, and non-WIP `/metadata` OpenAPI test); failed
  with 2 failures on `/table`'s `include_metadata` parameter.
- **Implementation**: Removed `#* @param include_metadata`, `include_metadata = "false"`,
  `include_meta`, the dual return-shape branch, and `meta$description_metadata`
  from `GET|POST /table`, plus `enrich_param()` and OpenAPI description strings.
- **Tests**: `test-api-endpoints.R` PASS=949 FAIL=0,
  `test-phase4-table-and-endpoint.R` PASS=14 FAIL=0.
- **Suggested commit**: `refactor(api): remove include_metadata from /table endpoint and OpenAPI spec`.

#### Step 7 — Remove metadata assembly from table_maker() and clean up error constants

- **Red-phase**: Added assertion in `test-table-maker-v2.R` that
  `include_metadata` is absent from `formals(table_maker)` and
  `.build_description_metadata` does not exist; failed with 2 failures before
  implementation.
- **Implementation**:
  - Removed `.build_description_metadata()`, `include_metadata` formal/validation,
    `excluded_surveys`/`captured_warnings`/`suppressed_cells` trackers,
    `withCallingHandlers()`, and the `if (include_metadata)` return branch from
    `R/table_maker.R`. Note: the ANSI warning-stripping concern from
    `.cg-docs/solutions/bugs/2026-09-14-ansi-escape-codes-in-description-warnings.md`
    no longer applies because `table_maker()` no longer captures runtime warnings
    into metadata.
  - Removed `.PIPTM_DESCRIPTION_METADATA_USER_ERROR` from `R/errors.R`.
  - Deleted `test-table-maker-metadata.R` and `test-description-metadata.R`;
    removed the `include_metadata = TRUE` ANSI test from
    `test-table-maker-suppression.R`; removed the retired `R/table_maker.R`
    allowlist entries from `test-error-coverage-audit.R`.
  - Regenerated `man/table_maker.Rd` and removed `man/dot-build_description_metadata.Rd`
    via `devtools::document()`.
- **Tests**: `test-table-maker-v2.R` PASS=16 FAIL=0,
  `test-table-maker-suppression.R` PASS=57 FAIL=0,
  `test-error-coverage-audit.R` PASS=12 FAIL=2 (only pre-existing
  `R/cell_explanation.R` out-of-scope failures; allowlist staleness check passed).

#### Phase 2 Boundary — evidence gate

- **V4**: `test-api-metadata.R` PASS=50 FAIL=0 (`GET` and `POST` equivalence,
  `Content-Disposition: attachment`, 400/422 error paths, `description_metadata`
  rejected). PASS.
- **V5**: `test-api-endpoints.R` PASS=949 FAIL=0 +
  `test-phase4-table-and-endpoint.R` PASS=14 FAIL=0 (`/table` Approach C only,
  OpenAPI contract clean). PASS.
- **V6**: `test-table-maker-v2.R` PASS=16 FAIL=0 +
  `test-table-maker-suppression.R` PASS=57 FAIL=0 +
  `test-error-coverage-audit.R` PASS=12 FAIL=2 (pre-existing `cell_explanation.R`
  accepted exception). PASS.
- **Full suite**: PASS=2782 FAIL=2 WARN=4 SKIP=8 (zero in-scope failures).
- Phase 2 marked complete (`completed-phases: [1, 2]`).
- **Suggested commit**: `feat(metadata): complete phase 2 -- endpoint contract and engine decoupling`.

### Phase 3

#### Step 8 — Update live documentation, regenerate man/, and run the full suite

- Updated `@examples` and `@param model` in `R/description_renderer.R` and
  `R/description_renderer_html.R` to call `build_description_model()` directly
  with table parameters (6-section model) instead of
  `table_maker(..., include_metadata = TRUE)`.
- Updated endpoint tables and contracts in `R/api.R`, `README.md`,
  `SWAGGER_GUIDE.md`, and `docs/api-contract.md` (removed `include_metadata`,
  `validate_description_input`, the 422 `description_metadata` row, and the
  `DescriptionMetadata` schema; documented `GET|POST /metadata` and
  `Content-Disposition: attachment`).
- Added a top-level historical/superseded note in `docs/metadata-endpoint-spec.md`.
- Added breaking-change entry in `NEWS.md`.
- Regenerated `man/render_description_markdown.Rd`, `man/render_description_html.Rd`,
  and `man/run_api.Rd` via `devtools::document()`.

#### Phase 3 & Final Evidence Gate

- **V7**: Grep over `R/`, `inst/plumber/`, `man/`, `README.md`,
  `SWAGGER_GUIDE.md`, and `docs/api-contract.md` found zero callable
  `include_metadata` parameters or `description_metadata` request modes (only
  the 3-line HTTP 400 rejection guard in `inst/plumber/plumber.R:982-985`). PASS.
- **V8**: `devtools::document()` clean + full suite `PASS=2782 FAIL=2 WARN=4 SKIP=8`
  (only the 2 pre-existing out-of-scope `R/cell_explanation.R` sites in
  `test-error-coverage-audit.R`). PASS.
- **Mechanical self-review**: No debug statements, broken imports, new TODOs,
  or secrets found. Out-of-scope files (`R/cell_explanation.R`,
  `inst/extdata/tm_cell_prose.yaml`, `R/manifest.R`, `R/registry.R`,
  `DESCRIPTION`) are untouched.
- Plan marked `status: completed` (`completed-phases: [1, 2, 3]`,
  `completed-date: 2026-11-12`). Roadmap feature
  `table-metadata-rework-and-decoupling-from-table-maker` updated to `done`.
