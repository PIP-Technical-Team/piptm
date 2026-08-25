---
date: 2026-08-24
title: "Step 3 Description Metadata Contract — Corrective Implementation"
status: completed
completed-date: 2026-08-24
scope: "Deep"
brainstorm: ".cg-docs/brainstorms/2026-08-24-step3-description-metadata-contract-redesign.md"
supersedes: ".cg-docs/plans/2026-08-20-step3-description-implementation.md"
language: "R"
estimated-effort: "large"
deviation-policy: "ask"
artifact-schema-version: 1
phases: 3
completed-phases: [1, 2, 3]
execution-report: ".cg-docs/work-reports/2026-08-24-step3-description-metadata-contract-corrective.md"
tags: [architecture, metadata, api, step-3, execution-truth, corrective, description]
---

# Plan: Step 3 Description Metadata Contract — Corrective Implementation

## Objective

Redesign the `table_maker(with_meta = TRUE)` metadata contract to achieve execution truth: the description must accurately reflect what actually happened during computation, not just what was requested. This corrective plan fixes 10 semantic defects identified in the post-implementation audit while maintaining backward compatibility for the default `with_meta = FALSE` path.

## Context

The Step 3 description feature was implemented (Aug 20-21, 2026) following brainstorms `2026-08-13-step3-description-file.md` and `2026-08-20-step3-description-architecture.md`. The implementation introduced a `with_meta = TRUE` sidecar for `table_maker()` that captures specification, execution, provenance, and warnings alongside computed data, plus a description model builder and markdown renderer.

**Post-implementation audit findings** (from review `2026-08-20-step3-description-implementation-review.md` and brainstorm `2026-08-24-step3-description-metadata-contract-redesign.md`):

1. **Survey status ambiguity**: `included_surveys` populated before dimension/filter exclusions (R/table_maker.R:424), reports surveys later excluded
2. **Exclusion tracking incomplete**: Filter-base exclusions not harvested
3. **Release resolution unclear**: No single authority release value used consistently
4. **PPP metadata missing**: Resolved PPP year and physical column name not tracked
5. **Measures vs families confusion**: `measures_computed` stores family names not measure names
6. **Filters unpopulated**: `filters_applied` field initialized but never assigned
7. **Layout role inference**: Roles guessed in renderer, not explicit in metadata
8. **Warning capture broken**: Handler defined but never registered (P0.1 — blocking)
9. **Specification vs execution blurred**: Schema doesn't clearly separate requested vs executed
10. **Edge case coverage weak**: Renderer assumes success, doesn't handle empty/excluded cases clearly

The description is a factual execution record for researchers, policy analysts, and citation purposes. Current metadata does not meet this standard.

**This is a corrective plan** that supersedes the completed `2026-08-20-step3-description-implementation.md` plan. The corrective approach redesigns the metadata schema while preserving backward compatibility and maintaining the existing implementation structure.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | Description accurately reflects what `table_maker()` actually executed | Audit finding, user requirement |
| R2 | Clear distinction between requested parameters and execution results | Audit findings 1, 5, 9 |
| R3 | Survey lifecycle tracked: requested → loaded → excluded (with reasons and stages) | Audit findings 1, 2 |
| R4 | Release and PPP resolution tracked: requested → resolved → physical column | Audit findings 3, 4 |
| R5 | Measures reported as actual measure names dispatched, not families | Audit finding 5 |
| R6 | Filters reported as normalized request (global, not per-survey availability) | Audit finding 6 |
| R7 | Layout roles documented or explicit | Audit finding 7 |
| R8 | Cell definition generated from execution truth | Audit finding 10 |
| R9 | Warnings remain as strings (human-readable) | Simplicity decision |
| R10 | Warning handler registered and functional | P0.1 blocking defect |
| R11 | `table_maker()` default path (`with_meta = FALSE`) unchanged | Backward compatibility |
| R12 | `/table` endpoint contract unchanged | Backward compatibility |
| R13 | Work phased into independently testable deliverables | User requirement |
| R14 | Metadata overhead < 10% of `table_maker(with_meta = TRUE)` runtime | Performance |
| R15 | Field names match semantic meaning (no misleading names) | Maintainability |
| R16 | Schema extensible for future audit needs without breaking changes | Future-proofing |

## Implementation Steps

## Phase 1: Execution Truth Schema

### 1. Redesign `execution` and `specification` schemas

- **Requirements**: R1, R2, R3, R4, R5, R6, R9, R15, R16
- **Files**: `R/table_maker.R` (lines 294-306 — `.meta_state` initialization)
- **Details**:
  
  Redesign the metadata schema to explicitly separate requested parameters from execution results:

  **New `specification` block** (what was requested, with labels from registry):
  ```r
  specification <- list(
    pip_id              = requested pip_id vector (before any exclusions),
    analysis_var        = list(name = "welfare", label = "Welfare"),
    measures            = list(
                            list(name = "mean", label = "Mean welfare", family = "summary_stats"),
                            list(name = "gini", label = "Gini index", family = "inequality")
                          ),
    poverty_line        = as requested (or NULL),
    ppp                 = as requested (or 2021L default),
    by                  = list(
                            list(name = "gender", label = "Gender", role = "rows", categories = ...),
                            list(name = "area", label = "Area", role = "columns", categories = ...)
                          ),
    filter_base         = list(
                            list(varname = "age_group", label = "Age group", kept = list("15-24", "25-64"))
                          ),
    pop_share_threshold = 0.01
  )
  ```

  **New `execution` block** (what actually happened):
  ```r
  execution <- list(
    # Survey lifecycle
    requested_pip_id    = original pip_id vector from function call,
    loaded_surveys      = data.table(pip_id, country_code, year, welfare_type),  # contributed data
    excluded_surveys    = data.table(pip_id, reason, stage),  # stage: "manifest"|"filter_pre"|"dimension_pre"
    
    # Resolution
    resolved_release    = single authority release ID used throughout,
    resolved_ppp        = PPP year after resolution,
    ppp_column_used     = physical column name (e.g., "welfare_ppp_2021"),
    
    # Execution state
    filters_applied     = normalized_filter_base (the request),
    measures_computed   = c("mean", "gini"),  # actual measure names, not families
    suppression         = list(threshold = 0.01, n_suppressed_cells = 2L)
  )
  ```

  **Provenance block** (unchanged structure, may add optional `generated_at`):
  ```r
  provenance <- list(
    package_version = "0.4.2",
    release         = "20260206"
  )
  ```

  **Warnings block** (unchanged — character vector of captured messages):
  ```r
  warnings <- c(
    "Excluding 2 surveys that lack all requested dimensions: ...",
    "Suppressing non-share measures for 2 cells with pop_share < 0.01: ..."
  )
  ```

  Update the `.meta_state` initialization at lines 294-306 to reflect the new schema.

- **Test Scenarios**:
  - Schema completeness: all fields present in returned `with_meta = TRUE` list
  - Type correctness: `data.table` for surveys, character vectors for measures, lists for nested structures
  
- **Tests**: Not yet — schema design step only. Tests in Step 2.

- **Acceptance criteria**: 
  - New schema documented in code comments
  - `.meta_state` initialization matches new schema structure
  - No runtime changes yet (harvest logic in subsequent steps)

---

### 2. Add `requested_pip_id` capture and fix `loaded_surveys` timing

- **Requirements**: R2, R3, R15
- **Files**: `R/table_maker.R` (lines 279-289 function signature, 422-430 old `included_surveys` harvest)
- **Details**:

  **Rename `included_surveys` → `loaded_surveys`** and fix timing:
  
  1. **Capture `requested_pip_id`** at function entry (before any processing):
     ```r
     # Line ~290, after `.meta_state` initialization
     if (with_meta) {
       .meta_state[["requested_pip_id"]] <- pip_id
     }
     ```

  2. **Delete old `included_surveys` harvest** at lines 422-430 (currently populates before exclusions)

  3. **Add `loaded_surveys` harvest** after all exclusions, before `load_surveys()` call (after line 569):
     ```r
     # After dimension pre-filter, before load_surveys()
     if (with_meta) {
       .meta_state[["loaded_surveys"]] <- data.table::data.table(
         pip_id        = entries[["pip_id"]],
         country_code  = entries[["country_code"]],
         surveyid_year = entries[["year"]],
         welfare_type  = entries[["welfare_type"]]
       )
     }
     ```

  Semantic: `requested_pip_id` = original user input; `loaded_surveys` = surveys that passed all pre-filters and will contribute data to the final result.

- **Test Scenarios**:
  - `requested_pip_id` matches original function argument exactly
  - `loaded_surveys` excludes dimension-filtered surveys
  - `loaded_surveys` excludes filter-base-filtered surveys
  - `loaded_surveys` populated only when `with_meta = TRUE`
  
- **Tests**: `tests/testthat/test-table-maker-with-meta.R` (new test: `test_that("requested_pip_id vs loaded_surveys distinction")`)

- **Acceptance criteria**:
  - `requested_pip_id` captures original `pip_id` argument
  - `loaded_surveys` populated after all exclusions, before loading
  - Old `included_surveys` code fully removed

---

### 3. Add `excluded_surveys` with stage tracking

- **Requirements**: R3
- **Files**: `R/table_maker.R` (lines 433-441 manifest miss, 466-487 filter-base exclusion, 539-557 dimension exclusion)
- **Details**:

  Add `excluded_surveys.stage` field to distinguish exclusion points. Harvest at three stages:

  1. **Manifest stage** (lines 433-441 — surveys not in manifest):
     - Currently warns but doesn't harvest
     - Add harvest: `stage = "manifest"`, `reason = "Not found in manifest"`

  2. **Filter-base pre-filter stage** (lines 466-487 — surveys lacking filter variables):
     - Currently warns, exclusion exists at lines 474-487 but NOT harvested
     - Add harvest block after line 487:
       ```r
       if (with_meta) {
         for (i in seq_len(nrow(dropped_entries))) {
           have <- dropped_entries$dimensions[[i]]
           miss <- setdiff(filter_vars, have)
           reason <- if (length(miss) == length(filter_vars)) {
             "No filter_base dimensions"
           } else {
             paste0("Missing filter_base dimensions: ", paste(miss, collapse = ", "))
           }
           .meta_state[["excluded_surveys"]] <- rbind(
             .meta_state[["excluded_surveys"]],
             data.table::data.table(
               pip_id = dropped_entries$pip_id[[i]],
               reason = reason,
               stage = "filter_pre"
             )
           )
         }
       }
       ```

  3. **Dimension pre-filter stage** (lines 539-557 — surveys lacking requested dimensions):
     - Already harvested at lines 547-557, but without `stage` field
     - Add `stage = "dimension_pre"` column to harvest block

  Ensure `excluded_surveys` is initialized as `data.table(pip_id = character(0), reason = character(0), stage = character(0))` at `.meta_state` initialization.

- **Test Scenarios**:
  - Manifest exclusion: survey in `requested_pip_id` but not in manifest → harvested with `stage = "manifest"`
  - Filter-base exclusion: survey missing filter variable → harvested with `stage = "filter_pre"`
  - Dimension exclusion: survey missing requested dimension → harvested with `stage = "dimension_pre"`
  - Mixed exclusions: multiple surveys excluded at different stages → all harvested with correct stages
  - No exclusions: `excluded_surveys` is empty data.table (not NULL)

- **Tests**: `tests/testthat/test-table-maker-with-meta.R` (new test: `test_that("excluded_surveys stage tracking")`)

- **Acceptance criteria**:
  - All three exclusion points harvest into `excluded_surveys` with `stage` field
  - `excluded_surveys` always a data.table (empty or populated)
  - Exclusion reasons are specific (list missing dimensions, not generic)

---

### 4. Add release and PPP resolution tracking

- **Requirements**: R4
- **Files**: `R/table_maker.R` (lines 409 manifest lookup, 588-597 load_surveys call + ppp_used harvest)
- **Details**:

  1. **Resolve release once and track it**:
     - At line 409, resolve release: `resolved_release <- release %||% piptm_current_release()`
     - Use `resolved_release` consistently for manifest lookup, registry calls, and load_surveys
     - Store in `.meta_state[["resolved_release"]]` immediately after resolution

  2. **Track resolved PPP**:
     - After `load_surveys()` call (line 592), `load_surveys()` may skip surveys lacking the PPP column
     - The effective PPP is the requested `ppp` (or 2021L default) that was actually used
     - Store in `.meta_state[["resolved_ppp"]]` after load_surveys returns successfully

  3. **Track physical PPP column used**:
     - `load_surveys()` resolves `ppp` to a physical column like `"welfare_ppp_2021"` or `"welfare_ppp_2021_01_02"`
     - This information is not currently exposed by `load_surveys()`
     - **Approach**: Construct the expected column name from manifest `welfare_vars` using `.find_welfare_col()` helper
     - Store in `.meta_state[["ppp_column_used"]]`
     - Implementation:
       ```r
       # After load_surveys() call, before nrow(dt) check
       if (with_meta) {
         # Assume all surveys in `entries` have the same welfare_vars structure
         # (load_surveys already errored if they diverge)
         welfare_vars_sample <- entries$welfare_vars[[1L]]
         matched_col <- .find_welfare_col(welfare_vars_sample, ppp)
         if (length(matched_col) > 0L) {
           .meta_state[["ppp_column_used"]] <- matched_col[[1L]]
         } else {
           .meta_state[["ppp_column_used"]] <- NA_character_
         }
       }
       ```

  Thread `resolved_release` through all downstream calls (manifest, registry, load_surveys, compute_measures).

- **Test Scenarios**:
  - Explicit release: `release = "20260101"` → `resolved_release = "20260101"`
  - NULL release: `release = NULL` → `resolved_release = piptm_current_release()`
  - Explicit PPP: `ppp = 2017L` → `resolved_ppp = 2017L`, `ppp_column_used = "welfare_ppp_2017"`
  - Default PPP: `ppp = 2021L` (default) → `resolved_ppp = 2021L`, `ppp_column_used = "welfare_ppp_2021"`
  
- **Tests**: `tests/testthat/test-table-maker-with-meta.R` (new test: `test_that("release and PPP resolution tracking")`)

- **Acceptance criteria**:
  - `resolved_release` assigned once, used consistently throughout
  - `resolved_ppp` reflects effective PPP year used for loading
  - `ppp_column_used` reflects physical welfare column name
  - All three fields populated in `execution` block

---

### 5. Fix `measures_computed` to store measure names not families

- **Requirements**: R5
- **Files**: `R/table_maker.R` (lines 332-337 classify measures + populate measures_computed)
- **Details**:

  Current code at line 336:
  ```r
  .meta_state[["measures_computed"]] <- names(families)
  ```
  This stores family names like `c("summary_stats", "inequality")` instead of measure names like `c("mean", "gini")`.

  **Fix**: Store the original `measures` argument (the measure names requested) instead:
  ```r
  if (with_meta) {
    .meta_state[["measures_computed"]] <- measures
  }
  ```

  Semantic: `measures_computed` = the actual measure names that were dispatched to compute functions. Since `table_maker()` dispatches all requested measures (it doesn't skip any), this equals the `measures` argument.

  If in the future `table_maker()` might skip some measures conditionally, this field would need to track what was actually computed. For now, it's identical to requested measures.

- **Test Scenarios**:
  - Single measure: `measures = "mean"` → `measures_computed = "mean"`
  - Multiple measures: `measures = c("mean", "gini", "median")` → `measures_computed = c("mean", "gini", "median")`
  - Mixed families: `measures = c("headcount", "gini", "mean")` → `measures_computed = c("headcount", "gini", "mean")`

- **Tests**: `tests/testthat/test-table-maker-with-meta.R` (new test: `test_that("measures_computed stores measure names not families")`)

- **Acceptance criteria**:
  - `measures_computed` field contains measure names (e.g., "mean", "gini")
  - `measures_computed` does not contain family names (e.g., "summary_stats", "inequality")
  - Field matches `measures` argument exactly

---

### 6. Populate `filters_applied` field

- **Requirements**: R6
- **Files**: `R/table_maker.R` (lines 380-406 filter normalization, 402-404 metadata assignment)
- **Details**:

  `filters_applied` field is initialized at line 298 but never populated. The normalized filter is computed at lines 380-400 as `normalized_filter_base`.

  **Add assignment** at line 404 (after normalization, inside `if (!is.null(filter_base))` block):
  ```r
  # Populate filters_applied metadata
  if (with_meta) {
    .meta_state[["filters_applied"]] <- normalized_filter_base
  }
  ```

  Semantic: `filters_applied` = the normalized request (list of integer vectors per variable) that was passed to `load_surveys()`. This is **request-level** (what was asked) not execution-level (which surveys had which categories), consistent with `load_surveys()` behavior (uniform filter applied to all surveys).

- **Test Scenarios**:
  - Single filter variable: `filter_base = list(gender = c(0, 1))` → `filters_applied = list(gender = c(0L, 1L))`
  - Multiple filter variables: `filter_base = list(age_group = c(1,2), gender = 0)` → normalized integers
  - NULL filter: `filter_base = NULL` → `filters_applied = NULL`

- **Tests**: `tests/testthat/test-table-maker-with-meta.R` (new test: `test_that("filters_applied populated from normalized_filter_base")`)

- **Acceptance criteria**:
  - `filters_applied` field populated when `filter_base` is non-NULL
  - Field contains normalized integer vectors (not raw user input)
  - Field is NULL when no filters requested

---

### 7. Register warning handler via `withCallingHandlers()`

- **Requirements**: R9, R10
- **Files**: `R/table_maker.R` (lines 308-316 handler definition, 322-750 compute body)
- **Details**:

  **Current defect** (P0.1): Warning handler defined at lines 309-316 but never registered. Warnings are lost.

  **Fix append target** (P0.2): Line 311 has wrong append pattern:
  ```r
  # WRONG (appends to .meta_state root, not to warnings field)
  .meta_state[[length(.meta_state) + 1L]] <<- conditionMessage(w)
  ```

  Should be:
  ```r
  # CORRECT (appends to warnings field)
  .meta_state[["warnings"]][[length(.meta_state[["warnings"]]) + 1L]] <<- conditionMessage(w)
  ```

  **Register handler**: Wrap the compute body (currently `expression({...})` at lines 322-750) with `withCallingHandlers()` when `with_meta = TRUE`:

  ```r
  .compute_body <- expression({
    # ... existing compute body (lines 323-750) ...
  })

  # Execute, optionally wrapped in warning handler
  if (with_meta) {
    result <- eval(withCallingHandlers(
      .compute_body,
      warning = .warn_handler
    ))
  } else {
    result <- eval(.compute_body)
  }
  ```

  **Return logic**: After compute body execution, check `with_meta` and return accordingly:
  ```r
  if (with_meta) {
    # Build specification from registry lookups
    spec <- .build_specification(
      pip_id, analysis_var, measures, poverty_line, by, filter_base,
      ppp, pop_share_threshold, resolved_release
    )
    
    # Return list with all metadata
    return(list(
      data          = result,
      specification = spec,
      execution     = list(
        requested_pip_id = .meta_state[["requested_pip_id"]],
        loaded_surveys   = .meta_state[["loaded_surveys"]],
        excluded_surveys = .meta_state[["excluded_surveys"]],
        resolved_release = .meta_state[["resolved_release"]],
        resolved_ppp     = .meta_state[["resolved_ppp"]],
        ppp_column_used  = .meta_state[["ppp_column_used"]],
        filters_applied  = .meta_state[["filters_applied"]],
        measures_computed = .meta_state[["measures_computed"]],
        suppression      = .meta_state[["suppression"]]
      ),
      provenance = list(
        package_version = as.character(utils::packageVersion("piptm")),
        release         = .meta_state[["resolved_release"]]
      ),
      warnings = .meta_state[["warnings"]]
    ))
  }
  
  # Default path: return data.table unchanged
  result
  ```

- **Test Scenarios**:
  - Dimension exclusion warning: exclude 2 surveys → warning captured in `warnings` field
  - Suppression warning: suppress 3 cells → warning captured
  - Multiple warnings: dimension + suppression → both captured in order
  - No warnings: successful table → `warnings = character(0)`

- **Tests**: `tests/testthat/test-table-maker-with-meta.R` (new test: `test_that("warnings captured via registered handler")`)

- **Acceptance criteria**:
  - Warning handler registered via `withCallingHandlers()`
  - Warnings appended to correct field (not `.meta_state` root)
  - Warnings captured in execution order
  - Default path (`with_meta = FALSE`) unchanged

---

### 8. Thread `release` through `.build_specification()` and registry calls

- **Requirements**: R4
- **Files**: `R/table_maker.R` (lines 98-180 `.build_specification()` function)
- **Details**:

  **Current defect**: `.build_specification()` doesn't accept a `release` parameter. All registry calls use default `release = NULL`, which resolves to current release regardless of the `release` passed to `table_maker()`. This causes inconsistency when a user requests a specific historical release.

  **Fix**:
  1. Add `release` parameter to `.build_specification()` signature (line 98):
     ```r
     .build_specification <- function(pip_id, analysis_var, measures, poverty_line,
                                      by, filter_base, ppp, pop_share_threshold, release) {
     ```

  2. Thread `release` to all registry calls:
     - Line 102: `piptm_stat_groups(release = release)`
     - Line 103: `piptm_layout_covariates(release = release)`
     - Line 104: `piptm_filter_categories(release = release)`
     - Line 110: `piptm_analysis_variables(release = release)`

  3. Update the call site in Step 7 (warning handler return block) to pass `resolved_release`:
     ```r
     spec <- .build_specification(
       pip_id, analysis_var, measures, poverty_line, by, filter_base,
       ppp, pop_share_threshold, resolved_release  # <- pass resolved_release
     )
     ```

- **Test Scenarios**:
  - Explicit release: `release = "20260101"` → registry calls use "20260101"
  - NULL release: `release = NULL` → registry calls use current release
  - Verify labels match the requested release's registry (if registry varies by release)

- **Tests**: `tests/testthat/test-table-maker-with-meta.R` (test already exists for specification labels; verify release consistency)

- **Acceptance criteria**:
  - `.build_specification()` accepts `release` parameter
  - All internal registry calls pass `release` consistently
  - Call site passes `resolved_release` from execution metadata

---

### 9. Rewrite `test-table-maker-with-meta.R` for new schema

- **Requirements**: R1, R2, R3, R4, R5, R6, R10, R13
- **Files**: `tests/testthat/test-table-maker-with-meta.R`
- **Details**:

  Rewrite all `with_meta = TRUE` tests to match the new schema. Replace old assertions for `included_surveys`, `measures_computed`, etc. with new assertions for `requested_pip_id`, `loaded_surveys`, `excluded_surveys`, `resolved_release`, `resolved_ppp`, `ppp_column_used`, `filters_applied`, `measures_computed` (corrected), `suppression`, and `warnings`.

  **Test structure**:
  1. Schema completeness: all expected fields present
  2. `requested_pip_id` vs `loaded_surveys` distinction (test with exclusions)
  3. `excluded_surveys` stage tracking (manifest, filter_pre, dimension_pre)
  4. Release resolution (`resolved_release` consistent)
  5. PPP resolution (`resolved_ppp`, `ppp_column_used`)
  6. `measures_computed` stores measure names
  7. `filters_applied` populated correctly
  8. Warning capture functional
  9. Backward compatibility: `with_meta = FALSE` returns data.table

  Use fixture surveys from manifest (real surveys: IDN, FJI, FSM, MYS, PHL with mixed CON/INC types).

- **Test Scenarios**: (covered above in Steps 2-8)

- **Tests**: `tests/testthat/test-table-maker-with-meta.R` (full rewrite)

- **Acceptance criteria**:
  - All tests pass with new schema
  - Old schema assertions removed
  - Coverage includes all new fields and edge cases
  - Backward compatibility test confirms `with_meta = FALSE` unchanged

---

## Phase 2: Description Model Update

### 10. Update `build_description_model()` to consume new schema

- **Requirements**: R1, R2, R7, R8
- **Files**: `R/description.R` (lines 34-138 `build_description_model()`)
- **Details**:

  Update `build_description_model()` to read from the new `execution` and `specification` schemas:

  1. **Input validation** (lines 37-50): Update to expect new field names:
     - Check for `specification`, `execution`, `provenance` (unchanged)
     - Check for `execution$requested_pip_id`, `execution$loaded_surveys`, `execution$excluded_surveys`, `execution$resolved_release`, etc.

  2. **Surveys section** (lines 64-70): Use `execution$loaded_surveys` (not `included_surveys`):
     ```r
     surveys <- list(
       included = exec$loaded_surveys  # renamed from included_surveys
     )
     ```

  3. **Exclusions** (lines 68-70): Use `execution$excluded_surveys` with `stage` field:
     ```r
     if (nrow(exec$excluded_surveys) > 0L) {
       surveys$excluded_surveys <- exec$excluded_surveys  # now has stage column
     }
     ```

  4. **Metadata section** (lines 57-61): Add resolved release and PPP:
     ```r
     metadata <- list(
       release         = exec$resolved_release,   # from execution, not provenance
       package_version = prov$package_version,
       ppp             = exec$resolved_ppp,       # from execution, not specification
       ppp_column      = exec$ppp_column_used     # new field
     )
     ```

  5. **Measures** (lines 86-89): Verify measures come from specification (measure names with labels), execution provides confirmation

  6. **Layout roles** (lines 124-127): Document the position-based inference contract clearly in comments:
     ```r
     # Layout roles are inferred from dimension order:
     #   last dimension = columns
     #   second-to-last = rows
     #   third-to-last = super_columns
     #   fourth-to-last = super_rows
     # This is the documented contract; roles are not stored in specification.by
     ```

  Update all field references to match new schema throughout the function.

- **Test Scenarios**:
  - New schema fields read correctly
  - `loaded_surveys` used (not `included_surveys`)
  - `excluded_surveys` with `stage` field displayed
  - Resolved release and PPP displayed
  - Layout role inference matches position-based contract

- **Tests**: `tests/testthat/test-description-model.R` (rewrite for new schema)

- **Acceptance criteria**:
  - Model builder reads from new schema correctly
  - All new fields incorporated into description model
  - Old field references removed
  - Layout role contract documented

---

### 11. Update `render_description_markdown()` for new schema

- **Requirements**: R1, R2, R3, R7, R8
- **Files**: `R/description.R` (lines 154-313 `render_description_markdown()`)
- **Details**:

  Update markdown renderer to display new metadata fields:

  1. **Metadata line** (lines 180-186): Add resolved PPP and physical column:
     ```markdown
     **Release:** {resolved_release} · **PPP year:** {resolved_ppp} · **Column:** {ppp_column_used}
     ```

  2. **Surveys section** (lines 188-217): Update header to use `loaded_surveys`:
     ```markdown
     ## Surveys Analyzed
     
     {nrow(loaded_surveys)} survey(s) contributed data to this table.
     
     | Country | Year | Welfare type | Survey ID |
     ...
     ```

  3. **Excluded surveys** (lines 203-217): Add `stage` column to exclusion table:
     ```markdown
     ### Excluded Surveys
     
     | Survey ID | Stage | Reason |
     |-----------|-------|--------|
     | {pip_id} | {stage} | {reason} |
     
     {n_excl} of {n_total} requested survey(s) were excluded.
     ```

  4. **Edge cases**:
     - If `loaded_surveys` is empty: "No surveys contributed data (all excluded)."
     - If `excluded_surveys` is empty: omit exclusion section
     - If no dimensions (`by = NULL`): "No disaggregation dimensions requested."

  Update all field references to match new schema.

- **Test Scenarios**:
  - Resolved PPP and column displayed
  - `loaded_surveys` count correct
  - Exclusion table shows stage column
  - Edge cases render correctly (no surveys, all excluded, no dimensions)

- **Tests**: `tests/testthat/test-description-renderer.R` (update for new schema)

- **Acceptance criteria**:
  - Markdown output reflects new metadata fields
  - Exclusion stage displayed
  - Edge cases handled gracefully
  - Output matches expected format

---

### 12. Rewrite `test-description-model.R` and `test-description-renderer.R`

- **Requirements**: R1, R13
- **Files**: `tests/testthat/test-description-model.R`, `tests/testthat/test-description-renderer.R`
- **Details**:

  Rewrite both test files to use the new schema:

  **`test-description-model.R`**:
  - Update mock `table_result` fixtures to use new schema (helper-description.R may need updates)
  - Test that model reads `loaded_surveys`, `excluded_surveys` with stage, resolved release/PPP
  - Test edge cases: empty exclusions, no dimensions, NULL poverty line

  **`test-description-renderer.R`**:
  - Update assertions to check for new markdown sections
  - Test that exclusion stage appears in rendered output
  - Test edge cases rendering

  Use `helper-description.R` for mock fixture generation. Update `make_mock_table_result()` to generate new schema (replace `runif()` with deterministic values per P3.1 finding).

- **Test Scenarios**: (covered in Steps 10-11)

- **Tests**: Both test files fully rewritten

- **Acceptance criteria**:
  - All model tests pass with new schema
  - All renderer tests pass with new schema
  - Mock fixtures use deterministic data (no `runif()`)
  - Edge cases tested

---

## Phase 3: Renderer & Edge Cases

### 13. Fix cell definition generation using execution truth

- **Requirements**: R8
- **Files**: `R/description.R` (lines 379-417 `.generate_cell_definition()`)
- **Details**:

  Update `.generate_cell_definition()` to use execution metadata where appropriate:

  1. Accept both `spec` and `exec` parameters (currently only accepts `spec`):
     ```r
     .generate_cell_definition <- function(spec, exec) {
     ```

  2. Use execution fields for accuracy:
     - Measures: use `exec$measures_computed` (actual measures) not `spec$measures` (requested)
     - Check if measures match specification; if divergence, note it

  3. Add poverty line from specification (unchanged — it's a request parameter)

  4. Handle edge cases:
     - 0 dimensions: "Statistics computed for the full survey sample."
     - 1 dimension: "Each cell represents one {var1} category."
     - 2 dimensions: "Each cell represents one combination of {var1} and {var2}."
     - 3-4 dimensions: "Each cell represents one combination of {var1}, {var2}, {var3}[, and {var4}]."

  Update call site in `build_description_model()` (line 135):
  ```r
  model$cell_definition <- .generate_cell_definition(spec, exec)
  ```

- **Test Scenarios**:
  - 0 dimensions: no `by` → correct sentence
  - 1 dimension: `by = "gender"` → correct sentence
  - 2 dimensions: `by = c("gender", "area")` → correct sentence
  - 4 dimensions: `by = c("age", "gender", "area", "educat")` → correct sentence
  - Poverty line: included when non-NULL, omitted when NULL

- **Tests**: `tests/testthat/test-description-renderer.R` (update cell definition tests)

- **Acceptance criteria**:
  - Cell definition uses execution truth
  - All dimension counts (0-4) render correctly
  - Poverty line handled correctly

---

### 14. Handle edge cases in description renderer

- **Requirements**: R1, R8
- **Files**: `R/description.R` (lines 154-313 `render_description_markdown()`)
- **Details**:

  Add explicit edge case handling:

  1. **Empty surveys** (all excluded):
     - Check `nrow(model$surveys$included) == 0`
     - Render: "No surveys contributed data to this table. All requested surveys were excluded."
     - Display exclusion table showing why all were excluded

  2. **No dimensions**:
     - Check `is.null(model$layout)` or `length(model$layout$variables) == 0`
     - Render: "## Table Structure\n\nNo disaggregation dimensions. Statistics computed at survey level only."

  3. **Suppression edge cases**:
     - If `n_suppressed_cells == 0`: "No cells were suppressed."
     - If `n_suppressed_cells > 0`: "X cell(s) were suppressed."
     - If `threshold` is NULL: "Suppression disabled."

  4. **Singular grammar**:
     - Use `{?s}` pluralization for counts: "X survey{?s}", "X cell{?s}"

  5. **Empty warnings**:
     - Check `length(model$warnings) == 0` → omit warnings section entirely

- **Test Scenarios**:
  - All surveys excluded → description explains why
  - No dimensions → structure section says "survey level only"
  - Suppression disabled → correct message
  - Singular counts: 1 survey, 1 cell → singular grammar
  - No warnings → warnings section omitted

- **Tests**: `tests/testthat/test-description-renderer.R` (add edge case tests)

- **Acceptance criteria**:
  - All edge cases render correct explanatory text
  - No cryptic output for empty/unusual cases
  - Grammar correct for singular/plural counts

---

### 15. Update API endpoint tests for new schema

- **Requirements**: R11, R12, R13
- **Files**: `tests/testthat/test-api-description-endpoint.R`
- **Details**:

  Update `/description` endpoint tests to expect new schema in JSON response:

  1. **Response structure**: Verify `data.model` and `data.markdown` present
  2. **Model schema**: Check that `data.model` has new `execution` and `specification` fields
  3. **Backward compatibility**: Verify `/table` endpoint unchanged (no `with_meta` flag exposed in API)
  4. **Warning capture**: Verify warnings from `table_maker` are included in response `warnings` field
  5. **Error handling**: Test validation errors, empty surveys, all excluded

  Use shared API test helpers from `helper-api.R` (created per P2.10 fix).

- **Test Scenarios**:
  - Happy path: valid request → model + markdown returned
  - Validation error: invalid `pip_id` → 400 error
  - Exclusion warning: some surveys excluded → warnings in response
  - Empty result: all surveys excluded → 422 error (table_maker aborts)

- **Tests**: `tests/testthat/test-api-description-endpoint.R` (update for new schema)

- **Acceptance criteria**:
  - API returns new schema correctly
  - Warnings captured and returned
  - Error handling unchanged
  - `/table` endpoint backward compatible

---

### 16. Update convenience wrapper `build_table_description()`

- **Requirements**: R1
- **Files**: `R/description.R` (lines 344-369 `build_table_description()`)
- **Details**:

  No structural changes needed — convenience wrapper already calls `table_maker(with_meta = TRUE)`, `build_description_model()`, and `render_description_markdown()` in sequence. The updates in Steps 10-14 propagate automatically.

  **Verify**:
  - Function signature unchanged (accepts same parameters as `table_maker()`)
  - Returns markdown string (not model)
  - Works end-to-end with new schema

- **Test Scenarios**:
  - Call with various parameter combinations → correct markdown returned
  - Warnings captured → included in markdown output
  - Edge cases → handled gracefully

- **Tests**: `tests/testthat/test-description-convenience.R` (verify works with new schema)

- **Acceptance criteria**:
  - Convenience wrapper works end-to-end with new schema
  - No code changes needed (changes propagate from model/renderer)
  - Tests pass

---

### 17. Run full verification suite

- **Requirements**: R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12, R13, R14, R15, R16
- **Files**: All project files
- **Details**:

  **Pre-merge verification checklist**:

  1. **Test suite**: Run `devtools::test()` — all 128+ default-path tests pass, all new `with_meta = TRUE` tests pass
  2. **Documentation**: Run `devtools::document()` — no warnings, all exports documented
  3. **Check**: Run `devtools::check()` — zero errors, zero warnings, zero notes
  4. **Performance benchmark**: Measure `table_maker(with_meta = TRUE)` overhead vs. default:
     - Test with 15-survey batch
     - Confirm metadata overhead < 10% (NF1 requirement)
     - If >10%, profile and optimize
  5. **API integration**: Test `/description` endpoint manually with Insomnia collection
  6. **Backward compatibility**: Confirm `/table` endpoint unchanged (no schema breaks)
  7. **Git status**: Clean working tree, all tests committed

  **Blocked-stop conditions**:
  - If tests fail: halt, fix, re-verify
  - If performance overhead >10%: halt, profile, optimize
  - If `R CMD check` has errors/warnings: halt, fix

- **Test Scenarios**: (comprehensive — covered throughout plan)

- **Tests**: Full `devtools::test()` suite

- **Acceptance criteria**:
  - All tests pass
  - Documentation complete
  - `R CMD check` clean
  - Performance overhead < 10%
  - Backward compatibility confirmed

---

## Testing Strategy

**Test organization**:
- `test-table-maker-with-meta.R`: Metadata harvesting, schema correctness, warning capture, backward compatibility
- `test-description-model.R`: Model builder reads new schema correctly, edge cases
- `test-description-renderer.R`: Markdown rendering, edge cases, layout roles, cell definition
- `test-api-description-endpoint.R`: API integration, error handling, JSON schema
- `test-description-convenience.R`: End-to-end convenience wrapper

**Test data**:
- Use fixture surveys from manifest (IDN, FJI, FSM, MYS, PHL)
- Mock `table_result` fixtures should use deterministic data (no `runif()` per P3.1)
- Test with mixed CON/INC welfare types, mixed dimensions

**Coverage targets**:
- Schema completeness: all new fields tested
- Exclusion stages: all three stages tested
- Edge cases: empty surveys, no dimensions, all excluded, singular grammar
- Backward compatibility: default path unchanged

## Documentation Checklist

- [ ] Update `@param with_meta` in `table_maker()` roxygen (line 230)
- [ ] Update `@return` to describe new schema (lines 234-253)
- [ ] Document `.build_specification()` new `release` parameter
- [ ] Document layout role inference contract in `build_description_model()` comments
- [ ] Update `README.md` to reference new metadata fields (if user-facing)
- [ ] Add inline comments documenting new schema at `.meta_state` initialization
- [ ] Update `NAMESPACE` if any new exports (none expected — internal refactor only)

## Risks & Mitigations

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Breaking `with_meta = TRUE` schema breaks downstream consumers | High | Low (no external users yet per user confirmation) | Documented as corrective plan; no migration needed |
| Metadata overhead >10% impacts performance | Medium | Low (lightweight metadata) | Benchmark in Phase 1, optimize if needed |
| Exclusion stage tracking adds complexity | Low | Medium | Clear documentation, comprehensive tests |
| Registry `release` parameter inconsistency | High | Low | Thread `resolved_release` consistently in Step 8 |
| Warning handler breaks default path | High | Low | Explicit `if (with_meta)` guards, test backward compatibility |
| Layout role inference unclear to future maintainers | Medium | Medium | Document contract explicitly in code comments |

## Out of Scope

- Per-survey PPP differences (PPP is uniform across batch)
- Structured warning objects (keeping strings for simplicity)
- Per-survey filter category availability tracking (requires new infrastructure)
- Reproducibility code snippets (deferred to future iteration)
- Timestamp metadata (`generated_at` in provenance — optional, low priority)
- Layout roles explicit in registry (deferred unless inference proves problematic)
- S3 `table_result` class wrapper (deferred)
- Server-side PDF generation (deferred)
- Multiple languages (deferred)

## Completion Contract

### Outcome

The `table_maker(with_meta = TRUE)` metadata contract accurately reflects execution truth. The description model and markdown renderer use renamed and corrected fields (`requested_pip_id`, `loaded_surveys`, `excluded_surveys` with stages, `resolved_release`, `resolved_ppp`, `ppp_column_used`, `filters_applied`, `measures_computed` as measure names, working warning capture). All tests pass, backward compatibility preserved, and performance overhead < 10%.

### Verification Surface

| ID | Evidence Required | Command/Artifact | Phase | Required |
|----|-------------------|------------------|-------|----------|
| V1 | All 128+ default-path tests pass unchanged | `devtools::test()` — 0 failures, 0 errors | final | yes |
| V2 | New `with_meta = TRUE` tests pass | `devtools::test()` — test-table-maker-with-meta.R all pass | 1 | yes |
| V3 | `requested_pip_id` captured at function entry | test assertion: `result$execution$requested_pip_id == pip_id` | 1 | yes |
| V4 | `loaded_surveys` populated after all exclusions | test assertion: excludes dimension-filtered surveys | 1 | yes |
| V5 | `excluded_surveys` with stage tracking | test assertion: stage = "manifest" / "filter_pre" / "dimension_pre" | 1 | yes |
| V6 | `resolved_release` used consistently | test assertion: release matches throughout | 1 | yes |
| V7 | `resolved_ppp` and `ppp_column_used` tracked | test assertion: both fields populated correctly | 1 | yes |
| V8 | `measures_computed` stores measure names | test assertion: no family names, only measure names | 1 | yes |
| V9 | `filters_applied` populated | test assertion: field matches normalized_filter_base | 1 | yes |
| V10 | Warning handler registered and functional | test assertion: warnings captured in result$warnings | 1 | yes |
| V11 | `release` threaded through `.build_specification()` | code inspection: all registry calls pass release | 1 | yes |
| V12 | Description model reads new schema | test-description-model.R — all tests pass | 2 | yes |
| V13 | Markdown renderer displays new fields | test-description-renderer.R — output includes resolved_release, ppp_column, stage | 2 | yes |
| V14 | Edge cases handled gracefully | tests for 0 dimensions, all excluded, singular grammar | 3 | yes |
| V15 | Cell definition uses execution truth | test assertion: cell definition accurate for 0-4 dimensions | 3 | yes |
| V16 | API endpoint returns new schema | test-api-description-endpoint.R — JSON schema matches | 3 | yes |
| V17 | Convenience wrapper works end-to-end | test-description-convenience.R — all tests pass | 3 | yes |
| V18 | Documentation complete | `devtools::document()` — 0 warnings, man pages updated | final | yes |
| V19 | `R CMD check` clean | `devtools::check()` — 0 errors, 0 warnings, 0 notes | final | yes |
| V20 | Performance overhead < 10% | Benchmark `with_meta = TRUE` vs. FALSE — overhead < 10% | final | yes |

### Constraints

| ID | Constraint | Check | Phase |
|----|------------|-------|-------|
| C1 | Default `table_maker()` return must be data.table | V1 — `is.data.table(table_maker(...))` is TRUE | final |
| C2 | `/table` endpoint JSON contract unchanged | V1 — all existing API tests pass without modification | final |
| C3 | No new hard dependencies | DESCRIPTION unchanged (no new Imports) | final |
| C4 | Backward compatibility for default path | V1 — 128+ tests pass unchanged | final |
| C5 | Metadata overhead < 10% | V20 — benchmark confirms | final |

### Boundaries

**Allowed**:
- Redesign `with_meta = TRUE` metadata schema (no external users)
- Rename fields (e.g., `included_surveys` → `loaded_surveys`)
- Add new fields (`requested_pip_id`, `resolved_release`, `resolved_ppp`, `ppp_column_used`, `stage`)
- Rewrite `with_meta = TRUE` tests to match new schema
- Update description model and renderer to consume new schema
- Modify internal functions (`.build_specification()`, `.generate_cell_definition()`)

**Out of scope**:
- Changing `/table` endpoint contract
- Adding new exports (internal refactor only)
- Breaking default `with_meta = FALSE` behavior
- Per-survey PPP tracking (PPP is uniform)
- Structured warning objects (keeping strings)
- Per-survey filter availability (request-level only)
- Reproducibility code snippets (deferred)
- Layout roles in registry (deferred)
- Server-side PDF (deferred)

### Iteration Policy

1. **Phase 1 first**: Complete all metadata harvesting fixes before touching model/renderer
2. **Test each step**: Write/update tests immediately after implementing each step
3. **Verify backward compatibility**: Run full test suite after each phase
4. **Benchmark after Phase 1**: Measure performance overhead; halt if >10%, optimize
5. **No partial shipment**: All 3 phases required; schema redesign is atomic

### Blocked-Stop Conditions

- If Phase 1 takes >5 days: halt, reassess complexity, consider simplification
- If metadata overhead >10%: halt, profile, optimize before proceeding to Phase 2
- If tests fail after Phase 1: halt, fix harvesting logic before model/renderer work
- If `R CMD check` has errors/warnings: halt, fix documentation/code issues
- If backward compatibility breaks: halt, investigate default path contamination
