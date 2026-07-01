---
date: 2026-07-01
title: "Table Endpoint & Computation Layer Refactor — Multi-Analysis-Variable Support"
status: completed
completed-date: 2026-07-01
completed-phases: [1, 2, 3, 4, 5]
current-phase: null
scope: Deep
brainstorm: null
language: R
estimated-effort: large
deviation-policy: ask
execution-report: .cg-docs/work-reports/2026-07-01-table-endpoint-computation-refactor.md
tags: [api, computation, refactor, multi-variable, welfare-analysis]
---

# Plan: Table Endpoint & Computation Layer Refactor

## Objective

Refactor the piptm computation engine and Plumber API to support multiple analysis variables (not just welfare) while maintaining backward compatibility with existing tests and architectural patterns. The refactor introduces a variable type system (`welfare`, `poverty`, `continuous`, `binary`) that routes requests to appropriate computation families (`summary_stats`, `inequality`, `poverty`, `shares`). All changes prioritize correctness (the specification takes precedence over existing code) and maintain the deterministic, reproducible outputs expected by downstream PIP platform consumers.

## Context

**Current state:**
- The engine assumes welfare is always the analysis variable
- `/table` endpoint accepts `poverty_lines` (plural, repeatable) and does not have an `analysis_var` parameter
- `table_maker()`, `compute_measures()`, and their test suites are built around single-welfare assumption
- `.MEASURE_REGISTRY` uses "summary_statistics" but canonical dispatch code refers to "summary_stats" (family mismatch)
- Three confirmed bugs in dispatch logic and conditional validation

**New requirements:**
- Support analysis variables with type-specific routing: welfare (inequality + summary_stats), poverty (special FGT routing), continuous (summary_stats), binary (shares)
- `pov_status` is a derived binary column, never loaded from Parquet; it routes poverty measure computation when used as analysis_var
- `poverty_line` becomes a scalar parameter (not repeatable); passed through to compute_poverty() as poverty_lines = poverty_line
- All test suites must be rewritten to reflect the new design (no backward compatibility with single-welfare tests)

**Project constraints (from compound-gpid.md):**
- Deterministic outputs required (identical inputs → identical results)
- All computations reproducible from survey data + parameters
- Fail loudly (no silent fallbacks for NA weights, missing data)
- Code must handle partitioned Arrow/Parquet datasets efficiently

## Requirements

| ID  | Requirement | Source |
|-----|-------------|--------|
| R1  | Fix three existing bugs (dispatch, registry family mismatch, conditional validation) | Task 1 spec |
| R2  | Add `analysis_var` as required query parameter to /table endpoint | Task 2 spec |
| R3  | Rename `poverty_lines` → `poverty_line` (scalar) in endpoint, validation, and table_maker() | Task 2 spec |
| R4  | Implement variable type system routing (welfare → inequality+summary_stats, poverty → poverty, continuous → summary_stats, binary → shares) | Task 3 spec |
| R5  | Derive pov_status binary column from welfare < poverty_line in table_maker() | Task 3 spec |
| R6  | Update compute_measures() to accept analysis_var instead of target_variable | Task 4 spec |
| R7  | Update needed_cols logic to conditionally load analysis_var and handle pov_status derivation | Task 3 spec |
| R8  | Replace all existing table_maker() and /table endpoint tests with new multi-variable test suite | Task 5 spec |
| R9  | Validate cross-requirement: poverty_line required when analysis_var == "pov_status" or "pov_status" ∈ by | Task 2 spec |
| R10 | Ensure pov_status excluded from manifest dimension pre-filter and NA-filling logic (it is derived, not loaded) | Task 3 spec |

## Implementation Steps

### Phase 1: Bug Fixes & Preparation

#### 1. Fix compute_measures() dispatch bug (R1)

**Files**: `R/compute_measures.R`

**Details**: 
- Line that currently reads: `results$summary_stats <- compute_shares(..., measures = classified$summary_stats, ...)`
- Overwrites the summary_stats slot and passes wrong measures vector to compute_shares
- Change to: `results$shares <- compute_shares(..., measures = classified$shares, target_variable = target_variable, ...)`
- Verify the shares result is keyed correctly before the rbindlist merge

**Test scenarios**:
- happy path: shares measures requested alongside summary_stats → both appear in final output, no overwrites
- error path: verify rbindlist(fill=TRUE) properly aligns shares columns (population, measure, value) with other results

**Acceptance criteria**: `results` list carries both $summary_stats and $shares slots; shares are not overwritten; rbindlist produces long-format output with correct measure labels.

---

#### 2. Fix .MEASURE_REGISTRY family key mismatch — use "summary_stats" consistently (R1, R2)

**Files**: `R/measures.R`

**Details**:
- `.MEASURE_REGISTRY` currently uses "summary_statistics"; `.classify_measures()` and dispatch code use "summary_stats"
- **Decision**: Standardize on "summary_stats" everywhere (shorter, clearer, consistent with existing dispatch comments)
- Three locations to update:
  1. `.MEASURE_REGISTRY` — rename "summary_statistics" → "summary_stats" for all summary stat measures
  2. `.classify_measures()` — canonical order lists "summary_stats" (not "summary_statistics")
  3. `compute_measures()` dispatch — use `if ("summary_stats" %in% families)` to match

**Test scenarios**:
- happy path: request mean + median → classified result has $summary_stats with both measures; dispatch sends correct vector to compute_summary_stats()
- verify no typo between registry key and dispatch condition

**Acceptance criteria**: All family names in .MEASURE_REGISTRY use "summary_stats"; canonical order in .classify_measures() lists "summary_stats"; dispatch conditions consistently use `"summary_stats"`.

---

#### 3. Fix compute_shares() boolean condition bug and redesign for multi-measure support (R1)

**Files**: `R/compute_shares.R`

**Details**:
- Current code: `if (is.null(target_variable || target_variable == "welfare"))` is invalid R
- Current design flaw: The function treats shares computation as a binary router (either population shares OR target shares), but the three share measures are independent and can be requested in any combination:
  - `pop_share`: fraction of total survey population in each cell (target_variable irrelevant)
  - `target_within_group_share`: within each cell, fraction with target_variable == 1
  - `target_survey_share`: fraction of total survey with both cell condition AND target_variable == 1

**Redesign**: Refactor compute_shares() to accept and compute any combination of these three measures in a single call:
1. Accept `measures` parameter (character vector, subset of the three share types)
2. Compute population shares unconditionally (always needed for denominators)
3. Compute target shares only when target_variable is non-NULL and not "welfare"
4. Return long-format output: one row per cell per requested measure (NOT split by values of target_variable)

**New structure**:
```r
compute_shares <- function(dt, by = NULL, measures = NULL, target_variable = NULL, grp = NULL) {
  # Validate that measures is subset of c("pop_share", "target_within_group_share", "target_survey_share")
  # Error if target_within_group_share/target_survey_share requested but target_variable is NULL or "welfare"
  # Compute weights and population per cell
  # For each requested measure:
  #   - pop_share: share of total survey population in cell
  #   - target_within_group_share: share within cell with target==1
  #   - target_survey_share: share of total survey in (cell AND target==1)
  # Return one row per cell per measure
}
```

**Test scenarios**:
- happy path: measures = "pop_share" only → output has one row per cell
- happy path: measures = c("pop_share", "target_within_group_share"), target_variable = "female", by = "area" → output has 2*n_areas rows (not 4*n_areas)
- happy path: measures = "target_survey_share", target_variable = "female" → output has one row per cell
- error path: measures includes target_within_group_share but target_variable = NULL → error
- error path: measures includes target_survey_share but target_variable = "welfare" → error

**Acceptance criteria**: 
- compute_shares() accepts `measures` parameter as character vector
- pop_share computed regardless of target_variable
- target shares computed only when target_variable is provided and is not NULL/"welfare"
- Output: exactly one row per cell per measure (not expanded by target_variable values)
- Proper error handling for invalid measure+target_variable combinations

---

### Phase 2: Endpoint & Input Validation Refactor

#### 4. Update /table Plumber endpoint signature and validation (R2, R3, R9)

**Files**: `inst/plumber/plumber.R`, `inst/plumber/helpers.R`

**Details**:

A. **Update endpoint roxygen + signature** (plumber.R, lines ~119–125):
   ```r
   #* @param analysis_var:character Analysis variable name (required)
   #* @param pip_id:character Survey identifiers (max 15, repeatable)
   #* @param measures:character Measure names (repeatable)
   #* @param poverty_line:numeric Single poverty line value (required when analysis_var is "pov_status")
   #* @param by:character Disaggregation dimensions (optional, repeatable)
   #* @param ppp:integer PPP reference year (optional; default 2021)
   #* @param filter_base:character JSON-encoded sample-base filter (optional)
   #* @param pop_share_threshold:numeric Cell suppression threshold (optional)
   #* @param release:character Release ID (optional)
   #* @get /table
   #* @post /table
   function(analysis_var, pip_id = NULL, measures = NULL, poverty_line = NULL,
            by = NULL, ppp = 2021L, filter_base = NULL,
            pop_share_threshold = 0.01, release = NULL, res)
   ```

B. **Call validate_table_input()** with new signature:
   ```r
   check <- validate_table_input(
     analysis_var = analysis_var,
     pip_id       = pip_id,
     measures     = measures,
     poverty_line = poverty_line,
     by           = by,
     ppp          = ppp,
     pop_share_threshold = pop_share_threshold
   )
   ```

C. **Pass to table_maker()**:
   ```r
   data <- piptm::table_maker(
     pip_id              = pip_id,
     analysis_var        = analysis_var,
     measures            = measures,
     poverty_line        = poverty_line,
     by                  = by,
     ppp                 = ppp,
     filter_base         = parsed_filter_base,
     release             = rel,
     pop_share_threshold = pop_share_threshold
   )
   ```

**Test scenarios**:
- happy path: analysis_var = "welfare", measures = c("gini", "mean"), no poverty_line → OK
- happy path: analysis_var = "pov_status", measures = c("headcount"), poverty_line = 2.15 → OK
- error path: analysis_var = "pov_status", no poverty_line → 400 error
- error path: analysis_var = "female", by includes "pov_status", no poverty_line → 400 error
- error path: analysis_var = "unknown_var" → 400 error

**Acceptance criteria**: Endpoint accepts analysis_var parameter; poverty_line is scalar; validation prevents pov_status without poverty_line.

---

#### 5. Update validate_table_input() in helpers.R (R2, R3, R9)

**Files**: `inst/plumber/helpers.R`

**Details**:

A. **New parameter**: analysis_var (character scalar, required, non-empty)
B. **Updated parameter**: poverty_line (replaces poverty_lines) — must be single positive finite numeric when provided; renamed in function signature, docstring, and error messages
C. **New validation logic**:
   ```r
   # analysis_var validation (new)
   if (is.null(analysis_var) || !is.character(analysis_var) || length(analysis_var) != 1L) {
     errors <- c(errors, "`analysis_var` must be a single non-empty character value.")
   }
   
   # poverty_line validation (replaces poverty_lines)
   coerced_pl <- NULL
   if (!is.null(poverty_line)) {
     coerced_pl <- suppressWarnings(as.numeric(poverty_line))
     if (length(coerced_pl) != 1L || is.na(coerced_pl)) {
       errors <- c(errors, "`poverty_line` must be a single positive numeric value when provided.")
       coerced_pl <- NULL
     } else if (!is.finite(coerced_pl) || coerced_pl <= 0) {
       errors <- c(errors, "`poverty_line` must be positive and finite.")
       coerced_pl <- NULL
     }
   }
   
   # Cross-validation: poverty_line required when analysis_var == "pov_status" or "pov_status" in by
   if ((analysis_var == "pov_status" || (analysis_var != "pov_status" && "pov_status" %in% by)) && is.null(coerced_pl)) {
     errors <- c(errors, "`poverty_line` is required when analysis_var is 'pov_status' or when 'pov_status' is used as a disaggregation dimension.")
   }
   ```

D. **Return value**: update docstring; return list includes `poverty_line` (not `poverty_lines`); remove the plural form entirely

**Test scenarios**:
- happy path: analysis_var = "welfare", poverty_line = NULL → passes, coerced_pl = NULL
- happy path: analysis_var = "pov_status", poverty_line = "2.15" → passes, coerced_pl = 2.15
- error path: analysis_var = NULL → error
- error path: analysis_var = "pov_status", poverty_line = NULL → error
- error path: poverty_line = "not_a_number" → error

**Acceptance criteria**: analysis_var validated as required scalar; poverty_line validated as optional scalar; cross-validation catches pov_status without poverty_line.

---

### Phase 3: Core Function Refactoring

#### 6. Update table_maker() signature and internal derivation (R3, R4, R5, R6, R7)

**Files**: `R/table_maker.R`

**Details**:

A. **New signature**:
   ```r
   table_maker <- function(
     pip_id              = NULL,
     analysis_var,              # NEW — required
     measures,
     poverty_line        = NULL, # RENAMED from poverty_lines; scalar
     by                  = NULL,
     filter_base         = NULL,
     ppp                 = 2021L,
     release             = NULL,
     pop_share_threshold = 0.01
   )
   ```

B. **Remove parameters**: 
   - country_code, year, welfare_type (document as out of scope; not removed but leave unimplemented — the triplet lookup pattern is handled upstream in pip_lookup())
   - target_variable (derived internally from analysis_var)

C. **Early derivation** (after pip_id resolution):
   ```r
   # analysis_var is the API-facing name (matches varname in /analysis-variables)
   # target_variable is the internal name passed to compute functions
   # They are identical except for pov_status which routes to compute_poverty()
   target_variable <- if (analysis_var == "pov_status") NULL else analysis_var
   ```

D. **Updated needed_cols** (replaces old logic, ~lines 250–280):
   ```r
   needs_welfare <- is.null(analysis_var) ||
                    analysis_var %in% c("welfare", "pov_status") ||
                    (!is.null(by) && "pov_status" %in% by)
   
   needed_cols <- unique(c(
     "pip_id", "country_code", "surveyid_year", "welfare_type",
     "weight",
     if (needs_welfare) "welfare",
     if (!is.null(analysis_var) && !analysis_var %in% c("welfare", "pov_status")) analysis_var,
     by[!by %in% "pov_status"],   # pov_status is derived, not loaded
     filter_vars
   ))
   ```

E. **Manifest dimension pre-filter** (replaces old lines ~320–330):
   ```r
   by_check <- by[!by %in% "pov_status"]
   if (!is.null(by_check) && length(by_check) > 0L) {
     # Only filter manifest if by_check is non-empty after exclusion
     # pov_status is never in entry$dimensions since it is not a Parquet column
   }
   ```

F. **pov_status derivation** (after load_surveys(), alongside age binning, ~lines 350–370):
   ```r
   pov_status_was_derived <- FALSE
   if (!is.null(by) && "pov_status" %in% by) {
     if (is.null(poverty_line) || !is.numeric(poverty_line) || 
         length(poverty_line) != 1L || poverty_line <= 0) {
       cli_abort(
         c(
           "{.arg poverty_line} is required when {.val pov_status} is used as a disaggregation dimension.",
           "i" = "Provide a single positive numeric scalar."
         )
       )
     }
     dt[, pov_status := as.integer(welfare < poverty_line)]
     pov_status_was_derived <- TRUE
   }
   ```

G. **Call to compute_measures()**:
   ```r
   result <- compute_measures(
     dt,
     measures     = measures,
     analysis_var = analysis_var,
     poverty_line = poverty_line,
     by           = by
   )
   ```

**Test scenarios**:
- happy path: analysis_var = "welfare", measures = c("gini", "mean"), by = c("gender", "area"), no poverty_line → needed_cols includes welfare; compute_measures receives analysis_var = "welfare"
- happy path: analysis_var = "pov_status", measures = c("headcount"), poverty_line = 2.15, by = NULL → needed_cols includes welfare; pov_status not derived (not in by); compute_measures routes to poverty
- happy path: analysis_var = "pov_status", by includes "pov_status", poverty_line = 2.15 → pov_status column created after load_surveys()
- happy path: analysis_var = "female" (binary), measures = "pop_share" → needed_cols includes female; compute_measures routes to shares
- error path: analysis_var = "pov_status", by includes "pov_status", no poverty_line → error at derivation step

**Acceptance criteria**: 
- analysis_var routed through entire pipeline
- target_variable derived correctly
- needed_cols includes only required columns (welfare when needed, analysis_var otherwise, pov_status excluded)
- pov_status column created in dt when needed
- compute_measures called with correct parameters

---

#### 7. Update compute_measures() signature and dispatch (R4, R6)

**Files**: `R/compute_measures.R`

**Details**:

A. **New signature**:
   ```r
   compute_measures <- function(dt, measures, analysis_var = NULL, 
                                poverty_line = NULL, by = NULL)
   ```
   - Remove `target_variable` parameter
   - Replace `poverty_lines` with `poverty_line` (scalar)

B. **Dynamic required columns guard** (replaces lines ~35–50):
   ```r
   required <- unique(c(
     "pip_id", "weight",
     if (is.null(analysis_var) || analysis_var %in% c("welfare", "pov_status")) "welfare",
     if (!is.null(analysis_var) && !analysis_var %in% c("welfare", "pov_status")) analysis_var
   ))
   missing_cols <- setdiff(required, names(dt))
   if (length(missing_cols)) {
     cli_abort(
       c("Required column{?s} missing from {.arg dt}: {.col {missing_cols}}."),
       call = NULL
     )
   }
   ```

C. **Updated validate_poverty_lines call** (lines ~70):
   ```r
   .validate_poverty_lines(poverty_line, families)
   ```
   (Note: .validate_poverty_lines() signature remains unchanged since it still validates a poverty_lines vector; internally poverty_line is the scalar passed, so wrap it: `.validate_poverty_lines(c(poverty_line), families)` if poverty_line is non-NULL)

D. **Updated dispatch logic** (replaces lines ~100–150):
   ```r
   # target_variable passed to summary_stats and shares
   # For poverty family, always operate on welfare column
   target_variable <- if (is.null(analysis_var) || analysis_var == "pov_status") NULL else analysis_var
   
   if ("poverty" %in% families) {
     # pov_status routes here — always operates on welfare column
     results$poverty <- compute_poverty(
       dt,
       poverty_lines = poverty_line,
       by            = batch_by,
       measures      = classified$poverty
     )
   }

   if ("inequality" %in% families) {
     results$inequality <- compute_inequality(
       dt,
       by       = batch_by,
       measures = classified$inequality,
       grp      = grp
     )
   }

   if ("summary_stats" %in% families) {  # Use canonical name "summary_stats"
     results$summary_stats <- compute_summary_stats(
       dt,
       by              = batch_by,
       measures        = classified$summary_stats,
       target_variable = target_variable,
       grp             = grp
     )
   }

   if ("shares" %in% families) {
     # shares computes any combination of pop_share, target_within_group_share, target_survey_share
     results$shares <- compute_shares(
       dt,
       by              = batch_by,
       measures        = classified$shares,
       target_variable = target_variable,
       grp             = grp
     )
   }
   ```

**Test scenarios**:
- happy path: analysis_var = "welfare", measures = c("gini", "mean") → "inequality" and "summary_stats" families active; inequality called with welfare implicit; summary_stats called with target_variable = "welfare"
- happy path: analysis_var = "pov_status", measures = c("headcount"), poverty_line = 2.15 → "poverty" family active; compute_poverty called with poverty_lines = 2.15 (poverty operates on welfare only)
- happy path: analysis_var = "female", measures = "pop_share" → "shares" family active; compute_shares called with target_variable = "female"
- error path: analysis_var = "pov_status" missing welfare column → early guard catches it

**Acceptance criteria**:
- Correct family dispatched based on analysis_var type
- poverty_line passed as scalar to compute_poverty()
- target_variable derived correctly for summary_stats and shares
- Required columns validated at entry
- All three bugs fixed (shares dispatch correct, family name matches registry, no conditional overwrites)

---

### Phase 4: Testing & Validation

#### 8. Rewrite table_maker() test suite (R8)

**Files**: `tests/testthat/test-table-maker.R`

**Details**:

**Test coverage required** (organized by analysis_var type):

A. **welfare analysis** (original behavior, now explicit):
   - Request: analysis_var = "welfare", measures = c("gini", "mean"), no poverty_line, no by
   - Request: analysis_var = "welfare", measures = c("gini", "mean"), by = c("gender", "area")
   - Verify: inequality and summary_stats families active; output includes gini and mean rows; population column present

B. **poverty analysis** (pov_status as analysis_var):
   - Request: analysis_var = "pov_status", measures = c("headcount", "poverty_gap"), poverty_line = 2.15, no by
   - Request: analysis_var = "pov_status", measures = c("headcount"), poverty_line = 2.15, by = c("gender")
   - Verify: poverty family active; headcount and poverty_gap in output; poverty_line column populated
   - Error path: analysis_var = "pov_status", no poverty_line → cli_abort

C. **pov_status as disaggregation dimension**:
   - Request: analysis_var = "welfare", measures = c("mean"), by = c("pov_status"), poverty_line = 3.65
   - Verify: pov_status column created from welfare < 3.65; output has two rows per other group (poor/non-poor); population correct per cell
   - Error path: analysis_var = "welfare", by = c("pov_status"), no poverty_line → cli_abort

D. **continuous variable analysis**:
   - Request: analysis_var = "age", measures = c("mean", "median"), no poverty_line, by = c("gender")
   - Verify: summary_stats family active; mean and median computed on age column; population correct
   - Error path: analysis_var = "age" but age missing from dt → cli_abort

E. **binary variable analysis** (shares):
   - Request: analysis_var = "female", measures = c("pop_share"), no poverty_line, by = NULL
   - Verify: shares family active; pop_share = 1 (entire survey); population = total weighted count
   - Request: analysis_var = "female", measures = c("pop_share", "target_survey_share"), by = c("area"), target_variable derived = "female"
   - Verify: pop_share computed per area (share of total in area); target_survey_share computed per area (share of total in area AND female==1); output has 2*n_areas rows (one row per area per measure), NOT split by female=0/1

F. **Multi-measure multi-by combinations**:
   - Request: analysis_var = "welfare", measures = c("gini", "mean", "headcount"), by = c("gender", "area"), poverty_line = 2.15
   - Verify: three families (inequality, summary_statistics, poverty) all active; output has correct number of rows (2 genders × 2 areas × n_measures); all measures present; population correct

G. **Error paths**:
   - Missing welfare column when needed
   - Missing analysis_var column when specified
   - Invalid analysis_var
   - Missing poverty_line when pov_status involved
   - NA values in welfare or weight

**Test structure** (recommended):
- Use test fixtures: at minimum one small 5-row dataset with welfare, weight, gender, area, age, female columns
- Use parametric tests (testthat::local_* or test matrix) to avoid code duplication across similar scenarios
- Snapshot output structure (column names, long format verification) for each scenario
- For shares tests, verify output has exactly one row per (cell, measure) combination, NOT one row per (cell, measure, target_var_value)

**Acceptance criteria**:
- All 7 scenario categories above have passing tests
- Error paths explicitly test error message content (cli_abort output)
- No tests assume welfare as implicit analysis_var
- Output format (long with poverty_line, measure, value, population columns) verified in every test

---

#### 9. Rewrite /table endpoint test suite (R8)

**Files**: `tests/testthat/test-api-endpoints.R`

**Details**:

**Test coverage required**:

A. **Parameter validation tests**:
   - analysis_var required, non-empty character scalar
   - poverty_line optional but required when analysis_var == "pov_status" or by includes "pov_status"
   - by excludes pov_status from dimension validation (except the cross-check)
   - pip_id, measures, ppp validation unchanged

B. **Happy-path requests**:
   - GET /table?analysis_var=welfare&pip_id=COL_2010_GEIH_INC_ALL&measures=gini,mean
   - POST /table with same params as JSON body
   - GET /table?analysis_var=pov_status&pip_id=COL_2010_GEIH_INC_ALL&measures=headcount&poverty_line=2.15
   - GET /table?analysis_var=female&pip_id=COL_2010_GEIH_INC_ALL&measures=pop_share,target_survey_share&by=area

C. **Error-case requests**:
   - Missing analysis_var → 400
   - analysis_var = "pov_status" without poverty_line → 400
   - analysis_var = "unknown" → 400
   - poverty_line = "not_a_number" → 400
   - by includes "pov_status" without poverty_line → 400

D. **Response structure** (unchanged from current contract):
   - `status = "success"`
   - `data` is a data.table with required columns: pip_id, poverty_line, measure, value, population, by columns
   - `meta` includes release and n_surveys

**Acceptance criteria**:
- All parameter validation tests pass
- All happy-path requests return 200 with valid data structure
- All error cases return 400 or 422 with descriptive error messages

---

### Phase 5: Documentation & Final Validation

#### 10. Update roxygen docstrings (R2, R3, R4, R6)

**Files**: `R/table_maker.R`, `R/compute_measures.R`, `inst/plumber/plumber.R`, `inst/plumber/helpers.R`

**Details**:
- table_maker() @param: add analysis_var, remove country_code/year/welfare_type triplet docs, update poverty_lines → poverty_line
- table_maker() @return: clarify that poverty_line column appears in output only for poverty measures
- compute_measures() @param: add analysis_var, remove target_variable, update poverty_lines → poverty_line
- /table endpoint roxygen: add analysis_var required parameter, update poverty_lines → poverty_line docs
- validate_table_input() docstring: add analysis_var and poverty_line params, describe cross-validation rule

**Acceptance criteria**: Roxygen documentation builds without warnings; all parameter names and defaults match function signatures.

---

#### 11. Confidence & acceptance validation

**Files**: No code changes; verification only

**Details**:

Verify via code inspection and test execution:
1. **Completeness**: Every requirement has at least one implementation step and test coverage
2. **Testability**: All acceptance criteria are observable via automated tests (not manual inspection only)
3. **Dependencies**: No unlisted packages or external APIs assumed (collapse, data.table, cli, jsonlite, plumber already listed in DESCRIPTION)
4. **Risk coverage**: Covered under Phase 2 & 4; see Risks section
5. **Scope clarity**: Out of Scope section populated below

**Acceptance criteria**: All five dimensions of confidence check pass; no blockers identified during phase execution.

---

## Testing Strategy

### Test Categories

1. **Unit tests** (single function):
   - .classify_measures() with analysis_var-aware variable type system
   - compute_summary_stats(), compute_poverty(), compute_inequality(), compute_shares() with non-welfare target variables
   - validate_table_input() with analysis_var and poverty_line params

2. **Integration tests** (multi-function):
   - table_maker() with five analysis_var types (welfare, pov_status, age, female, etc.)
   - compute_measures() dispatch to correct family based on analysis_var

3. **API tests** (endpoint layer):
   - /table GET and POST with all analysis_var types
   - validate_table_input() edge cases (cross-validation of pov_status + poverty_line)

4. **Regression tests**:
   - Existing welfare-based aggregates unchanged in behavior (determinism check)
   - Known inequality/poverty benchmarks still match (numerical stability)

### Test Execution

- Use testthat framework (existing project standard)
- Run `devtools::test()` to validate all tests pass before Phase 5
- Use `devtools::check()` to ensure no new warnings/notes

---

## Documentation Checklist

- [x] Roxygen docstrings updated (table_maker, compute_measures, validate_table_input, /table endpoint)
- [ ] Update `docs/project-context.md` with variable type system diagram if needed
- [ ] Update `README.md` with analysis_var examples for new variable types
- [x] Add NEWS.md entry: "Breaking change: table_maker() now requires analysis_var parameter; poverty_lines renamed to poverty_line (scalar)"
- [ ] Inline code comments clarify pov_status special handling (derived, not loaded)

---

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|---------|-----------|
| **Test suite incompleteness** | Medium | High | Phase 4 Step 8–9 require 7 scenario categories + error paths. Each must pass before proceeding. Use parametric/snapshot tests to scale coverage. |
| **Mismatch in family registry naming** | Low | High | Phase 1 Step 2 requires exact verification: .MEASURE_REGISTRY keys must match .classify_measures() output AND dispatch conditions. Use grep to verify no "summary_stats" vs "summary_statistics" inconsistencies remain. |
| **pov_status derivation timing** | Medium | Medium | pov_status must be created after load_surveys() but before compute_measures(). Table 3 Step 6 specifies exact location (after age binning). Integration tests (Phase 4 Step 8C) verify correct cell counts per poverty status. |
| **Backward incompatibility breaks downstream** | Low | High | Breaking change explicitly documented in NEWS.md. No shim for old single-welfare tests — specification takes precedence. Verify all existing test files replaced, not partially modified. |
| **poverty_line scalar vs vector confusion** | Medium | Medium | Phase 2 Steps 4–5 enforce scalar coercion in validate_table_input(). Phase 3 Step 7 passes c(poverty_line) to .validate_poverty_lines() only if non-NULL. Comment the wrapping clearly. |
| **NA weights/welfare escape validation** | Low | High | Phase 1 Step 1: compute_measures() adds early guard for NA in welfare/weight (already present in compute_inequality()). Phase 4 tests include NA error paths. |
| **Performance regression** | Low | Medium | No new loops or data copies expected. Single-batch compute_measures() call (existing behavior). Profiling during Phase 4 optional if time permits. |

---

## Out of Scope

- **Backward compatibility with single-welfare tests**: All tests are rewritten; old tests removed or replaced entirely.
- **New measures or computation families**: Limited to poverty, inequality, summary_stats, shares (existing families).
- **UI changes**: Phase 2 assumes API already receives analysis_var from Step 2 of the wizard; no UI frontend work here.
- **Triplet fallback (country_code, year, welfare_type) re-implementation**: Not needed for /table; only /lookup uses triplets. Leave table_maker() signature with placeholders; document as future work if needed.
- **Documentation of analysis variable availability per survey**: Manifest dimension checking handles this; new /analysis-variables endpoint assumed to return full variable catalog.
- **Breaking change mitigation for external API consumers**: Release notes will document the breaking change. No grace period or dual-mode operation.

---

phases: 5  # Phase 1: Bug Fixes | Phase 2: Endpoint Refactor | Phase 3: Core Refactoring | Phase 4: Testing | Phase 5: Documentation

## Completion Contract

### Outcome

The piptm computation engine and /table Plumber endpoint now support multiple analysis variables (welfare, poverty, continuous, binary) with correct routing to appropriate computation families. Three existing bugs are fixed. All existing tests are replaced with a comprehensive suite covering five analysis variable types and their disaggregation combinations. The refactor maintains deterministic outputs and reproducibility while unblocking the PIP platform's multi-variable Table Maker feature.

### Verification Surface

| ID | Evidence Required | Command/Artifact | Phase |
|----|-------------------|------------------|-------|
| V1 | All three Phase 1 bugs fixed (dispatch, registry mismatch, conditional); shares redesigned for multi-measure | Code inspection + passing unit tests (Phase 1 Steps 1–3) | 1 |
| V2 | /table endpoint accepts analysis_var and poverty_line (scalar) parameters | Integration test: GET /table?analysis_var=welfare&... returns 200 | 2 |
| V3 | validate_table_input() rejects pov_status without poverty_line (when analysis_var or by includes pov_status) | Test: analysis_var=pov_status, no poverty_line → error 400; by includes pov_status, no poverty_line → error 400 | 2 |
| V4 | table_maker() signature updated; needed_cols excludes pov_status; pov_status derived only when in by | Code inspection + passing integration test (welfare, pov_status-as-analysis_var, pov_status-in-by, continuous, binary) | 3 |
| V5 | compute_measures() dispatches to correct family; pov_status as analysis_var routes to poverty (separate from pov_status in by) | 6 scenario tests: welfare→inequality+summary_stats, pov_status-as-analysis_var→poverty, pov_status-in-by→creates column, age→summary_stats, female→shares, multi-measure combinations | 4 |
| V6 | compute_shares() computes any combination of pop_share, target_within_group_share, target_survey_share; one row per cell per measure | Unit test: compute_shares() with measures=c("pop_share","target_survey_share"), target_variable="female" produces 2 rows per cell (not 2*2) | 4 |
| V7 | table_maker() output matches required long format (poverty_line, measure, value, population, by cols) | Snapshot/structure tests in Phase 4 Step 8 (7 categories, 3+ tests each) | 4 |
| V8 | /table endpoint API tests pass (all parameter validation, happy paths, error cases) | Phase 4 Step 9: 5+ happy-path + 5+ error-case tests all pass | 4 |
| V9 | Roxygen docstrings updated and devtools::check() passes | No warnings from roxygen; check output clean | 5 |
| V10 | NEWS.md updated with breaking change note | Artifact review: NEWS.md entry mentions analysis_var requirement and poverty_lines→poverty_line rename | 5 |
| V11 | All existing welfare-based benchmarks unchanged (determinism check) | Regression test: known gini/headcount/mean outputs match reference values to machine epsilon | 4 |

### Constraints

| ID | Constraint | Check |
|----|-----------|-------|
| C1 | No backward compatibility with single-welfare tests; old tests removed or replaced | Code inspection: zero "# old test" comments; all test files in Phase 4 are new rewrites |
| C2 | Specification takes precedence over existing code; bugs in existing code are fixed, not preserved | All three Phase 1 bugs corrected; no legacy behavior expected |
| C3 | Deterministic outputs required (identical inputs → identical results) | Regression tests (V10) verify numerical stability; no randomness introduced |
| C4 | Fail loudly for missing weights, NA values, invalid inputs | guards in compute_measures(), table_maker(), validate_table_input() all present and tested |
| C5 | No new external dependencies (only collapse, data.table, cli, jsonlite, plumber) | DESCRIPTION unchanged; all new code uses existing imports |
| C6 | pov_status is derived (not loaded); excluded from manifest dimension checks | Code inspection: by_check excludes pov_status before manifest filter; pov_status created after load_surveys() |

### Boundaries

**Allowed:**
- Refactoring existing functions to support analysis_var parameter
- Fixing the three identified bugs
- Rewriting all table_maker() and /table endpoint tests from scratch
- Adding pov_status derivation logic
- Updating roxygen docstrings and NEWS.md
- Profiling/optimization during Phase 4 if time permits

**Out of scope:**
- Backward compatibility with old tests or single-welfare assumption
- New computation families or measures
- Triplet fallback re-implementation in table_maker()
- UI frontend changes
- Dual-mode operation or grace period for API consumers

### Iteration Policy

1. **Phase 1 completion block**: All three bugs must be fixed and tested before proceeding to Phase 2. If any bug remains unfixed, halt and debug before continuing.
2. **Phase 2 completion block**: validate_table_input() and /table endpoint must accept analysis_var and poverty_line, and reject pov_status without poverty_line, before Phase 3 starts.
3. **Phase 3 completion block**: table_maker() and compute_measures() signatures must match spec exactly, and needed_cols logic must be verified by code inspection, before Phase 4 testing starts.
4. **Phase 4 acceptance**: All 7 scenario categories in Step 8 must have passing tests before considering Phase 4 complete. If any scenario has a failing test, debug and retest within that phase.
5. **Phase 5 sign-off**: devtools::check() must pass cleanly; NEWS.md must be updated; roxygen must build without warnings.

### Blocked-Stop Conditions

- **Any Phase 1 bug remains unfixed after code inspection**: halt Phase 1, re-read bug spec, retest.
- **validate_table_input() does not reject pov_status without poverty_line**: halt Phase 2, debug validate_table_input() logic.
- **table_maker() or compute_measures() signature does not match spec**: halt Phase 3, reread Task 3 and Task 4 specs.
- **More than 10% of Phase 4 tests fail** (>4 test cases): halt Phase 4, debug failing scenarios, retest.
- **devtools::check() produces new warnings not present before refactor**: halt Phase 5, investigate and resolve before sign-off.

---

## Model Context

This plan inherits your GitHub Copilot model picker. If Copilot Auto is selected, the underlying model is not disclosed. If the actual model matters for implementation details, check Copilot UI/hover details.

---

## Deviation Policy

**Deviation-policy: `ask`**

If during implementation you encounter ambiguities, conflicts with existing code, or requirements that seem to contradict each other:
1. **Document the conflict** in a comment (e.g., "Spec says X, but existing code does Y")
2. **Propose a resolution** (e.g., "Following spec, not existing code, because specification takes precedence")
3. **Proceed with the proposed resolution** unless explicitly blocked

The specification document provided by the user takes precedence over existing code, tests, and architectural assumptions. All three bugs must be fixed as specified.
