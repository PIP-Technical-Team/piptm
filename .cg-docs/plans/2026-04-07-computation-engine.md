---
date: 2026-04-07
title: "Computation Engine — Family-Based Orchestrator"
status: active
brainstorm: ".cg-docs/brainstorms/2026-04-07-computation-engine-design.md"
language: "R"
estimated-effort: "large"
tags: [computation, engine, orchestrator, measures, phase-1, performance]
---

# Plan: Computation Engine — Family-Based Orchestrator

## Objective

Implement the core computation engine for {piptm}: a family-based measure
orchestrator that computes indicators across three families (poverty,
inequality, welfare) and disaggregation dimensions, returning long-format
results ready for JSON serialization. The engine must be fast, correct, and
cleanly integrated with the existing manifest → load pipeline.anifest → load pipeline.

## Context

**What exists today:**

- `manifest.R` — manifest loading, caching, accessor functions (fully implemented)
- `load_data.R` — `load_survey_microdata()` and `load_surveys()` with
  partition-pushdown via `open_dataset() |> filter()` (fully implemented)
- `schema.R` — canonical Arrow schema with 14 columns (8 required, 6 optional)
- `validate_parquet.R` — schema and data validation functions
- `zzz.R` — `.piptm_env`, `.onLoad()` with manifest scanning and release
  resolution

**What this plan adds:**

1. Measure registry — canonical mapping of measure names to families
2. Age binning helper — `.bin_age()`
3. Three family compute functions — `compute_poverty()`, `compute_inequality()`,
   `compute_welfare()`
4. Orchestrator — `compute_measures()` dispatching to families
5. Top-level API — `table_maker()` tying manifest → load → compute → output

**Key design decisions (from brainstorm):**

- Family-based dispatch: poverty (gap-based), inequality (sort-based), welfare
  (simple aggregation) — shared intermediates computed once per family
- Long format throughout — one row per (survey × poverty_line × dimension ×
  measure). Maps directly to JSON for API layer
- `poverty_line` always present; `NA_real_` for non-poverty measures
- Full cross-tabulation only — no marginals or totals
- Age bins at query time: 0–14, 15–24, 25–64, 65+
- Multi-measure per call
- Per-survey computation (not pooled)
- {collapse} for weighted statistics, {data.table} for data manipulation
  (per R instructions)

**Critical data flow boundary:**

`load_surveys()` returns a **single flat data.table** with all requested
surveys row-bound together. The `pip_id` column (stored in every Parquet
file) identifies which rows belong to which survey. There are no per-survey
attributes on the batch result — only a `"release"` attribute on the whole
table.

`compute_measures()` operates on the **full multi-survey batch** (all
`pip_id`s at once). It builds a compound `GRP(dt, by = c("pip_id", by))`
once, shared across `compute_inequality()` and `compute_welfare()`. The
per-survey `lapply()` loop was removed in the Approach B refactor
(`.cg-docs/plans/2026-04-28-table-maker-approach-b-refactor.md`). `pip_id`
is always present in the grouping and always appears in `compute_measures()`
output. Metadata (`country_code`, `surveyid_year`, `welfare_type`) is
attached via a keyed join in `table_maker()` **after** computation.

Dimension availability (which `by` columns exist for a survey) is checked
against the **manifest** before loading. Surveys with **zero** overlap
with the requested `by` dimensions are dropped from `entries` before
`load_surveys()` is called. Surveys with **partial** overlap are kept —
missing dimension columns are filled with `NA` before computation, producing
a single NA group for that dimension in the cross-tabulation.
(See `.cg-docs/brainstorms/2026-04-16-dimension-prefilter-partial-match.md`.)

**Dependencies already in DESCRIPTION:**

- `collapse` — in Imports (for `fmean`, `fmedian`, `fnth`, `fvar`, `fsd`,
  `fmax`, `fmin`, `fnobs`, `fsum`, `GRP`, `TRA`)
- `data.table` — in Imports
- `arrow`, `dplyr` — in Imports (for data loading)
- `cli` — in Imports (for user-facing errors)

## Implementation Steps

### Step 1: Measure Registry and Input Validation Helpers

- **Files**: `R/measures.R` (new)
- **Details**:
  1. Define a package-level constant `.MEASURE_REGISTRY` — a named list mapping
     canonical measure names to their family:
     ```r
     .MEASURE_REGISTRY <- list(
       # Poverty family (all require poverty_lines)
       headcount   = "poverty",
       poverty_gap = "poverty",
       severity    = "poverty",
       watts       = "poverty",
       pop_poverty = "poverty",
       # Inequality family
       gini        = "inequality",
       mld         = "inequality",
       # Welfare family
       mean        = "welfare",
       median      = "welfare",
       sd          = "welfare",
       var         = "welfare",
       min         = "welfare",
       max         = "welfare",
       nobs        = "welfare",
       p10         = "welfare",
       p25         = "welfare",
       p75         = "welfare",
       p90         = "welfare"
     )
     ```
     Percentile measures (`p10`, `p25`, `p75`, `p90`) use `fnth()` with the
     corresponding probability (0.10, 0.25, 0.75, 0.90). Additional percentiles
     can be added to the registry without structural changes.
  2. Export `pip_measures()` — returns a character vector of all valid measure
     names (`names(.MEASURE_REGISTRY)`). Useful for user discovery and
     documentation.
  3. Create internal `.classify_measures(measures)` — takes a character vector
     of requested measure names, validates them against the registry, and
     returns a named list of character vectors keyed by family:
     ```r
     .classify_measures(c("headcount", "gini", "mean"))
     # list(poverty = "headcount", inequality = "gini", welfare = "mean")
     ```
     Errors with `cli_abort()` if any measure name is not in the registry.
  4. Create internal `.validate_by(by, dimensions)` — validates the `by`
     argument:
     - `by` must be `NULL` or a character vector
     - All elements must be in the allowed set: `c("gender", "area",
       "educat4", "educat5", "educat7", "age")`
     - At most one education column (`educat4`, `educat5`, `educat7`)
     - Maximum 4 dimensions
     - If `dimensions` is provided (from manifest), warn if any requested
       dimension is not available for a survey
  5. Create internal `.validate_poverty_lines(poverty_lines, families)` —
     errors if any poverty measure is requested but `poverty_lines` is NULL
     or empty. Errors if `poverty_lines` contains non-positive or non-finite
     values.
- **Tests**: `tests/testthat/test-measures.R` (new)
  - `pip_measures()` returns all 17 measure names
  - `.classify_measures()` correctly groups measures by family
  - `.classify_measures()` errors on unknown measure names
  - `.validate_by()` accepts valid dimension combinations
  - `.validate_by()` errors on >4 dimensions
  - `.validate_by()` errors on multiple education columns
  - `.validate_by()` errors on unknown dimension names
  - `.validate_poverty_lines()` errors when poverty measures requested without
    poverty lines
  - `.validate_poverty_lines()` passes when no poverty measures requested and
    poverty_lines is NULL
- **Acceptance criteria**: All validation helpers work correctly and produce
  informative cli-formatted error messages.

### Step 2: Age Binning Helper

- **Files**: `R/measures.R` (append to file from Step 1)
- **Details**:
  1. Create internal `.bin_age(dt)` — modifies `dt` in place (by reference):
     - Uses `data.table::fcase()` on the `age` column to create `age_group`:
       ```r
       dt[, age_group := fcase(
         age <= 14L,              "0-14",
         age >= 15L & age <= 24L, "15-24",
         age >= 25L & age <= 64L, "25-64",
         age >= 65L,              "65+",
         default = NA_character_
       )]
       ```
     - Convert `age_group` to factor with ordered levels:
       `c("0-14", "15-24", "25-64", "65+")`
     - Drop the original `age` column from `dt` (no longer needed after
       binning)
     - Handle `NA` age values: rows with `age = NA` get `age_group = NA`
       (they remain in the data but will produce an NA group in cross-tabs)
  2. Export `pip_age_bins()` — returns the named character vector of age bin
     labels and cut points. Useful for documentation and downstream consumers.
  3. In the `by` argument processing (later in `compute_measures()`), replace
     `"age"` with `"age_group"` after binning so downstream functions group
     by the binned factor column.
- **Tests**: `tests/testthat/test-measures.R` (append)
  - `.bin_age()` creates `age_group` factor with correct levels
  - `.bin_age()` handles boundary values (0, 14, 15, 24, 25, 64, 65, 130)
  - `.bin_age()` handles NA age values
  - `.bin_age()` drops original `age` column
  - `.bin_age()` modifies dt in place (by reference)
  - `pip_age_bins()` returns expected labels
- **Acceptance criteria**: Age binning is correct at all boundaries, handles
  NAs, and modifies in place.

### Step 3: `compute_poverty()` — Poverty Family Function

- **Files**: `R/compute_poverty.R` (new)
- **Details**:
  1. `compute_poverty(dt, poverty_lines, by = NULL, measures = NULL)`:
     - **Signature**: `dt` is a data.table with at minimum `welfare` and
       `weight`. `poverty_lines` is a numeric vector. `by` is a character
       vector of grouping column names (already validated). `measures` is a
       character vector of poverty measures to compute (subset of
       `c("headcount", "poverty_gap", "severity", "watts", "pop_poverty")`);
       if NULL, compute all 5.
     - **Algorithm** (single-pass grouped aggregation):
       1. Build working copy with only needed columns:
          `work <- dt[, c("welfare", "weight", by), with = FALSE]`
       2. Cross-join with poverty lines:
          `pl_dt <- data.table(poverty_line = poverty_lines)`
          `work <- work[pl_dt, on = .NATURAL, allow.cartesian = TRUE]`
       3. Compute vectorized intermediates (in place via `:=`):
          - `gap := fcase(welfare < poverty_line, (poverty_line - welfare) / poverty_line, default = 0)`
          - `is_poor := welfare < poverty_line` (strictly less than)
          - `log_ratio := fcase(is_poor & welfare > 0, log(poverty_line / welfare), default = 0)`
            (Zero-welfare rule: contribute 0 to Watts sum. This means
            zero-welfare individuals who are poor are counted in headcount
            and pop_poverty but contribute 0 to Watts. Documented.)
       4. Single grouped aggregation using {collapse} `GRP()` + fast
          functions for performance:
          ```r
          grp <- GRP(work, by = c("poverty_line", by))
          w   <- work$weight
          
          headcount   <- fsum(w * work$is_poor, g = grp) / fsum(w, g = grp)
          poverty_gap <- fsum(w * work$gap,     g = grp) / fsum(w, g = grp)
          severity    <- fsum(w * work$gap^2,   g = grp) / fsum(w, g = grp)
          watts       <- fsum(w * work$log_ratio, g = grp) / fsum(w, g = grp)
          pop_poverty <- fsum(w * work$is_poor, g = grp)
          population  <- fsum(w, g = grp)
          ```
       5. Assemble wide result with group columns from `grp$groups`.
       6. Subset to only the requested measures.
       7. `melt()` to long format: `measure` + `value` columns.
       8. Add `population` column.
     - **Return**: data.table in long format with columns:
       `poverty_line`, `[by cols]`, `measure`, `value`, `population`
  2. Export `compute_poverty()`.
- **Tests**: `tests/testthat/test-compute-poverty.R` (new)
  - **Hand-computed fixtures**: Create small (5–10 row) data.tables with
    known welfare/weight values and manually compute expected FGT values.
  - Headcount: simple case (3 poor out of 5, equal weights) → 0.6
  - Poverty gap: verify against hand computation
  - Severity: verify against hand computation
  - Watts: verify log-based computation; test with zero-welfare rows
  - Pop_poverty: verify weighted sum of poor
  - Population: verify total weighted sum
  - Multiple poverty lines: verify each line produces correct values
  - Grouped computation: verify by `gender` produces correct per-group values
  - Multiple dimensions: verify full cross-tabulation
  - Edge case: all rows poor → headcount = 1.0
  - Edge case: no rows poor → headcount = 0.0
  - Edge case: zero welfare rows → Watts uses 0 contribution, headcount
    still counts them as poor
  - Output format: long format, correct column names and types
  - `measures` parameter: subset works (only "headcount" returns only headcount)
- **Acceptance criteria**: All 5 poverty measures are numerically correct
  against hand-computed fixtures. Single grouped aggregation (not 5 separate
  passes). Long-format output.

### Step 4: `compute_inequality()` — Inequality Family Function

- **Files**: `R/compute_inequality.R` (new)
- **Details**:
  1. `compute_inequality(dt, by = NULL, measures = NULL)`:
     - **Signature**: `dt` with `welfare`, `weight`. `by` as character vector.
       `measures` subset of `c("gini", "mld")`; NULL = both.
     - **Algorithm**:
       1. Pre-compute grouping: `grp <- GRP(dt, by = by)` (reused for both
          measures).
       2. **Gini** (per group, using sorted cumulative weights):
          - For each group, sort by welfare.
          - Compute cumulative weight shares and cumulative weighted welfare
            shares.
          - Gini = 1 - 2 × (area under Lorenz curve).
          - Use the trapezoid formula on cumulative shares.
          - Implementation: `BY()` or `rsplit()` from {collapse} to split
            by group, then vectorized Gini per group.
          - Alternative: use a vectorized data.table approach — sort dt
            by group + welfare, compute cumulative sums within groups,
            derive Gini algebraically. This avoids explicit loops.
          - **Chosen approach**: data.table sorted cumulative sums (avoids
            per-group function calls; fully vectorized).
       3. **MLD (Mean Log Deviation)** per group:
          - `wmean <- fmean(welfare, w = weight, g = grp)` — weighted mean
            per group
          - Sweep mean back to rows: `TRA(welfare, wmean, g = grp, "-")`
            is not what we need; we need `log(wmean / welfare)`.
          - Compute: `log_ratio := log(wmean_expanded / welfare)` where
            `wmean_expanded` is the group mean broadcast to each row.
          - Use `fmean(welfare, w = weight, g = grp, TRA = "replace")` to
            get the expanded mean.
          - `mld_per_group <- fmean(log_ratio, w = weight, g = grp)`
          - **Zero-welfare rule**: same as Watts — rows with `welfare == 0`
            contribute 0 to MLD (set `log_ratio` to 0 for those rows).
            Document this.
       4. Assemble wide result → subset to requested measures → `melt()` to
          long format.
     - **Return**: data.table in long format:
       `[by cols]`, `measure`, `value`, `population`
  2. Create internal `.gini_sorted(welfare, weight)` — computes Gini from
     pre-sorted welfare and weight vectors. Pure vectorized computation:
     ```r
     # Lorenz curve via cumulative shares
     cum_w <- cumsum(weight) / sum(weight)
     cum_wy <- cumsum(weight * welfare) / sum(weight * welfare)
     # Trapezoid area under Lorenz curve
     B <- sum(diff(cum_w) * (head(cum_wy, -1) + tail(cum_wy, -1)) / 2)
     gini <- 1 - 2 * B
     ```
  3. Export `compute_inequality()`.
- **Tests**: `tests/testthat/test-compute-inequality.R` (new)
  - **Gini fixtures**:
    - Perfect equality (all welfare identical) → Gini = 0
    - Maximum inequality (one person has all welfare) → Gini ≈ 1 - 1/n
    - Known distribution: 5 observations [1, 2, 3, 4, 10], equal weights →
      hand-compute expected Gini
    - Weighted case: same welfare but different weights → verify
  - **MLD fixtures**:
    - Perfect equality → MLD = 0
    - Known distribution → hand-compute expected MLD
    - Zero-welfare rows → contribute 0 to MLD
  - Grouped computation: verify Gini and MLD per group
  - Multiple dimensions: verify full cross-tabulation
  - `measures` subset: only "gini" returns only gini
  - Output format: long format, correct columns, `population` present
- **Acceptance criteria**: Gini and MLD are numerically correct against
  hand-computed fixtures. GRP reused across both measures. Long-format output.

### Step 5: `compute_welfare()` — Welfare Family Function

- **Files**: `R/compute_welfare.R` (new)
- **Details**:
  1. `compute_welfare(dt, by = NULL, measures = NULL, grp = NULL)`:
     - **Signature**: `dt` with `welfare`, `weight`. `by` as character vector.
       `measures` subset of all welfare measure names (see registry); NULL = all.
       `grp` optional pre-computed `GRP` object.
     - **collapse function mapping**:
       | Measure | collapse function | Notes |
       |---------|------------------|-------|
       | `mean`   | `fmean(welfare, w = weight, g = grp)` | Weighted mean |
       | `median` | `fmedian(welfare, w = weight, g = grp)` | Weighted median |
       | `sd`     | `fsd(welfare, w = weight, g = grp)` | Weighted standard deviation |
       | `var`    | `fvar(welfare, w = weight, g = grp)` | Weighted variance |
       | `min`    | `fmin(welfare, g = grp)` | No weight arg — applies to observed values |
       | `max`    | `fmax(welfare, g = grp)` | No weight arg — applies to observed values |
       | `nobs`   | `fnobs(welfare, g = grp)` | Unweighted observation count |
       | `p10`    | `fnth(welfare, 0.10, w = weight, g = grp)` | Weighted 10th percentile |
       | `p25`    | `fnth(welfare, 0.25, w = weight, g = grp)` | Weighted 25th percentile |
       | `p75`    | `fnth(welfare, 0.75, w = weight, g = grp)` | Weighted 75th percentile |
       | `p90`    | `fnth(welfare, 0.90, w = weight, g = grp)` | Weighted 90th percentile |
     - **Algorithm**:
       1. Pre-compute grouping if not provided: `grp <- GRP(dt, by = by)`
       2. Compute only the requested measures (subset via `measures` arg)
       3. Population: `fsum(weight, g = grp)` (always computed)
       4. Assemble wide result as data.table from `grp$groups` + measure columns
       5. Subset columns to requested measures + `population`
       6. `melt()` to long format — variable column renamed to `measure`,
          value column to `value`
     - **Note on `min`/`max`**: `fmin()` and `fmax()` in {collapse} do not
       accept a weight argument — they operate on observed values. This is
       correct: min/max welfare is the lowest/highest observed welfare value,
       not a weighted quantity.
     - **Note on `nobs`**: `fnobs()` returns the unweighted observation count
       (number of rows per group). Useful for reporting sample sizes alongside
       estimates.
     - **Return**: data.table in long format:
       `[by cols]`, `measure`, `value`, `population`
  2. Export `compute_welfare()`.
- **Tests**: `tests/testthat/test-compute-welfare.R` (new)
  - **Mean/median**: equal and unequal weights, grouped
  - **SD/var**: equal weights → population SD; verify relationship `sd^2 == var`
  - **Min/max**: correct group min and max
  - **nobs**: correct row count per group (unweighted)
  - **Percentiles**:
    - p10, p25, p75, p90 on known distribution → hand-computed expected values
    - Weighted vs unweighted percentiles differ correctly
  - Grouped computation: all measures correct per group
  - Multiple dimensions: cross-tabulation
  - `measures` subset: requesting only `c("mean", "p90")` returns only those
  - Output format: long format, correct columns, `population` present
- **Acceptance criteria**: All welfare measures numerically correct. All
  {collapse} fast functions used with pre-computed `GRP`. Long-format output.

### Step 6: `compute_measures()` — Orchestrator

- **Files**: `R/compute_measures.R` (new)
- **Details**:
  1. `compute_measures(dt, measures, poverty_lines = NULL, by = NULL)`:
     - **Signature**: `dt` is a data.table for **exactly one survey** (one
       unique `pip_id`). This function must never receive multi-survey data.
       The caller (`table_maker()`) is responsible for splitting the batch
       result of `load_surveys()` by `pip_id` before passing slices here.
       `measures` is a character vector of requested measure names.
       `poverty_lines` is numeric or NULL. `by` is character or NULL.
     - **Algorithm**:
       1. **Guard**: assert `uniqueN(dt$pip_id) == 1L` — error if `dt`
          contains more than one survey. This is a programming error in the
          caller, not a user error.
       2. Classify measures into families via `.classify_measures()`.
       2. Pre-compute `GRP(dt, by = by)` once — pass to family functions
          to avoid redundant grouping. (Design note: family functions
          accept an optional `grp` argument; if provided, they skip their
          own `GRP()` call.)
       3. Dispatch to each active family:
          ```r
          results <- list()
          if ("poverty" %in% families)
            results$poverty <- compute_poverty(dt, poverty_lines, by,
                                               measures = classified$poverty)
          if ("inequality" %in% families)
            results$inequality <- compute_inequality(dt, by,
                                                     measures = classified$inequality)
          if ("welfare" %in% families)
            results$welfare <- compute_welfare(dt, by,
                                               measures = classified$welfare)
          ```
       4. `rbindlist(results, fill = TRUE)` — poverty results have
          `poverty_line` column; others don't, so `fill = TRUE` fills with
          `NA_real_`.
     - **Return**: data.table in long format:
       `poverty_line`, `[by cols]`, `measure`, `value`, `population`
  2. Do NOT export `compute_measures()` — it is internal. `table_maker()` is
     the public API.
- **Tests**: `tests/testthat/test-compute-measures.R` (new)
  - Multi-survey guard: passing a dt with 2 pip_ids errors immediately
  - All measures requested → output has 17 unique measure names
  - Only poverty measures → no inequality/welfare rows
  - Only inequality + welfare → `poverty_line` is NA for all rows
  - Mixed request → poverty rows have poverty_line; others have NA
  - `by = NULL` → one row per (poverty_line × measure) per survey
  - `by = c("gender")` → rows per (poverty_line × gender × measure)
  - Poverty lines interact only with poverty measures (correct row count)
  - Output column names and types are correct
- **Acceptance criteria**: Orchestrator correctly dispatches to families and
  merges results. `rbindlist(fill = TRUE)` produces the expected schema.

### Step 7: `table_maker()` and `pip_lookup()` — Top-Level API

- **Files**: `R/table_maker.R` (new)
- **Brainstorm**: `.cg-docs/brainstorms/2026-04-27-table-maker-input-parameters.md`
  (status: decided, chosen approach: "Approach A with built-in triplet fallback")
- **Details**:
  1. `pip_lookup(country_code, year, welfare_type, release = NULL)`:
     - **Purpose**: Resolve human-friendly survey triplets to `pip_id` strings
       via the manifest. Exported utility for ad-hoc R users and the
       `table_maker()` triplet fallback path.
     - **Signature**:
       - `country_code` — character vector
       - `year` — integer vector (same length as `country_code`)
       - `welfare_type` — character vector (same length as `country_code`)
       - `release` — character scalar or NULL (defaults to current release)
     - **Algorithm**:
       1. Guard: all three vectors must have equal length.
       2. Build a query `data.table(country_code, year, welfare_type)`.
       3. Join against `piptm_manifest(release)` on
          `.(country_code, year, welfare_type)` with `nomatch = NULL`.
       4. If fewer matches than queries, emit `cli_warn()` listing
          unmatched triplets.
       5. Return `matched$pip_id` (character vector).
     - **Return**: Character vector of pip_id strings.
  2. `table_maker(pip_id = NULL, country_code = NULL, year = NULL, welfare_type = NULL, measures, poverty_lines = NULL, by = NULL, release = NULL)`:
     - **Signature** (dual-input — decided in brainstorm):
       - `pip_id` — character vector of survey identifiers (primary input,
         used by the API layer). Exact manifest PK lookup via `%chin%`.
       - `country_code` — character vector (fallback input for ad-hoc R use)
       - `year` — integer vector (fallback, same length as `country_code`)
       - `welfare_type` — character vector (fallback, same length)
       - `measures` — character vector of measure names
       - `poverty_lines` — numeric vector or NULL
       - `by` — character vector of dimension names or NULL
       - `release` — character scalar or NULL (defaults to current release)

       **Input dispatch rule**: Either `pip_id` OR all three triplet params
       (`country_code` + `year` + `welfare_type`) must be provided. If
       `pip_id` is NULL, the function calls `pip_lookup()` internally to
       resolve triplets. If both are provided, `pip_id` takes precedence.

       | `pip_id` | Triplets | Outcome |
       |----------|----------|---------|
       | provided | NULL | Use pip_id directly |
       | NULL | all three provided | Resolve via `pip_lookup()` |
       | NULL | any missing | Error |
       | provided | also provided | Use pip_id, ignore triplets |
     - **Algorithm**:
       0. **Resolve survey identifiers**:
          ```r
          if (is.null(pip_id)) {
            if (is.null(country_code) || is.null(year) || is.null(welfare_type)) {
              cli::cli_abort(
                "Provide either {.arg pip_id} or all of {.arg country_code},
                 {.arg year}, and {.arg welfare_type}."
              )
            }
            pip_id <- pip_lookup(country_code, year, welfare_type, release)
          }
          ```
       1. Validate inputs:
          - `.classify_measures(measures)` — validates measure names
          - `.validate_poverty_lines(poverty_lines, families)` — poverty
            lines required if poverty measures requested
          - `.validate_by(by)` — dimension constraints
       2. Resolve manifest:
          ```r
          mf <- piptm_manifest(release)
          ```
       3. Filter manifest to requested surveys (exact PK lookup):
          ```r
          entries <- mf[pip_id %chin% .pip_id]
          if (nrow(entries) == 0L) {
            cli::cli_abort("No matching surveys found in manifest.")
          }
          ```
       4. **Dimension pre-filter (before loading)**. When `by` is non-NULL,
          check each entry's `dimensions` list column against the requested
          `by` vector and classify entries into full-match, partial-match,
          and zero-overlap groups.
          (See `.cg-docs/brainstorms/2026-04-16-dimension-prefilter-partial-match.md`.)

          The manifest stores `dimensions` as a **list column** — each
          element is a character vector of dimension names available for
          that survey (e.g. `c("gender", "area", "age")`). The pre-filter
          uses `lengths(intersect(...))` to compute overlap:

          ```r
          if (!is.null(by)) {
            # Compute per-entry overlap count between user's `by` and
            # the survey's available dimensions (list column).
            overlap <- vapply(
              entries$dimensions,
              function(d) length(intersect(by, d)),
              integer(1L)
            )

            # --- Zero-overlap entries: drop with warning ---
            zero_idx <- overlap == 0L
            if (any(zero_idx)) {
              dropped_ids <- entries$pip_id[zero_idx]
              cli::cli_warn(c(
                "Excluding {length(dropped_ids)} survey{?s} with none of",
                " the requested dimensions ({.val {by}}):",
                "i" = "{.val {dropped_ids}}"
              ))
              entries <- entries[!zero_idx]
              overlap <- overlap[!zero_idx]
            }

            # --- Partial-match entries: keep, but warn about missing dims ---
            partial_idx <- overlap > 0L & overlap < length(by)
            if (any(partial_idx)) {
              # Build a named list: pip_id → missing dimensions
              partial_entries <- entries[partial_idx]
              missing_info <- vapply(seq_len(nrow(partial_entries)), function(i) {
                miss <- setdiff(by, partial_entries$dimensions[[i]])
                paste0(partial_entries$pip_id[i], ": ", paste(miss, collapse = ", "))
              }, character(1L))
              cli::cli_warn(c(
                "{sum(partial_idx)} survey{?s} {?is/are} missing some",
                " requested dimensions. Results will have {.val NA} for",
                " missing dimensions.",
                "i" = "{missing_info}"
              ))
            }
          }
          ```

          **Inclusion rule**: keep any entry with **≥1** overlapping
          dimension. Only drop entries with **zero** overlap. Missing
          dimension columns are filled with `NA_character_` in step 7a
          (per-survey loop), so `compute_measures()` always receives the
          full `by` vector regardless of partial availability.

          Example walkthrough for `by = c("gender", "area")`:

          | Survey | `dimensions` | Overlap | Action |
          |--------|-------------|---------|--------|
          | COL_2010 | `c("gender", "area")` | 2 (full) | Kept as-is |
          | BOL_2015 | `c("gender")` | 1 (partial) | Kept; `area` filled with `NA` in step 7a |
          | PER_2008 | `c("educat4")` | 0 | Dropped with warning |
       5. **Load**: `load_surveys()` returns one flat data.table with all
          remaining surveys row-bound. The `pip_id` column identifies rows:
          ```r
          # entries now has only dimension-compatible surveys
          dt <- load_surveys(entries, release = release)
          # dt is a flat data.table:
          #   pip_id == "COL_2010_..." for rows from survey 1
          #   pip_id == "ARG_2004_..." for rows from survey 2
          #   etc.
          ```
       6. **Pre-process**: if `"age"` is in `by`, call `.bin_age(dt)` (modifies
          in place) and replace `"age"` with `"age_group"` in the `by` vector.
       7. **Split and compute — one survey at a time**:
          `compute_measures()` always receives a single-survey slice. The split
          happens here, not inside `compute_measures()`:
          ```r
          survey_ids <- unique(dt$pip_id)
          results <- lapply(survey_ids, function(pid) {
            # Slice to exactly one survey
            survey_dt <- dt[pip_id == pid]
            # 7a. Fill missing dimension columns with NA so that
            #     compute_measures() always receives the full `by` vector.
            #     GRP will produce a single NA group for missing dimensions.
            missing_dims <- setdiff(by, names(survey_dt))
            for (d in missing_dims) {
              data.table::set(survey_dt, j = d, value = NA_character_)
            }
            # compute_measures() sees only one pip_id — enforced by internal guard
            res <- compute_measures(survey_dt, measures, poverty_lines, by)
            # Attach survey metadata AFTER computation (not needed by compute_measures)
            res[, pip_id       := pid]
            res[, country_code  := survey_dt$country_code[1L]]
            res[, surveyid_year := survey_dt$surveyid_year[1L]]
            res[, welfare_type  := survey_dt$welfare_type[1L]]
            res
          })
          rbindlist(results)
          ```
       8. Reorder columns: metadata (`pip_id`, `country_code`, `surveyid_year`,
          `welfare_type`) first, then dimension columns, then `poverty_line`,
          then `measure`, `value`, `population`.
     - **Return**: data.table in long format. Ready for `jsonlite::toJSON()`.
  2. Export `table_maker()`.
- **Tests**: `tests/testthat/test-table-maker.R` (new)
  - **`pip_lookup()` tests**:
    - Single triplet → correct pip_id
    - Multiple triplets → correct pip_id vector
    - Unmatched triplet → warning listing missing surveys
    - Mismatched vector lengths → error
  - **Dual-input dispatch tests**:
    - `pip_id` provided → uses pip_id directly (no manifest triplet lookup)
    - Triplets provided (no `pip_id`) → resolves via `pip_lookup()`
    - Neither provided → informative error
    - Partial triplets (e.g. `country_code` without `year`) → error
    - Both `pip_id` and triplets provided → uses `pip_id`, ignores triplets
  - **Full integration tests** using Parquet fixtures (reuse
    `write_fixture_parquet` and `write_fixture_manifest` helpers from
    `test-load-data.R`):
    - Single survey (via pip_id), all measures → correct output shape
    - Multiple surveys (via pip_id) → results for each survey
    - Multiple surveys via triplet fallback → same results as pip_id path
    - Multiple poverty lines → rows per poverty line for poverty measures
    - `by = c("gender", "area")` → cross-tabulated rows
    - `by = c("age")` → age bins appear in output
    - Partial dimension match: request `by = c("gender", "area")` where one
      survey has only `gender` → survey included, `area = NA` in its results,
      warning emitted
    - Zero dimension overlap: survey with none of the requested dimensions →
      survey excluded with warning
    - Error: unknown measure name → informative error
    - Error: poverty measures without poverty_lines → error
    - Error: >4 dimensions → error
    - Error: multiple education columns → error
    - Output column order: metadata, dimensions, poverty_line, measure, value,
      population
  - JSON serialization test: verify `jsonlite::toJSON(result, na = "null")`
    produces valid JSON matching the expected schema
- **Acceptance criteria**: End-to-end pipeline works from fixture data through
  to long-format output. Both input paths (pip_id and triplets) produce
  identical results. All validation errors produce informative messages.
  JSON output matches the brainstorm schema.

### Step 8: Performance Optimization and Benchmarking

> **Status: COMPLETED — 2026-04-28**
> Orchestration benchmark ran; **Approach B (grouped collapse) adopted**.
> Implementation: `.cg-docs/plans/2026-04-28-table-maker-approach-b-refactor.md`
> Benchmark script: `benchmarks/orchestration-strategy.R`
> Results doc: `.cg-docs/solutions/performance-issues/2026-04-28-orchestration-benchmark-results.md`
> Plot: `benchmarks/ab-comparison.png`

- **Files modified**: `R/compute_measures.R`, `R/table_maker.R`, `R/zzz.R`
- **Benchmark summary** (15 surveys, 336K rows, 4 measures, 3 poverty lines,
  3 disaggregation dimensions, 5 iterations, `bench::mark(check = TRUE)`):

  | Approach | nthreads=1 | nthreads=4 | Peak memory |
  |----------|-----------|-----------|-------------|
  | A: per-slice `lapply()` | 0.43 s | 0.36 s | 164 MB |
  | B: grouped `GRP(c("pip_id", by))` | 0.13 s | **0.12 s** | **123 MB** |

  **Speedup: 72% faster** (exceeds the ≥2× decision threshold relative to
  the original per-slice baseline when measured at comparable thread counts).
  All approaches verified identical output via `all.equal(tolerance = 1e-10)`.

- **Decision outcome**: Approach B adopted as the production strategy.

- **Implementation changes made**:
  1. `R/compute_measures.R` — single-survey guard removed; compound
     `GRP(dt, by = c("pip_id", by))` replaces the per-by-only GRP.
     Multi-survey batches now handled natively. `compute_poverty()` builds
     its own GRP internally (cross-join invalidates the shared GRP);
     `compute_inequality()` and `compute_welfare()` receive the shared GRP.
  2. `R/table_maker.R` — `lapply()` loop (old Steps 6–7) replaced with:
     - Step 6: batch NA-fill for missing dimension columns
     - Step 7: single `compute_measures(dt, ...)` call
     - Step 8: keyed metadata join (`meta[result, on = "pip_id"]`)
  3. `R/zzz.R` — `.onLoad()` now sets
     `collapse::set_collapse(nthreads = min(4L, parallel::detectCores(logical = FALSE)))`
     on package load (silent fallback if OpenMP unavailable).

- **Tests**: All pre-existing tests pass (414 passing). New multi-survey
  regression tests added in `test-compute-measures.R` and `test-table-maker.R`
  verify numerical identity with tolerance 1e-10 against sequential references.

- **Acceptance criteria met**:
  - ✅ All family functions accept optional `grp` argument (pre-existing)
  - ✅ Benchmark confirms <1s for 15-survey typical workload at nthreads=4
  - ✅ Results numerically identical to Approach A

### Step 9: Documentation and DESCRIPTION Updates

- **Files**: `DESCRIPTION`, `NAMESPACE` (via roxygen2), `R/*.R` (roxygen tags)
- **Details**:
  1. Verify all new exported functions have complete roxygen2 documentation:
     - `@param`, `@return`, `@export`, `@examples`, `@family`
     - `compute_poverty()`, `compute_inequality()`, `compute_welfare()` —
       family `"compute"` or `"measures"`
     - `table_maker()` — family `"api"` or standalone
     - `pip_measures()`, `pip_age_bins()` — family `"schema"` or `"measures"`
  2. Add `@importFrom` tags for all collapse functions used:
     - `collapse::fsum`, `collapse::fmean`, `collapse::fmedian`, `collapse::fnth`,
       `collapse::fvar`, `collapse::fsd`, `collapse::fmin`, `collapse::fmax`,
       `collapse::fnobs`, `collapse::GRP`, `collapse::TRA`, `collapse::BY`
  3. Add `@importFrom` tags for data.table functions:
     - `data.table::melt`, `data.table::fcase`, `data.table::rbindlist`,
       `data.table::setindex`, `data.table::copy`
  4. Run `devtools::document()` to regenerate `NAMESPACE` and `.Rd` files.
  5. Run `devtools::check()` — no errors, no warnings, no notes.
  6. Update `docs/project-context.md`:
     - Update "Computation Engine" section with actual function names
     - Update data flow diagram
     - Add JSON output example
  7. Update `compound-gpid.md`:
     - Update "Architecture Notes" key components list
     - Update "Current Focus" to reflect Phase 1 implementation
- **Tests**: `devtools::check()` passes cleanly.
- **Acceptance criteria**: All functions documented. Package passes R CMD check.
  Project docs reflect the actual implementation.

## Testing Strategy

### Test Data Approach

All tests use **inline fixture data** — small data.tables constructed directly
in the test file. No dependency on external files, network, or the real Arrow
repository.

**Fixture builder helpers** (shared across test files via a helper file or
defined at the top of each test file):

```r
# Minimal survey data.table for computation tests
make_survey_dt <- function(n = 10L, dims = character(0L)) {
  dt <- data.table(
    welfare = seq(1, by = 1, length.out = n),
    weight  = rep(1.0, n)
  )
  if ("gender" %in% dims)
    dt[, gender := factor(rep(c("male", "female"), length.out = n),
                          levels = c("male", "female"))]
  if ("area" %in% dims)
    dt[, area := factor(rep(c("urban", "rural"), length.out = n),
                        levels = c("urban", "rural"))]
  if ("age" %in% dims)
    dt[, age := as.integer(seq(5L, by = 10L, length.out = n))]
  dt
}
```

### Coverage Targets

| File | Target | Notes |
|------|--------|-------|
| `R/measures.R` | 100% | Small, pure validation logic |
| `R/compute_poverty.R` | ≥95% | Core computation — every branch tested |
| `R/compute_inequality.R` | ≥95% | Gini + MLD with edge cases |
| `R/compute_welfare.R` | ≥95% | Mean + median |
| `R/compute_measures.R` | ≥90% | Dispatch logic + merge |
| `R/table_maker.R` | ≥85% | Integration — some paths tested via sub-function tests |

### Edge Cases

- Empty group (all rows filtered out by dimension) → row for that group with
  NA values? Or omitted? Decision: omitted (data.table drops empty groups by
  default).
- Single row per group → Gini = 0, MLD = 0, median = welfare
- All identical welfare → Gini = 0, MLD = 0
- All zero welfare → headcount = 1.0 (all poor), Watts = 0 (zero contribution),
  MLD = 0 (zero contribution), mean = 0, median = 0
- Poverty line below all welfare → headcount = 0
- Poverty line above all welfare → headcount = 1.0
- Very large weights (>1e9) → numerical stability
- NA in dimension column → separate NA group in cross-tabulation

## Documentation Checklist

- [ ] `compute_poverty()` — roxygen2 with formula, parameters, return, examples
- [ ] `compute_inequality()` — roxygen2 with Gini/MLD formulas, zero-welfare rule
- [ ] `compute_welfare()` — roxygen2 with table of collapse function mapping
- [ ] `compute_measures()` — internal documentation (not exported, no roxygen @export)
- [ ] `table_maker()` — roxygen2 with full API docs, dual-input examples, JSON example in @examples
- [ ] `pip_lookup()` — roxygen2 with triplet resolution examples
- [ ] `pip_measures()` — roxygen2
- [ ] `pip_age_bins()` — roxygen2
- [ ] `.bin_age()` — @keywords internal
- [ ] `.classify_measures()` — @keywords internal
- [ ] `.validate_by()` — @keywords internal
- [ ] `.validate_poverty_lines()` — @keywords internal
- [ ] `.gini_sorted()` — @keywords internal
- [ ] `docs/project-context.md` — updated with final function names and data flow
- [ ] `compound-gpid.md` — updated Architecture Notes and Current Focus

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Gini numerical precision for very unequal distributions | Low | Incorrect results | Validate against known extreme-distribution values. Use double precision throughout. |
| Zero-welfare rule inconsistency between Watts and MLD | Medium | Methodological error | Single internal helper `.handle_zero_welfare()` used by both families. Document rule prominently. |
| Cross-join memory for very large surveys + many poverty lines | Low | OOM | Guard: warn if expansion >5M rows. At typical scale (50K × 5), this is 250K rows — negligible. |
| {collapse} `fmedian` weighted behaviour | Low | Incorrect median | Verify against hand-computed weighted median in tests. {collapse} `fmedian` supports weights natively. |
| Missing dimension in survey data | Medium | Runtime error or silent NA | `table_maker()` Step 7.4 computes per-entry overlap via `vapply()` over manifest `dimensions` list column. Entries with ≥1 match kept; missing columns filled with `NA_character_` in Step 7.7a. Zero-overlap entries dropped. Consolidated `cli_warn()` for both partial and zero cases. (Brainstorm: `2026-04-16-dimension-prefilter-partial-match.md`.) |
| GRP object compatibility across collapse versions | Low | Breakage on upgrade | Pin collapse minimum version in DESCRIPTION. Test with current version. |
| Gini per-group sort performance with many groups | Low | Slow | Pre-sort dt by `by` + welfare. Use `setindex()`. Benchmark confirms <1s at target scale. |

## Out of Scope

- Societal Poverty Rate (SPR) — out of scope for this project phase
- Prosperity Gap — out of scope for this project phase
- Marginals and totals — explicitly excluded from this iteration
- API layer (plumber/JSON serving) — separate package/component
- **API → `table_maker()` parameter interface** — ~~not yet defined~~
  **Resolved** in brainstorm `2026-04-27-table-maker-input-parameters.md`.
  `table_maker()` uses a dual-input signature: `pip_id` (primary, for API)
  or `country_code`/`year`/`welfare_type` triplets (fallback, for ad-hoc R).
  The plumber API layer passes `pip_id` directly; triplet resolution happens
  via `pip_lookup()`. See brainstorm for full pipeline analysis
  (UI → URL → Plumber → table_maker).
- Caching of computed results — future optimization
- Parallel computation across surveys — future optimization
- Wide-format output option — not needed per brainstorm decision
- Poverty line validation against PPP standards — external concern
- Survey weight normalization — upstream responsibility ({pipdata})

## File Summary

| File | Status | Description |
|------|--------|-------------|
| `R/measures.R` | New | Measure registry, validation helpers, age binning |
| `R/compute_poverty.R` | New | Poverty family: FGT + Watts + pop_poverty |
| `R/compute_inequality.R` | New | Inequality family: Gini + MLD |
| `R/compute_welfare.R` | New | Welfare family: mean + median |
| `R/compute_measures.R` | New | Orchestrator: family dispatch + merge |
| `R/table_maker.R` | New | Top-level API: `table_maker()` (dual-input) + `pip_lookup()` |
| `tests/testthat/test-measures.R` | New | Registry, validation, age binning tests |
| `tests/testthat/test-compute-poverty.R` | New | Poverty measure tests |
| `tests/testthat/test-compute-inequality.R` | New | Inequality measure tests |
| `tests/testthat/test-compute-welfare.R` | New | Welfare measure tests |
| `tests/testthat/test-compute-measures.R` | New | Orchestrator tests |
| `tests/testthat/test-table-maker.R` | New | Integration tests |
| `tests/testthat/test-performance.R` | New | Benchmark tests (skip_on_cran) |
