# ============================================================================
# benchmarks/new-pipeline-comparison.R
#
# Purpose:
#   Compare three end-to-end pipeline approaches for computing PIP welfare
#   measures across surveys, with explicit timing of I/O and compute phases.
#
# Usage:
#   source("benchmarks/new-pipeline-comparison.R")
#
# Objective:
#   Which end-to-end approach is faster for computing PIP welfare measures?
#
#   A   Column-pruned I/O + full R compute
#       Arrow select(6 needed cols) before collect(); then compute_measures()
#       for all 4 measures in R using collapse.
#
#   B   Arrow push-down + partial R compute
#       Two Arrow scans:
#         Scan 1: group_by + summarise inside Arrow (before collect) for the
#                 measures that are Arrow-feasible (headcount, sum).
#                 Returns one aggregated row per group, not per person.
#         Scan 2: column-pruned collect of full microdata -> collapse for
#                 the measures NOT Arrow-feasible for PIP (gini, median).
#       Arrow handles scalar-aggregate measures; R only receives what
#       it strictly needs for order-dependent algorithms.
#
#   A1  Historical reference -- load_surveys() reads all 14 schema columns
#       then compute_measures() for all 4 measures.  Included to quantify
#       the column-pruning gain vs the un-pruned production baseline.
#
# Measure Arrow-feasibility:
#   headcount  YES  scalar aggregate: sum(weight * 1[welfare < pl]) / sum(weight)
#   sum        YES  scalar aggregate: sum(welfare * weight)
#   median     NO*  Arrow's median() is unweighted; PIP requires weighted
#                   quantile (collapse::fmedian).  Must be computed in R.
#   gini       NO   requires a sorted welfare vector for the Lorenz curve;
#                   not expressible as a single-pass scalar aggregate.
#
# Full .MEASURE_REGISTRY Arrow-feasibility (19 measures):
#   YES:  headcount, poverty_gap, severity, watts, pop_poverty  [poverty]
#         mld                                                    [inequality]
#         mean, sd, var, min, max, nobs, sum                    [welfare]
#   NO*:  median, p10, p25, p75, p90  (Arrow unweighted only)  [welfare]
#   NO:   gini                                                  [inequality]
#
# Iteration design -- random vs fixed surveys:
#   Each iteration draws a NEW random sample of N_SURVEYS surveys.  All
#   approaches within a given iteration use the SAME sample, so
#   inter-approach timing differences are not confounded by survey selection.
#
#   Fixed surveys (same 15 every iteration):
#     + Cleaner isolation of OS/network noise; lower cross-iteration variance.
#     - Unrepresentative: a single draw may skew small or large.
#   Random surveys (this script -- CHOSEN APPROACH):
#     + Covers the real production distribution of survey sizes
#       (~7,500-525,000 rows; median ~49,000).
#     + Results generalise to the full workload, not just one lucky draw.
#     - Higher cross-iteration variance -- but this variance is informative:
#       it reflects realistic sensitivity to batches dominated by large surveys.
#       The IQR whiskers on the bar chart make this visible rather than hidden.
#
# Key outputs:
#   benchmarks/pipeline-comparison-results.png
#   .cg-docs/solutions/performance-issues/2026-04-29-pipeline-comparison-results.md
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(collapse)
  library(arrow)
  library(dplyr,  warn.conflicts = FALSE)
  library(rlang,  warn.conflicts = FALSE)
  library(DBI)
  library(duckdb)
  library(ggplot2)
  library(patchwork)
})

devtools::load_all(quiet = TRUE)

# -- Configuration -------------------------------------------------------------

# Measures -- one from each Arrow-feasibility category (see header):
#   headcount  YES  Arrow-feasible (conditional sum / total weight)
#   sum        YES  Arrow-feasible (sum(welfare * weight))
#   median     NO*  NOT Arrow-feasible for PIP (Arrow's median() is unweighted;
#                   PIP requires collapse::fmedian -- weighted quantile)
#   gini       NO   NOT Arrow-feasible (Lorenz curve requires sorted vector)
MEASURES      <- c(
   "headcount", "sum",
  "median", "gini")
POVERTY_LINES <- c(3.0, 4.10)

# Disaggregation dimensions.
# `age` is stored as a plain integer in the Arrow schema (NOT dictionary-
# encoded) and must be binned into four groups (0-14, 15-24, 25-64, 65+)
# before grouping.  This is done inside the Arrow lazy query in Approach B
# Scan 1 (via dplyr::case_when()), or in R after collect() for Approach A
# and Approach B Scan 2 (via .bm_bin_age()).
BY_DIMS <- c("age", "gender", "educat4")

N_SURVEYS    <- 10L
N_ITERATIONS <- 50L
BASE_SEED    <- 2026L

# Columns needed after pruning: pip_id + welfare + weight + 3 dimensions.
# `age` is included as a raw integer; it is binned before compute.
# Dropped vs the full 14-col schema: country_code, surveyid_year,
# welfare_type, version, survey_acronym, area, educat5, educat7.
NEEDED_COLS <- c("pip_id", "welfare", "weight", BY_DIMS)

# Dictionary-encoded dimension columns in the PIP Arrow schema.
# Arrow's aggregation engine requires these to be cast to character before
# group_by + summarise across multiple Parquet files (different files may
# encode the same category with different integer indices, which Arrow
# cannot unify automatically during a lazy grouped query).
# NOTE: `age` is NOT in this list -- it is a plain integer handled via
# dplyr::case_when() binning, which also converts it to character.
DICT_SCHEMA_COLS <- c("gender", "area", "educat4", "educat5", "educat7")

ADOPT_THRESHOLD <- 0.15   # require >= 15% improvement to change the pipeline

# -- 1. Manifest setup ---------------------------------------------------------

mf_full <- piptm_manifest()

# Restrict the sampling pool to surveys that carry ALL requested BY_DIMS.
# This avoids NA-fill complexity during compute and keeps the comparison clean.
mf <- mf_full[vapply(mf_full$dimensions, function(d) all(BY_DIMS %in% d), logical(1L))]

if (nrow(mf) < N_SURVEYS) {
  cli::cli_abort(
    c(
      "Only {nrow(mf)} surveys have all of {.val {BY_DIMS}}; need {N_SURVEYS}.",
      "i" = "Reduce N_SURVEYS or change BY_DIMS."
    )
  )
}

message(sprintf(
  "Manifest pool: %d / %d surveys have all dimensions (%s).",
  nrow(mf), nrow(mf_full), paste(BY_DIMS, collapse = ", ")
))

arrow_root <- piptm_arrow_root()

# -- 2. Internal helpers -------------------------------------------------------

# Build the vector of Parquet file paths for a manifest subset.
# Wraps .build_parquet_paths() (package-internal, available after load_all()).
.bm_paths <- function(entries_dt) {
  unlist(lapply(seq_len(nrow(entries_dt)), function(i) {
    .build_parquet_paths(
      arrow_root    = arrow_root,
      country_code  = entries_dt$country_code[[i]],
      year          = entries_dt$year[[i]],
      welfare_type  = entries_dt$welfare_type[[i]],
      version       = entries_dt$version[[i]]
    )
  }))
}

# Elapsed wall-clock seconds from a proc.time() difference.
.elapsed <- function(pt) unname(pt["elapsed"])

# Age binning helper -- mirrors the age-binning step performed by table_maker()
# between load_surveys() and compute_measures():
#   .bin_age(dt) adds `age_group` (ordered factor, levels = .AGE_BIN_LEVELS)
#   and drops the raw `age` integer column.  We then rename `age_group` back
#   to `age` so that by = BY_DIMS (which contains "age") works downstream.
# Applied in-place after collect() in run_a1() and run_a2().
# In run_a3(), Scan 2 also calls this; Scan 1 replicates the same binning
# inside the Arrow lazy query with dplyr::case_when() (Arrow cannot execute
# data.table::fcase() lazily).
.bm_bin_age <- function(dt) {
  .bin_age(dt)                                   # adds age_group, drops age
  data.table::setnames(dt, "age_group", "age")   # restore column name
  invisible(dt)
}

# -- 3. Approach functions -----------------------------------------------------
#
# Each function returns a list: result (data.table), t_io (seconds), t_cmp (s).
# I/O and compute phases are timed separately.

# -- A1: Historical reference -- all columns, full R compute ------------------
# load_surveys() reads all 14 schema columns (no pruning).  Included to
# quantify the column-pruning gain established in earlier benchmarks.
# Age is binned in R after load, mirroring table_maker()'s internal pipeline.
run_a1 <- function(entries_dt) {
  t0   <- proc.time()
  dt   <- load_surveys(entries_dt)
  t_io <- .elapsed(proc.time() - t0)

  t0 <- proc.time()
  .bm_bin_age(dt)   # bin raw age integers -> ordered factor (in-place)
  result <- compute_measures(dt, MEASURES, POVERTY_LINES, BY_DIMS)
  t_cmp  <- .elapsed(proc.time() - t0)

  list(result = result, t_io = t_io, t_cmp = t_cmp)
}

# -- Approach A: Column-pruned I/O, full R compute ----------------------------
# Arrow select(NEEDED_COLS) before collect() -- only the 6 needed columns are
# transferred over the network.  All four measures are computed in R via
# compute_measures() (collapse GRP dispatch).
#
# Why not call load_surveys() and subset after collect()?
# The I/O cost scales with columns read off disk/network, not with columns
# kept in memory after collection.  select() MUST precede collect() to skip
# Parquet column chunks on the wire.
run_a2 <- function(entries_dt) {
  t0    <- proc.time()
  files <- .bm_paths(entries_dt)
  dt    <- arrow::open_dataset(files, format = "parquet") |>
    dplyr::select(dplyr::all_of(NEEDED_COLS)) |>
    dplyr::collect() |>
    data.table::as.data.table()
  t_io <- .elapsed(proc.time() - t0)

  t0 <- proc.time()
  .bm_bin_age(dt)   # bin raw age integers -> ordered factor (in-place)
  result <- compute_measures(dt, MEASURES, POVERTY_LINES, BY_DIMS)
  t_cmp  <- .elapsed(proc.time() - t0)

  list(result = result, t_io = t_io, t_cmp = t_cmp)
}

# -- Approach B: Arrow push-down aggregation, partial R compute ---------------
#
# Two Arrow scans, both timed as I/O (network -> R boundary):
#
#   Scan 1 -- Arrow group_by + summarise BEFORE collect().
#     Arrow executes the aggregation on the Parquet files and returns one row
#     per (pip_id x age_bin x gender x educat4) group instead of per person.
#     For 15 surveys this reduces the collected row count from ~millions to
#     ~hundreds.
#     Computes in one scan:
#       sum_w    -- total weighted population per group (denominator)
#       sum_ww   -- sum(welfare * weight) per group -> the `sum` measure directly
#       poor_w_j -- sum(weight * 1[welfare < pl_j]) per group x poverty line
#                   -> headcount numerator per poverty line
#
#     Age binning: `age` (plain integer) is mapped to four character labels
#     with dplyr::case_when() inside the Arrow lazy query, BEFORE group_by.
#     Arrow supports case_when() natively.  data.table::fcase() cannot be
#     executed lazily by Arrow -- do NOT use it here.
#
#     Dict-encoded cols (gender, educat4) must be cast to character before
#     group_by to avoid Arrow's "Unifying differing dictionaries" error.
#     `age` needs no such cast -- it is NOT dictionary-encoded.
#
#   Scan 2 -- column-pruned collect of full microdata.
#     Identical to Approach A's I/O step.  Needed because:
#       gini   -- requires sorted welfare vector (Lorenz curve); not a scalar.
#       median -- Arrow's median() is unweighted; PIP needs weighted quantile.
#     Age is binned in R after collect() via .bm_bin_age().
#
#   Compute (after both scans, timed separately):
#     sum + headcount -- pure R arithmetic on the tiny aggregated table:
#                        sum       = sum_ww      (direct column, no division)
#                        headcount = poor_w_j / sum_w
#     gini + median   -- collapse via compute_measures() on Scan 2 microdata.
#
# Performance expectation:
#   Scan 1 transfers far less data than Approach A's single scan (aggregated
#   rows vs full microdata).  But Scan 2 is the same size as Approach A's
#   scan.  Net I/O is slightly MORE than Approach A (two traversals vs one).
#   Any net advantage must come from compute: the collapse GRP + fsum
#   overhead for `sum` and headcount is eliminated.  Empirically measured here.
run_a3 <- function(entries_dt) {
  files    <- .bm_paths(entries_dt)
  batch_by <- c("pip_id", BY_DIMS)               # pip_id + age + gender + educat4
  dict_by  <- intersect(BY_DIMS, DICT_SCHEMA_COLS)   # gender, educat4 (not age)

  t0 <- proc.time()

  # -- Scan 1: Arrow group_by + summarise (push-down aggregation) -------------
  # Build one conditional-sum expression per poverty line so all headcount
  # numerators are computed in a single Arrow scan (no repeated file reads).
  hc_exprs <- setNames(
    lapply(POVERTY_LINES, function(pl) {
      rlang::expr(sum(dplyr::if_else(welfare < !!pl, weight, 0), na.rm = TRUE))
    }),
    paste0("poor_w_", seq_along(POVERTY_LINES))
  )

  agg <- arrow::open_dataset(files, format = "parquet") |>
    dplyr::select(dplyr::all_of(NEEDED_COLS)) |>
    # Age binning in Arrow: integer -> character label, same cut-points as
    # .bin_age()/.AGE_BIN_LEVELS.  Must happen BEFORE group_by.
    # dplyr::case_when() is Arrow-native; data.table::fcase() is not.
    dplyr::mutate(age = dplyr::case_when(
      age >= 0L  & age <= 14L ~ "0-14",
      age >= 15L & age <= 24L ~ "15-24",
      age >= 25L & age <= 64L ~ "25-64",
      age >= 65L              ~ "65+",
      .default                = NA_character_
    )) |>
    # Cast dict-encoded cols to string so Arrow can hash-aggregate across files.
    # `age` needs no cast here -- it is already plain character after case_when.
    dplyr::mutate(dplyr::across(dplyr::all_of(dict_by), as.character)) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(batch_by))) |>
    dplyr::summarise(
      sum_w  = sum(weight,           na.rm = TRUE),
      sum_ww = sum(welfare * weight, na.rm = TRUE),
      !!!hc_exprs,
      .groups = "drop"
    ) |>
    dplyr::collect() |>
    data.table::as.data.table()

  # -- Scan 2: column-pruned microdata for gini + median ----------------------
  dt <- arrow::open_dataset(files, format = "parquet") |>
    dplyr::select(dplyr::all_of(NEEDED_COLS)) |>
    dplyr::collect() |>
    data.table::as.data.table()
  .bm_bin_age(dt)   # bin age in R (integer -> ordered factor, in-place)

  t_io <- .elapsed(proc.time() - t0)

  # -- Compute: sum from aggregated table (direct column, no division) ---------
  # sum(welfare * weight) = sum_ww exactly; no further arithmetic needed.
  t0 <- proc.time()

  r_sum <- agg[, .(
    value        = sum_ww,
    population   = sum_w,
    measure      = "sum",
    poverty_line = NA_real_
  ), by = batch_by]

  # -- Compute: headcount from aggregated table (conditional sum / total w) ----
  r_hc <- rbindlist(lapply(seq_along(POVERTY_LINES), function(j) {
    pl     <- POVERTY_LINES[[j]]
    pw_col <- paste0("poor_w_", j)
    agg[, .(
      value        = get(pw_col) / sum_w,
      population   = sum_w,
      measure      = "headcount",
      poverty_line = pl
    ), by = batch_by]
  }))

  # -- Compute: gini + median from microdata via collapse ----------------------
  # poverty_lines = NULL: neither measure requires a poverty threshold.
  # compute_measures() prepends pip_id to by internally.
  r_gm <- compute_measures(dt, c("gini", "median"), poverty_lines = NULL,
                            by = BY_DIMS)

  result <- rbindlist(list(r_sum, r_hc, r_gm), fill = TRUE)
  t_cmp  <- .elapsed(proc.time() - t0)

  list(result = result, t_io = t_io, t_cmp = t_cmp)
}

# -- Approach C: DuckDB push-down aggregation, partial R compute ---------------
#
# Mirrors Approach B (A3) but replaces Arrow's lazy grouped query (Scan 1)
# with a DuckDB SQL aggregation executed against the same Parquet files.
#
# Why DuckDB over Arrow for Scan 1:
#   - DuckDB reads Parquet natively via read_parquet() SQL; no Arrow lazy
#     query construction required.
#   - Dictionary-encoded columns (gender, educat4) are decoded to VARCHAR
#     automatically -- no explicit mutate(across(..., as.character)) needed.
#   - Age binning (CASE WHEN) and GROUP BY execute in one CTE; no separate
#     mutate() + group_by() + summarise() chain.
#   - All poverty-line conditional sums execute in a single SQL statement
#     (one Parquet scan) via CASE WHEN inside SUM().
#   - DuckDB's vectorised execution engine provides an independent data point
#     vs Arrow's compute kernel on this network-mounted Parquet workload.
#
# Architecture (identical to A3 except Scan 1 + Scan 2 engine):
#   Scan 1: DuckDB SQL -> aggregated table (one row per group)
#   Scan 2: DuckDB SQL -> column-pruned microdata (same NEEDED_COLS as A/A3)
#   Compute: sum + headcount from agg table; gini + median from microdata
#
# Both scans share a single in-memory DuckDB connection to amortise
# connection initialisation overhead (~5-20ms fixed cost).
#
# Tradeoffs vs Approach A (A2):
#   - Two file scans (same as A3) -- net I/O may exceed A.
#   - If DuckDB aggregation is faster than collapse GRP + fsum for
#     Arrow-feasible measures, C wins on compute; otherwise A wins.
#   - R still required for gini and median (same limitation as A3).
#
# SQL design notes:
#   - CTE bins `age` (plain integer) into four character labels BEFORE the
#     outer GROUP BY, avoiding DuckDB alias-vs-source-column ambiguity.
#   - CAST(gender/educat4 AS VARCHAR): explicit decoding of Parquet dict
#     columns; DuckDB resolves dict encoding implicitly but the cast
#     documents intent and ensures VARCHAR output type.
#   - Poverty-line conditional sums use SUM(CASE WHEN welfare < pl THEN
#     weight ELSE 0 END): all pls computed in ONE SQL scan, not looped.
#   - UNC paths normalised to forward slashes for SQL string literal safety.
#
# Assumptions:
#   - DuckDB >= 0.8 (CTE, read_parquet list syntax, GROUP BY expressions).
#   - DuckDB decodes Parquet dictionary-encoded columns to VARCHAR strings,
#     matching Arrow's factor-then-character output for the same columns.
#   - UNC paths (//server/...) are handled by DuckDB's VFS on Windows.
#
# Connection lifecycle:
#   `con` is passed in from outside -- the caller owns the connection.
#   Creating and shutting down a DuckDB driver on every iteration causes
#   crashes (50 rapid driver teardown cycles exhaust DuckDB's R process).
#   The connection is created once before the benchmark loop and closed
#   once after it. run_a4() does NOT close the connection.
run_a4 <- function(entries_dt, con) {
  files    <- .bm_paths(entries_dt)
  batch_by <- c("pip_id", BY_DIMS)

  # Normalise backslashes -> forward slashes: safer in SQL string literals
  # on Windows UNC paths (DuckDB accepts both, but forward slashes avoid
  # potential escape issues in sprintf-built SQL strings).
  files_norm <- gsub("\\\\", "/", files)
  files_sql  <- paste0("['", paste(files_norm, collapse = "', '"), "']")

  t0 <- proc.time()

  # -- Scan 1: DuckDB aggregation + age binning in SQL ------------------------
  # Build one SUM(CASE WHEN welfare < pl ...) column per poverty line so that
  # all headcount numerators are computed in a SINGLE Parquet scan.
  # %.15g preserves full double precision in the SQL numeric literal.
  hc_sql <- paste(
    vapply(seq_along(POVERTY_LINES), function(j) {
      sprintf(
        "SUM(CASE WHEN welfare < %.15g THEN weight ELSE 0 END) AS poor_w_%d",
        POVERTY_LINES[[j]], j
      )
    }, character(1L)),
    collapse = ",\n      "
  )

  agg_sql <- sprintf(
    "WITH binned AS (
      SELECT
        pip_id,
        CASE
          WHEN age >= 0  AND age <= 14 THEN '0-14'
          WHEN age >= 15 AND age <= 24 THEN '15-24'
          WHEN age >= 25 AND age <= 64 THEN '25-64'
          WHEN age >= 65              THEN '65+'
          ELSE NULL
        END AS age,
        CAST(gender  AS VARCHAR) AS gender,
        CAST(educat4 AS VARCHAR) AS educat4,
        welfare,
        weight
      FROM read_parquet(%s)
    )
    SELECT
      pip_id,
      age,
      gender,
      educat4,
      SUM(weight)           AS sum_w,
      SUM(welfare * weight) AS sum_ww,
      %s
    FROM binned
    GROUP BY pip_id, age, gender, educat4",
    files_sql, hc_sql
  )

  agg <- data.table::as.data.table(DBI::dbGetQuery(con, agg_sql))

  # -- Scan 2: column-pruned microdata for gini + median ----------------------
  # DuckDB SELECT of NEEDED_COLS -- `age` is returned as integer (not dict-
  # encoded), as expected by .bm_bin_age().  gender/educat4 decoded to VARCHAR;
  # compute_measures() / collapse::GRP handles character grouping columns.
  # Reuses the same connection: no second connection initialisation overhead.
  micro_sql <- sprintf(
    "SELECT
      pip_id,
      welfare,
      weight,
      age,
      CAST(gender  AS VARCHAR) AS gender,
      CAST(educat4 AS VARCHAR) AS educat4
    FROM read_parquet(%s)",
    files_sql
  )
  dt <- data.table::as.data.table(DBI::dbGetQuery(con, micro_sql))
  .bm_bin_age(dt)   # bin raw age integers -> ordered factor (in-place)

  t_io <- .elapsed(proc.time() - t0)

  # -- Compute phase (mirrors A3 exactly) -------------------------------------
  t0 <- proc.time()

  r_sum <- agg[, .(
    value        = sum_ww,
    population   = sum_w,
    measure      = "sum",
    poverty_line = NA_real_
  ), by = batch_by]

  r_hc <- rbindlist(lapply(seq_along(POVERTY_LINES), function(j) {
    pl     <- POVERTY_LINES[[j]]
    pw_col <- paste0("poor_w_", j)
    agg[, .(
      value        = get(pw_col) / sum_w,
      population   = sum_w,
      measure      = "headcount",
      poverty_line = pl
    ), by = batch_by]
  }))

  r_gm <- compute_measures(dt, c("gini", "median"), poverty_lines = NULL,
                            by = BY_DIMS)

  result <- rbindlist(list(r_sum, r_hc, r_gm), fill = TRUE)
  t_cmp  <- .elapsed(proc.time() - t0)

  list(result = result, t_io = t_io, t_cmp = t_cmp)
}

# -- 4. Correctness check ------------------------------------------------------
#
# Run on a small 5-survey fixture (same manifest pool) so the check is fast.
# All three approaches must produce numerically identical results before
# we permit them into the benchmark loop.

message("\n-- Correctness check ---------------------------------------------------")

set.seed(BASE_SEED)
chk_entries <- mf[sample(.N, 5L)]

# Canonical sort so all.equal() is order-independent.
# Factor columns (e.g. `age` as ordered factor from .bm_bin_age()) are coerced
# to character so type differences between approaches do not cause spurious
# failures: Approach B Scan 1 returns `age` as character from Arrow, while
# Approach A returns it as an ordered factor from .bm_bin_age().
.norm <- function(res) {
  key_cols <- intersect(c("pip_id", BY_DIMS, "poverty_line", "measure"), names(res))
  val_cols <- intersect(c("value", "population"), names(res))
  res <- res[, c(key_cols, val_cols), with = FALSE]
  fac_cols <- names(res)[vapply(res, is.factor, logical(1L))]
  for (col in fac_cols) data.table::set(res, j = col, value = as.character(res[[col]]))
  setkeyv(res, key_cols)
  res[]
}

r_chk1 <- tryCatch(run_a1(chk_entries)$result, error = function(e) {
  message("  A1 correctness error: ", e$message); NULL
})
r_chk2 <- tryCatch(run_a2(chk_entries)$result, error = function(e) {
  message("  Approach A correctness error: ", e$message); NULL
})
r_chk3 <- tryCatch(run_a3(chk_entries)$result, error = function(e) {
  message("  Approach B correctness error: ", e$message); NULL
})
r_chk4 <- tryCatch({
  local({
    con_chk <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
    result  <- run_a4(chk_entries, con_chk)$result
    DBI::dbDisconnect(con_chk, shutdown = TRUE)
    result
  })
}, error = function(e) {
  message("  Approach C correctness error: ", e$message); NULL
})

# Approach A: identical compute path to A1 -- expect exact match.
chk_a2 <- !is.null(r_chk1) && !is.null(r_chk2) &&
  isTRUE(all.equal(.norm(r_chk1), .norm(r_chk2), tolerance = 1e-10))

# Approach B: Arrow sum/headcount vs collapse -- allow small floating-point delta.
chk_a3 <- tryCatch({
  !is.null(r_chk1) && !is.null(r_chk3) && {
    ref <- .norm(r_chk1[measure %in% MEASURES])
    cnd <- .norm(r_chk3[measure %in% MEASURES])
    isTRUE(all.equal(ref, cnd, tolerance = 1e-8))
  }
}, error = function(e) FALSE)

# Approach C: DuckDB sum/headcount vs collapse -- allow small floating-point delta.
chk_a4 <- tryCatch({
  !is.null(r_chk1) && !is.null(r_chk4) && {
    ref <- .norm(r_chk1[measure %in% MEASURES])
    cnd <- .norm(r_chk4[measure %in% MEASURES])
    isTRUE(all.equal(ref, cnd, tolerance = 1e-8))
  }
}, error = function(e) FALSE)

message("  A  vs A1 (full result):    ", if (chk_a2) "PASS" else "FAIL -- Approach A excluded")
message("  B  vs A1 (all measures):   ", if (chk_a3) "PASS" else "FAIL -- Approach B excluded")
message("  C  vs A1 (all measures):   ", if (chk_a4) "PASS" else "FAIL -- Approach C excluded")

run_fns <- Filter(Negate(is.null), list(
  A1 = run_a1,
  A2 = if (chk_a2) run_a2 else NULL,
  A3 = if (chk_a3) run_a3 else NULL,
  A4 = if (chk_a4) run_a4 else NULL
))

message("  Proceeding with: ", paste(names(run_fns), collapse = ", "))

# -- 5. Benchmark loop ---------------------------------------------------------
#
# Each iteration:
#   1. Resample N_SURVEYS surveys (different set each iteration).
#   2. Run all passing approaches on the SAME survey set.
#   3. Record t_io, t_cmp, t_total per approach.

message(sprintf(
  "\n-- Benchmark: %d iterations x %d surveys --------------------------------",
  N_ITERATIONS, N_SURVEYS
))
message(sprintf(
  "   measures: %s  |  by: %s  |  pl: %s",
  paste(MEASURES,      collapse = ", "),
  paste(BY_DIMS,       collapse = ", "),
  paste(POVERTY_LINES, collapse = ", ")
))

# Create the DuckDB connection once for all Approach C iterations.
# Closing and reopening a DuckDB driver 50 times causes crashes -- the driver
# must be instantiated once and reused.  Closed in the on.exit() below.
# If Approach C was excluded by the correctness gate, con_duckdb is NULL
# and run_a4 is not in run_fns, so it is never called.
con_duckdb <- if ("A4" %in% names(run_fns)) {
  DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
} else {
  NULL
}

iter_rows <- vector("list", N_ITERATIONS * length(run_fns))
rec_idx   <- 0L

for (i in seq_len(N_ITERATIONS)) {
  set.seed(BASE_SEED + i)
  entries_i <- mf[sample(.N, N_SURVEYS)]

  for (nm in names(run_fns)) {
    out <- tryCatch(
      if (nm == "A4") run_fns[[nm]](entries_i, con_duckdb) else run_fns[[nm]](entries_i),
      error = function(e) {
        message(sprintf("  [iter %02d / %s] FAILED: %s", i, nm, e$message))
        NULL
      }
    )
    if (is.null(out)) next

    rec_idx <- rec_idx + 1L
    iter_rows[[rec_idx]] <- data.table(
      iter      = i,
      approach  = nm,
      t_io_s    = out$t_io,
      t_cmp_s   = out$t_cmp,
      t_total_s = out$t_io + out$t_cmp
    )
  }

  if (i %% 10L == 0L || i == N_ITERATIONS) {
    message(sprintf("  Iteration %d / %d done.", i, N_ITERATIONS))
  }
}

iter_dt <- rbindlist(iter_rows[seq_len(rec_idx)])

# Close the persistent DuckDB connection now that all iterations are done.
if (!is.null(con_duckdb)) {
  DBI::dbDisconnect(con_duckdb, shutdown = TRUE)
  con_duckdb <- NULL
}

# -- 6. Summary table ----------------------------------------------------------

message("\n-- Results -------------------------------------------------------------")

APPROACH_LABELS <- c(
  A1 = "A1: Baseline (all cols)",
  A2 = "A:  Column-pruned I/O",
  A3 = "B:  Arrow push-down",
  A4 = "C:  DuckDB push-down"
)

summary_dt <- iter_dt[, .(
  median_io_s  = median(t_io_s),
  p25_io_s     = quantile(t_io_s,    0.25),
  p75_io_s     = quantile(t_io_s,    0.75),
  median_cmp_s = median(t_cmp_s),
  p25_cmp_s    = quantile(t_cmp_s,   0.25),
  p75_cmp_s    = quantile(t_cmp_s,   0.75),
  median_tot_s = median(t_total_s),
  p25_tot_s    = quantile(t_total_s, 0.25),
  p75_tot_s    = quantile(t_total_s, 0.75)
), by = approach]

summary_dt[, label := APPROACH_LABELS[approach]]

tbl <- summary_dt[, .(
  approach        = label,
  `I/O med (s)`   = round(median_io_s,  3),
  `CMP med (s)`   = round(median_cmp_s, 3),
  `Total med (s)` = round(median_tot_s, 3),
  `Total p25 (s)` = round(p25_tot_s,    3),
  `Total p75 (s)` = round(p75_tot_s,    3)
)]
print(tbl)

# -- 7. Decisions --------------------------------------------------------------

message("\n-- Decisions -----------------------------------------------------------")

.sp <- function(base, new) {
  if (!is.na(base) && !is.na(new) && base > 0) (base - new) / base else NA_real_
}
.fmt <- function(sp) {
  if (is.na(sp)) return("N/A")
  sprintf("%.0f%% %s", abs(sp) * 100, if (sp > 0) "faster" else "slower")
}

# Safe extraction (approach may have been excluded by correctness gate)
.get <- function(col) {
  function(nm) {
    v <- summary_dt[approach == nm, get(col)]
    if (length(v) == 1L) v else NA_real_
  }
}
get_tot <- .get("median_tot_s")
get_io  <- .get("median_io_s")
get_cmp <- .get("median_cmp_s")

a1_tot <- get_tot("A1"); a2_tot <- get_tot("A2"); a3_tot <- get_tot("A3"); a4_tot <- get_tot("A4")
a1_io  <- get_io("A1");  a2_io  <- get_io("A2")

sp_a2_total <- .sp(a1_tot, a2_tot)
sp_a2_io    <- .sp(a1_io,  a2_io)
sp_a3_total <- .sp(a2_tot, a3_tot)
sp_a4_total <- .sp(a2_tot, a4_tot)

dec_a2 <- if (!chk_a2) {
  "Approach A excluded (correctness check failed)."
} else if (is.na(sp_a2_total)) {
  "Approach A result unavailable."
} else if (sp_a2_total >= ADOPT_THRESHOLD) {
  sprintf(
    "ADOPT column pruning in load_surveys(): Approach A is **%s** total vs A1 (I/O alone: %s).",
    .fmt(sp_a2_total), .fmt(sp_a2_io)
  )
} else {
  sprintf(
    "KEEP current pipeline: Approach A is only %s total vs A1 -- below the %.0f%% threshold.",
    .fmt(sp_a2_total), ADOPT_THRESHOLD * 100
  )
}

dec_a3 <- if (!chk_a3) {
  "Approach B excluded (correctness check failed)."
} else if (is.na(sp_a3_total)) {
  "Approach B result unavailable."
} else if (sp_a3_total >= ADOPT_THRESHOLD) {
  sprintf(
    "ADOPT Arrow push-down: Approach B is **%s** total vs Approach A.",
    .fmt(sp_a3_total)
  )
} else {
  sprintf(
    "KEEP Approach A compute path: Approach B is only %s total vs A -- below the %.0f%% threshold.",
    .fmt(sp_a3_total), ADOPT_THRESHOLD * 100
  )
}

dec_a4 <- if (!chk_a4) {
  "Approach C excluded (correctness check failed)."
} else if (is.na(sp_a4_total)) {
  "Approach C result unavailable."
} else if (sp_a4_total >= ADOPT_THRESHOLD) {
  sprintf(
    "ADOPT DuckDB push-down: Approach C is **%s** total vs Approach A.",
    .fmt(sp_a4_total)
  )
} else {
  sprintf(
    "KEEP Approach A compute path: Approach C is only %s total vs A -- below the %.0f%% threshold.",
    .fmt(sp_a4_total), ADOPT_THRESHOLD * 100
  )
}

message("  [A  vs A1]  I/O: ", .fmt(sp_a2_io), "  |  Total: ", .fmt(sp_a2_total))
message("  [B  vs A ]  Total: ", .fmt(sp_a3_total))
message("  [C  vs A ]  Total: ", .fmt(sp_a4_total))
message("  [A ] ", dec_a2)
message("  [B ] ", dec_a3)
message("  [C ] ", dec_a4)

# -- 8. Visualisation ----------------------------------------------------------

dir.create("benchmarks", showWarnings = FALSE)

# Factor levels: A1 at top of y-axis (last in factor = topmost bar)
app_order <- rev(intersect(c("A1", "A2", "A3", "A4"), names(run_fns)))
lev_order <- APPROACH_LABELS[app_order]

# -- 8a. Stacked bar: I/O + compute, IQR whisker on total ---------------------

long_dt <- rbindlist(list(
  summary_dt[approach %in% names(run_fns),
    .(approach, phase = "I/O",     time_s = median_io_s)],
  summary_dt[approach %in% names(run_fns),
    .(approach, phase = "Compute", time_s = median_cmp_s)]
))
long_dt[, approach_f := factor(APPROACH_LABELS[approach], levels = lev_order)]
long_dt[, phase_f    := factor(phase, levels = c("Compute", "I/O"))]  # I/O on top

totals_dt <- summary_dt[approach %in% names(run_fns), .(
  approach_f = factor(APPROACH_LABELS[approach], levels = lev_order),
  total      = median_tot_s,
  p25        = p25_tot_s,
  p75        = p75_tot_s,
  label      = sprintf("%.3fs", median_tot_s)
)]

p_bar <- ggplot(long_dt, aes(y = approach_f, x = time_s, fill = phase_f)) +
  geom_col(width = 0.6) +
  geom_errorbar(
    data = totals_dt,
    aes(y = approach_f, xmin = p25, xmax = p75, fill = NULL),
    width = 0.25, colour = "grey25", linewidth = 0.7, inherit.aes = FALSE,
    orientation = "y"
  ) +
  geom_text(
    data = totals_dt,
    aes(y = approach_f, x = total, label = label, fill = NULL),
    hjust = -0.2, size = 3.4, inherit.aes = FALSE
  ) +
  scale_fill_manual(
    values = c("I/O" = "#2c7bb6", "Compute" = "#f39c12"),
    name   = "Phase"
  ) +
  scale_x_continuous(expand = expansion(mult = c(0, 0.22))) +
  labs(
    title    = "Pipeline comparison -- median time by phase (IQR whiskers on total)",
    subtitle = sprintf(
      "%d surveys x %d iterations  |  measures: %s  |  by: %s  |  pl: %s",
      N_SURVEYS, N_ITERATIONS,
      paste(MEASURES,      collapse = ", "),
      paste(BY_DIMS,       collapse = ", "),
      paste(POVERTY_LINES, collapse = ", ")
    ),
    y = NULL, x = "Median time (seconds)"
  ) +
  theme_minimal(base_size = 11) +
  theme(
    panel.grid.major.y = element_blank(),
    legend.position    = "right"
  )

# -- 8b. Per-iteration scatter: variance from survey resampling ----------------
#
# Each iteration uses a different set of 15 surveys.  This plot shows how much
# run-to-run variance comes from "which surveys were sampled" vs the approach.

iter_dt[, approach_f := factor(
  APPROACH_LABELS[approach],
  levels = APPROACH_LABELS[intersect(c("A1", "A2", "A3", "A4"), names(run_fns))]
)]

ITER_COLS <- c(
  "A1: Baseline (all cols)" = "#e74c3c",
  "A:  Column-pruned I/O"   = "#2c7bb6",
  "B:  Arrow push-down"     = "#27ae60",
  "C:  DuckDB push-down"    = "#8e44ad"
)

p_iter <- ggplot(
  iter_dt[approach %in% names(run_fns)],
  aes(x = iter, y = t_total_s, colour = approach_f)
) +
  geom_line(alpha = 0.55, linewidth = 0.5) +
  geom_point(size = 1.3, alpha = 0.75) +
  scale_colour_manual(
    values = ITER_COLS[intersect(names(ITER_COLS), levels(iter_dt$approach_f))],
    name   = NULL
  ) +
  labs(
    title    = "Per-iteration total time (survey set resampled each iteration)",
    subtitle = "Vertical spread = variance from which 15 surveys were selected",
    x = "Iteration",
    y = "Total time (s)"
  ) +
  theme_minimal(base_size = 11) +
  theme(legend.position = "bottom")

plot_out  <- p_bar / p_iter + plot_layout(heights = c(2, 3))
plot_path <- "benchmarks/pipeline-comparison-results.png"
ggsave(plot_path, plot_out, width = 12, height = 9, dpi = 150)
message("\nPlot saved to ", plot_path)

# -- 9. Results document -------------------------------------------------------

results_dir  <- ".cg-docs/solutions/performance-issues"
results_path <- file.path(
  results_dir,
  "2026-04-29-pipeline-comparison-results.md"
)
dir.create(results_dir, showWarnings = FALSE, recursive = TRUE)

si        <- sessionInfo()
cpu_info  <- tryCatch(paste0(si$running, " / ", si$platform),
                      error = function(e) si$platform)
tbl_lines <- capture.output(print(tbl))

results_md <- c(
  "---",
  "date: 2026-04-29",
  'title: "Pipeline Comparison Benchmark Results"',
  "status: completed",
  "tags: [performance, arrow, io, benchmark, pipeline]",
  "---",
  "",
  "# Pipeline Comparison Benchmark Results",
  "",
  "## Environment",
  "",
  paste0("- **R version**: ",         R.version$version.string),
  paste0("- **Platform**: ",           cpu_info),
  paste0("- **Logical cores**: ",      parallel::detectCores(logical = TRUE)),
  paste0("- **arrow version**: ",      packageVersion("arrow")),
  paste0("- **collapse version**: ",   packageVersion("collapse")),
  paste0("- **data.table version**: ", packageVersion("data.table")),
  "",
  "## Configuration",
  "",
  paste0("- **Surveys per iteration**: ", N_SURVEYS),
  paste0("- **Iterations**: ",            N_ITERATIONS),
  paste0("- **Measures**: `",             paste(MEASURES,      collapse = "`, `"), "`"),
  paste0("- **Poverty lines**: ",         paste(POVERTY_LINES, collapse = ", ")),
  paste0("- **By dimensions**: `",        paste(BY_DIMS,       collapse = "`, `"), "`"),
  paste0("- **Survey pool size**: ",      nrow(mf),
         " surveys (filtered from ", nrow(mf_full),
         " total to those with all BY_DIMS)"),
  "",
  "## Approaches",
  "",
  "| | I/O | Compute |",
  "| --- | --- | --- |",
  paste0(
    "| **A1 Baseline** | `load_surveys()` -- all 14 schema cols | ",
    "`compute_measures()` for ", paste(MEASURES, collapse = ", "), " |"
  ),
  paste0(
    "| **A Column-pruned** | Arrow `select(", length(NEEDED_COLS), " cols)` before `collect()` ",
    "| `compute_measures()` for ", paste(MEASURES, collapse = ", "), " |"
  ),
  paste0(
    "| **B Arrow push-down** | ",
    "Scan 1: Arrow `group_by+summarise` -> tiny aggregated table ",
    "(one row/group, ", length(NEEDED_COLS), " cols); ",
    "Scan 2: Arrow `select(", length(NEEDED_COLS), " cols)` -> full microdata ",
    "| headcount + sum: R arithmetic on aggregated table; ",
    "gini + median (not Arrow-feasible for PIP): `compute_measures()` |"
  ),
  "",
  "## Arrow Push-Down Feasibility",
  "",
  "| Measure | Arrow-feasible? | Reason |",
  "| --- | --- | --- |",
  "| headcount | YES | Conditional sum / total weight -- scalar aggregate |",
  "| sum | YES | sum(welfare * weight) -- scalar aggregate |",
  "| median | NO (Arrow unweighted only) | Arrow median() is unweighted; PIP requires collapse::fmedian |",
  "| gini | NO | Lorenz curve requires sorted welfare vector; not a scalar aggregate |",
  "",
  "## Results",
  "",
  "```",
  tbl_lines,
  "```",
  "",
  "## Speedup Summary",
  "",
  "| Comparison | I/O speedup | Total speedup | Decision |",
  "| ---------- | ----------- | ------------- | -------- |",
  sprintf(
    "| A vs A1 | %s | %s | %s |",
    .fmt(sp_a2_io), .fmt(sp_a2_total),
    if (!chk_a2) "EXCLUDED"
    else if (!is.na(sp_a2_total) && sp_a2_total >= ADOPT_THRESHOLD) "ADOPT"
    else "KEEP A1"
  ),
  sprintf(
    "| B vs A | same as A | %s | %s |",
    .fmt(sp_a3_total),
    if (!chk_a3) "EXCLUDED"
    else if (!is.na(sp_a3_total) && sp_a3_total >= ADOPT_THRESHOLD) "ADOPT"
    else "KEEP A"
  ),
  sprintf(
    "| C vs A | same as A | %s | %s |",
    .fmt(sp_a4_total),
    if (!chk_a4) "EXCLUDED"
    else if (!is.na(sp_a4_total) && sp_a4_total >= ADOPT_THRESHOLD) "ADOPT"
    else "KEEP A"
  ),
  "",
  "## Decisions",
  "",
  paste0("**A vs A1**: ", dec_a2),
  "",
  paste0("**B vs A**: ", dec_a3),
  "",
  paste0("**C vs A**: ", dec_a4),
  "",
  "## Correctness",
  "",
  paste0("- A  vs A1 (full result):  ", if (chk_a2) "PASS" else "FAIL"),
  paste0("- B  vs A1 (all measures): ", if (chk_a3) "PASS" else "FAIL"),
  paste0("- C  vs A1 (all measures): ", if (chk_a4) "PASS" else "FAIL"),
  "",
  paste0("*Generated: ", format(Sys.time(), "%Y-%m-%d %H:%M"), "*")
)

writeLines(results_md, results_path)
message("Results saved to ", results_path)
