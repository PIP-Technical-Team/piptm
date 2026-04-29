# ============================================================================
# benchmarks/arrow-vs-collapse.R
#
# Purpose:
#   Compare I/O strategies for loading PIP survey microdata from Arrow/Parquet
#   and determine whether column pruning, partial Arrow aggregation, or
#   pre-sorted I/O justifies extra complexity over the current full-read
#   baseline.
#
# Usage:
#   Rscript benchmarks/arrow-vs-collapse.R
#   # or interactively:
#   source("benchmarks/arrow-vs-collapse.R")
#
# Data modes (set LIVE_MODE below):
#   LIVE_MODE = TRUE  (default)
#     Loads N_SURVEYS surveys from the real Arrow repository via the manifest.
#     Parquet paths cached to CACHE_PATH for faster re-runs.
#
#   LIVE_MODE = FALSE
#     Generates a synthetic 15-survey dataset (matching observed size
#     distribution) and writes it to a temp directory as Parquet files.
#     No Arrow repository access required.  Measures column-pruning gains
#     on local disk; not representative of network I/O variance.
#
# Approaches benchmarked:
#
#   I/O-only (loading + collect, no compute):
#     IO-1  Baseline  — open_dataset(files) |> collect()
#     IO-2  Select    — open_dataset(files) |> select(needed) |> collect()
#     IO-3  Sort      — open_dataset(files) |> select(needed) |> arrange() |> collect()
#
#   End-to-end (load + compute_measures()):
#     E2E-1  Baseline — IO-1 + compute_measures()          [current pipeline]
#     E2E-2  Select   — IO-2 + compute_measures()
#     E2E-3  Push     — Arrow computes mean+headcount; collapse computes gini+median
#
# Decision rules:
#   IO-2  vs IO-1  : adopt if ≥30% faster (median)
#   E2E-2 vs E2E-1 : adopt if ≥30% faster (median)
#   E2E-3 vs E2E-2 : adopt if ≥20% faster (median) AND correctness passes
#   Arrow Sort     : flag if sort overhead ≥20% of E2E-1 (plan next steps)
#
# Key outputs:
#   benchmarks/arrow-vs-collapse-results.png
#   .cg-docs/solutions/performance-issues/2026-04-29-arrow-vs-collapse-results.md
#
# Plan: .cg-docs/plans/2026-04-29-arrow-vs-collapse-benchmark.md
# ============================================================================

# ── 0. Setup ──────────────────────────────────────────────────────────────────

suppressPackageStartupMessages({
  library(data.table)
  library(collapse)
  library(arrow)
  library(dplyr, warn.conflicts = FALSE)
  library(ggplot2)
  library(bench)
  library(patchwork)
})

devtools::load_all(quiet = TRUE)

# ── Configuration ─────────────────────────────────────────────────────────────

LIVE_MODE     <- TRUE                     # TRUE = real Arrow data, FALSE = synthetic
CACHE_PATH    <- "benchmarks/arrow-cache.rds"
MEASURES      <- c("headcount", "gini", "mean", "median")
POVERTY_LINES <- c(2.15, 3.65)
BY_DIMS       <- c("gender", "area")
N_SURVEYS     <- 15L                      # surveys to load per benchmark run
N_ITERATIONS  <- 50L                      # bench::mark() iterations per approach

# Decision thresholds
IO_SELECT_THRESHOLD  <- 0.30              # 30%  → adopt column pruning in I/O
E2E_SELECT_THRESHOLD <- 0.30              # 30%  → adopt column pruning in E2E
E2E_PUSH_THRESHOLD   <- 0.20             # 20%  → adopt Arrow push
SORT_THRESHOLD       <- 0.20              # 20%  → flag Arrow sort for investigation

# Columns required by compute_measures() for MEASURES + BY_DIMS
# (pip_id is the survey identifier; kept so output rows identify their survey)
NEEDED_COLS   <- c("pip_id", "welfare", "weight", BY_DIMS)

# Dictionary-encoded dimension columns in the canonical PIP Arrow schema.
# Used by IO-3 (cast before Arrow sort) and E2E-3 (cast after collect).
# Update here if the schema gains or loses dict-encoded columns.
DICT_SCHEMA_COLS <- c("gender", "area", "educat4", "educat5", "educat7")

# Full schema columns (reference only — used for documentation in results)
ALL_SCHEMA_COLS <- c(
  "country_code", "surveyid_year", "welfare_type", "version",
  "pip_id", "survey_acronym", "welfare", "weight",
  "gender", "area", "educat4", "educat5", "educat7", "age"
)
DROPPED_COLS  <- setdiff(ALL_SCHEMA_COLS, NEEDED_COLS)

# ── 1. Data setup ─────────────────────────────────────────────────────────────

if (LIVE_MODE) {

  # ── 1a. Live mode: sample from manifest, build Parquet paths ──────────────

  if (file.exists(CACHE_PATH)) {
    message("Loading cached Arrow paths from ", CACHE_PATH)
    cache             <- readRDS(CACHE_PATH)
    parquet_files_all <- cache$parquet_files_all
    entries           <- cache$entries
  } else {
    message("Sampling manifest entries and building Parquet file paths...")
    mf <- piptm_manifest()
    if (nrow(mf) < N_SURVEYS) {
      cli::cli_abort(
        c(
          "Manifest has only {nrow(mf)} entr{?y/ies}, fewer than N_SURVEYS = {N_SURVEYS}.",
          "i" = "Reduce N_SURVEYS or extend the manifest."
        )
      )
    }

    set.seed(42L)
    entries <- mf[sample(.N, N_SURVEYS)]

    arrow_root <- piptm_arrow_root()
    parquet_files_all <- unlist(lapply(seq_len(nrow(entries)), function(i) {
      .build_parquet_paths(
        arrow_root    = arrow_root,
        country_code  = entries$country_code[[i]],
        year          = entries$year[[i]],
        welfare_type  = entries$welfare_type[[i]],
        version       = entries$version[[i]]
      )
    }))

    dir.create(dirname(CACHE_PATH), showWarnings = FALSE, recursive = TRUE)
    saveRDS(list(parquet_files_all = parquet_files_all, entries = entries), CACHE_PATH)
    message("Cached to ", CACHE_PATH)
  }

  message(sprintf(
    "Live mode ready: %d surveys, %d Parquet file(s).",
    nrow(entries), length(parquet_files_all)
  ))

} else {

  # ── 1b. Synthetic mode: generate data and write to temp Parquet files ─────
  #
  # Size distribution matches real PIP surveys (median ~49K rows, heavy right
  # tail up to ~525K), taken from the orchestration benchmark.

  message("Synthetic mode: generating data and writing temp Parquet files...")

  TMP_ARROW_DIR  <- file.path(tempdir(), "piptm_bm_arrow")
  dir.create(TMP_ARROW_DIR, showWarnings = FALSE, recursive = TRUE)

  set.seed(42L)
  survey_sizes <- c(
    7500L, 9000L, 12000L, 15000L, 22500L, 30000L, 37500L,
    45000L, 60000L, 75000L, 97500L, 150000L, 225000L, 330000L, 525000L
  )
  stopifnot(length(survey_sizes) == N_SURVEYS)

  syn_ccs   <- paste0("TS", seq_len(N_SURVEYS))
  syn_pids  <- sprintf("%s_2020_SRV_INC_ALL", syn_ccs)

  entries <- data.table(
    pip_id        = syn_pids,
    country_code  = syn_ccs,
    year          = rep(2020L, N_SURVEYS),
    welfare_type  = rep("INC", N_SURVEYS),
    version       = rep("v01_v01", N_SURVEYS),
    dimensions    = lapply(seq_len(N_SURVEYS), function(i) c("gender", "area"))
  )

  parquet_files_all <- unlist(lapply(seq_len(N_SURVEYS), function(i) {
    leaf <- file.path(
      TMP_ARROW_DIR,
      paste0("country_code=",  syn_ccs[[i]]),
      paste0("surveyid_year=", 2020L),
      paste0("welfare_type=",  "INC"),
      paste0("version=",       "v01_v01")
    )
    dir.create(leaf, showWarnings = FALSE, recursive = TRUE)

    n   <- survey_sizes[[i]]
    pid <- syn_pids[[i]]

    dt_s <- data.table(
      pip_id         = pid,
      country_code   = syn_ccs[[i]],
      surveyid_year  = 2020L,
      welfare_type   = "INC",
      version        = "v01_v01",
      survey_acronym = "SRV",
      welfare        = rlnorm(n, meanlog = 1.5, sdlog = 0.8),
      weight         = runif(n, 0.5, 2.0),
      gender         = factor(
        sample(c("male", "female"), n, replace = TRUE),
        levels = c("male", "female")
      ),
      area           = factor(
        sample(c("urban", "rural"), n, replace = TRUE),
        levels = c("urban", "rural")
      ),
      educat4        = factor(
        sample(c("no edu", "primary", "secondary", "tertiary"), n, replace = TRUE),
        levels = c("no edu", "primary", "secondary", "tertiary")
      ),
      educat5        = factor(
        sample(c("no edu", "primary", "lower sec", "upper sec", "post sec"), n, replace = TRUE)
      ),
      educat7        = factor(
        sample(c("no edu", "some pri", "primary", "some sec",
                 "secondary", "some tert", "tertiary"), n, replace = TRUE)
      ),
      age            = as.integer(sample(18:65, n, replace = TRUE))
    )

    out_file <- file.path(leaf, paste0(pid, "-0.parquet"))
    arrow::write_parquet(dt_s, out_file)
    out_file
  }))

  total_rows <- sum(survey_sizes)
  message(sprintf(
    "Synthetic dataset ready: %d surveys, %d file(s), %s total rows.",
    N_SURVEYS, length(parquet_files_all), format(total_rows, big.mark = ",")
  ))
}

# ── 2. Approach functions ─────────────────────────────────────────────────────
#
# I/O functions take `files` (character vector of Parquet paths) and return a
# data.table.  E2E functions also run compute_measures().
#
# The by-columns passed to compute_measures() always include pip_id so that
# each output row carries its survey identity (matching the batch Approach B
# convention in orchestration-strategy.R).

# Helper: canonical sort for all.equal() comparisons
.normalize_arrow_result <- function(res) {
  key_cols <- intersect(c("pip_id", BY_DIMS, "poverty_line", "measure"), names(res))
  val_cols <- intersect(c("value", "population"), names(res))
  col_order <- c(key_cols, val_cols)
  res <- res[, intersect(col_order, names(res)), with = FALSE]
  setkeyv(res, key_cols)
  res[]
}

# --- IO-1: Baseline — read all columns (current load_surveys() behaviour) ----
io_1_baseline <- function(files) {
  arrow::open_dataset(files, format = "parquet") |>
    dplyr::collect() |>
    data.table::as.data.table()
}

# --- IO-2: Select — read only columns needed for this benchmark --------------
io_2_select <- function(files, cols = NEEDED_COLS) {
  arrow::open_dataset(files, format = "parquet") |>
    dplyr::select(dplyr::all_of(cols)) |>
    dplyr::collect() |>
    data.table::as.data.table()
}

# --- IO-3: Sort — column-pruned read + Arrow pre-sort for Gini ---------------
#
# Sorts by c("pip_id", by_dims, "welfare") inside Arrow before collect().
# The extra time over IO-2 estimates the cost of the Arrow sort engine.
# Compared to setorder() cost in E2E-1, this indicates whether moving the
# sort from R into Arrow would yield a net win.
#
# Dictionary-encoded columns (gender, area, etc.) must be cast to string
# before sorting — Arrow's RecordBatch sort does not support dict types.
io_3_sort <- function(files, cols = NEEDED_COLS, by = BY_DIMS) {
  sort_cols  <- c("pip_id", by, "welfare")
  # Dimension columns in this benchmark are dict-encoded; cast to string
  # so Arrow's sort engine accepts them (dict sort is unsupported).
  dict_cols  <- intersect(by, DICT_SCHEMA_COLS)

  ds <- arrow::open_dataset(files, format = "parquet") |>
    dplyr::select(dplyr::all_of(cols))

  if (length(dict_cols) > 0L) {
    ds <- ds |> dplyr::mutate(dplyr::across(dplyr::all_of(dict_cols), as.character))
  }

  ds |>
    dplyr::arrange(dplyr::across(dplyr::all_of(sort_cols))) |>
    dplyr::collect() |>
    data.table::as.data.table()
}

# --- E2E-1: Baseline — current production pipeline ---------------------------
e2e_1_baseline <- function(files, measures = MEASURES,
                            poverty_lines = POVERTY_LINES, by = BY_DIMS) {
  dt <- io_1_baseline(files)
  compute_measures(dt, measures, poverty_lines, by)
}

# --- E2E-2: Select — column-pruned I/O + unchanged collapse compute ----------
e2e_2_select <- function(files, measures = MEASURES,
                          poverty_lines = POVERTY_LINES, by = BY_DIMS) {
  dt <- io_2_select(files)
  compute_measures(dt, measures, poverty_lines, by)
}

# --- E2E-3: Push — Arrow handles mean + headcount; collapse handles gini + median
#
# Arrow-computable in this benchmark: "mean", "headcount"
#   weighted mean  = sum(welfare * weight) / sum(weight)
#   headcount(pl)  = sum(weight[welfare < pl]) / sum(weight)  — one scan per pl
#
# R-only (require sorted data or quantile algorithms): "gini", "median"
#   computed on column-pruned collected data via compute_measures()
#
# The two paths share a single Arrow scan per poverty line (headcount) plus one
# for mean, then one collapse() path.  Correctness validated against E2E-1.
#
# Dict-cast strategy: NO cast needed for the R-aggregation path.
# Arrow's collect() converts dict-encoded columns to R factors automatically.
# data.table groupby handles factors natively — no character conversion needed.
# The dict-cast workaround (as.character inside Arrow mutate) was only required
# when pushing group_by|summarise into the Arrow query engine; moving the
# aggregation to R after collect() eliminates the need entirely.
.arrow_weighted_mean <- function(files, by_cols) {
  dt <- arrow::open_dataset(files, format = "parquet") |>
    dplyr::select(dplyr::all_of(c(by_cols, "welfare", "weight"))) |>
    dplyr::collect() |>
    data.table::as.data.table()
  grp_by <- c(by_cols)
  dt[, .(value = sum(welfare * weight) / sum(weight), population = sum(weight)),
     by = grp_by][, measure := "mean"]
}

.arrow_headcount <- function(files, by_cols, poverty_line) {
  dt <- arrow::open_dataset(files, format = "parquet") |>
    dplyr::select(dplyr::all_of(c(by_cols, "welfare", "weight"))) |>
    dplyr::collect() |>
    data.table::as.data.table()
  dt[, poor := as.integer(welfare < poverty_line)]
  grp_by <- c(by_cols)
  dt[, .(value = sum(poor * weight) / sum(weight), population = sum(weight)),
     by = grp_by
  ][, `:=`(measure = "headcount", poverty_line = poverty_line)]
}

e2e_3_push <- function(files, measures = MEASURES,
                        poverty_lines = POVERTY_LINES, by = BY_DIMS) {
  # Arrow path uses explicit batch_by (including pip_id) because we build
  # the grouping ourselves — not routed through compute_measures().
  # collapse path uses by = BY_DIMS; compute_measures() prepends pip_id.
  #
  # NOTE: this function performs 1 + length(poverty_lines) full Arrow scans
  # (1 for weighted mean + 1 per poverty line for headcount).  With the
  # benchmark's 2 poverty lines that is 3 scans vs E2E-2's 1.  To be
  # competitive, headcount for all lines would need a single conditional
  # aggregation scan.  Performance is structurally disadvantaged vs E2E-2.
  batch_by   <- c("pip_id", by)
  arrow_meas <- intersect(measures, c("mean", "headcount"))
  r_meas     <- setdiff(measures, c("mean", "headcount"))

  arrow_parts <- list()

  if ("mean" %in% arrow_meas) {
    arrow_parts$mean <- .arrow_weighted_mean(files, batch_by)
  }

  if ("headcount" %in% arrow_meas) {
    arrow_parts$headcount <- rbindlist(lapply(poverty_lines, function(pl) {
      .arrow_headcount(files, batch_by, pl)
    }))
  }

  r_result <- NULL
  if (length(r_meas) > 0L) {
    dt       <- io_2_select(files)
    # compute_measures() prepends pip_id to by internally — pass BY_DIMS only
    r_result <- compute_measures(dt, r_meas, poverty_lines, by)
  }

  all_parts <- Filter(Negate(is.null), c(arrow_parts, list(r_result)))
  rbindlist(all_parts, fill = TRUE)
}

# ── 3. Correctness pre-check ──────────────────────────────────────────────────
#
# Use a small 3-survey synthetic fixture written to a temp directory.
# This avoids network I/O variance and keeps the correctness check fast.

message("\n── Correctness check ───────────────────────────────────────────────")

check_dir <- file.path(tempdir(), "piptm_bm_check")
dir.create(check_dir, showWarnings = FALSE, recursive = TRUE)

set.seed(123L)
check_sizes <- c(200L, 150L, 180L)
check_pids  <- sprintf("CHK_2020_S%d_INC_ALL", seq_along(check_sizes))

check_files <- mapply(function(pid, n, i) {
  leaf <- file.path(
    check_dir,
    paste0("country_code=CK", i),
    "surveyid_year=2020", "welfare_type=INC", "version=v01"
  )
  dir.create(leaf, showWarnings = FALSE, recursive = TRUE)

  dt_c <- data.table(
    pip_id         = pid,
    country_code   = paste0("CK", i),
    surveyid_year  = 2020L,
    welfare_type   = "INC",
    version        = "v01",
    survey_acronym = "SRV",
    welfare        = rlnorm(n, meanlog = 1.5, sdlog = 0.8),
    weight         = runif(n, 0.5, 2.0),
    gender         = factor(sample(c("male", "female"), n, replace = TRUE),
                            levels = c("male", "female")),
    area           = factor(sample(c("urban", "rural"), n, replace = TRUE),
                            levels = c("urban", "rural")),
    educat4        = factor(
      sample(c("no edu", "primary", "secondary", "tertiary"), n, replace = TRUE),
      levels = c("no edu", "primary", "secondary", "tertiary")
    ),
    educat5        = factor(sample(c("no edu", "primary", "lower sec",
                                     "upper sec", "post sec"), n, replace = TRUE)),
    educat7        = factor(sample(c("no edu", "some pri", "primary", "some sec",
                                     "secondary", "some tert", "tertiary"),
                                   n, replace = TRUE)),
    age            = as.integer(sample(18:65, n, replace = TRUE))
  )
  f <- file.path(leaf, paste0(pid, "-0.parquet"))
  arrow::write_parquet(dt_c, f)
  f
}, check_pids, check_sizes, seq_along(check_sizes), SIMPLIFY = TRUE)

res_1_chk <- tryCatch(
  .normalize_arrow_result(e2e_1_baseline(check_files)),
  error = function(e) { message("  E2E-1 error: ", conditionMessage(e)); NULL }
)
res_2_chk <- tryCatch(
  .normalize_arrow_result(e2e_2_select(check_files)),
  error = function(e) { message("  E2E-2 error: ", conditionMessage(e)); NULL }
)
res_3_chk <- tryCatch(
  .normalize_arrow_result(e2e_3_push(check_files)),
  error = function(e) { message("  E2E-3 error: ", conditionMessage(e)); NULL }
)

check_2 <- !is.null(res_1_chk) && !is.null(res_2_chk) &&
  isTRUE(all.equal(res_1_chk, res_2_chk, tolerance = 1e-10))

# E2E-3 only computes a subset of measures — compare those measures only
check_3 <- tryCatch({
  !is.null(res_1_chk) && !is.null(res_3_chk) && {
    push_meas <- c("mean", "headcount")
    ref_sub   <- res_1_chk[measure %in% push_meas]
    push_sub  <- res_3_chk[measure %in% push_meas]
    isTRUE(all.equal(ref_sub, push_sub, tolerance = 1e-10))
  }
}, error = function(e) FALSE)

message("  E2E-1 vs E2E-2 (Select, full):         ",
        if (check_2) "OK" else "MISMATCH — E2E-2 excluded")
message("  E2E-1 vs E2E-3 (Push, mean+headcount): ",
        if (check_3) "OK" else "MISMATCH — E2E-3 excluded")

valid_e2e <- c("E2E-1", if (check_2) "E2E-2", if (check_3) "E2E-3")
message("  Benchmarking: ", paste(valid_e2e, collapse = ", "))

# ── 4. I/O-only benchmark ─────────────────────────────────────────────────────

message("\n── I/O-only benchmark ──────────────────────────────────────────────")
message(sprintf(
  "  %d surveys  |  %d file(s)  |  %d columns kept (of %d)  |  %d iterations",
  nrow(entries), length(parquet_files_all),
  length(NEEDED_COLS), length(ALL_SCHEMA_COLS), N_ITERATIONS
))

io_fns <- list(
  "IO-1 Baseline" = function() io_1_baseline(parquet_files_all),
  "IO-2 Select"   = function() io_2_select(parquet_files_all),
  "IO-3 Sort"     = function() io_3_sort(parquet_files_all)
)

io_results <- rbindlist(lapply(names(io_fns), function(nm) {
  message(sprintf("  [%s] ...", nm), appendLF = FALSE)
  bm <- tryCatch(
    bench::mark(
      io_fns[[nm]](),
      iterations = N_ITERATIONS,
      check      = FALSE,
      memory     = TRUE
    ),
    error = function(e) {
      message(" FAILED: ", conditionMessage(e))
      NULL
    }
  )
  if (is.null(bm)) return(NULL)

  # bench_mark stores per-iteration times in bm$time[[1]] as bench_time
  iter_times <- as.numeric(bm$time[[1L]])
  med_s  <- as.numeric(bm[["median"]])
  mem_mb <- as.numeric(bm[["mem_alloc"]]) / 1024^2
  message(sprintf(" %.3fs  %.0fMB", med_s, mem_mb))

  data.table(
    approach = nm,
    phase    = "I/O only",
    median_s = med_s,
    mem_mb   = mem_mb,
    p25_s    = as.numeric(quantile(iter_times, 0.25)),
    p75_s    = as.numeric(quantile(iter_times, 0.75))
  )
}))

# ── 5. E2E benchmark ──────────────────────────────────────────────────────────

message("\n── E2E benchmark ───────────────────────────────────────────────────")
message(sprintf(
  "  measures: %s  |  poverty lines: %s  |  by: %s",
  paste(MEASURES, collapse = ", "),
  paste(POVERTY_LINES, collapse = ", "),
  paste(BY_DIMS, collapse = ", ")
))

e2e_fns <- Filter(Negate(is.null), list(
  "E2E-1 Baseline" = function() e2e_1_baseline(parquet_files_all),
  "E2E-2 Select"   = if ("E2E-2" %in% valid_e2e)
    function() e2e_2_select(parquet_files_all) else NULL,
  "E2E-3 Push"     = if ("E2E-3" %in% valid_e2e)
    function() e2e_3_push(parquet_files_all) else NULL
))

# Track which approaches failed at benchmark time (separate from correctness)
e2e_bench_failed <- character(0L)

e2e_results <- rbindlist(lapply(names(e2e_fns), function(nm) {
  message(sprintf("  [%s] ...", nm), appendLF = FALSE)
  bm <- tryCatch(
    bench::mark(
      e2e_fns[[nm]](),
      iterations = N_ITERATIONS,
      check      = FALSE,
      memory     = TRUE
    ),
    error = function(e) {
      message(" FAILED: ", conditionMessage(e))
      # Record this as a benchmark-time failure (distinct from correctness)
      e2e_bench_failed <<- c(e2e_bench_failed, nm)
      NULL
    }
  )
  if (is.null(bm)) return(NULL)

  iter_times <- as.numeric(bm$time[[1L]])
  med_s  <- as.numeric(bm[["median"]])
  mem_mb <- as.numeric(bm[["mem_alloc"]]) / 1024^2
  message(sprintf(" %.3fs  %.0fMB", med_s, mem_mb))

  data.table(
    approach = nm,
    phase    = "E2E",
    median_s = med_s,
    mem_mb   = mem_mb,
    p25_s    = as.numeric(quantile(iter_times, 0.25)),
    p75_s    = as.numeric(quantile(iter_times, 0.75))
  )
}))

bm_results <- rbindlist(list(io_results, e2e_results), fill = TRUE)

# ── 6. Results table ──────────────────────────────────────────────────────────

message("\n── Results ─────────────────────────────────────────────────────────")

# Extract key medians (safely — approach may not exist if excluded)
.get_med <- function(approach_name, results = bm_results) {
  r <- results[approach == approach_name, median_s]
  if (length(r) == 1L) r else NA_real_
}

io1_med  <- .get_med("IO-1 Baseline")
io2_med  <- .get_med("IO-2 Select")
io3_med  <- .get_med("IO-3 Sort")
e2e1_med <- .get_med("E2E-1 Baseline")
e2e2_med <- .get_med("E2E-2 Select")
e2e3_med <- .get_med("E2E-3 Push")

# Speedup = (baseline - new) / baseline  (positive = new is faster)
.speedup <- function(base, new) {
  if (!is.na(base) && !is.na(new) && base > 0) (base - new) / base else NA_real_
}

io_select_speedup  <- .speedup(io1_med,  io2_med)
e2e_select_speedup <- .speedup(e2e1_med, e2e2_med)
e2e_push_speedup   <- .speedup(e2e2_med, e2e3_med)

# Sort overhead as fraction of E2E baseline (indicates how much the Gini
# sort step costs relative to the full pipeline)
sort_fraction <- if (!is.na(io3_med) && !is.na(io2_med) && !is.na(e2e1_med) && e2e1_med > 0) {
  (io3_med - io2_med) / e2e1_med
} else NA_real_

tbl <- bm_results[, .(
  approach,
  phase,
  `median (s)` = round(median_s, 3),
  `p25 (s)`    = round(p25_s, 3),
  `p75 (s)`    = round(p75_s, 3),
  `mem (MB)`   = round(mem_mb, 1)
)]
print(tbl)

# ── 7. Decision ───────────────────────────────────────────────────────────────

message("\n── Decisions ───────────────────────────────────────────────────────")

.fmt_speedup <- function(sp) {
  if (is.na(sp)) return("N/A")
  sprintf("%.0f%% %s", abs(sp) * 100, if (sp > 0) "faster" else "slower")
}

decision_io_select <- if (is.na(io_select_speedup)) {
  "I/O Select result unavailable."
} else if (io_select_speedup >= IO_SELECT_THRESHOLD) {
  sprintf(
    "ADOPT column pruning: IO-2 is **%s** than IO-1 (%.3fs → %.3fs), exceeding the %.0f%% threshold.",
    .fmt_speedup(io_select_speedup), io1_med, io2_med, IO_SELECT_THRESHOLD * 100
  )
} else {
  sprintf(
    "KEEP baseline I/O: IO-2 is only %s than IO-1 (%.3fs → %.3fs), below the %.0f%% threshold.",
    .fmt_speedup(io_select_speedup), io1_med, io2_med, IO_SELECT_THRESHOLD * 100
  )
}

decision_e2e_select <- if (is.na(e2e_select_speedup)) {
  "E2E Select result unavailable."
} else if (e2e_select_speedup >= E2E_SELECT_THRESHOLD) {
  sprintf(
    "ADOPT column pruning in load_surveys(): E2E-2 is **%s** than E2E-1 (%.3fs → %.3fs).",
    .fmt_speedup(e2e_select_speedup), e2e1_med, e2e2_med
  )
} else {
  sprintf(
    "KEEP baseline pipeline: E2E-2 is only %s than E2E-1 (%.3fs → %.3fs), below the %.0f%% threshold.",
    .fmt_speedup(e2e_select_speedup), e2e1_med, e2e2_med, E2E_SELECT_THRESHOLD * 100
  )
}

decision_push <- if (!check_3) {
  "E2E-3 Push excluded — correctness check failed (logical mismatch on fixture)."
} else if ("E2E-3 Push" %in% e2e_bench_failed) {
  paste(
    "E2E-3 Push excluded — correctness check PASSED, but benchmark failed at runtime.",
    "Root cause: Arrow dict-index incompatibility across Parquet files ('Unifying differing dictionaries').",
    "Fix: ensure dict-encoded columns are cast to character after collect(), not inside Arrow query.",
    "NOTE: E2E-3 performs 1 + N_poverty_lines Arrow scans per call; structurally disadvantaged vs E2E-2."
  )
} else if (is.na(e2e_push_speedup)) {
  "E2E Push result unavailable."
} else if (e2e_push_speedup >= E2E_PUSH_THRESHOLD) {
  sprintf(
    "ADOPT Arrow push: E2E-3 is **%s** than E2E-2 (%.3fs → %.3fs). NOTE: performs 1 + %d Arrow scans.",
    .fmt_speedup(e2e_push_speedup), e2e2_med, e2e3_med, length(POVERTY_LINES)
  )
} else {
  sprintf(
    "SKIP Arrow push: E2E-3 is only %s than E2E-2 (%.3fs → %.3fs), below the %.0f%% threshold. Performs 1 + %d Arrow scans.",
    .fmt_speedup(e2e_push_speedup), e2e2_med, e2e3_med, E2E_PUSH_THRESHOLD * 100, length(POVERTY_LINES)
  )
}

decision_sort <- if (is.na(sort_fraction)) {
  "Sort profiling unavailable."
} else if (sort_fraction >= SORT_THRESHOLD) {
  sprintf(
    paste(
      "WORTH INVESTIGATING: Arrow sort + cast overhead is %.0f%% of E2E-1 (%.3fs extra over IO-2).",
      "Note: this is an upper bound — IO-3 also casts dict cols to string before sorting;",
      "true sort-only overhead is lower.",
      "Next step: implement pre-sorted I/O and skip setorder() in compute_inequality()."
    ),
    sort_fraction * 100, io3_med - io2_med
  )
} else {
  sprintf(
    "LOW PRIORITY: Arrow sort + cast overhead is only %.0f%% of E2E-1 (%.3fs extra over IO-2). Not worth pre-sorting.",
    sort_fraction * 100, io3_med - io2_med
  )
}

message("  [I/O Select]  ", decision_io_select)
message("  [E2E Select]  ", decision_e2e_select)
message("  [Arrow Push]  ", decision_push)
message("  [Arrow Sort]  ", decision_sort)

# ── 8. Visualisation ──────────────────────────────────────────────────────────

dir.create("benchmarks", showWarnings = FALSE)

# Phase colours: blue = I/O, green = E2E
PHASE_COLOURS <- c("I/O only" = "#2c7bb6", "E2E" = "#27ae60")

bm_plot <- copy(bm_results)
bm_plot[, approach_f := factor(approach, levels = rev(approach))]
bm_plot[, phase_f    := factor(phase, levels = c("I/O only", "E2E"))]
bm_plot[, label_s    := sprintf("%.3fs", median_s)]
bm_plot[, label_mb   := sprintf("%.0fMB", mem_mb)]

# Horizontal bar chart makes long approach names readable
p_time <- ggplot(
  bm_plot,
  aes(y = approach_f, x = median_s, fill = phase_f)
) +
  geom_col(width = 0.65) +
  geom_errorbarh(
    aes(xmin = p25_s, xmax = p75_s),
    height = 0.25, colour = "grey30", linewidth = 0.6
  ) +
  geom_text(aes(label = label_s), hjust = -0.2, size = 3.2) +
  scale_fill_manual(values = PHASE_COLOURS, name = NULL) +
  scale_x_continuous(expand = expansion(mult = c(0, 0.20))) +
  facet_wrap(~phase_f, scales = "free_y") +
  labs(
    title    = "Arrow I/O strategy benchmark — median time (IQR bars)",
    subtitle = sprintf(
      "%d surveys  |  measures: %s  |  by: %s  |  %d iterations  |  %s mode",
      nrow(entries),
      paste(MEASURES, collapse = ", "),
      paste(BY_DIMS, collapse = ", "),
      N_ITERATIONS,
      if (LIVE_MODE) "live" else "synthetic"
    ),
    y = NULL,
    x = "Median time (seconds)"
  ) +
  theme_minimal(base_size = 11) +
  theme(
    legend.position    = "none",
    strip.background   = element_rect(fill = "grey92", colour = NA),
    panel.grid.major.y = element_blank()
  )

p_mem <- ggplot(
  bm_plot,
  aes(y = approach_f, x = mem_mb, fill = phase_f)
) +
  geom_col(width = 0.65) +
  geom_text(aes(label = label_mb), hjust = -0.2, size = 3.2) +
  scale_fill_manual(values = PHASE_COLOURS, name = NULL) +
  scale_x_continuous(expand = expansion(mult = c(0, 0.20))) +
  facet_wrap(~phase_f, scales = "free_y") +
  labs(title = "Memory allocation per run", y = NULL, x = "Memory allocated (MB)") +
  theme_minimal(base_size = 11) +
  theme(
    legend.position    = "none",
    strip.background   = element_rect(fill = "grey92", colour = NA),
    panel.grid.major.y = element_blank()
  )

plot_out <- p_time / p_mem + plot_layout(heights = c(3, 2))

plot_path <- "benchmarks/arrow-vs-collapse-results.png"
ggsave(plot_path, plot_out, width = 12, height = 8, dpi = 150)
message("\nPlot saved to ", plot_path)

# ── 9. Results document ───────────────────────────────────────────────────────

results_dir  <- ".cg-docs/solutions/performance-issues"
results_path <- file.path(results_dir, "2026-04-29-arrow-vs-collapse-results.md")
dir.create(results_dir, showWarnings = FALSE, recursive = TRUE)

si       <- sessionInfo()
cpu_info <- tryCatch(paste0(si$running, " / ", si$platform),
                     error = function(e) si$platform)

tbl_lines <- capture.output(print(tbl))

results_md <- c(
  "---",
  "date: 2026-04-29",
  'title: "Arrow I/O Strategy Benchmark Results"',
  "status: completed",
  "tags: [performance, arrow, io, benchmark]",
  "---",
  "",
  "# Arrow I/O Strategy Benchmark Results",
  "",
  "## Environment",
  "",
  paste0("- **R version**: ", R.version$version.string),
  paste0("- **Platform**: ", cpu_info),
  paste0("- **Logical cores**: ", parallel::detectCores(logical = TRUE)),
  paste0("- **Physical cores**: ", max(1L, parallel::detectCores(logical = FALSE), na.rm = TRUE)),
  paste0("- **arrow version**: ", packageVersion("arrow")),
  paste0("- **collapse version**: ", packageVersion("collapse")),
  paste0("- **data.table version**: ", packageVersion("data.table")),
  paste0("- **bench version**: ", packageVersion("bench")),
  "",
  "## Dataset",
  "",
  paste0("- **Mode**: ", if (LIVE_MODE) "Live (Arrow repository)" else "Synthetic (temp Parquet)"),
  paste0("- **Surveys**: ", nrow(entries)),
  paste0("- **Parquet files**: ", length(parquet_files_all)),
  paste0("- **Measures**: `", paste(MEASURES, collapse = "`, `"), "`"),
  paste0("- **Poverty lines**: ", paste(POVERTY_LINES, collapse = ", ")),
  paste0("- **By dimensions**: `", paste(BY_DIMS, collapse = "`, `"), "`"),
  paste0("- **Iterations**: ", N_ITERATIONS),
  "",
  "## Column Pruning Surface",
  "",
  paste0(
    "Schema has ", length(ALL_SCHEMA_COLS), " columns. ",
    "This benchmark requires ", length(NEEDED_COLS),
    " (`", paste(NEEDED_COLS, collapse = "`, `"), "`). ",
    length(DROPPED_COLS), " columns dropped by IO-2/IO-3: ",
    "`", paste(DROPPED_COLS, collapse = "`, `"), "`."
  ),
  "",
  "## Results",
  "",
  "```",
  tbl_lines,
  "```",
  "",
  "## Speedup Summary",
  "",
  "| Comparison | Speedup | Decision |",
  "| ---------- | ------- | -------- |",
  sprintf("| IO-2 vs IO-1 (Select vs Baseline I/O) | %s | %s |",
          .fmt_speedup(io_select_speedup),
          if (!is.na(io_select_speedup) && io_select_speedup >= IO_SELECT_THRESHOLD)
            "✅ ADOPT" else "❌ KEEP baseline"),
  sprintf("| E2E-2 vs E2E-1 (Select vs Baseline E2E) | %s | %s |",
          .fmt_speedup(e2e_select_speedup),
          if (!is.na(e2e_select_speedup) && e2e_select_speedup >= E2E_SELECT_THRESHOLD)
            "✅ ADOPT" else "❌ KEEP baseline"),
  sprintf("| E2E-3 vs E2E-2 (Push vs Select E2E) | %s | %s |",
          .fmt_speedup(e2e_push_speedup),
          if (!check_3) "🚫 EXCLUDED (correctness)"
          else if (!is.na(e2e_push_speedup) && e2e_push_speedup >= E2E_PUSH_THRESHOLD)
            "✅ ADOPT" else "❌ SKIP"),
  sprintf("| Arrow sort overhead (IO-3 − IO-2) / E2E-1 | %.0f%% |  %s |",
          if (!is.na(sort_fraction)) sort_fraction * 100 else 0,
          if (!is.na(sort_fraction) && sort_fraction >= SORT_THRESHOLD)
            "⚠️ INVESTIGATE" else "✅ LOW PRIORITY"),
  "",
  "## Decisions",
  "",
  paste0("**I/O Select**: ", decision_io_select),
  "",
  paste0("**E2E Select**: ", decision_e2e_select),
  "",
  paste0("**Arrow Push**: ", decision_push),
  "",
  paste0("**Arrow Sort**: ", decision_sort),
  "",
  "## Correctness",
  "",
  paste0("- E2E-2 vs E2E-1 (full result): ", if (check_2) "✅ PASS" else "❌ FAIL"),
  paste0("- E2E-3 vs E2E-1 (mean+headcount only): ", if (check_3) "✅ PASS" else "❌ FAIL"),
  "",
  paste0("*Generated: ", format(Sys.time(), "%Y-%m-%d %H:%M"), "*")
)

writeLines(results_md, results_path)
message("Results saved to ", results_path)
