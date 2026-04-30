# ============================================================================
# benchmarks/pipeline-comparison.R
#
# Purpose:
#   Compare three end-to-end pipeline approaches for computing PIP welfare
#   measures across surveys, with explicit timing of I/O and compute phases.
#
# Usage:
#   source("benchmarks/pipeline-comparison.R")
#
# Design:
#   50 iterations.  Each iteration resamples 15 surveys from the manifest and
#   runs all three approaches on the SAME survey set, so differences in total
#   time are attributable to the approach, not to survey selection.
#
#   I/O and compute phases are timed separately with proc.time() so that the
#   stacked breakdown (load vs compute) is visible in results.
#
# Approaches:
#
#   A1  Current pipeline    — load_surveys() reads all 14 columns; then
#                             compute_measures() for all 4 measures.
#
#   A2  Column-pruned I/O   — Arrow select(5 needed cols) before collect();
#                             then compute_measures() for all 4 measures.
#                             Same compute as A1, cheaper I/O.
#
#   A3  Arrow push-down      — Two Arrow scans:
#                             Scan 1: group_by + summarise inside Arrow
#                               (before collect) for mean + headcount.
#                               Collects a tiny aggregated table
#                               (one row per group, not per person).
#                             Scan 2: column-pruned collect of full
#                               microdata → collapse for gini + median.
#                             Arrow's aggregation engine handles the
#                             closed-form measures; R only receives what
#                             it strictly needs.
#
# Key outputs:
#   benchmarks/pipeline-comparison-results.png
#   .cg-docs/solutions/performance-issues/2026-04-29-pipeline-comparison-results.md
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(collapse)
  library(arrow)
  library(dplyr, warn.conflicts = FALSE)
  library(ggplot2)
  library(patchwork)
})

devtools::load_all(quiet = TRUE)

# ── Configuration ─────────────────────────────────────────────────────────────

MEASURES      <- c("headcount", "gini", "mean", "median")
POVERTY_LINES <- c(2.15, 3.65)
BY_DIMS       <- c("gender", "area", "educat4")   # 3 breakdown dimensions
N_SURVEYS     <- 15L
N_ITERATIONS  <- 50L
BASE_SEED     <- 2026L

# Columns needed after pruning: survey ID + welfare + weight + breakdown dims.
# All other schema columns (country_code, surveyid_year, welfare_type, version,
# survey_acronym, educat5, educat7, age) are dropped at I/O time for A2/A3.
NEEDED_COLS <- c("pip_id", "welfare", "weight", BY_DIMS)

# Dictionary-encoded dimension columns in the PIP Arrow schema.
# Arrow's aggregation engine requires these to be cast to character before
# group_by + summarise across multiple Parquet files (different files may
# encode the same category with different integer indices, which Arrow
# cannot unify automatically during a lazy grouped query).
DICT_SCHEMA_COLS <- c("gender", "area", "educat4", "educat5", "educat7")

# ── 1. Manifest setup ─────────────────────────────────────────────────────────

mf_full <- piptm_manifest()

# Restrict the sampling pool to surveys that carry ALL requested BY_DIMS.
# This avoids NA-fill complexity during compute and keeps the comparison clean.
mf <- mf_full[vapply(dimensions, function(d) all(BY_DIMS %in% d), logical(1L))]

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

# ── 2. Internal helpers ───────────────────────────────────────────────────────

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

# ── 3. Approach functions ─────────────────────────────────────────────────────
#
# Each function returns a list: result (data.table), t_io (seconds), t_cmp (s).
# I/O and compute phases are timed separately.

# ── A1: Current pipeline ──────────────────────────────────────────────────────
# Exactly how table_maker() works internally: load_surveys() reads every column
# in the Parquet files, then compute_measures() dispatches to collapse.
run_a1 <- function(entries_dt) {
  t0    <- proc.time()
  dt    <- load_surveys(entries_dt)
  t_io  <- .elapsed(proc.time() - t0)

  t0     <- proc.time()
  result <- compute_measures(dt, MEASURES, POVERTY_LINES, BY_DIMS)
  t_cmp  <- .elapsed(proc.time() - t0)

  list(result = result, t_io = t_io, t_cmp = t_cmp)
}

# ── A2: Column-pruned I/O ─────────────────────────────────────────────────────
# Replaces the open_dataset → collect step inside load_surveys() with a
# column-selected variant.  Compute is identical to A1.
#
# Why not call load_surveys() and subset after? Because the I/O cost scales
# with the columns read off disk/network, not with columns kept in memory.
# The select() must happen before collect() to benefit from Parquet's column
# chunking.
run_a2 <- function(entries_dt) {
  t0 <- proc.time()
  files <- .bm_paths(entries_dt)
  dt <- arrow::open_dataset(files, format = "parquet") |>
    dplyr::select(dplyr::all_of(NEEDED_COLS)) |>
    dplyr::collect() |>
    data.table::as.data.table()
  t_io <- .elapsed(proc.time() - t0)

  t0     <- proc.time()
  result <- compute_measures(dt, MEASURES, POVERTY_LINES, BY_DIMS)
  t_cmp  <- .elapsed(proc.time() - t0)

  list(result = result, t_io = t_io, t_cmp = t_cmp)
}

# \u2500\u2500 A3: Arrow push-down aggregation \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#
# Compute split:
#
# Two Arrow scans, both timed as I/O (network → R boundary):
#
#   Scan 1 — Arrow group_by + summarise BEFORE collect().
#     Arrow executes the aggregation on the Parquet files and returns one row
#     per (pip_id × gender × area × educat4) group instead of one row per
#     person.  For 15 surveys this reduces the collected row count from
#     ~millions to ~hundreds.
#     Computes in one scan:
#       sum_w        — total weighted population per group (denominator)
#       sum_ww       — sum(welfare * weight) per group  → mean numerator
#       poor_w_1..N  — sum(weight * 1[welfare < pl]) per group × poverty line
#                      → headcount numerator per poverty line
#     Dict-encoded columns (gender, area, educat4) must be cast to character
#     inside the Arrow query before group_by — Arrow cannot hash-aggregate
#     on dictionary types across Parquet files with different encodings.
#
#   Scan 2 — column-pruned collect of full microdata.
#     Identical to A2's I/O step.  Needed because gini and median require
#     the full per-person welfare vector (gini needs a sorted vector for the
#     Lorenz curve; median needs a weighted quantile algorithm).  These
#     cannot be expressed as scalar aggregations in Arrow.
#
#   Compute (after both scans):
#     mean + headcount — pure R arithmetic on the tiny aggregated table
#                        (division of two columns, no iteration over rows).
#     gini + median    — dispatched to collapse via compute_measures() on
#                        the full microdata from Scan 2.
#
# Performance expectation:
#   Scan 1 transfers far less data than A2's single scan (aggregated rows
#   vs full microdata).  But Scan 2 is the same size as A2's scan.  The
#   net I/O is therefore slightly MORE than A2 (two scans vs one).  Any
#   advantage must come from compute: the collapse overhead for mean +
#   headcount is eliminated.  Whether that outweighs the extra scan cost
#   is what this approach measures.
run_a3 <- function(entries_dt) {
  files    <- .bm_paths(entries_dt)
  batch_by <- c("pip_id", BY_DIMS)
  dict_by  <- intersect(BY_DIMS, DICT_SCHEMA_COLS)  # cols needing char cast

  t0 <- proc.time()

  # ── Scan 1: Arrow group_by + summarise (push-down aggregation) ─────────────
  # Build one conditional-sum expression per poverty line dynamically so that
  # a single Arrow scan computes all headcount numerators simultaneously.
  hc_exprs <- setNames(
    lapply(POVERTY_LINES, function(pl) {
      rlang::expr(sum(dplyr::if_else(welfare < !!pl, weight, 0), na.rm = TRUE))
    }),
    paste0("poor_w_", seq_along(POVERTY_LINES))
  )

  agg <- arrow::open_dataset(files, format = "parquet") |>
    dplyr::select(dplyr::all_of(NEEDED_COLS)) |>
    # Cast dict-encoded cols to string so Arrow can hash-aggregate across files
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

  # ── Scan 2: column-pruned microdata for gini + median ──────────────────────
  dt <- arrow::open_dataset(files, format = "parquet") |>
    dplyr::select(dplyr::all_of(NEEDED_COLS)) |>
    dplyr::collect() |>
    data.table::as.data.table()

  t_io <- .elapsed(proc.time() - t0)

  # ── Compute: mean + headcount from aggregated table (pure arithmetic) ───────
  t0 <- proc.time()

  r_mean <- agg[, .(
    value        = sum_ww / sum_w,
    population   = sum_w,
    measure      = "mean",
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

  # ── Compute: gini + median from microdata via collapse ──────────────────────
  # poverty_lines = NULL: neither measure requires a poverty threshold.
  # compute_measures() prepends pip_id to by internally.
  r_gm <- compute_measures(dt, c("gini", "median"), poverty_lines = NULL, by = BY_DIMS)

  result <- rbindlist(list(r_mean, r_hc, r_gm), fill = TRUE)
  t_cmp  <- .elapsed(proc.time() - t0)

  list(result = result, t_io = t_io, t_cmp = t_cmp)
}

# ── 4. Correctness check ──────────────────────────────────────────────────────
#
# Run on a small 5-survey fixture (same manifest pool) so the check is fast.
# All three approaches must produce numerically identical results before
# we permit them into the benchmark loop.

message("\n── Correctness check ─────────────────────────────────────────────────")

set.seed(BASE_SEED)
chk_entries <- mf[sample(.N, 5L)]

# Canonical sort so all.equal() is order-independent.
.norm <- function(res) {
  key_cols <- intersect(c("pip_id", BY_DIMS, "poverty_line", "measure"), names(res))
  val_cols <- intersect(c("value", "population"), names(res))
  res <- res[, c(key_cols, val_cols), with = FALSE]
  setkeyv(res, key_cols)
  res[]
}

r_chk1 <- tryCatch(run_a1(chk_entries)$result, error = function(e) {
  message("  A1 correctness error: ", e$message); NULL
})
r_chk2 <- tryCatch(run_a2(chk_entries)$result, error = function(e) {
  message("  A2 correctness error: ", e$message); NULL
})
r_chk3 <- tryCatch(run_a3(chk_entries)$result, error = function(e) {
  message("  A3 correctness error: ", e$message); NULL
})

# A2: identical compute path to A1 — expect exact match.
chk_a2 <- !is.null(r_chk1) && !is.null(r_chk2) &&
  isTRUE(all.equal(.norm(r_chk1), .norm(r_chk2), tolerance = 1e-10))

# A3: data.table mean/headcount vs collapse — allow small floating-point delta.
chk_a3 <- tryCatch({
  !is.null(r_chk1) && !is.null(r_chk3) && {
    ref <- .norm(r_chk1[measure %in% MEASURES])
    cnd <- .norm(r_chk3[measure %in% MEASURES])
    isTRUE(all.equal(ref, cnd, tolerance = 1e-8))
  }
}, error = function(e) FALSE)

message("  A2 vs A1 (full result):    ", if (chk_a2) "PASS" else "FAIL — A2 excluded")
message("  A3 vs A1 (all measures):   ", if (chk_a3) "PASS" else "FAIL — A3 excluded")

run_fns <- Filter(Negate(is.null), list(
  A1 = run_a1,
  A2 = if (chk_a2) run_a2 else NULL,
  A3 = if (chk_a3) run_a3 else NULL
))

message("  Proceeding with: ", paste(names(run_fns), collapse = ", "))

# ── 5. Benchmark loop ─────────────────────────────────────────────────────────
#
# Each iteration:
#   1. Resample N_SURVEYS surveys (different set each iteration).
#   2. Run all passing approaches on the SAME survey set.
#   3. Record t_io, t_cmp, t_total per approach.

message(sprintf(
  "\n── Benchmark: %d iterations × %d surveys ──────────────────────────────────",
  N_ITERATIONS, N_SURVEYS
))
message(sprintf(
  "   measures: %s  |  by: %s  |  pl: %s",
  paste(MEASURES,      collapse = ", "),
  paste(BY_DIMS,       collapse = ", "),
  paste(POVERTY_LINES, collapse = ", ")
))

iter_rows  <- vector("list", N_ITERATIONS * length(run_fns))
rec_idx    <- 0L

for (i in seq_len(N_ITERATIONS)) {
  set.seed(BASE_SEED + i)
  entries_i <- mf[sample(.N, N_SURVEYS)]

  for (nm in names(run_fns)) {
    out <- tryCatch(
      run_fns[[nm]](entries_i),
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

# ── 6. Summary table ──────────────────────────────────────────────────────────

message("\n── Results ───────────────────────────────────────────────────────────")

APPROACH_LABELS <- c(
  A1 = "A1: Current (all cols)",
  A2 = "A2: Column-pruned I/O",
  A3 = "A3: Hybrid compute"
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

# ── 7. Decisions ──────────────────────────────────────────────────────────────

message("\n── Decisions ─────────────────────────────────────────────────────────")

.sp  <- function(base, new) {
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

a1_tot <- get_tot("A1"); a2_tot <- get_tot("A2"); a3_tot <- get_tot("A3")
a1_io  <- get_io("A1");  a2_io  <- get_io("A2")

ADOPT_THRESHOLD <- 0.15   # require ≥15% improvement to change the pipeline

sp_a2_total <- .sp(a1_tot, a2_tot)
sp_a2_io    <- .sp(a1_io,  a2_io)
sp_a3_total <- .sp(a2_tot, a3_tot)

dec_a2 <- if (!chk_a2) {
  "A2 excluded (correctness check failed)."
} else if (is.na(sp_a2_total)) {
  "A2 result unavailable."
} else if (sp_a2_total >= ADOPT_THRESHOLD) {
  sprintf(
    "ADOPT column pruning in load_surveys(): A2 is **%s** total vs A1 (I/O alone: %s).",
    .fmt(sp_a2_total), .fmt(sp_a2_io)
  )
} else {
  sprintf(
    "KEEP current pipeline: A2 is only %s total vs A1 — below the %.0f%% threshold.",
    .fmt(sp_a2_total), ADOPT_THRESHOLD * 100
  )
}

dec_a3 <- if (!chk_a3) {
  "A3 excluded (correctness check failed)."
} else if (is.na(sp_a3_total)) {
  "A3 result unavailable."
} else if (sp_a3_total >= ADOPT_THRESHOLD) {
  sprintf(
    "ADOPT hybrid compute: A3 is **%s** total vs A2.",
    .fmt(sp_a3_total)
  )
} else {
  sprintf(
    "KEEP A2 compute path: A3 is only %s total vs A2 — below the %.0f%% threshold.",
    .fmt(sp_a3_total), ADOPT_THRESHOLD * 100
  )
}

message("  [A2 vs A1]  I/O: ", .fmt(sp_a2_io), "  |  Total: ", .fmt(sp_a2_total))
message("  [A3 vs A2]  Total: ", .fmt(sp_a3_total))
message("  [A2] ", dec_a2)
message("  [A3] ", dec_a3)

# ── 8. Visualisation ──────────────────────────────────────────────────────────

dir.create("benchmarks", showWarnings = FALSE)

# Factor levels: A1 at top of y-axis (last in factor = topmost bar)
app_order <- rev(intersect(c("A1", "A2", "A3"), names(run_fns)))
lev_order <- APPROACH_LABELS[app_order]

# ── 8a. Stacked bar: I/O + compute, IQR whisker on total ──────────────────────

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
  geom_errorbarh(
    data = totals_dt,
    aes(y = approach_f, xmin = p25, xmax = p75, fill = NULL),
    height = 0.25, colour = "grey25", linewidth = 0.7, inherit.aes = FALSE
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
    title    = "Pipeline comparison — median time by phase (IQR whiskers on total)",
    subtitle = sprintf(
      "%d surveys × %d iterations  |  measures: %s  |  by: %s  |  pl: %s",
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

# ── 8b. Per-iteration scatter: variance from survey resampling ─────────────────
#
# Each iteration uses a different set of 15 surveys. This plot shows how much
# run-to-run variance comes from "which surveys were sampled" vs the approach.

iter_dt[, approach_f := factor(APPROACH_LABELS[approach], levels = APPROACH_LABELS[intersect(c("A1","A2","A3"), names(run_fns))])]

ITER_COLS <- c(
  "A1: Current (all cols)" = "#e74c3c",
  "A2: Column-pruned I/O"  = "#2c7bb6",
  "A3: Hybrid compute"     = "#27ae60"
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

# ── 9. Results document ───────────────────────────────────────────────────────

results_dir  <- ".cg-docs/solutions/performance-issues"
results_path <- file.path(
  results_dir,
  "2026-04-29-pipeline-comparison-results.md"
)
dir.create(results_dir, showWarnings = FALSE, recursive = TRUE)

si       <- sessionInfo()
cpu_info <- tryCatch(paste0(si$running, " / ", si$platform),
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
  paste0("- **R version**: ",        R.version$version.string),
  paste0("- **Platform**: ",          cpu_info),
  paste0("- **Logical cores**: ",     parallel::detectCores(logical = TRUE)),
  paste0("- **arrow version**: ",     packageVersion("arrow")),
  paste0("- **collapse version**: ",  packageVersion("collapse")),
  paste0("- **data.table version**: ",packageVersion("data.table")),
  "",
  "## Configuration",
  "",
  paste0("- **Surveys per iteration**: ",  N_SURVEYS),
  paste0("- **Iterations**: ",             N_ITERATIONS),
  paste0("- **Measures**: `",              paste(MEASURES,      collapse = "`, `"), "`"),
  paste0("- **Poverty lines**: ",          paste(POVERTY_LINES, collapse = ", ")),
  paste0("- **By dimensions**: `",         paste(BY_DIMS,       collapse = "`, `"), "`"),
  paste0("- **Survey pool size**: ",       nrow(mf),
         " surveys (filtered from ", nrow(mf_full),
         " total to those with all BY_DIMS)"),
  "",
  "## Approaches",
  "",
  "| | I/O | Compute |",
  "| --- | --- | --- |",
  "| **A1 Current** | `load_surveys()` — all 14 schema columns | `compute_measures()` for headcount, gini, mean, median |",
  paste0(
    "| **A2 Column-pruned** | Arrow `select(", length(NEEDED_COLS), " cols)` before `collect()` ",
    "| `compute_measures()` for headcount, gini, mean, median |"
  ),
  paste0(
    "| **A3 Arrow push-down** | Scan 1: Arrow `group_by + summarise` → tiny aggregated table (",
    "one row/group); Scan 2: Arrow `select(", length(NEEDED_COLS), " cols)` → full microdata ",
    "| mean + headcount: arithmetic on aggregated table; gini + median: `compute_measures()` |"
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
  "| Comparison | I/O speedup | Total speedup | Decision |",
  "| ---------- | ----------- | ------------- | -------- |",
  sprintf(
    "| A2 vs A1 | %s | %s | %s |",
    .fmt(sp_a2_io), .fmt(sp_a2_total),
    if (!chk_a2) "🚫 EXCLUDED"
    else if (!is.na(sp_a2_total) && sp_a2_total >= ADOPT_THRESHOLD) "✅ ADOPT"
    else "❌ KEEP A1"
  ),
  sprintf(
    "| A3 vs A2 | same as A2 | %s | %s |",
    .fmt(sp_a3_total),
    if (!chk_a3) "🚫 EXCLUDED"
    else if (!is.na(sp_a3_total) && sp_a3_total >= ADOPT_THRESHOLD) "✅ ADOPT"
    else "❌ KEEP A2"
  ),
  "",
  "## Decisions",
  "",
  paste0("**A2 vs A1**: ", dec_a2),
  "",
  paste0("**A3 vs A2**: ", dec_a3),
  "",
  "## Correctness",
  "",
  paste0("- A2 vs A1 (full result):  ", if (chk_a2) "✅ PASS" else "❌ FAIL"),
  paste0("- A3 vs A1 (all measures): ", if (chk_a3) "✅ PASS" else "❌ FAIL"),
  "",
  paste0("*Generated: ", format(Sys.time(), "%Y-%m-%d %H:%M"), "*")
)

writeLines(results_md, results_path)
message("Results saved to ", results_path)
