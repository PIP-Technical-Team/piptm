# ============================================================================
# benchmarks/orchestration-strategy.R
#
# Purpose:
#   Compare orchestration strategies for table_maker()'s per-survey
#   computation loop and determine the fastest approach for 15 surveys.
#
# Usage:
#   Rscript benchmarks/orchestration-strategy.R
#   # or interactively:
#   source("benchmarks/orchestration-strategy.R")
#
# Data modes (set LIVE_MODE below):
#   LIVE_MODE = FALSE (default)
#     Generates a synthetic 15-survey data.table (~1.6M rows) matching the
#     observed distribution (median ~49K rows, heavy right tail up to ~525K).
#     No Arrow repository access required. Always available.
#
#   LIVE_MODE = TRUE
#     Loads 15 real surveys from the Arrow repository via load_surveys().
#     Result is cached to benchmarks/data-cache.rds for re-runs.
#     Also profiles I/O time separately from compute time.
#
# Approaches benchmarked:
#   A  — Current baseline:   lapply() over dt[pip_id == pid] slices
#   B  — Grouped collapse:   single GRP(c("pip_id", by)) across full batch
#   C  — data.table by=:     dt[, compute_measures(.SD, ...), by = pip_id]
#   E  — split + lapply():   split(dt, by = "pip_id") then lapply()
#
#   Each structural approach is tested at nthreads = 1, 2, 4 (collapse
#   OpenMP threading — orthogonal axis, zero code changes).
#
# Output:
#   benchmarks/orchestration-results.png
#     Faceted bar chart: compute time and memory × approach × nthreads.
#
#   .cg-docs/solutions/performance-issues/
#     2026-04-28-orchestration-benchmark-results.md
#     Markdown summary: environment, dataset, results table, decision.
#
# Decision rule:
#   1. Pick the simplest approach meeting ≤3s wall-clock (compute only).
#   2. If multiple meet it, prefer the simpler one unless a more complex
#      approach is ≥40% faster.
#   3. If none meets it, I/O is the bottleneck — pivot to I/O optimisation.
#
# Preference order (simplest to most complex): A > E > B > C.
# ============================================================================

# ── 0. Setup ──────────────────────────────────────────────────────────────────

suppressPackageStartupMessages({
  library(data.table)
  library(collapse)
  library(ggplot2)
  library(bench)
  library(patchwork)
})

# Load the piptm package (all exported + internal functions available)
devtools::load_all(quiet = TRUE)

# ── Configuration (edit these) ────────────────────────────────────────────────

LIVE_MODE     <- TRUE               # TRUE = real Arrow data, FALSE = synthetic
CACHE_PATH    <- "benchmarks/data-cache.rds"
POVERTY_LINES <- c(2.15, 3.65, 6.85)
# NOTE: "age" is the pre-binning column name (.validate_by accepts "age" not
# "age_group").  Age binning is a table_maker() pre-processing step that is
# upstream of compute_measures().  To avoid wrapping that step into every
# approach function (which would obscure orchestration overhead), we benchmark
# with 3 dimensions that compute_measures() can consume directly.
BY_DIMS       <- c("gender", "area", "educat4")   # 3 valid dims, no binning needed
# Typical user request: max 4 measures.  One from each family (poverty,
# inequality, welfare) plus a second welfare stat covers realistic usage.
ALL_MEASURES  <- c("headcount", "gini", "mean", "median")
N_ITERATIONS  <- 5L                 # bench::mark() iterations per approach

# Detect available nthreads: test 1, 2, 4 but cap at physical core count
max_cores       <- max(1L, parallel::detectCores(logical = FALSE))
NTHREADS_CANDS  <- c(1L, 2L, 4L)
NTHREADS        <- NTHREADS_CANDS[NTHREADS_CANDS <= max_cores]

# Check OpenMP availability in this collapse build
openmp_ok <- tryCatch({
  collapse::set_collapse(nthreads = 2L)
  ok <- collapse::get_collapse("nthreads") == 2L
  collapse::set_collapse(nthreads = 1L)
  ok
}, error = function(e) FALSE)

if (!openmp_ok) {
  message("NOTE: collapse OpenMP not available — benchmarking nthreads = 1 only.")
  NTHREADS <- 1L
} else {
  message(sprintf("nthreads to test: %s", paste(NTHREADS, collapse = ", ")))
}

# ── 1. Data setup ─────────────────────────────────────────────────────────────

if (LIVE_MODE) {

  if (file.exists(CACHE_PATH)) {
    message("Loading cached dataset from ", CACHE_PATH)
    dt_bench <- readRDS(CACHE_PATH)
  } else {
    message("Loading 15 surveys from Arrow repository...")
    mf      <- piptm_manifest()
    pip_ids <- mf$pip_id[seq_len(min(15L, nrow(mf)))]
    entries <- mf[pip_id %chin% pip_ids]

    t_io <- system.time(
      dt_bench <- load_surveys(entries)
    )
    message(sprintf(
      "I/O: %.2f seconds (%s rows, %d surveys)",
      t_io["elapsed"], format(nrow(dt_bench), big.mark = ","), length(pip_ids)
    ))

    # Pre-bin age column if present (mirrors table_maker() Step 5)
    if ("age" %in% names(dt_bench)) {
      .bin_age(dt_bench)
      dt_bench[, age := age_group][, age_group := NULL]
      setnames(dt_bench, "age", "age_group")
    }

    saveRDS(dt_bench, CACHE_PATH)
    message("Dataset cached to ", CACHE_PATH)
  }

} else {

  message("Generating synthetic 15-survey dataset...")
  set.seed(42L)

  # Survey sizes calibrated to the observed distribution in this project:
  #   60 real surveys — median ~49K rows, mean ~241K, max ~784K.
  # 15 surveys with a similar spread:
  survey_sizes <- c(
     7500L,   9000L,  12000L,  15000L,  22500L,
    30000L,  37500L,  45000L,  60000L,  75000L,
    97500L, 150000L, 225000L, 330000L, 525000L
  )   # total ~1.64M rows

  countries  <- c(
    "COL", "BOL", "PER", "BRA", "ARG", "MEX", "CHL", "ECU",
    "PRY", "URY", "VEN", "GTM", "HND", "NIC", "SLV"
  )
  years      <- c(
    2018L, 2019L, 2020L, 2018L, 2019L, 2020L, 2018L, 2019L,
    2020L, 2018L, 2019L, 2020L, 2018L, 2019L, 2020L
  )
  welf_types <- rep(c("INC", "CON"), length.out = 15L)
  pip_ids    <- paste0(countries, "_", years, "_SURVEY_", welf_types, "_ALL")

  dt_bench <- rbindlist(lapply(seq_along(survey_sizes), function(i) {
    n <- survey_sizes[[i]]
    data.table(
      pip_id        = pip_ids[[i]],
      # Right-skewed welfare distribution (log-normal, typical for income)
      welfare       = rlnorm(n, meanlog = 1.5, sdlog = 1.0),
      weight        = runif(n, min = 0.5, max = 5.0),
      gender        = sample(c("male", "female"),                           n, replace = TRUE),
      area          = sample(c("urban", "rural"),                            n, replace = TRUE),
      # Education (4-level): realistic shares across no/primary/secondary/tertiary
      educat4       = sample(c("no edu", "primary", "secondary", "tertiary"), n, replace = TRUE,
                             prob = c(0.15, 0.40, 0.35, 0.10)),
      country_code  = countries[[i]],
      surveyid_year = years[[i]],
      welfare_type  = welf_types[[i]]
    )
  }))

  # Factor columns to match the schema produced by the real pipeline
  dt_bench[, gender  := factor(gender,  levels = c("male", "female"))]
  dt_bench[, area    := factor(area,    levels = c("urban", "rural"))]
  dt_bench[, educat4 := factor(educat4, levels = c("no edu", "primary", "secondary", "tertiary"))]

  message(sprintf(
    "Synthetic dataset ready: %s rows across %d surveys.",
    format(nrow(dt_bench), big.mark = ","), uniqueN(dt_bench$pip_id)
  ))
}

# Metadata lookup: pip_id → country_code / surveyid_year / welfare_type.
# Used by approaches B and C to re-attach survey-level metadata.
metadata <- unique(dt_bench[, .(pip_id, country_code, surveyid_year, welfare_type)])

# ── 2. Approach implementations ───────────────────────────────────────────────
#
# Each approach is a self-contained function taking the same arguments and
# returning a long-format data.table with identical schema.  Metadata columns
# (country_code, surveyid_year, welfare_type) are always attached before
# returning so comparisons are meaningful.
#
# Canonical output schema:
#   pip_id | country_code | surveyid_year | welfare_type |
#   [by_dims] | poverty_line | measure | value | population

# Helper: reorder columns and sort rows to a canonical form for all.equal()
.normalize_result <- function(res, by_dims) {
  meta_cols <- c("pip_id", "country_code", "surveyid_year", "welfare_type")
  key_cols  <- c(meta_cols, by_dims, "poverty_line", "measure")
  val_cols  <- c("value", "population")
  col_order <- intersect(c(key_cols, val_cols), names(res))
  res       <- res[, col_order, with = FALSE]
  setkeyv(res, intersect(key_cols, names(res)))
  res[]
}

# Family membership lookup — avoids calling internal .classify_measures()
# from outside the package namespace.
.POVERTY_MEASURES  <- c("headcount", "poverty_gap", "severity", "watts", "pop_poverty")
.INEQ_MEASURES     <- c("gini", "mld")
.WELFARE_MEASURES  <- c(
  "mean", "median", "sd", "var", "min", "max", "nobs",
  "p10", "p25", "p75", "p90"
)

# --- Approach A: Per-slice lapply() — current baseline -----------------------
#
# Mirrors the per-survey loop in table_maker() Steps 6–7 exactly, stripped of
# the manifest-lookup and load layers.  This is the reference implementation
# all other approaches must match.
approach_a_baseline <- function(dt, measures, poverty_lines, by) {
  survey_ids <- unique(dt[["pip_id"]])

  results <- lapply(survey_ids, function(pid) {
    sdt <- dt[pip_id == pid]

    # Fill any dimension columns missing for this survey with NA_character_
    # (same as table_maker() Step 6a)
    for (d in setdiff(by, names(sdt))) {
      set(sdt, j = d, value = NA_character_)
    }

    res <- compute_measures(sdt, measures, poverty_lines, by)

    res[, pip_id        := pid]
    res[, country_code  := sdt[["country_code"]][[1L]]]
    res[, surveyid_year := sdt[["surveyid_year"]][[1L]]]
    res[, welfare_type  := sdt[["welfare_type"]][[1L]]]
    res
  })

  rbindlist(results, fill = TRUE)
}

# --- Approach B: Grouped collapse — single GRP(c("pip_id", by)) --------------
#
# Skips the per-survey lapply() entirely.  Builds a compound GRP object once
# across the full batch (pip_id × by_dims) and dispatches directly to family
# functions.  The family functions are naturally multi-group capable when
# pip_id is included in `by`, producing pip_id as a group column in their
# output.
#
# Key consequence: GRP() is called once (welfare + inequality share it);
# compute_poverty() builds its own GRP internally because its grouping must
# also include poverty_line (which varies per loop iteration).
#
# Batch-level NA-fill for missing dimension columns happens before GRP.
# Metadata is attached via a keyed join rather than scalar assignment.
approach_b_collapse_grouped <- function(dt, measures, poverty_lines, by,
                                        metadata) {
  batch_by <- c("pip_id", by)

  # Batch-level NA-fill for any dimension column absent from dt
  for (d in by) {
    if (!d %in% names(dt)) set(dt, j = d, value = NA_character_)
  }

  # Pre-compute compound GRP once — shared by welfare and inequality
  grp <- collapse::GRP(dt, by = batch_by)

  pov_m  <- intersect(measures, .POVERTY_MEASURES)
  ineq_m <- intersect(measures, .INEQ_MEASURES)
  welf_m <- intersect(measures, .WELFARE_MEASURES)

  results <- list()

  if (length(pov_m) > 0L) {
    # compute_poverty() builds GRP(work, by = batch_by) internally (needed
    # because its grouping also includes poverty_line after the per-line loop).
    # The outer grp is NOT passed — it would be stale after the cross-join.
    results$poverty <- compute_poverty(
      dt,
      poverty_lines = poverty_lines,
      by            = batch_by,
      measures      = pov_m
    )
  }

  if (length(ineq_m) > 0L) {
    # compute_inequality() accepts the pre-computed compound GRP directly.
    # Gini's per-group sort uses grp$group.id which already encodes pip_id ×
    # by_dims — no structural change needed for multi-survey data.
    results$inequality <- compute_inequality(
      dt,
      by       = batch_by,
      measures = ineq_m,
      grp      = grp
    )
  }

  if (length(welf_m) > 0L) {
    # compute_welfare() also accepts the pre-computed compound GRP.
    results$welfare <- compute_welfare(
      dt,
      by       = batch_by,
      measures = welf_m,
      grp      = grp
    )
  }

  result <- rbindlist(results, fill = TRUE)
  if (!"poverty_line" %in% names(result)) result[, poverty_line := NA_real_]

  # pip_id is already in result (it was part of batch_by / grp$groups).
  # Attach remaining metadata via keyed join.
  metadata[result, on = "pip_id"]
}

# --- Approach C: data.table by= grouping -------------------------------------
#
# Uses data.table's native grouping to split by pip_id without an explicit
# lapply().  Each .SD passed to compute_measures() contains exactly one
# pip_id, satisfying the single-survey guard (uniqueN(pip_id) == 1L).
#
# .SDcols = names(dt) is required so pip_id is present in .SD; otherwise
# data.table omits the by= column from .SD by default and the guard aborts.
approach_c_datatable_by <- function(dt, measures, poverty_lines, by,
                                    metadata) {
  result <- dt[,
    compute_measures(.SD, measures, poverty_lines, by),
    by      = pip_id,
    .SDcols = names(dt)
  ]

  # pip_id is the data.table by= column (prepended to result automatically).
  # Attach remaining metadata via keyed join.
  metadata[result, on = "pip_id"]
}

# --- Approach E: split() + lapply() ------------------------------------------
#
# Variant of A.  Replaces 15 individual dt[pip_id == pid] subset operations
# with a single split(dt, by = "pip_id") pass that materialises all slices
# at once.  The lapply() loop over the resulting list is otherwise identical
# to approach A.
approach_e_split_lapply <- function(dt, measures, poverty_lines, by) {
  # split.data.table: single pass over dt, returns named list of data.tables.
  # keep.by = TRUE retains the pip_id column in each sub-table.
  slices <- split(dt, by = "pip_id", keep.by = TRUE)

  results <- lapply(slices, function(sdt) {
    pid <- sdt[["pip_id"]][[1L]]

    for (d in setdiff(by, names(sdt))) {
      set(sdt, j = d, value = NA_character_)
    }

    res <- compute_measures(sdt, measures, poverty_lines, by)

    res[, pip_id        := pid]
    res[, country_code  := sdt[["country_code"]][[1L]]]
    res[, surveyid_year := sdt[["surveyid_year"]][[1L]]]
    res[, welfare_type  := sdt[["welfare_type"]][[1L]]]
    res
  })

  rbindlist(results, fill = TRUE)
}

# ── 3. Correctness pre-check ──────────────────────────────────────────────────
#
# Before timing anything, verify all four approaches produce numerically
# identical results on a small 3-survey fixture.  Failed checks exclude the
# approach from the benchmark and flag it in the results document.

message("\n── Correctness check ───────────────────────────────────────────────")

set.seed(123L)

dt_check <- rbindlist(lapply(1:3, function(i) {
  data.table(
    pip_id        = sprintf("TST_2020_S%d_INC_ALL", i),
    welfare       = rlnorm(120L, meanlog = 1.5, sdlog = 0.8),
    weight        = runif(120L, 0.5, 2.0),
    gender        = factor(sample(c("male", "female"), 120L, replace = TRUE)),
    area          = factor(sample(c("urban", "rural"),  120L, replace = TRUE)),
    age_group     = factor(
      sample(c("0-14", "15-24", "25-64", "65+"), 120L, replace = TRUE),
      levels = c("0-14", "15-24", "25-64", "65+"), ordered = TRUE
    ),
    country_code  = paste0("TS", i),
    surveyid_year = 2020L,
    welfare_type  = "INC"
  )
}))

meta_check    <- unique(dt_check[, .(pip_id, country_code, surveyid_year, welfare_type)])
check_by      <- c("gender", "area")
check_pl      <- c(2.15, 3.65)
check_meas    <- c("headcount", "gini", "mean", "median")

res_a <- .normalize_result(
  approach_a_baseline(dt_check, check_meas, check_pl, check_by),
  check_by
)
res_b <- tryCatch(
  .normalize_result(
    approach_b_collapse_grouped(dt_check, check_meas, check_pl, check_by, meta_check),
    check_by
  ),
  error = function(e) { message("  Approach B error: ", conditionMessage(e)); NULL }
)
res_c <- tryCatch(
  .normalize_result(
    approach_c_datatable_by(dt_check, check_meas, check_pl, check_by, meta_check),
    check_by
  ),
  error = function(e) { message("  Approach C error: ", conditionMessage(e)); NULL }
)
res_e <- tryCatch(
  .normalize_result(
    approach_e_split_lapply(dt_check, check_meas, check_pl, check_by),
    check_by
  ),
  error = function(e) { message("  Approach E error: ", conditionMessage(e)); NULL }
)

check_b <- !is.null(res_b) && isTRUE(all.equal(res_a, res_b, tolerance = 1e-10))
check_c <- !is.null(res_c) && isTRUE(all.equal(res_a, res_c, tolerance = 1e-10))
check_e <- !is.null(res_e) && isTRUE(all.equal(res_a, res_e, tolerance = 1e-10))

message("  A vs B: ", if (check_b) "OK (identical)" else "MISMATCH — approach B excluded")
message("  A vs C: ", if (check_c) "OK (identical)" else "MISMATCH — approach C excluded")
message("  A vs E: ", if (check_e) "OK (identical)" else "MISMATCH — approach E excluded")

# Approaches to include in the benchmark (A is always included as baseline)
valid_approaches <- c("A", if (check_b) "B", if (check_c) "C", if (check_e) "E")
message("  Benchmarking: ", paste(valid_approaches, collapse = ", "))

# ── 4. I/O profiling (live mode only) ────────────────────────────────────────

io_median_s <- NA_real_

if (LIVE_MODE) {
  message("\n── I/O profiling ───────────────────────────────────────────────────")
  mf      <- piptm_manifest()
  pip_ids <- unique(dt_bench[["pip_id"]])
  entries <- mf[pip_id %chin% pip_ids]

  io_bm <- bench::mark(
    load_surveys(entries),
    iterations = 3L,
    check      = FALSE,
    memory     = TRUE
  )
  io_median_s <- as.numeric(io_bm$median)
  message(sprintf("  Median load_surveys() time: %.3fs", io_median_s))

  if (io_median_s > 2.0) {
    message(
      "  WARNING: I/O exceeds 2s — orchestration optimisation alone cannot",
      " meet the 3s end-to-end target.  Consider I/O optimisation."
    )
  }
}

# ── 5. Compute benchmark grid ─────────────────────────────────────────────────
#
# Grid: valid_approaches × NTHREADS.
# Each cell: N_ITERATIONS timed runs via bench::mark(), check = FALSE
# (correctness already verified above).
#
# nthreads is set globally on the collapse package before each cell and
# reset to 1 immediately after via on.exit().

message("\n── Compute benchmarks ──────────────────────────────────────────────")
message(sprintf(
  "  Dataset: %s rows / %d surveys | %d measures | %d poverty lines | %d dims",
  format(nrow(dt_bench), big.mark = ","), uniqueN(dt_bench[["pip_id"]]),
  length(ALL_MEASURES), length(POVERTY_LINES), length(BY_DIMS)
))
message(sprintf(
  "  Grid: %d approaches × %d nthreads × %d iterations = %d runs",
  length(valid_approaches), length(NTHREADS), N_ITERATIONS,
  length(valid_approaches) * length(NTHREADS) * N_ITERATIONS
))

# Approach function registry
approach_fns <- list(
  A = function() approach_a_baseline(
    dt_bench, ALL_MEASURES, POVERTY_LINES, BY_DIMS
  ),
  B = function() approach_b_collapse_grouped(
    dt_bench, ALL_MEASURES, POVERTY_LINES, BY_DIMS, metadata
  ),
  C = function() approach_c_datatable_by(
    dt_bench, ALL_MEASURES, POVERTY_LINES, BY_DIMS, metadata
  ),
  E = function() approach_e_split_lapply(
    dt_bench, ALL_MEASURES, POVERTY_LINES, BY_DIMS
  )
)
approach_fns <- approach_fns[names(approach_fns) %in% valid_approaches]

bm_results <- rbindlist(lapply(NTHREADS, function(nt) {
  message(sprintf("  nthreads = %d", nt))
  collapse::set_collapse(nthreads = nt)
  on.exit(collapse::set_collapse(nthreads = 1L), add = TRUE)

  rbindlist(Filter(Negate(is.null), lapply(names(approach_fns), function(ap) {
    message(sprintf("    [%s] ...", ap), appendLF = FALSE)
    fn  <- approach_fns[[ap]]
    bm  <- tryCatch(
      bench::mark(fn(), iterations = N_ITERATIONS, check = FALSE, memory = TRUE),
      error = function(e) {
        message(sprintf(" FAILED: %s", conditionMessage(e)))
        NULL
      }
    )
    if (is.null(bm)) return(NULL)

    # bench_time values are doubles in seconds; bench_bytes in bytes
    med_s  <- as.numeric(bm[["median"]])
    mem_mb <- as.numeric(bm[["mem_alloc"]]) / 1024^2

    message(sprintf(" %.2fs  %.0fMB", med_s, mem_mb))

    data.table(
      approach    = ap,
      nthreads    = nt,
      median_s    = med_s,
      mem_mb      = mem_mb,
      itr_per_sec = bm[["itr/sec"]]
    )
  })))
}))

collapse::set_collapse(nthreads = 1L)  # final reset

# ── 6. Results table ──────────────────────────────────────────────────────────

message("\n── Results ─────────────────────────────────────────────────────────")

approach_labels <- c(
  A = "A: lapply() slices",
  B = "B: collapse grouped",
  C = "C: data.table by=",
  E = "E: split+lapply()"
)

bm_results[, approach_label := factor(
  approach_labels[approach],
  levels = approach_labels[intersect(c("A", "B", "C", "E"), approach)]
)]
bm_results[, meets_target := median_s <= 3.0]
bm_results[, label_s      := sprintf("%.2fs", median_s)]
bm_results[, label_mb     := sprintf("%.0fMB", mem_mb)]

tbl <- bm_results[order(nthreads, approach), .(
  approach, nthreads,
  `median (s)` = round(median_s, 3),
  `mem (MB)`   = round(mem_mb, 1),
  `itr/sec`    = round(itr_per_sec, 2),
  meets_3s     = meets_target
)]
print(tbl)

# ── 7. Decision ───────────────────────────────────────────────────────────────

# Preference order: A (already impl.) > E (trivial) > B (medium) > C (medium)
PREF_ORDER <- c("A", "E", "B", "C")

candidates <- bm_results[meets_target == TRUE]
decision_msg <- if (nrow(candidates) == 0L) {
  best <- bm_results[which.min(median_s)]
  sprintf(
    paste(
      "NO approach meets the 3s target.",
      "Best: Approach %s at %.2fs (nthreads = %d).",
      "I/O is likely the bottleneck — pivot to I/O optimisation."
    ),
    best$approach, best$median_s, best$nthreads
  )
} else {
  # Simplest candidate meeting the target
  simplest <- candidates[
    order(match(approach, PREF_ORDER), nthreads, median_s)
  ][1L]

  # Fastest candidate overall
  fastest  <- candidates[which.min(median_s)]

  speedup <- if (simplest$median_s > 0) {
    (simplest$median_s - fastest$median_s) / simplest$median_s
  } else 0

  if (fastest$approach != simplest$approach && speedup >= 0.40) {
    sprintf(
      paste(
        "RECOMMENDATION: Use Approach %s (nthreads = %d, %.2fs).",
        "It is %.0f%% faster than the simpler Approach %s (%.2fs)",
        "and exceeds the 40%% speedup threshold."
      ),
      fastest$approach, fastest$nthreads, fastest$median_s,
      speedup * 100,
      simplest$approach, simplest$median_s
    )
  } else {
    sprintf(
      paste(
        "RECOMMENDATION: Keep Approach %s (nthreads = %d, %.2fs).",
        "Simplest approach meeting the 3s target.",
        if (fastest$approach != simplest$approach)
          sprintf(
            "Fastest alternative (%s) is only %.0f%% faster — below 40%% threshold.",
            fastest$approach, speedup * 100
          )
        else
          "It is also the fastest."
      ),
      simplest$approach, simplest$nthreads, simplest$median_s
    )
  }
}

message("\n", decision_msg)

# ── 8. Visualisation ──────────────────────────────────────────────────────────

dir.create("benchmarks", showWarnings = FALSE)

n_threads_f <- length(unique(bm_results$nthreads))
fill_vals   <- c(`TRUE` = "#27ae60", `FALSE` = "#e74c3c")

bm_plot <- copy(bm_results)
bm_plot[, nthreads_label := factor(
  paste0(nthreads, " thread", ifelse(nthreads == 1L, "", "s"))
)]

# Panel 1: compute time
p_time <- ggplot(
  bm_plot,
  aes(x = approach_label, y = median_s, fill = meets_target)
) +
  geom_col(width = 0.72) +
  geom_text(aes(label = label_s), vjust = -0.35, size = 3.2) +
  geom_hline(yintercept = 3.0, linetype = "dashed",
             colour = "#c0392b", linewidth = 0.85) +
  annotate("text", x = Inf, y = 3.0, label = "3s target",
           hjust = 1.05, vjust = -0.4, colour = "#c0392b", size = 3) +
  facet_wrap(~nthreads_label, ncol = n_threads_f) +
  scale_fill_manual(
    values = fill_vals,
    labels = c(`TRUE` = "Meets 3s target", `FALSE` = "Over target"),
    name   = NULL
  ) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.15))) +
  labs(
    title    = "Orchestration strategy benchmark — compute time",
    subtitle = sprintf(
      "%d surveys  |  %s rows  |  %d poverty lines  |  %d dims  |  %d measures",
      uniqueN(dt_bench[["pip_id"]]), format(nrow(dt_bench), big.mark = ","),
      length(POVERTY_LINES), length(BY_DIMS), length(ALL_MEASURES)
    ),
    x = NULL,
    y = "Median time (seconds)"
  ) +
  theme_minimal(base_size = 11) +
  theme(
    axis.text.x       = element_text(angle = 25, hjust = 1),
    legend.position   = "bottom",
    strip.background  = element_rect(fill = "grey92", colour = NA),
    panel.grid.major.x = element_blank()
  )

# Panel 2: memory allocation
p_mem <- ggplot(
  bm_plot,
  aes(x = approach_label, y = mem_mb, fill = meets_target)
) +
  geom_col(width = 0.72) +
  geom_text(aes(label = label_mb), vjust = -0.35, size = 3.2) +
  facet_wrap(~nthreads_label, ncol = n_threads_f) +
  scale_fill_manual(values = fill_vals, guide = "none") +
  scale_y_continuous(expand = expansion(mult = c(0, 0.15))) +
  labs(
    title = "Memory allocation per run",
    x     = NULL,
    y     = "Memory allocated (MB)"
  ) +
  theme_minimal(base_size = 11) +
  theme(
    axis.text.x       = element_text(angle = 25, hjust = 1),
    strip.background  = element_rect(fill = "grey92", colour = NA),
    panel.grid.major.x = element_blank()
  )

plot_out <- p_time / p_mem + plot_layout(heights = c(3, 2))

plot_path <- "benchmarks/orchestration-results.png"
ggsave(
  plot_path, plot_out,
  width  = max(8, n_threads_f * 3.5),
  height = 10,
  dpi    = 150
)
message("\nPlot saved to ", plot_path)

# ── 9. Results document ───────────────────────────────────────────────────────

results_dir  <- ".cg-docs/solutions/performance-issues"
results_path <- file.path(results_dir, "2026-04-28-orchestration-benchmark-results.md")
dir.create(results_dir, showWarnings = FALSE, recursive = TRUE)

si       <- sessionInfo()
cpu_info <- tryCatch(paste0(si$running, " / ", si$platform), error = function(e) si$platform)

tbl_lines <- capture.output(print(tbl))

io_section <- if (!is.na(io_median_s)) {
  c(
    paste0(
      "Median `load_surveys()` time for ", uniqueN(dt_bench[["pip_id"]]),
      " surveys: **", sprintf("%.3f", io_median_s), " seconds**."
    ),
    "",
    if (io_median_s > 2.0) paste(
      "> **WARNING**: I/O alone exceeds 2 seconds.",
      "No orchestration change can meet the 3s end-to-end target.",
      "Pivot to I/O optimisation."
    ) else paste(
      "> I/O budget used:", sprintf("%.1f%%", io_median_s / 3.0 * 100),
      "of the 3s target.",
      sprintf("%.2f seconds remain for compute.", 3.0 - io_median_s)
    )
  )
} else {
  c(
    "I/O profiling not run (set `LIVE_MODE <- TRUE` to profile).",
    "",
    "> Set `LIVE_MODE <- TRUE` in the script and re-run to measure real",
    "> `load_surveys()` time and establish how much of the 3s budget I/O consumes."
  )
}

results_md <- c(
  "---",
  "date: 2026-04-28",
  'title: "Orchestration Strategy Benchmark Results"',
  "status: completed",
  "tags: [performance, orchestration, table-maker, benchmark]",
  "---",
  "",
  "# Orchestration Strategy Benchmark Results",
  "",
  "## Environment",
  "",
  paste0("- **R version**: ", R.version$version.string),
  paste0("- **Platform**: ", cpu_info),
  paste0("- **Logical cores**: ", parallel::detectCores(logical = TRUE)),
  paste0("- **Physical cores**: ", parallel::detectCores(logical = FALSE)),
  paste0("- **collapse version**: ", packageVersion("collapse")),
  paste0("- **data.table version**: ", packageVersion("data.table")),
  paste0("- **bench version**: ", packageVersion("bench")),
  "",
  "## Dataset",
  "",
  paste0("- **Mode**: ", if (LIVE_MODE) "Live (Arrow repository)" else "Synthetic"),
  paste0("- **Surveys**: ", uniqueN(dt_bench[["pip_id"]])),
  paste0("- **Total rows**: ", format(nrow(dt_bench), big.mark = ",")),
  paste0("- **Measures**: ", length(ALL_MEASURES)),
  paste0("- **Poverty lines**: ", paste(POVERTY_LINES, collapse = ", ")),
  paste0("- **Dimensions**: ", paste(BY_DIMS, collapse = ", ")),
  "",
  "## I/O Profile",
  "",
  io_section,
  "",
  "## Compute Benchmark Results",
  "",
  "```",
  tbl_lines,
  "```",
  "",
  "## Correctness Check",
  "",
  paste0("- A vs B: ", if (check_b) "**PASS** (identical)" else "**FAIL** — excluded from benchmark"),
  paste0("- A vs C: ", if (check_c) "**PASS** (identical)" else "**FAIL** — excluded from benchmark"),
  paste0("- A vs E: ", if (check_e) "**PASS** (identical)" else "**FAIL** — excluded from benchmark"),
  "",
  "## Decision",
  "",
  decision_msg,
  "",
  "## Notes",
  "",
  paste0("- Iterations per approach × nthreads cell: ", N_ITERATIONS),
  paste0("- nthreads tested: ", paste(NTHREADS, collapse = ", ")),
  "- `check = FALSE` in `bench::mark()` — correctness verified separately.",
  "- Preference order (simplest → most complex): A > E > B > C.",
  "- See `benchmarks/orchestration-results.png` for the timing chart.",
  ""
)

writeLines(results_md, results_path)
message("Results document saved to ", results_path)

message("\n── Done ────────────────────────────────────────────────────────────")

# ── 10. A vs B comparison plot ────────────────────────────────────────────────
#
# Shows approach A vs B side-by-side across nthreads settings.
# Bars are stacked: I/O time (fixed, same for both) on the bottom,
# compute time (from benchmark) on top.  The total bar height = what the
# user actually waits for in a full table_maker() call.
#
# nthreads in approach A: set_collapse(nthreads = n) is a global setting.
# collapse primitives (fsum, fmean, .gini_sorted, …) inside compute_measures()
# ARE threaded — so A does benefit.  The difference is that A re-enters the
# collapse dispatch layer 15× (one per survey lapply iteration), paying the
# thread-spawn overhead 15× instead of 1×.  That is why A's threading gain
# is smaller than B's.

ab_data <- bm_results[approach %in% c("A", "B")]

# If I/O was not profiled (synthetic mode), use 0 so the plot still renders
io_s <- if (is.na(io_median_s)) 0 else io_median_s

# Build long-format data for stacked bars
ab_stack <- rbindlist(list(
  ab_data[, .(
    approach, nthreads,
    segment  = "Compute",
    time_s   = median_s,
    approach_label = approach_labels[approach]
  )],
  ab_data[, .(
    approach, nthreads,
    segment  = "I/O (load_surveys)",
    time_s   = io_s,
    approach_label = approach_labels[approach]
  )]
))

ab_stack[, segment := factor(segment, levels = c("I/O (load_surveys)", "Compute"))]
ab_stack[, nthreads_label := factor(
  paste0(nthreads, ifelse(nthreads == 1L, " thread", " threads")),
  levels = paste0(sort(unique(nthreads)), ifelse(sort(unique(nthreads)) == 1L, " thread", " threads"))
)]
ab_stack[, approach_label := factor(approach_label,
  levels = c("A: lapply() slices", "B: collapse grouped"))]

# Total bar height per group (for label positioning)
ab_totals <- ab_stack[, .(total_s = sum(time_s)), by = .(approach, nthreads, nthreads_label, approach_label)]

# Segment colours: muted blue for I/O, green/orange for compute by approach
seg_fills <- c(
  "I/O (load_surveys)" = "#aec6cf",
  "Compute"            = "#2c7bb6"
)
# Override compute fill per approach using a secondary mapping via alpha or
# a manual two-tone scheme — simplest: use approach as fill for compute only,
# grey for I/O.
ab_stack[, fill_key := fcase(
  segment == "I/O (load_surveys)", "I/O",
  approach == "A",                  "Compute — A",
  approach == "B",                  "Compute — B"
)]
ab_stack[, fill_key := factor(fill_key,
  levels = c("I/O", "Compute — A", "Compute — B"))]

fill_palette <- c(
  "I/O"         = "#c8d8e8",
  "Compute — A" = "#e07b39",
  "Compute — B" = "#2c7bb6"
)

# Approach annotation text — only rendered in the leftmost (1-thread) facet.
# x values must exactly match the factor levels in ab_stack$approach_label.
approach_notes <- data.table(
  approach_label = factor(
    c("A: lapply() slices", "B: collapse grouped"),
    levels = c("A: lapply() slices", "B: collapse grouped")
  ),
  nthreads_label = factor(
    paste0(min(NTHREADS), ifelse(min(NTHREADS) == 1L, " thread", " threads")),
    levels = paste0(sort(unique(ab_stack$nthreads)),
                    ifelse(sort(unique(ab_stack$nthreads)) == 1L, " thread", " threads"))
  ),
  label = c(
    "15\u00d7 filter + GRP per call:\neach survey gets its own\ncompute_measures() call.\nThreads launched 15\u00d7.",
    "1\u00d7 GRP across all surveys:\nfamily functions called directly\nwith compound pip_id \u00d7 by GRP.\nThreads launched once."
  ),
  y_pos = c(2.1, 1.3)   # above bar tops (~0.59 / ~0.28), clear of bar labels
)

# nthreads footnote: explains threading behaviour for each approach
thread_note <- paste0(
  "nthreads: collapse OpenMP threading applied globally via set_collapse(nthreads = n).\n",
  "Both approaches benefit, but A re-enters collapse dispatch 15\u00d7 per call (one per survey),\n",
  "paying thread-spawn overhead each time. B enters once across all surveys."
)

p_ab <- ggplot(ab_stack,
  aes(x = approach_label, y = time_s, fill = fill_key)) +
  geom_col(width = 0.6, colour = "white", linewidth = 0.4) +
  # Total time label on top of each bar
  geom_text(
    data    = ab_totals,
    mapping = aes(x = approach_label, y = total_s,
                  label = sprintf("%.2fs", total_s)),
    inherit.aes = FALSE,
    vjust = -0.5, size = 4.6, fontface = "bold", colour = "grey20"
  ) +
  # Approach annotation (what the approach does) — only in first facet
  geom_label(
    data    = approach_notes,
    mapping = aes(x = approach_label, y = y_pos, label = label),
    inherit.aes = FALSE,
    vjust = 1, hjust = 0.5, size = 3.8, colour = "grey10",
    fill = alpha("white", 0.95), linewidth = 0.4,
    label.padding = unit(0.55, "lines"), label.r = unit(0.1, "lines")
  ) +
  # 3s target line
  geom_hline(yintercept = 3.0, linetype = "dashed",
             colour = "#c0392b", linewidth = 0.7) +
  annotate("text", x = Inf, y = 3.0,
           label = "3s end-to-end target",
           hjust = 1.05, vjust = -0.45, colour = "#c0392b", size = 3.8,
           fontface = "italic") +
  facet_wrap(~nthreads_label, ncol = length(NTHREADS)) +
  scale_x_discrete(labels = c(
    "A: lapply() slices"  = "A: lapply()\nslices",
    "B: collapse grouped" = "B: collapse\ngrouped"
  )) +
  scale_fill_manual(
    values = fill_palette,
    labels = c(
      "I/O"         = sprintf("I/O \u2014 load_surveys()  [%.0f ms]", io_s * 1000),
      "Compute \u2014 A" = "Compute \u2014 A:  lapply() slices",
      "Compute \u2014 B" = "Compute \u2014 B:  collapse grouped"
    ),
    name = NULL
  ) +
  scale_y_continuous(
    expand = expansion(mult = c(0, 0.05)),
    limits = c(0, 3.3),
    breaks = seq(0, 3.0, 0.5),
    labels = scales::label_number(suffix = "s")
  ) +
  labs(
    title    = "Approach A vs B \u2014 total time (I/O + compute) across thread counts",
    subtitle = sprintf(
      "Real data  \u2022  %d surveys  \u2022  %s rows  \u2022  %d measures  \u2022  %d poverty lines  \u2022  %d dims",
      uniqueN(dt_bench$pip_id), format(nrow(dt_bench), big.mark = ","),
      length(ALL_MEASURES), length(POVERTY_LINES), length(BY_DIMS)
    ),
    x       = NULL,
    y       = "Time (seconds)",
    caption = thread_note
  ) +
  theme_minimal(base_size = 13) +
  theme(
    plot.title         = element_text(face = "bold", size = 15),
    plot.subtitle      = element_text(colour = "grey40", size = 11),
    plot.caption       = element_text(colour = "grey50", size = 9.5,
                                      hjust = 0, lineheight = 1.4),
    axis.text.x        = element_text(size = 11, face = "bold", lineheight = 1.2),
    axis.text.y        = element_text(size = 11),
    axis.title.y       = element_text(size = 11),
    legend.position    = "bottom",
    legend.text        = element_text(size = 10.5),
    legend.key.size    = unit(0.6, "cm"),
    strip.text         = element_text(face = "bold", size = 12),
    strip.background   = element_rect(fill = "grey93", colour = NA),
    panel.grid.major.x = element_blank(),
    panel.grid.minor   = element_blank(),
    plot.margin        = margin(14, 20, 10, 14)
  )

ab_plot_path <- "benchmarks/ab-comparison.png"
ggsave(
  ab_plot_path, p_ab,
  width  = 12,
  height = 8,
  dpi    = 150
)
message("A vs B comparison plot saved to ", ab_plot_path)
