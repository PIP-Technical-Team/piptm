# ── Summary statistics computation ─────────────────────────────────────────────

#' Compute summary statistics for a single survey
#'
#' Computes one or more summary measures — mean, median, standard deviation,
#' variance, min, max, non-missing observation count, and four weighted
#' percentiles (p10, p25, p75, p90) — for a single survey slice, optionally
#' disaggregated across one or more dimensions.
#'
#' By default, statistics are computed on the `welfare` column. A different
#' target variable can be supplied via `target_variable`.
#'
#' All weighted statistics use `weight` as weights via
#' the `w` argument of the corresponding {collapse} function. `min`, `max`,
#' and `nobs` are unweighted. `nobs` counts non-missing observations of the
#' target variable.
#'
#' **{collapse} function mapping**:
#' | Measure  | collapse function                     | Notes                         |
#' |----------|---------------------------------------|-------------------------------|
#' | `mean`   | `fmean(x, w = weight, g = grp)`       | Weighted mean                 |
#' | `median` | `fmedian(x, w = weight, g = grp)`     | Weighted median               |
#' | `sd`     | `fsd(x, w = weight, g = grp)`         | Weighted SD (sample, `n-1`)   |
#' | `var`    | `fvar(x, w = weight, g = grp)`        | Weighted variance (sample)    |
#' | `min`    | `fmin(x, g = grp)`                    | Observed minimum              |
#' | `max`    | `fmax(x, g = grp)`                    | Observed maximum              |
#' | `nobs`   | `fnobs(x, g = grp)`                   | Non-missing observation count |
#' | `p10`    | `fnth(x, 0.10, w = weight, g = grp)`  | Weighted 10th percentile      |
#' | `p25`    | `fnth(x, 0.25, w = weight, g = grp)`  | Weighted 25th percentile      |
#' | `p75`    | `fnth(x, 0.75, w = weight, g = grp)`  | Weighted 75th percentile      |
#' | `p90`    | `fnth(x, 0.90, w = weight, g = grp)`  | Weighted 90th percentile      |
#' | `sum`    | `fsum(x, w = weight, g = grp)`        | Weighted sum                  |
#'
#' @param dt A [data.table::data.table()] containing at minimum `weight`
#'   (numeric) and the target variable. This must be a
#'   **single-survey slice** — rows for exactly one `pip_id`.
#'   The data.table is not modified.
#'
#' @param by A character vector of grouping column names present in `dt`
#'   (e.g. `c("gender", "area")`), or `NULL` for the aggregate
#'   (no disaggregation).
#'
#' @param measures A character vector of summary measure names to compute —
#'   a subset of:
#'   `c("mean", "median", "sd", "var", "min", "max", "nobs",
#'     "p10", "p25", "p75", "p90", "sum")`.
#'   `NULL` (default) computes all twelve measures.
#'
#' @param target_variable Character scalar naming the variable on which
#'   statistics should be computed. Defaults to `"welfare"`.
#'
#' @param grp An optional pre-computed [collapse::GRP()] object built from
#'   the same `dt` and grouped by `by`. When provided, the internal
#'   `GRP()` call is skipped. Ignored when `by` is `NULL`.
#'
#' @return A [data.table::data.table()] in **long format** with columns:
#' \describe{
#'   \item{`[by cols]`}{One column per element of `by` (if non-NULL).}
#'   \item{`measure`}{(character) The measure name.}
#'   \item{`value`}{(numeric) The computed statistic.}
#'   \item{`population`}{(numeric) Total weighted population in the group,
#'   computed as `fsum(weight, g = grp)`.}
#' }
#'
#' @family compute
#'
#' @examples
#' \dontrun{
#' library(data.table)
#'
#' dt <- data.table(
#'   welfare = c(1, 2, 3, 4, 5),
#'   weight  = rep(1, 5)
#' )
#'
#' compute_summary_stats(
#'   dt,
#'   measures = c("mean", "median")
#' )
#' }
compute_summary_stats <- function(dt, by = NULL, measures = NULL, target_variable = "welfare", grp = NULL) {

  # Suppress R CMD check NOTEs for data.table NSE column references
  measure <- NULL

  all_summary_stats_measures <- c(
    "mean", "median", "sd", "var", "min", "max", "nobs",
    "p10", "p25", "p75", "p90", "sum"
  )

  if (is.null(measures)) measures <- all_summary_stats_measures

  measures <- unique(
    match.arg(
      measures,
      all_summary_stats_measures,
      several.ok = TRUE
    )
  )

  # ── 1. Validate required columns ───────────────────────────────────────────

  required_cols <- c(target_variable, "weight")
  missing_cols  <- setdiff(required_cols, names(dt))

  if (length(missing_cols) > 0L) {
    cli_abort(
      c("Required column{?s} missing from {.arg dt}: {.col {missing_cols}}.")
    )
  }

  # ── 2. Extract core vectors ────────────────────────────────────────────────

  x <- dt[[target_variable]]
  w <- dt[["weight"]]

  # ── 3. Pre-compute grouping ────────────────────────────────────────────────

  if (!is.null(by)) {
    if (is.null(grp)) grp <- collapse::GRP(dt, by = by)
  } else {
    grp <- NULL
  }

  # ── 4. Population (always computed, reused) ────────────────────────────────

  population <- collapse::fsum(w, g = grp)

  # ── 5. Compute only requested measures ─────────────────────────────────────

  vals <- list()

  if ("mean"   %in% measures) vals[["mean"]]   <- collapse::fmean(x, w = w, g = grp)
  if ("median" %in% measures) vals[["median"]] <- collapse::fmedian(x, w = w, g = grp)
  if ("sd"     %in% measures) vals[["sd"]]     <- collapse::fsd(x, w = w, g = grp)
  if ("var"    %in% measures) vals[["var"]]    <- collapse::fvar(x, w = w, g = grp)
  if ("min"    %in% measures) vals[["min"]]    <- collapse::fmin(x, g = grp)
  if ("max"    %in% measures) vals[["max"]]    <- collapse::fmax(x, g = grp)
  if ("nobs"   %in% measures) vals[["nobs"]]   <- as.double(collapse::fnobs(x, g = grp))
  if ("p10"    %in% measures) vals[["p10"]]    <- collapse::fnth(x, 0.10, w = w, g = grp)
  if ("p25"    %in% measures) vals[["p25"]]    <- collapse::fnth(x, 0.25, w = w, g = grp)
  if ("p75"    %in% measures) vals[["p75"]]    <- collapse::fnth(x, 0.75, w = w, g = grp)
  if ("p90"    %in% measures) vals[["p90"]]    <- collapse::fnth(x, 0.90, w = w, g = grp)
  if ("sum"    %in% measures) vals[["sum"]]    <- collapse::fsum(x, w = w, g = grp)

  # ── 6. Assemble wide result ────────────────────────────────────────────────

  if (!is.null(by)) {
    result <- as.data.table(grp$groups)
  } else {
    result <- data.table::data.table()
  }

  result[, c(names(vals), "population") := c(vals, list(population))]

  # ── 7. Melt to long format ─────────────────────────────────────────────────

  result <- melt(
    result,
    id.vars       = c(by, "population"),
    measure.vars  = measures,
    variable.name = "measure",
    value.name    = "value"
  )

  result[, measure := as.character(measure)]

  result
}
