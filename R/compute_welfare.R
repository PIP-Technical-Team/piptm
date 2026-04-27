#' @importFrom collapse fmean fmedian fnth fvar fsd fmin fmax fnobs fsum GRP
#' @importFrom data.table melt as.data.table
#' @importFrom cli cli_abort
NULL

# ── Welfare family computation ─────────────────────────────────────────────────

#' Compute welfare summary statistics for a single survey
#'
#' Computes one or more welfare summary measures — mean, median, standard
#' deviation, variance, min, max, unweighted observation count, and four
#' weighted percentiles (p10, p25, p75, p90) — for a single survey slice,
#' optionally disaggregated across one or more dimensions.
#'
#' All weighted statistics use `weight` as frequency/probability weights via
#' the `w` argument of the corresponding {collapse} function.  `min`, `max`,
#' and `nobs` are unweighted (they operate on observed values / row counts).
#'
#' **{collapse} function mapping**:
#' | Measure  | collapse function                             | Notes                          |
#' |----------|-----------------------------------------------|--------------------------------|
#' | `mean`   | `fmean(welfare, w = weight, g = grp)`         | Weighted mean                  |
#' | `median` | `fmedian(welfare, w = weight, g = grp)`       | Weighted median                |
#' | `sd`     | `fsd(welfare, w = weight, g = grp)`           | Weighted SD (sample, `n-1`)    |
#' | `var`    | `fvar(welfare, w = weight, g = grp)`          | Weighted variance (sample)     |
#' | `min`    | `fmin(welfare, g = grp)`                      | Observed minimum               |
#' | `max`    | `fmax(welfare, g = grp)`                      | Observed maximum               |
#' | `nobs`   | `fnobs(welfare, g = grp)`                     | Unweighted row count           |
#' | `p10`    | `fnth(welfare, 0.10, w = weight, g = grp)`    | Weighted 10th percentile       |
#' | `p25`    | `fnth(welfare, 0.25, w = weight, g = grp)`    | Weighted 25th percentile       |
#' | `p75`    | `fnth(welfare, 0.75, w = weight, g = grp)`    | Weighted 75th percentile       |
#' | `p90`    | `fnth(welfare, 0.90, w = weight, g = grp)`    | Weighted 90th percentile       |
#' | `sum`    | `fsum(welfare, w = weight, g = grp)`          | Weighted sum of welfare        |
#'
#' @param dt A [data.table::data.table()] containing at minimum `welfare`
#'   (numeric) and `weight` (numeric) columns, plus any columns named in `by`.
#'   This must be a **single-survey slice** — rows for exactly one `pip_id`.
#'   The data.table is not modified.
#' @param by A character vector of grouping column names present in `dt`
#'   (e.g. `c("gender", "area")`), or `NULL` for the aggregate (no
#'   disaggregation).
#' @param measures A character vector of welfare measure names to compute —
#'   a subset of `c("mean", "median", "sd", "var", "min", "max", "nobs",
#'   "p10", "p25", "p75", "p90", "sum")`.  `NULL` (default) computes all twelve.
#' @param grp An optional pre-computed [collapse::GRP()] object built on `dt`
#'   grouped by `by`.  When provided, the internal `GRP()` call is skipped.
#'   Ignored when `by` is `NULL`.
#'
#' @return A [data.table::data.table()] in **long format** with columns:
#' \describe{
#'   \item{`[by cols]`}{One column per element of `by` (if non-NULL).}
#'   \item{`measure`}{(character) The measure name.}
#'   \item{`value`}{(numeric) The computed statistic.}
#'   \item{`population`}{(numeric) Total weighted population in the group.}
#' }
#'
#' @family compute
#'
#' @examples
#' library(data.table)
#' dt <- data.table(
#'   welfare = c(1, 2, 3, 4, 5),
#'   weight  = rep(1, 5)
#' )
#' compute_welfare(dt, measures = c("mean", "median"))
#'
#' @export
compute_welfare <- function(dt, by = NULL, measures = NULL, grp = NULL) {
  # Suppress R CMD check NOTEs for data.table NSE column references
  measure <- NULL

  all_welfare_measures <- c(
    "mean", "median", "sd", "var", "min", "max", "nobs",
    "p10", "p25", "p75", "p90", "sum"
  )
  if (is.null(measures)) measures <- all_welfare_measures
  measures <- unique(match.arg(measures, all_welfare_measures, several.ok = TRUE))

  # ── 1. Validate required columns ───────────────────────────────────────────
  missing_cols <- setdiff(c("welfare", "weight"), names(dt))
  if (length(missing_cols) > 0L) {
    cli_abort(
      c("Required column{?s} missing from {.arg dt}: {.col {missing_cols}}.")
    )
  }

  # ── 2. Extract core vectors ─────────────────────────────────────────────────
  welfare_v <- dt[["welfare"]]
  w         <- dt[["weight"]]

  # ── 3. Pre-compute grouping ─────────────────────────────────────────────────
  if (!is.null(by)) {
    if (is.null(grp)) grp <- collapse::GRP(dt, by = by)
  } else {
    grp <- NULL  # ignore any caller-supplied grp when no disaggregation
  }

  # ── 4. Population (always computed, reused) ─────────────────────────────────
  population <- collapse::fsum(w, g = grp)

  # ── 5. Compute only requested measures ─────────────────────────────────────
  # Each result is a numeric vector of length equal to the number of groups
  # (or length 1 when grp is NULL).

  vals <- list()

  if ("mean"   %in% measures) vals[["mean"]]   <- collapse::fmean(welfare_v, w = w, g = grp)
  if ("median" %in% measures) vals[["median"]] <- collapse::fmedian(welfare_v, w = w, g = grp)
  if ("sd"     %in% measures) vals[["sd"]]     <- collapse::fsd(welfare_v, w = w, g = grp)
  if ("var"    %in% measures) vals[["var"]]    <- collapse::fvar(welfare_v, w = w, g = grp)
  if ("min"    %in% measures) vals[["min"]]    <- collapse::fmin(welfare_v, g = grp)
  if ("max"    %in% measures) vals[["max"]]    <- collapse::fmax(welfare_v, g = grp)
  if ("nobs"   %in% measures) vals[["nobs"]]   <- as.double(collapse::fnobs(welfare_v, g = grp))
  if ("p10"    %in% measures) vals[["p10"]]    <- collapse::fnth(welfare_v, 0.10, w = w, g = grp)
  if ("p25"    %in% measures) vals[["p25"]]    <- collapse::fnth(welfare_v, 0.25, w = w, g = grp)
  if ("p75"    %in% measures) vals[["p75"]]    <- collapse::fnth(welfare_v, 0.75, w = w, g = grp)
  if ("p90"    %in% measures) vals[["p90"]]    <- collapse::fnth(welfare_v, 0.90, w = w, g = grp)
  if ("sum"    %in% measures) vals[["sum"]]    <- collapse::fsum(welfare_v, w = w, g = grp)

  # ── 6. Assemble wide result ─────────────────────────────────────────────────
  # Extract unique group rows in GRP order, then assign all measure columns
  # and population in a single batch := call (avoids per-column data.table
  # overhead of a loop; ~9x faster in scalar path, ~1.2x in grouped path).
  if (!is.null(by)) {
    # grp$groups holds the unique group rows in GRP order
    result <- as.data.table(grp$groups)
  } else {
    result <- data.table::data.table()
  }

  result[, c(names(vals), "population") := c(vals, list(population))]

  # ── 7. Melt to long format ──────────────────────────────────────────────────
  # Melt only the requested measure columns to long
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
