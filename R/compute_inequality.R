#' @importFrom collapse fmean GRP fsum fcumsum flag
#' @importFrom data.table melt as.data.table setorder fifelse
NULL

# ── Inequality family computation ─────────────────────────────────────────────

#' Compute Gini coefficient from sorted welfare and weight vectors
#'
#' Pure vectorised computation of the Gini coefficient using the Brown (1994)
#' lag-based trapezoid formula, implemented entirely with `collapse` C-level
#' functions for speed.  The input vectors **must already be sorted in
#' ascending order** of welfare — the caller is responsible for sorting.
#'
#' **Formula** (Brown 1994 lag-based trapezoid):
#' \deqn{
#'   B = \frac{1}{W \cdot WY} \sum_{i=1}^{n}
#'     \Bigl( \sum_{j < i} w_j y_j + \tfrac{1}{2} w_i y_i \Bigr) w_i
#'   \quad\text{then}\quad \text{Gini} = 1 - 2B
#' }
#' where \eqn{W = \sum_i w_i} and \eqn{WY = \sum_i w_i y_i}.
#'
#' Returns `NA_real_` when \eqn{WY = 0} (all welfare is zero), avoiding a
#' silent `NaN` from division by zero in the Lorenz normalisation.
#'
#' @param welfare Numeric vector of welfare values, **sorted ascending**.
#' @param weight  Numeric vector of weights, same length as `welfare`.
#'
#' @return A single numeric value in \eqn{[0, 1)}, or `NA_real_` when all
#'   welfare values are zero.
#'
#' @keywords internal
.gini_sorted <- function(welfare, weight) {
  if (length(welfare) == 1L) return(0)

  ww <- weight * welfare
  if (collapse::fsum(ww) == 0) return(NA_real_)  # P0.1: all-zero welfare guard

  sw  <- collapse::fsum(weight)
  swy <- collapse::fsum(ww)

  # Brown (1994) trapezoid via lag-based cumulative sum (all C-level):
  #   area = sum[ (cumsum_of_lagged_ww + ww/2) * w ] / W / WY
  v   <- (collapse::fcumsum(collapse::flag(ww, fill = 0)) + ww / 2) * weight
  auc <- collapse::fsum(v) / sw / swy
  1 - 2 * auc
}

#' Compute inequality measures for a single survey
#'
#' Computes one or both of the supported inequality measures — the **Gini
#' coefficient** and the **Mean Log Deviation (MLD)** — for a single survey
#' slice, optionally disaggregated across one or more dimensions.
#'
#' **Gini** is computed via a fully vectorised data.table sort + cumulative-sum
#' approach (no per-group explicit loop).  The grouping GRP is used to assign
#' group ids; within each group the data are sorted by welfare, then the
#' trapezoid Lorenz formula is applied through `.gini_sorted()`.
#'
#' **MLD (Mean Log Deviation)** is the Theil L index:
#' \deqn{
#'   \text{MLD} = \frac{1}{\sum w_i} \sum w_i \log\!\left(
#'     \frac{\bar{y}_w}{y_i}\right)
#' }
#' where \eqn{\bar{y}_w} is the weighted mean of welfare.
#'
#' **Zero-welfare rule**: rows with `welfare == 0` contribute **0** to MLD
#' (rather than \eqn{+\infty}).  They are counted in the population and in
#' the weighted mean, but their individual log-ratio is set to 0.  This is
#' consistent with the zero-welfare treatment in `compute_poverty()` for the
#' Watts index.
#'
#' @param dt A [data.table::data.table()] containing at minimum `welfare`
#'   (numeric) and `weight` (numeric) columns, plus any columns named in `by`.
#'   This must be a **single-survey slice** — rows for exactly one `pip_id`.
#'   The data.table is not modified; a working copy is created internally.
#' @param by A character vector of grouping column names present in `dt`
#'   (e.g. `c("gender", "area")`), or `NULL` for the aggregate (no
#'   disaggregation).
#' @param measures A character vector of inequality measure names to compute —
#'   a subset of `c("gini", "mld")`.  `NULL` (default) computes both.
#' @param grp An optional pre-computed [collapse::GRP()] object built on `dt`
#'   grouped by `by`.  When provided, the internal `GRP()` call is skipped.
#'   Ignored when `by` is `NULL`.
#'
#' @return A [data.table::data.table()] in **long format** with columns:
#' \describe{
#'   \item{`[by cols]`}{One column per element of `by` (if non-NULL).}
#'   \item{`measure`}{(character) `"gini"` or `"mld"`.}
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
#' compute_inequality(dt, measures = "gini")
#'
#' @export
compute_inequality <- function(dt, by = NULL, measures = NULL, grp = NULL) {
  # Suppress R CMD check NOTEs for data.table NSE column references
  welfare <- NULL
  weight  <- NULL
  .grp_id <- NULL
  measure <- NULL

  all_ineq_measures <- c("gini", "mld")
  if (is.null(measures)) measures <- all_ineq_measures
  # P1.2: validate measure names; P3.3: silently deduplicate
  measures <- unique(match.arg(measures, all_ineq_measures, several.ok = TRUE))

  # ── 1. Extract core vectors directly — avoids a copy when by = NULL ─────────
  welfare_v <- dt[["welfare"]]
  w         <- dt[["weight"]]

  # ── 2. Pre-compute grouping ──────────────────────────────────────────────────
  if (!is.null(by)) {
    if (is.null(grp)) grp <- collapse::GRP(dt, by = by)
  } else {
    grp <- NULL
  }

  # Group-level population (reused for both measures)
  population <- collapse::fsum(w, g = grp)

  # ── 3. Gini — per-group sort + collapse C-level cumulative sum ───────────────
  if ("gini" %in% measures) {
    if (!is.null(by)) {
      # Need .grp_id for per-group sort; create working copy with group ids
      work_g <- dt[, c("welfare", "weight", by), with = FALSE]
      work_g[, .grp_id := grp$group.id]
      setorder(work_g, .grp_id, welfare)
      gini_dt <- work_g[,
        .(gini = .gini_sorted(welfare, weight)),
        by = .grp_id
      ]
      setorder(gini_dt, .grp_id)
      gini_vals <- gini_dt[["gini"]]
    } else {
      # Single group: sort vectors directly — no data.table copy needed
      ord       <- order(welfare_v)
      gini_vals <- .gini_sorted(welfare_v[ord], w[ord])
    }
  }

  # ── 4. MLD — weighted mean log deviation ────────────────────────────────────
  if ("mld" %in% measures) {
    # Weighted mean per group, broadcast back to every row
    wmean_row <- collapse::fmean(welfare_v, w = w, g = grp, TRA = "replace_fill")

    # log(mean / welfare); zero-welfare rule: contribute 0 when welfare == 0
    # or when group mean == 0 (P2.7: guards -Inf from log(near-zero / welfare))
    log_ratio_v <- fifelse(
      welfare_v > 0 & wmean_row > 0,
      log(wmean_row / welfare_v),
      0
    )

    mld_vals <- collapse::fmean(log_ratio_v, w = w, g = grp)
  }

  # ── 5. Assemble wide result ──────────────────────────────────────────────────
  # Extract unique group rows in GRP order
  if (!is.null(by)) {
    grp_dt <- as.data.table(grp$groups)
  } else {
    grp_dt <- data.table()
  }

  result <- grp_dt
  if ("gini" %in% measures) result[, gini := gini_vals]
  if ("mld"  %in% measures) result[, mld  := mld_vals]
  result[, population := population]

  # ── 6. Subset to requested measures + melt to long ──────────────────────────
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
