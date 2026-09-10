#' @importFrom collapse fsum GRP
#' @importFrom data.table melt rbindlist as.data.table
NULL

# ── Poverty family computation ────────────────────────────────────────────────

#' Compute poverty measures for a single survey
#'
#' Computes one or more FGT (Foster-Greer-Thorbecke) poverty measures and the
#' Watts index for a single survey slice.  Measures are computed in a
#' **per-poverty-line loop** that keeps memory at O(n) throughout: the
#' original dataset is never expanded.  `log(welfare)` and the grouping
#' object are both pre-computed once and reused across all poverty lines,
#' so requesting multiple lines costs little extra.
#'
#' The poverty measures are:
#' \describe{
#'   \item{`headcount`}{Share of the weighted population below the poverty
#'     line: \eqn{H = \sum w_i \mathbf{1}[y_i < z] / \sum w_i}.}
#'   \item{`poverty_gap`}{Mean normalised shortfall:
#'     \eqn{PG = \sum w_i g_i / \sum w_i} where
#'     \eqn{g_i = (z - y_i)/z} for poor individuals, 0 otherwise.}
#'   \item{`severity`}{FGT(2) — squared-gap index:
#'     \eqn{\sum w_i g_i^2 / \sum w_i}.}
#'   \item{`watts`}{Watts index:
#'     \eqn{W = \sum w_i \log(z / y_i) \mathbf{1}[y_i < z] / \sum w_i}.
#'     Individuals with \eqn{y_i = 0} contribute 0 to the Watts sum (see
#'     **Zero-welfare rule** below).}
#'   \item{`pop_poverty`}{Weighted count of poor individuals:
#'     \eqn{\sum w_i \mathbf{1}[y_i < z]}.}
#' }
#'
#' **Zero-welfare rule**: individuals with `welfare == 0` are counted as poor
#' (their welfare is strictly below any positive poverty line) but contribute
#' **0** to the Watts index (rather than \eqn{+\infty}).  The rule is
#' consistent with the treatment in `compute_inequality()` for MLD.
#'
#' @param dt A [data.table::data.table()] containing at minimum `welfare`
#'   (numeric) and `weight` (numeric) columns, plus any columns named in `by`.
#'   This must be a **single-survey slice** — rows for exactly one `pip_id`.
#'   The data.table is not modified; a working copy is created internally.
#' @param poverty_lines A positive numeric vector of poverty line values.
#'   Must be non-empty.
#' @param by A character vector of grouping column names present in `dt`
#'   (e.g. `c("gender", "area")`), or `NULL` for the aggregate (no
#'   disaggregation).
#' @param measures A character vector of poverty measure names to compute —
#'   a subset of `c("headcount", "poverty_gap", "severity", "watts",
#'   "pop_poverty")`.  `NULL` (default) computes all five.
#' @param grp An optional pre-computed [collapse::GRP()] object built on `dt`
#'   (or a working copy with the same rows in the same order) grouped by `by`.
#'   When provided, the internal `GRP()` call is skipped — useful when the
#'   caller already holds a grouping for the same data.  Ignored when `by` is
#'   `NULL`.
#'
#' @return A [data.table::data.table()] in **long format** with columns:
#' \describe{
#'   \item{`poverty_line`}{(numeric) The poverty threshold.}
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
#' compute_poverty(dt, poverty_lines = 3.5, measures = "headcount")
#'
#' @export
compute_poverty <- function(dt, poverty_lines, by = NULL, measures = NULL,
                            grp = NULL) {
  # Suppress R CMD check NOTEs for data.table NSE column references
  welfare      <- NULL
  poverty_line <- NULL
  measure      <- NULL

  all_poverty_measures <- c(
    "headcount", "poverty_gap", "severity", "watts", "pop_poverty"
  )
  if (is.null(measures)) measures <- all_poverty_measures

  # ── 1. Working copy with only needed columns ────────────────────────────────
  # When by = NULL, skip the copy — just extract the two vectors directly.
  welfare_v <- dt[["welfare"]]
  w         <- dt[["weight"]]
  if (!is.null(by)) {
    work <- dt[, c("welfare", "weight", by), with = FALSE]
  }

  # ── 2. Pre-compute once — reused across all poverty lines ───────────────────
  # GRP is built on the original n-row data and stays valid for every loop
  # iteration because rows are never expanded or reordered.
  if (is.null(grp) && !is.null(by)) {
    grp <- collapse::GRP(work, by = by)  # work exists iff by is non-NULL
  } else if (is.null(by)) {
    grp <- NULL  # ignore any caller-supplied grp when no disaggregation
  }

  w_total <- collapse::fsum(w, g = grp)

  # log(welfare) computed once outside the loop.
  # Zero-welfare rule: welfare == 0 → logw = 0 here; those rows are masked
  # by pos inside the loop so no +Inf or NaN can arise.
  # log(welfare_v + (welfare_v == 0)): adds 1 only where welfare==0 so
  # log(1)=0, avoiding fifelse's eager double-evaluation on n rows.
  pos  <- welfare_v > 0                       # loop-invariant; hoisted
  logw <- log(welfare_v + (welfare_v == 0L))  # single allocation, no branch

  # ── 3. Loop over poverty lines — O(n) memory per iteration ─────────────────
  grp_cols <- c("poverty_line", by)
  n_pl     <- length(poverty_lines)
  results  <- vector("list", n_pl)
  grp_dt   <- if (!is.null(grp)) as.data.table(grp$groups) else NULL  # hoisted

  # Pre-allocate loop temporaries once.  Inside the loop []<- replacement
  # modifies in-place (ref-count == 1) instead of allocating fresh vectors
  # on every iteration — reduces GC pressure across all poverty lines.
  # neg_logw is loop-invariant; hoisting it avoids recomputing -logw and
  # allocating the negation vector on every iteration.
  n             <- length(welfare_v)
  poor          <- logical(n)
  poor_pos      <- logical(n)
  gap           <- numeric(n)
  watts_contrib <- numeric(n)
  neg_logw      <- -logw                      # loop-invariant; hoisted

  for (i in seq_along(poverty_lines)) {
    z <- poverty_lines[[i]]

    poor[]          <- welfare_v < z
    w_poor          <- w * poor                     # P1.1: computed once

    # Normalised gap: (z - y) / z for poor; 0 for non-poor.
    # Logical coercion: TRUE * x = x, FALSE * x = 0.
    gap[]           <- poor * (z - welfare_v) / z

    # Watts contribution: log(z / y) for poor with y > 0; 0 otherwise.
    # poor_pos reuses a pre-allocated logical vector; neg_logw is hoisted.
    poor_pos[]      <- poor & pos
    watts_contrib[] <- poor_pos * (log(z) + neg_logw)

    hc <- collapse::fsum(w_poor,            g = grp) / w_total
    pg <- collapse::fsum(w * gap,           g = grp) / w_total
    sv <- collapse::fsum(w * gap^2,         g = grp) / w_total
    wa <- collapse::fsum(w * watts_contrib, g = grp) / w_total
    pp <- collapse::fsum(w_poor,            g = grp)

    pl_row <- data.table(
      poverty_line = z,
      headcount    = hc,
      poverty_gap  = pg,
      severity     = sv,
      watts        = wa,
      pop_poverty  = pp,
      population   = w_total
    )

    results[[i]] <- if (!is.null(grp_dt)) cbind(grp_dt, pl_row) else pl_row
  }

  result <- rbindlist(results)

  # ── 4. Subset to requested measures ─────────────────────────────────────────
  result <- result[, c(grp_cols, measures, "population"), with = FALSE]

  # ── 5. Melt to long format ───────────────────────────────────────────────────
  result <- melt(
    result,
    id.vars       = c(grp_cols, "population"),
    measure.vars  = measures,
    variable.name = "measure",
    value.name    = "value"
  )
  result[, measure := as.character(measure)]

  result
}
