#' @importFrom collapse fsum GRP
#' @importFrom data.table melt as.data.table data.table
#' @importFrom cli cli_abort
NULL

# ── Shares computation ────────────────────────────────────────────────────────

# ── Internal helpers ──────────────────────────────────────────────────────────

#' Prepare common inputs for shares computation
#'
#' Validates required columns, extracts the weight vector, computes the survey
#' total, and builds the grouping object. Called by `.shares_population()` and
#' `.shares_target()` before any measure-specific logic.
#'
#' @param dt A [data.table::data.table()] for a single survey.
#' @param by Character vector of grouping column names, or `NULL`.
#' @param extra_cols Character vector of additional required column names
#'   (e.g. the target variable name). Checked alongside `"weight"`.
#' @param grp Optional pre-computed [collapse::GRP()] matching `by`.
#'
#' @return A named list with elements:
#' \describe{
#'   \item{`w`}{Numeric weight vector.}
#'   \item{`denom_survey`}{Scalar total weighted population of the survey.}
#'   \item{`grp`}{A [collapse::GRP()] object, or `NULL` when `by = NULL`.}
#'   \item{`cell_wpop`}{Numeric vector of per-cell weighted populations
#'     (length = number of groups), or scalar survey total when `by = NULL`.}
#'   \item{`groups_dt`}{A [data.table::data.table()] of group keys with a
#'     `population` column, or a one-row table with only `population` when
#'     `by = NULL`.}
#' }
#' @keywords internal
.shares_setup <- function(dt, by = NULL, extra_cols = NULL, grp = NULL) {

  # Validate columns
  required_cols <- c("weight", extra_cols)
  missing_cols  <- setdiff(required_cols, names(dt))

  if (length(missing_cols) > 0L) {
    cli_abort(c("Required column{?s} missing from {.arg dt}: {.col {missing_cols}}."))
  }

  w            <- dt[["weight"]]
  denom_survey <- collapse::fsum(w)

  if (!is.null(by)) {
    if (is.null(grp)) grp <- collapse::GRP(dt, by = by)
    cell_wpop <- collapse::fsum(w, g = grp)
    groups_dt <- as.data.table(grp$groups)
    groups_dt[, population := cell_wpop]
  } else {
    grp       <- NULL
    cell_wpop <- denom_survey
    groups_dt <- data.table::data.table(population = denom_survey)
  }

  list(
    w            = w,
    denom_survey = denom_survey,
    grp          = grp,
    cell_wpop    = cell_wpop,
    groups_dt    = groups_dt
  )
}


#' Compute subgroup population shares (no target variable)
#'
#' For each cell defined by `by`, computes the share of the survey's total
#' weighted population that falls in that cell:
#'
#' `pop_share = weighted_pop_in_subgroup / total_weighted_pop_in_survey`
#'
#' When `by = NULL` a single row is returned with `pop_share = 1` (the whole
#' survey over itself).
#'
#' @param dt A [data.table::data.table()] for a single survey.
#' @param by Character vector of grouping column names, or `NULL`.
#' @param grp Optional pre-computed [collapse::GRP()] matching `by`.
#'
#' @return A [data.table::data.table()] in long format with columns:
#' \describe{
#'   \item{`[by cols]`}{Grouping columns (present only when `by` is provided).}
#'   \item{`population`}{Weighted population of the subgroup.}
#'   \item{`measure`}{Always `"pop_share"`.}
#'   \item{`value`}{The computed share (0–1); `NA` if the survey total is zero.}
#' }
#' @keywords internal
.shares_population <- function(dt, by = NULL, grp = NULL) {

  measure <- NULL

  s <- .shares_setup(dt, by = by, grp = grp)

  id_vars <- if (!is.null(by)) c(by, "population") else "population"

  s$groups_dt[, pop_share := ifelse(
    s$denom_survey == 0, NA_real_, s$cell_wpop / s$denom_survey
  )]

  result <- melt(
    s$groups_dt,
    id.vars       = id_vars,
    measure.vars  = "pop_share",
    variable.name = "measure",
    value.name    = "value"
  )

  result[, measure := as.character(measure)]
  result
}


#' Compute target-variable shares per subgroup
#'
#' Assumes `target_variable` is binary (0/1, validated upstream). For each
#' cell defined by `by`, computes two shares based on the weighted population
#' with `target_variable == 1`:
#'
#' - `target_within_group_share`:
#'   `weighted_pop(target == 1 in subgroup) / weighted_pop(subgroup)`
#' - `target_survey_share`:
#'   `weighted_pop(target == 1 in subgroup) / weighted_pop(survey)`
#'
#' When `by = NULL` both measures are identical by definition, since the only
#' group is the whole survey.
#'
#' @param dt A [data.table::data.table()] for a single survey.
#' @param by Character vector of grouping column names, or `NULL`.
#' @param target_variable Character name of the binary (0/1) column in `dt`.
#' @param grp Optional pre-computed [collapse::GRP()] matching `by`.
#'
#' @return A [data.table::data.table()] in long format with columns:
#' \describe{
#'   \item{`[by cols]`}{Grouping columns (present only when `by` is provided).}
#'   \item{`population`}{Weighted population of the subgroup (all values,
#'     not just target-positive).}
#'   \item{`measure`}{One of `"target_within_group_share"` or
#'     `"target_survey_share"`.}
#'   \item{`value`}{The computed share (0–1); `NA` if the relevant denominator
#'     is zero.}
#' }
#' @keywords internal
.shares_target <- function(dt, by = NULL, target_variable, grp = NULL) {

  measure <- NULL

  s <- .shares_setup(dt, by = by, extra_cols = target_variable, grp = grp)

  tvec          <- as.integer(!is.na(dt[[target_variable]]) & dt[[target_variable]] == 1L)
  targ_in_group <- collapse::fsum(s$w * tvec, g = s$grp)

  within_share <- ifelse(s$cell_wpop    == 0, NA_real_, targ_in_group / s$cell_wpop)
  survey_share <- ifelse(s$denom_survey == 0, NA_real_, targ_in_group / s$denom_survey)

  id_vars <- if (!is.null(by)) c(by, "population") else "population"

  s$groups_dt[, target_within_group_share := within_share]
  s$groups_dt[, target_survey_share       := survey_share]

  result <- melt(
    s$groups_dt,
    id.vars       = id_vars,
    measure.vars  = c("target_within_group_share", "target_survey_share"),
    variable.name = "measure",
    value.name    = "value"
  )

  result[, measure := as.character(measure)]
  result
}


# ── Public interface ──────────────────────────────────────────────────────────

#' Compute population shares for a single survey (pip_id-scoped)
#'
#' Dispatches to [.shares_population()] or [.shares_target()] depending on
#' whether `target_variable` is `NULL`. All denominators are scoped to the
#' survey in `dt`; shares are never pooled across surveys.
#'
#' See [.shares_population()] and [.shares_target()] for full details on each
#' mode.
#'
#' @param dt A [data.table::data.table()] containing at minimum a numeric
#'   `weight` column and, when `target_variable` is supplied, the column named
#'   by `target_variable` (binary 0/1, validated upstream).
#' @param by Character vector of grouping column names, or `NULL` for the
#'   aggregate (one row per measure).
#' @param target_variable Optional character name of a binary (0/1) column in
#'   `dt`. When `NULL` (default), subgroup population shares are returned.
#'   When provided, within-group and survey-level target shares are returned.
#' @param grp Optional pre-computed [collapse::GRP()] built from `dt` grouped
#'   by `by`. If supplied it must match `by`. Ignored when `by = NULL`.
#'
#' @return A [data.table::data.table()] in long format. See
#'   [.shares_population()] or [.shares_target()] for the full column
#'   description depending on the mode used.
#'
#' @family compute
#' @keywords internal
compute_shares <- function(dt, by = NULL, target_variable = NULL, grp = NULL) {

  if (is.null(target_variable || target_variable == "welfare")) {
    .shares_population(dt, by = by, grp = grp)
  } else {
    .shares_target(dt, by = by, target_variable = target_variable, grp = grp)
  }
}
