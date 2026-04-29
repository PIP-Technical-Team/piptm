#' @importFrom collapse GRP
#' @importFrom data.table rbindlist
#' @importFrom cli cli_abort
NULL

# ── Measures orchestrator ─────────────────────────────────────────────────────

#' Dispatch welfare, inequality, and poverty computations across one or more surveys
#'
#' Internal orchestrator that classifies requested measures into their
#' computation families, builds a single compound [collapse::GRP()] object
#' keyed by `c("pip_id", by)` (shared across inequality and welfare family
#' functions), and dispatches to [compute_poverty()], [compute_inequality()],
#' and [compute_welfare()] as required.  Results are row-bound via
#' [data.table::rbindlist()] with `fill = TRUE`, so poverty rows carry a
#' `poverty_line` column while inequality and welfare rows receive `NA_real_`.
#'
#' `pip_id` is always included in the grouping structure so every output row
#' identifies its source survey.  A single-survey call is a degenerate batch
#' of one and behaves identically to the previous single-survey contract.
#'
#' @param dt A [data.table::data.table()] for **one or more surveys**.  Must
#'   contain at minimum `welfare`, `weight`, and `pip_id` columns, plus any
#'   columns named in `by`.
#' @param measures A non-empty character vector of measure names drawn from
#'   [pip_measures()].  Validated by [.classify_measures()].
#' @param poverty_lines A positive numeric vector of poverty line values, or
#'   `NULL`.  Required when any poverty-family measure is requested.
#' @param by A character vector of grouping column names present in `dt`, or
#'   `NULL` for the aggregate (no disaggregation).  Passed unchanged to all
#'   three family functions.
#'
#' @return A [data.table::data.table()] in **long format** with columns:
#' \describe{
#'   \item{`poverty_line`}{(numeric) Poverty threshold; `NA_real_` for
#'     non-poverty measures.}
#'   \item{`[by cols]`}{One column per element of `by` (if non-NULL).}
#'   \item{`measure`}{(character) The measure name.}
#'   \item{`value`}{(numeric) The computed statistic.}
#'   \item{`population`}{(numeric) Total weighted population in the group.}
#' }
#'
#' @family compute
#' @keywords internal
compute_measures <- function(dt, measures, poverty_lines = NULL, by = NULL) {

  # ── 1. Guard: required columns present ─────────────────────────────────────
  required     <- c("pip_id", "welfare", "weight")
  missing_cols <- setdiff(required, names(dt))
  if (length(missing_cols)) {
    cli_abort(
      c("Required column{?s} missing from {.arg dt}: {.col {missing_cols}}."),
      call = NULL
    )
  }

  # ── 2. Classify measures → families ────────────────────────────────────────
  classified <- .classify_measures(measures)
  families   <- names(classified)

  # ── 3. Validate inputs ──────────────────────────────────────────────────────
  .validate_poverty_lines(poverty_lines, families)
  .validate_by(by)

  # ── 4. Build compound grouping: pip_id × by ─────────────────────────────────
  # pip_id is always the first grouping key so every output row identifies its
  # source survey.  A single-survey call is a degenerate batch of 1 and
  # produces identical results to the previous single-survey contract.
  # The compound GRP is shared by inequality and welfare.
  # compute_poverty() must NOT receive the shared grp — it builds its own GRP
  # after its poverty_line cross-join, which changes the row count and would
  # invalidate the outer GRP object.
  batch_by <- if (!is.null(by)) c("pip_id", by) else "pip_id"
  grp      <- collapse::GRP(dt, by = batch_by)

  # ── 5. Dispatch to each active family ──────────────────────────────────────
  results <- list()

  if ("poverty" %in% families) {
    results$poverty <- compute_poverty(
      dt,
      poverty_lines = poverty_lines,
      by            = batch_by,
      measures      = classified$poverty
      # grp intentionally omitted — poverty builds its own after cross-join
    )
  }

  if ("inequality" %in% families) {
    results$inequality <- compute_inequality(
      dt,
      by       = batch_by,
      measures = classified$inequality,
      grp      = grp
    )
  }

  if ("welfare" %in% families) {
    results$welfare <- compute_welfare(
      dt,
      by       = batch_by,
      measures = classified$welfare,
      grp      = grp
    )
  }

  # ── 6. Merge — poverty rows have poverty_line; others receive NA_real_ ──────
  # rbindlist(fill=TRUE) only creates poverty_line when at least one source
  # table carries it.  When no poverty measures are requested the column never
  # materialises via fill, so we guarantee its presence explicitly.
  result <- data.table::rbindlist(results, fill = TRUE)
  if (!"poverty_line" %in% names(result)) result[, poverty_line := NA_real_]
  result
}
