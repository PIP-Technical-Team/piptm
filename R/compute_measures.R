#' @importFrom collapse GRP
#' @importFrom data.table uniqueN rbindlist
#' @importFrom cli cli_abort
NULL

# ── Measures orchestrator ─────────────────────────────────────────────────────

#' Dispatch welfare, inequality, and poverty computations for a single survey
#'
#' Internal orchestrator that classifies requested measures into their
#' computation families, builds a single [collapse::GRP()] object (shared
#' across all family functions), and dispatches to [compute_poverty()],
#' [compute_inequality()], and [compute_welfare()] as required.  Results are
#' row-bound via [data.table::rbindlist()] with `fill = TRUE`, so poverty rows
#' carry a `poverty_line` column while inequality and welfare rows receive
#' `NA_real_` in that column.
#'
#' **This function must never receive multi-survey data.**  It asserts
#' `uniqueN(dt$pip_id) == 1L` and aborts with a clear programming-error
#' message otherwise.  The caller ([table_maker()]) is responsible for
#' splitting the batch result of [load_surveys()] by `pip_id` before passing
#' slices here.
#'
#' @param dt A [data.table::data.table()] for **exactly one survey** — one
#'   unique value in the `pip_id` column.  Must contain at minimum `welfare`,
#'   `weight`, and `pip_id` columns, plus any columns named in `by`.
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
#' @keywords internal
compute_measures <- function(dt, measures, poverty_lines = NULL, by = NULL) {

  # ── 1. Guard: single-survey slice only ─────────────────────────────────────
  # This is a programming error in the caller — not a user-facing validation.
  n_surveys <- data.table::uniqueN(dt[["pip_id"]])
  if (n_surveys != 1L) {
    found <- unique(dt[["pip_id"]])
    cli_abort(
      c(
        "{.fn compute_measures} received data for {n_surveys} surveys.",
        i = "Pass a single-survey slice — exactly one unique {.col pip_id}.",
        x = "Found: {.val {found}}."
      ),
      call = NULL
    )
  }

  # ── 2. Classify measures → families ────────────────────────────────────────
  classified <- .classify_measures(measures)
  families   <- names(classified)

  # ── 3. Validate poverty lines ───────────────────────────────────────────────
  .validate_poverty_lines(poverty_lines, families)

  # ── 4. Pre-compute single GRP — shared across all family dispatches ─────────
  # Family functions accept an optional `grp` argument and skip their own
  # GRP() call when it is provided, avoiding redundant grouping computation.
  if (!is.null(by)) {
    grp <- collapse::GRP(dt, by = by)
  } else {
    grp <- NULL
  }

  # ── 5. Dispatch to each active family ──────────────────────────────────────
  results <- list()

  if ("poverty" %in% families) {
    results$poverty <- compute_poverty(
      dt,
      poverty_lines = poverty_lines,
      by            = by,
      measures      = classified$poverty,
      grp           = grp
    )
  }

  if ("inequality" %in% families) {
    results$inequality <- compute_inequality(
      dt,
      by       = by,
      measures = classified$inequality,
      grp      = grp
    )
  }

  if ("welfare" %in% families) {
    results$welfare <- compute_welfare(
      dt,
      by       = by,
      measures = classified$welfare,
      grp      = grp
    )
  }

  # ── 6. Merge — poverty rows have poverty_line; others receive NA_real_ ──────
  data.table::rbindlist(results, fill = TRUE)
}
