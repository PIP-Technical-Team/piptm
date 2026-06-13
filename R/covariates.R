# ---------------------------------------------------------------------------
# pip_tablemaker_covariates()
# ---------------------------------------------------------------------------

#' Return the static catalogue of layout covariates for the Table Maker UI
#'
#' Returns a list serialisable to JSON describing all covariates available
#' for the Decision 3 layout panel in Table Maker Step 2. The user drags
#' covariates from this list into one of four layout slots (Columns, Rows,
#' Super Columns, Super Rows) to define the shape of the output table.
#'
#' The catalogue is built by transforming the output of
#' [pip_tablemaker_categories()] — retaining only `varname`, `label`, and
#' deriving `n_categories` from the subcategory count. This ensures the
#' covariate list is always consistent with the sample filter catalogue in
#' Decision 1 without duplicating variable definitions.
#'
#' The only hardcoded entry is `pov_status` (Poverty status), which is
#' specific to Decision 3 and does not appear in the Decision 1 filter
#' catalogue. Poverty status is subject to a mutual exclusivity rule: if
#' selected as an analysis variable in Decision 2, it must be disabled as
#' a layout covariate in Decision 3, and vice versa. This rule is enforced
#' by the UI using the `varname` field — the backend does not enforce it.
#'
#' This endpoint is static — it always returns the complete universe of
#' covariates regardless of which surveys the user selected in Step 1.
#' Whether a given covariate is actually present in a specific survey is
#' handled by the backend at query time, not by this endpoint.
#'
#' @return A list of named lists, each with three fields:
#'   \describe{
#'     \item{`varname`}{Character scalar. Column name as it exists in the
#'       data at query time (derived column name for binned variables such
#'       as `age_group` and `hsize_group`).}
#'     \item{`label`}{Character scalar. Human-readable covariate name
#'       displayed in the UI covariate panel.}
#'     \item{`n_categories`}{Integer scalar. Number of categories this
#'       covariate produces in the table layout. Displayed as a small count
#'       next to the covariate name when assigned to a slot.}
#'   }
#' @seealso [pip_tablemaker_categories()] for the Decision 1 filter
#'   catalogue from which this list is derived.
#'   [pip_tablemaker_measures()] for the Decision 2 analysis variable
#'   catalogue.
#' @family tablemaker
#' @export
#' @examples
#' \dontrun{
#' covariates <- pip_tablemaker_covariates()
#' length(covariates)
#' sapply(covariates, `[[`, "varname")
#' }
pip_tablemaker_covariates <- function() {

  # ---------------------------------------------------------------------------
  # Step 1: derive covariate entries from pip_tablemaker_categories()
  # Each entry keeps varname and label, and computes n_categories from the
  # length of the subcategories list. This ensures consistency with the
  # Decision 1 filter catalogue without duplicating variable definitions.
  # ---------------------------------------------------------------------------
  cats <- pip_tablemaker_categories()

  derived <- lapply(cats, function(entry) {
    list(
      varname      = entry$varname,
      label        = entry$label,
      n_categories = length(entry$subcategories)
    )
  })

  # ---------------------------------------------------------------------------
  # Step 2: build the pov_status entry
  # Poverty status is specific to Decision 3 — it does not appear in the
  # Decision 1 filter catalogue. It produces a binary poor / non-poor split
  # (internally: pov_status == TRUE for poor, FALSE for non-poor).
  # It is placed first in the list because it is the only covariate unique
  # to this decision and carries the special mutual exclusivity rule with
  # the Decision 2 poverty analysis variable.
  # ---------------------------------------------------------------------------
  pov_status_entry <- list(
    varname      = "pov_status",
    label        = "Poverty status",
    n_categories = 2L
  )

  # ---------------------------------------------------------------------------
  # Step 3: assemble final list — pov_status first, then all derived entries
  # ---------------------------------------------------------------------------
  c(list(pov_status_entry), derived)
}
