#' @importFrom cli cli_abort cli_warn
#' @importFrom data.table fcase setattr
NULL

# ── Measure Registry ──────────────────────────────────────────────────────────

#' Internal measure registry: maps canonical measure names to computation
#' families.
#'
#' @keywords internal
.MEASURE_REGISTRY <- list(
  # Poverty family — all require poverty_lines
  headcount   = "poverty",
  poverty_gap = "poverty",
  severity    = "poverty",
  watts       = "poverty",
  pop_poverty = "poverty",
  # Inequality family
  gini        = "inequality",
  mld         = "inequality",
  # Summary stats family
  mean        = "summary_stats",
  median      = "summary_stats",
  sd          = "summary_stats",
  var         = "summary_stats",
  min         = "summary_stats",
  max         = "summary_stats",
  nobs        = "summary_stats",
  p10         = "summary_stats",
  p25         = "summary_stats",
  p75         = "summary_stats",
  p90         = "summary_stats",
  sum         = "summary_stats",
  # Shares  family
    # Shares family
  pop_share                 = "shares",
  target_within_group_share = "shares",
  target_survey_share       = "shares"
)

# ── Exported helpers ──────────────────────────────────────────────────────────

#' Age bin labels used by the computation engine
#'
#' Returns the ordered character vector of age bin labels applied when
#' `"age"` is included as a disaggregation dimension.  The bins are:
#' 0–14, 15–24, 25–64, 65+.
#'
#' @return A character vector of four age bin labels in ascending order.
#'
#' @family measures
#'
#' @examples
#' pip_age_bins()
#'
#' @export
pip_age_bins <- function() {
  .AGE_BIN_LEVELS
}

#' Valid disaggregation dimension names
#'
#' Returns the character vector of disaggregation dimension names accepted by
#' [table_maker()] and the API.  Use this instead of accessing the internal
#' constant `piptm:::.VALID_DIMENSIONS` directly (internal constants may be
#' renamed without notice).
#'
#' @return A character vector of valid dimension names.
#'
#' @family measures
#'
#' @examples
#' pip_valid_dimensions()
#'
#' @export
pip_valid_dimensions <- function() {
  #.VALID_DIMENSIONS
  pip_optional_dims()
}


# ── Internal validators ───────────────────────────────────────────────────────

#' Classify requested measures by computation family
#'
#' Validates that every element of `measures` exists in `.MEASURE_REGISTRY`
#' and returns a named list of character vectors grouped by family.  Only
#' families with at least one requested measure appear in the result.  The
#' output list is always ordered: `poverty` → `inequality` → `summary_stats` → `shares`.
#'
#' @param measures A non-empty character vector of measure names.
#'
#' @return A named list of character vectors, one per active family.
#'
#' @keywords internal
.classify_measures <- function(measures) {
  if (!is.character(measures) || length(measures) == 0L) {
    cli_abort(
      "{.arg measures} must be a non-empty character vector.",
      call = NULL
    )
  }

  unknown <- setdiff(measures, names(.MEASURE_REGISTRY))
  if (length(unknown) > 0L) {
    cli_abort(
      c(
        "Unknown measure{?s}: {.val {unknown}}.",
        i = "Valid measures: {.val {names(.MEASURE_REGISTRY)}}."
      ),
      call = NULL
    )
  }

  families <- unlist(.MEASURE_REGISTRY[measures], use.names = FALSE)
  out      <- split(measures, families)

  # Return in canonical family order (only active families included)
  canonical <- c("poverty", "inequality", "summary_stats", "shares")
  out[intersect(canonical, names(out))]
}

#' Validate the `by` disaggregation argument
#'
#' Checks that:
#' \itemize{
#'   \item `by` is `NULL` or a non-empty character vector
#'   \item All elements are in the allowed set: `gender`, `area`, `educat4`,
#'     `educat5`, `educat7`, `age`
#'   \item At most one education column (`educat4`, `educat5`, `educat7`) is
#'     requested
#'   \item At most 4 dimensions are requested
#' }
#'
#' When `dimensions` is provided (from a manifest entry), a `cli_warn()` is
#' issued for any requested dimension absent from that survey.  The function
#' does not error in that case — the caller decides whether to skip the survey.
#'
#' @param by Character vector of requested dimension names, or `NULL`.
#' @param dimensions Character vector of dimensions available for a specific
#'   survey, or `NULL` to skip availability checking.
#'
#' @return `by` invisibly (validated, unchanged).
#'
#' @keywords internal
.validate_by <- function(by, dimensions = NULL, release = NULL) {
  if (is.null(by)) return(invisible(NULL))

  if (!is.character(by) || length(by) == 0L) {
    cli_abort(
      "{.arg by} must be {.code NULL} or a non-empty character vector.",
      call = NULL
    )
  }

  covariates <- piptm_layout_covariates(release = release)
  valid_dims <- unique(vapply(covariates, `[[`, character(1), "varname"))

  if (length(valid_dims) == 0L) {
    cli_abort(
      "Could not derive valid dimensions from {.fn piptm_layout_covariates}.",
      call = NULL
    )
  }

  unknown_dims <- setdiff(by, valid_dims)
  if (length(unknown_dims) > 0L) {
    cli_abort(
      c(
        "Unknown dimension{?s}: {.val {unknown_dims}}.",
        i = "Valid dimensions: {.val {valid_dims}}."
      ),
      call = NULL
    )
  }


  if (length(by) > 4L) {
    cli_abort(
      c(
        "At most 4 dimensions may be requested; \\
         got {length(by)}: {.val {by}}."
      ),
      call = NULL
    )
  }

  if (!is.null(dimensions)) {
    missing_dims <- setdiff(by, dimensions)
    if (length(missing_dims) > 0L) {
      cli_warn(
        c(
          "Requested dimension{?s} not available for this survey: \\
           {.val {missing_dims}}.",
          i = "Available dimensions: {.val {dimensions}}."
        ),
        call = NULL
      )
    }
  }

  invisible(by)
}

#' Validate poverty lines when poverty measures are requested
#'
#' Errors if any poverty-family measure is requested but `poverty_lines` is
#' `NULL`, empty, non-numeric, or contains non-positive / non-finite values.
#' Passes silently when no poverty measures are requested.
#'
#' @param poverty_lines Numeric vector of poverty lines, or `NULL`.
#' @param families Character vector of active computation families (names of
#'   the list returned by `.classify_measures()`).
#'
#' @return `poverty_lines` invisibly (validated, unchanged).
#'
#' @keywords internal
.validate_poverty_lines <- function(poverty_lines, families) {
  if (!"poverty" %in% families) return(invisible(poverty_lines))

  if (is.null(poverty_lines) || length(poverty_lines) == 0L) {
    cli_abort(
      c(
        "Poverty measures require {.arg poverty_lines}.",
        i = "Provide a numeric vector of one or more positive poverty lines."
      ),
      call = NULL
    )
  }

  if (!is.numeric(poverty_lines)) {
    cli_abort(
      paste0(
        "{.arg poverty_lines} must be a numeric vector, ",
        "not {.cls {class(poverty_lines)}}."
      ),
      call = NULL
    )
  }

  bad <- !is.finite(poverty_lines) | poverty_lines <= 0
  if (any(bad)) {
    bad_vals <- poverty_lines[bad]
    cli_abort(
      c(
        "{.arg poverty_lines} must contain only positive finite values.",
        x = "Problematic values: {.val {bad_vals}}."
      ),
      call = NULL
    )
  }

  invisible(poverty_lines)
}

# ── Age binning ───────────────────────────────────────────────────────────────

#' Bin the `age` column into four age groups (modifies `dt` in place)
#'
#' Creates an ordered factor column `age_group` using
#' [data.table::fcase()] and then removes the original `age` column.
#' Rows with `NA` age receive `NA` in `age_group` and are retained in the
#' data (they appear as an NA group in cross-tabulations).
#'
#' Bin boundaries:
#' \itemize{
#'   \item 0–14
#'   \item 15–24
#'   \item 25–64
#'   \item 65+
#' }
#'
#' @param dt A [data.table::data.table()] containing an integer `age` column.
#'   Modified **in place** by reference.
#'
#' @return `dt` invisibly (modified in place).
#'
#' @keywords internal
.bin_age <- function(dt) {
  age       <- NULL  # suppress R CMD check NOTE for NSE column reference
  age_group <- NULL  # suppress R CMD check NOTE for NSE column reference

  dt[, age_group := factor(
    fcase(
      age >= 0L  & age <= 14L, "0-14",
      age >= 15L & age <= 24L, "15-24",
      age >= 25L & age <= 64L, "25-64",
      age >= 65L,              "65+",
      default = NA_character_
    ),
    levels  = .AGE_BIN_LEVELS,
    ordered = TRUE
  )]

  dt[, age := NULL]

  invisible(dt)
}
