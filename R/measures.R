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
  # Welfare family
  mean        = "welfare",
  median      = "welfare",
  sd          = "welfare",
  var         = "welfare",
  min         = "welfare",
  max         = "welfare",
  nobs        = "welfare",
  p10         = "welfare",
  p25         = "welfare",
  p75         = "welfare",
  p90         = "welfare"
)

#' Valid disaggregation dimensions
#'
#' @keywords internal
.VALID_DIMENSIONS <- c("gender", "area", "educat4", "educat5", "educat7", "age")

#' Education dimension columns — at most one may be requested per call
#'
#' @keywords internal
.EDUCATION_DIMS <- c("educat4", "educat5", "educat7")

#' Age bin levels in ascending order
#'
#' @keywords internal
.AGE_BIN_LEVELS <- c("0-14", "15-24", "25-64", "65+")

# ── Exported helpers ──────────────────────────────────────────────────────────

#' List all valid measure names
#'
#' Returns a named character vector of all canonical measure names recognised
#' by the computation engine.  Names are the measure identifiers; values are
#' the corresponding computation family (`"poverty"`, `"inequality"`, or
#' `"welfare"`).  Useful for user discovery and validation.
#'
#' @return A named character vector of length 18.
#'
#' @family measures
#'
#' @examples
#' pip_measures()
#'
#' @export
pip_measures <- function() {
  unlist(.MEASURE_REGISTRY, use.names = TRUE)
}

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

# ── Internal validators ───────────────────────────────────────────────────────

#' Classify requested measures by computation family
#'
#' Validates that every element of `measures` exists in `.MEASURE_REGISTRY`
#' and returns a named list of character vectors grouped by family.  Only
#' families with at least one requested measure appear in the result.  The
#' output list is always ordered: `poverty` → `inequality` → `welfare`.
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
  canonical <- c("poverty", "inequality", "welfare")
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
.validate_by <- function(by, dimensions = NULL) {
  if (is.null(by)) return(invisible(NULL))

  if (!is.character(by) || length(by) == 0L) {
    cli_abort(
      "{.arg by} must be {.code NULL} or a non-empty character vector.",
      call = NULL
    )
  }

  unknown_dims <- setdiff(by, .VALID_DIMENSIONS)
  if (length(unknown_dims) > 0L) {
    valid_dims <- .VALID_DIMENSIONS
    cli_abort(
      c(
        "Unknown dimension{?s}: {.val {unknown_dims}}.",
        i = "Valid dimensions: {.val {valid_dims}}."
      ),
      call = NULL
    )
  }

  edu_requested <- intersect(by, .EDUCATION_DIMS)
  if (length(edu_requested) > 1L) {
    cli_abort(
      c(
        "At most one education dimension may be requested; \\
         got {length(edu_requested)}: {.val {edu_requested}}."
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
