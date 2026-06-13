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
  p90         = "welfare",
  sum         = "welfare",
  obs_share   = "welfare",
  pop_share   = "welfare"
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
  .VALID_DIMENSIONS
}

#' Full catalogue of analysis variables for the Table Maker Step 2 UI
#'
#' @description
#' Returns the static catalogue of all analysis variables available in the
#' Table Maker UI Step 2 (analysis variable dropdown).  For each variable,
#' the catalogue encodes its type, human-readable label, available statistics,
#' and whether the poverty line slider should be rendered.
#'
#' The catalogue is fully static and always returns the complete universe of
#' analysis variables regardless of which surveys were selected in Step 1.
#' Whether a specific variable is computable for a given survey is handled by
#' the backend at query time.
#'
#' @details
#' Analysis variable types:
#'
#' \describe{
#'   \item{`"welfare"`}{The welfare aggregate; computed via welfare family
#'     statistics (mean, median, distributional quantiles, etc.)}
#'   \item{`"poverty"`}{Poverty status relative to a poverty line; requires
#'     the poverty line slider.}
#'   \item{`"inequality"`}{Inequality of the welfare distribution (Gini, MLD).}
#'   \item{`"continuous"`}{A continuous variable (currently: age); uses the
#'     same statistics as the welfare type.}
#'   \item{`"binary"`}{A binary indicator derived from a source column (e.g.
#'     primary education completion from `educat7`, infrastructure access from
#'     dedicated columns, employment from `lstatus`).  Available statistics are
#'     population share and observation share only.}
#' }
#'
#' `pop_share` and `obs_share` appear exclusively in the `binary` type stats
#' list.  They are not meaningful in a welfare or continuous variable context.
#'
#' The values in this catalogue have been verified against the Colombia 2010
#' survey data (COL_2010) and the GLD harmonisation codebook.  Re-verify
#' against those sources if changes are needed.
#'
#' @return A list of 12 entries in canonical display order
#'   (welfare \eqn{\to} poverty \eqn{\to} inequality \eqn{\to} age
#'    \eqn{\to} education binaries \eqn{\to} infrastructure binaries
#'    \eqn{\to} labour binary).
#'   Each entry is a named list with five fields:
#'   \describe{
#'     \item{`varname`}{Character scalar: identifier used by the backend.}
#'     \item{`label`}{Character scalar: human-readable name shown in the UI
#'       dropdown.}
#'     \item{`type`}{Character scalar: one of `"welfare"`, `"poverty"`,
#'       `"inequality"`, `"continuous"`, `"binary"`.}
#'     \item{`poverty_line_slider`}{Logical scalar: `TRUE` only for the
#'       `"poverty"` type; tells the UI whether to render the poverty line
#'       slider.}
#'     \item{`available_stats`}{List of named lists, each with:
#'       `measure` — character scalar backend measure identifier — and
#'       `label` — character scalar human-readable label for the UI.}
#'   }
#'
#' @family measures
#'
#' @examples
#' m <- pip_tablemaker_measures()
#' length(m)                              # 12 analysis variables
#' m[[1L]]$varname                        # "welfare"
#' m[[2L]]$poverty_line_slider            # TRUE  (poverty type only)
#' m[[1L]]$available_stats[[1L]]$measure  # "mean"
#'
#' @export
pip_tablemaker_measures <- function() {
  # ── Stat-type lists (local; reused across entries of the same type) ─────────

  welfare_stats <- list(
    list(measure = "mean",   label = "Mean"),
    list(measure = "median", label = "Median"),
    list(measure = "sd",     label = "Standard deviation"),
    list(measure = "var",    label = "Variance"),
    list(measure = "min",    label = "Minimum"),
    list(measure = "max",    label = "Maximum"),
    list(measure = "sum",    label = "Sum"),
    list(measure = "p10",    label = "10th percentile"),
    list(measure = "p25",    label = "25th percentile"),
    list(measure = "p75",    label = "75th percentile"),
    list(measure = "p90",    label = "90th percentile")
  )

  poverty_stats <- list(
    list(measure = "headcount",   label = "Poverty rate"),
    list(measure = "poverty_gap", label = "Poverty gap"),
    list(measure = "severity",    label = "Poverty severity"),
    list(measure = "watts",       label = "Watts index"),
    list(measure = "pop_poverty", label = "Poor population")
  )

  inequality_stats <- list(
    list(measure = "gini", label = "Gini index"),
    list(measure = "mld",  label = "Mean log deviation")
  )

  # Continuous variables share the same statistics as welfare — assign by
  # reference to avoid duplication.
  continuous_stats <- welfare_stats

  # pop_share and obs_share appear exclusively here; they must not appear in
  # any other stats list (not meaningful for welfare or continuous variables).
  binary_stats <- list(
    list(measure = "pop_share", label = "Share of population"),
    list(measure = "obs_share", label = "Share of observations")
  )

  # ── Catalogue (12 entries in canonical display order) ──────────────────────
  list(
    # ── Welfare ──────────────────────────────────────────────────────────────
    list(
      varname             = "welfare",
      label               = "Welfare",
      type                = "welfare",
      poverty_line_slider = FALSE,
      available_stats     = welfare_stats
    ),
    # ── Poverty ──────────────────────────────────────────────────────────────
    list(
      varname             = "poverty",
      label               = "Poverty status",
      type                = "poverty",
      poverty_line_slider = TRUE,
      available_stats     = poverty_stats
    ),
    # ── Inequality ────────────────────────────────────────────────────────────
    list(
      varname             = "inequality",
      label               = "Inequality",
      type                = "inequality",
      poverty_line_slider = FALSE,
      available_stats     = inequality_stats
    ),
    # ── Continuous ────────────────────────────────────────────────────────────
    list(
      varname             = "age",
      label               = "Age",
      type                = "continuous",
      poverty_line_slider = FALSE,
      available_stats     = continuous_stats
    ),
    # ── Binary — education (source variable: educat7; verified vs COL_2010) ──
    list(
      varname             = "primary_completed",
      label               = "Primary education completed",
      type                = "binary",
      poverty_line_slider = FALSE,
      available_stats     = binary_stats
    ),
    list(
      varname             = "secondary_completed",
      label               = "Secondary education completed",
      type                = "binary",
      poverty_line_slider = FALSE,
      available_stats     = binary_stats
    ),
    list(
      varname             = "higher_than_secondary",
      label               = "Higher than secondary education",
      type                = "binary",
      poverty_line_slider = FALSE,
      available_stats     = binary_stats
    ),
    list(
      varname             = "university",
      label               = "University education",
      type                = "binary",
      poverty_line_slider = FALSE,
      available_stats     = binary_stats
    ),
    # ── Binary — infrastructure (source variables: imp_wat_rec, imp_san_rec,
    #                             electricity) ─────────────────────────────────
    list(
      varname             = "imp_wat_rec",
      label               = "Access to improved water",
      type                = "binary",
      poverty_line_slider = FALSE,
      available_stats     = binary_stats
    ),
    list(
      varname             = "imp_san_rec",
      label               = "Access to improved sanitation",
      type                = "binary",
      poverty_line_slider = FALSE,
      available_stats     = binary_stats
    ),
    list(
      varname             = "electricity",
      label               = "Access to electricity",
      type                = "binary",
      poverty_line_slider = FALSE,
      available_stats     = binary_stats
    ),
    # ── Binary — labour (source variable: lstatus) ────────────────────────────
    list(
      varname             = "employed",
      label               = "Employed",
      type                = "binary",
      poverty_line_slider = FALSE,
      available_stats     = binary_stats
    )
  )
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
