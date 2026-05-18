#' Canonical Arrow schema definition for PIP survey Parquet files
#'
#' Returns the single source of truth for the schema contract between
#' \{pipdata\} (write path) and \{piptm\} (validation path). Both packages
#' derive their type constraints and column lists from this function.
#'
#' The returned list has two top-level elements:
#' - `$fields`: named list of field specs, each with `$type` (Arrow type
#'   object) and `$required` (logical).
#' - `$levels`: named list of allowed character vectors for fixed-level
#'   dictionary columns (`gender`, `area`, `welfare_type`). Education
#'   columns (`educat4`, `educat5`, `educat7`) have survey-specific levels
#'   and are not included here.
#'
#' @return A named list describing the canonical schema.
#' @importFrom arrow utf8 int32 float64 dictionary
#' @export
#' @family schema
#' @examples
#' s <- pip_arrow_schema()
#' s$fields$version$type
#' s$levels$gender
pip_arrow_schema <- function() {
  dict_type <- arrow::dictionary(arrow::int32(), arrow::utf8())

  list(
    fields = list(
      country_code   = list(type = arrow::utf8(),     required = TRUE),
      surveyid_year  = list(type = arrow::int32(),    required = TRUE),
      welfare_type   = list(type = arrow::utf8(),     required = TRUE),
      version        = list(type = arrow::utf8(),     required = TRUE),
      pip_id         = list(type = arrow::utf8(),     required = TRUE),
      weight         = list(type = arrow::float64(),  required = TRUE),
      gender         = list(type = dict_type,         required = FALSE),
      area           = list(type = dict_type,         required = FALSE),
      educat4        = list(type = dict_type,         required = FALSE),
      educat5        = list(type = dict_type,         required = FALSE),
      educat7        = list(type = dict_type,         required = FALSE),
      # age: continuous int32 — NOT a dictionary column (per schema rules)
      age            = list(type = arrow::int32(),    required = FALSE)
    ),
    levels = list(
      gender       = c("male", "female"),
      area         = c("urban", "rural"),
      welfare_type = c("INC", "CON")
    )
  )
}

#' Build welfare field specs for a set of welfare column names
#'
#' Returns a named list of field specs (each with `$type` = `float64()` and
#' `$required = TRUE`) for every column name in `welfare_vars`. This is the
#' companion to [pip_arrow_schema()] for the dynamic welfare columns written
#' by `{pipdata}` — `welfare_lcu` and one or more `welfare_ppp_*` variants.
#'
#' @param welfare_vars Character vector of welfare column names, e.g.
#'   `c("welfare_lcu", "welfare_ppp_2017_01_02")`. Must be non-empty.
#'
#' @return A named list of field specs (`$type`, `$required`).
#' @importFrom arrow float64
#' @export
#' @family schema
#' @examples
#' wf <- pip_welfare_schema(c("welfare_lcu", "welfare_ppp_2017_01_02"))
#' wf[["welfare_ppp_2017_01_02"]]$type
pip_welfare_schema <- function(welfare_vars) {
  if (!is.character(welfare_vars) || length(welfare_vars) == 0L) {
    cli::cli_abort(
      "{.arg welfare_vars} must be a non-empty character vector."
    )
  }
  field_spec <- list(type = arrow::float64(), required = TRUE)
  stats::setNames(rep(list(field_spec), length(welfare_vars)), welfare_vars)
}

#' Extract required column names from the canonical base schema
#'
#' Returns the 6 fixed required columns (no welfare columns — those are
#' survey-specific and enumerated via [pip_welfare_schema()]).
#'
#' @return Character vector of 6 required column names:
#'   `country_code`, `surveyid_year`, `welfare_type`, `version`,
#'   `pip_id`, `weight`.
#' @export
#' @family schema
pip_required_cols <- function() {
  s <- pip_arrow_schema()
  names(Filter(function(f) isTRUE(f$required), s$fields))
}

#' Extract all allowed column names from the canonical schema
#'
#' Returns the 12 base columns (required + optional breakdown dimensions).
#' Pass `welfare_vars` to append the survey-specific welfare columns for a
#' particular Parquet file.
#'
#' @param welfare_vars Optional character vector of welfare column names (e.g.
#'   from a manifest entry's `welfare_vars` field). When `NULL` (default)
#'   only the 12 base columns are returned.
#'
#' @return Character vector of allowed column names.
#' @export
#' @family schema
#' @examples
#' pip_allowed_cols()  # 12 base columns
#' pip_allowed_cols(c("welfare_lcu", "welfare_ppp_2017_01_02"))  # 14 columns
pip_allowed_cols <- function(welfare_vars = NULL) {
  base <- names(pip_arrow_schema()$fields)
  if (!is.null(welfare_vars)) c(base, welfare_vars) else base
}