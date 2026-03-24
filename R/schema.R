#' Canonical Arrow schema definition for PIP survey Parquet files
#'
#' Returns the single source of truth for the schema contract between
#' \{pipdata\} (write path) and \{piptm\} (validation path). Both packages
#' derive their type constraints and column lists from this function.
#'
#' The returned list has two top-level elements:
#' - `$fields`: named list of field specs, each with `$type` (Arrow type
#'   object) and `$required` (logical).
#' - `$levels`: named list of allowed character vectors for dictionary columns.
#'
#' @return A named list describing the canonical schema.
#' @export
#' @family schema
#' @examples
#' s <- pip_arrow_schema()
#' s$fields$welfare$type
#' s$levels$gender
pip_arrow_schema <- function() {
  dict_type <- arrow::dictionary(arrow::int32(), arrow::utf8())

  list(
    fields = list(
      country_code   = list(type = arrow::utf8(),     required = TRUE),
      surveyid_year  = list(type = arrow::int32(),    required = TRUE),
      welfare_type   = list(type = arrow::utf8(),     required = TRUE),
      survey_id      = list(type = arrow::utf8(),     required = TRUE),
      survey_acronym = list(type = arrow::utf8(),     required = TRUE),
      welfare        = list(type = arrow::float64(),  required = TRUE),
      weight         = list(type = arrow::float64(),  required = TRUE),
      gender         = list(type = dict_type,         required = FALSE),
      area           = list(type = dict_type,         required = FALSE),
      education      = list(type = dict_type,         required = FALSE),
      age            = list(type = arrow::int32(),    required = FALSE)
    ),
    levels = list(
      gender    = c("male", "female"),
      area      = c("urban", "rural"),
      education = c("No education", "Primary", "Secondary", "Tertiary"),
      welfare_type = c("INC", "CON")
    )
  )
}

#' Extract required column names from the canonical schema
#'
#' @return Character vector of required column names.
#' @export
#' @family schema
pip_required_cols <- function() {
  s <- pip_arrow_schema()
  names(Filter(function(f) isTRUE(f$required), s$fields))
}

#' Extract all allowed column names from the canonical schema
#'
#' @return Character vector of all column names (required + optional).
#' @export
#' @family schema
pip_allowed_cols <- function() {
  names(pip_arrow_schema()$fields)
}