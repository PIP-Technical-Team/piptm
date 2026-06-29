#' Canonical Arrow schema definition for PIP survey Parquet files
#'
#' Returns the single source of truth for the schema contract between
#' \{pipdata\} (write path) and \{piptm\} (validation path). Both packages
#' derive their type constraints and column lists from this function.
#'
#' Optional fields are controlled by the `piptm.optional_vars` R option,
#' initialised in `.onLoad()`. When that option is `NULL`, no optional fields
#' are included and the schema contains only the 6 required base columns.
#' When set to a character vector of variable names, only those variables are
#' eligible for inclusion as optional fields.
#'
#' The returned list has two top-level elements:
#' - `$fields`: named list of field specs, each with `$type` (Arrow type
#'   object) and `$required` (logical).
#' - `$levels`: named list of allowed character vectors for fixed-level
#'   dictionary columns. Populated from the variable registry when available.
#'
#' @return A named list describing the canonical schema.
#' @importFrom arrow utf8 int32 float64
#' @export
#' @family schema
#' @examples
#' s <- pip_arrow_schema()
#' s$fields$version$type
#' s$levels$welfare_type
pip_arrow_schema <- function() {

  # --- Required base fields (always present, never affected by option) -------
  fields <- list(
    country_code  = list(type = arrow::utf8(),    required = TRUE),
    surveyid_year = list(type = arrow::int32(),   required = TRUE),
    welfare_type  = list(type = arrow::utf8(),    required = TRUE),
    version       = list(type = arrow::utf8(),    required = TRUE),
    pip_id        = list(type = arrow::utf8(),    required = TRUE),
    weight        = list(type = arrow::float64(), required = TRUE)
  )

  levels <- list(welfare_type = c("INC", "CON"))

  # --- Read allowlist option -------------------------------------------------
  # When NULL: no optional fields admitted — return required fields only.
  optional_vars <- getOption("piptm.optional_vars")

  if (is.null(optional_vars)) {
    return(list(fields = fields, levels = levels))
  }

  if (!is.character(optional_vars) || length(optional_vars) == 0L) {
    cli::cli_abort(
      c(
        "Option {.code piptm.optional_vars} must be a non-empty character vector or NULL.",
        "i" = "Set it with {.code options(piptm.optional_vars = c('gender', 'area', ...))}."
      )
    )
  }

  # --- Resolve registry for current release ----------------------------------
  piptm_env  <- if (exists(".piptm_env", inherits = TRUE)) get(".piptm_env", inherits = TRUE) else NULL
  registries <- if (is.null(piptm_env)) NULL else piptm_env$registries
  registry   <- NULL

  if (!is.null(registries) && length(registries) > 0L) {
    release <- tryCatch(piptm_current_release(), error = function(e) NULL)
    if (!is.null(release) && release %in% names(registries)) {
      registry <- registries[[release]]
    } else {
      registry <- registries[[1L]]
    }
  }

  # --- Build optional fields from allowlist ----------------------------------
  # Continuous vars declared as float64 — everything else defaults to int32.
  continuous_vars <- c("age", "hsize")
  special_names   <- c("welfare", "pov_status", "weight")
  required_names  <- names(fields)

  for (varname in optional_vars) {
    # Skip vars that clash with required or special names
    if (varname %in% c(required_names, special_names)) next

    arrow_type        <- if (varname %in% continuous_vars) arrow::float64() else arrow::int32()
    fields[[varname]] <- list(type = arrow_type, required = FALSE)

    if (!is.null(registry)) {
      reg_entry <- registry[[varname]]
      if (!is.null(reg_entry) &&
          !is.null(reg_entry$categories) &&
          length(reg_entry$categories) > 0L) {
        cat_codes         <- vapply(reg_entry$categories, function(e) as.character(e$code), character(1L))
        levels[[varname]] <- cat_codes
      }
    }
  }

  list(fields = fields, levels = levels)
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
#' survey-specific and enumerated via [piptm::pip_welfare_schema()]).
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


#' Extract optional dimension column names from the canonical base schema
#'
#' Returns the names of all non-required fields in [pip_arrow_schema()].
#' This is the single authoritative list of columns that are eligible to be
#' written to Parquet as breakdown dimensions and recorded in the release
#' manifest. Both `{pipdata}` and `{piptm}` derive their dimension lists from
#' this function so they stay in sync automatically as the schema evolves.
#'
#' The set of optional dimensions is controlled by the `piptm.optional_vars`
#' R option (initialised in `.onLoad()`). When that option is `NULL`, this
#' function returns an empty character vector.
#'
#' @return Character vector of optional field names in schema definition order.
#' @export
#' @family schema
#' @examples
#' pip_optional_dims()
pip_optional_dims <- function() {
  s <- pip_arrow_schema()
  names(Filter(function(f) !isTRUE(f$required), s$fields))
}


#' Extract all allowed column names from the canonical schema
#'
#' Returns the base columns (required + optional breakdown dimensions).
#' Pass `welfare_vars` to append the survey-specific welfare columns for a
#' particular Parquet file.
#'
#' @param welfare_vars Optional character vector of welfare column names (e.g.
#'   from a manifest entry's `welfare_vars` field). When `NULL` (default)
#'   only the base columns are returned.
#'
#' @return Character vector of allowed column names.
#' @export
#' @family schema
#' @examples
#' pip_allowed_cols()
#' pip_allowed_cols(c("welfare_lcu", "welfare_ppp_2017_01_02"))
pip_allowed_cols <- function(welfare_vars = NULL) {
  base <- names(pip_arrow_schema()$fields)
  if (!is.null(welfare_vars)) c(base, welfare_vars) else base
}
