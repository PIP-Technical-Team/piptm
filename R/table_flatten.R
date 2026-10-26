# Table Download Flattener (Approach C -> Shape A)
#
# Stateless, registry-free flattening of Approach C `/table` response objects
# (per-survey `layout` + `cells[]`) into a rectangular Shape A data.table for
# `POST /table/download` CSV/RDS export.
#
# Deliberately kept in its own module, separate from table_response.R, which
# owns the registry-backed outbound /table serialization path
# (build_table_layout(), serialize_table_response(), etc.). This flattener
# must remain strictly registry- and Arrow-free: it only re-shapes a JSON
# payload the client already received from /table.

#' Flatten an Approach C `/table` response into a Shape A data.table
#'
#' Statelessly converts a list of per-survey Approach C objects (as returned by
#' [serialize_table_response()] or parsed from the `/table` JSON response with
#' `simplifyVector = FALSE`) into a rectangular [data.table::data.table()]
#' suitable for CSV or RDS export by `POST /table/download`.
#'
#' Each element of a survey's `cells` array becomes one row in the returned
#' table, with columns ordered as:
#' \enumerate{
#'   \item Six leading survey header columns (always present): `pip_id`
#'     (character), `country_code` (character), `surveyid_year` (integer),
#'     `welfare_type` (character), `analysis_var` (character), and
#'     `poverty_line` (numeric; `NA_real_` when `NULL`).
#'   \item Active dimension columns in visual-hierarchy order
#'     (`superRows` \eqn{\rightarrow} `rows` \eqn{\rightarrow} `superColumns`
#'     \eqn{\rightarrow} `columns`), each named after `layout[[slot]]$varname`
#'     (e.g. `educat4`, `area`, `gender`) and populated with the cell's
#'     category `label` (character). Unused slots (`NULL` in `layout`) are
#'     omitted; a national table (all slots `NULL`) emits zero dimension
#'     columns.
#'   \item One character column per measure in the survey's `measures` order,
#'     preserving formatted display strings verbatim (`"4.50"`, `"NR"`, or
#'     `NA_character_` when `NULL`). Measure values are never coerced to
#'     numeric.
#' }
#'
#' @param data A non-empty list of per-survey Approach C objects (each carrying
#'   header fields, `measures`, `layout`, and `cells`).
#'
#' @return A [data.table::data.table()] with one row per cell across all
#'   surveys in `data`.
#'
#' @section Errors:
#' Aborts via [cli::cli_abort()] if `data` is `NULL`, empty, not a list of
#' Approach C survey objects (for example, legacy flat-row objects lacking
#' `layout` and `cells`), missing required survey/layout/cell fields, if
#' active layout slot `varname` values or `measures` names are duplicated or
#' collide with each other or with the reserved header columns, if survey
#' header scalar fields are missing/malformed, or if multiple surveys in
#' `data` do not share identical active slot variable names and measure names.
#'
#' @family api
#' @export
#' @examples
#' survey <- list(
#'   pip_id = "COL_2010_ECH_INC_ALL",
#'   country_code = "COL",
#'   surveyid_year = 2010L,
#'   welfare_type = "INC",
#'   poverty_line = NULL,
#'   analysis_var = "welfare",
#'   measures = list("mean"),
#'   layout = list(
#'     columns = list(
#'       varname = "gender",
#'       label = "Gender",
#'       categories = list(
#'         list(code = 0L, label = "Female"),
#'         list(code = 1L, label = "Male")
#'       )
#'     ),
#'     rows = NULL,
#'     superColumns = NULL,
#'     superRows = NULL
#'   ),
#'   cells = list(
#'     list(
#'       superRow = NULL,
#'       row = NULL,
#'       superColumn = NULL,
#'       column = list(code = 0L, label = "Female"),
#'       measures = list(mean = "4.50")
#'     ),
#'     list(
#'       superRow = NULL,
#'       row = NULL,
#'       superColumn = NULL,
#'       column = list(code = 1L, label = "Male"),
#'       measures = list(mean = "5.15")
#'     )
#'   )
#' )
#' flatten_table_response(list(survey))
flatten_table_response <- function(data) {
  if (is.null(data) || !is.list(data) || is.data.frame(data) || length(data) == 0L) {
    cli::cli_abort(
      "`data` must be a non-empty array of Approach C survey objects (with `layout` and `cells`)."
    )
  }

  ref_slot_keys <- NULL
  ref_dim_cols  <- NULL
  ref_measures  <- NULL

  survey_tables <- vector("list", length(data))

  for (s_idx in seq_along(data)) {
    spec <- .flatten_single_survey(data[[s_idx]])

    if (s_idx == 1L) {
      ref_slot_keys <- spec$active_cell_slots
      ref_dim_cols  <- spec$active_varnames
      ref_measures  <- spec$measures_vec
    } else {
      if (!identical(spec$active_varnames, ref_dim_cols) ||
          !identical(spec$active_cell_slots, ref_slot_keys)) {
        cli::cli_abort(
          "All surveys in `data` must share identical active layout slot variables."
        )
      }
      if (!identical(spec$measures_vec, ref_measures)) {
        cli::cli_abort(
          "All surveys in `data` must share identical `measures`."
        )
      }
    }

    survey_tables[[s_idx]] <- spec$table
  }

  data.table::rbindlist(survey_tables, use.names = FALSE, fill = FALSE)
}

#' Validate and flatten a single Approach C survey object into Shape A columns
#'
#' Internal helper for [flatten_table_response()]. Validates one survey
#' object's schema (required keys, `layout` slots, `cells` shape, scalar
#' header fields, and column-name collisions) and builds its per-survey
#' [data.table::data.table()].
#'
#' @param s A single Approach C survey object (one element of `data` passed to
#'   [flatten_table_response()]).
#'
#' @return A list with elements `table` (a `data.table` for this survey),
#'   `active_cell_slots` (character vector of active layout slot keys, in
#'   visual-hierarchy order), `active_varnames` (character vector of the
#'   corresponding `varname` values), and `measures_vec` (character vector of
#'   measure names) — the latter three are used by [flatten_table_response()]
#'   to enforce cross-survey schema consistency.
#' @keywords internal
.flatten_single_survey <- function(s) {
  req_survey_keys <- c(
    "pip_id", "country_code", "surveyid_year",
    "welfare_type", "analysis_var", "measures", "layout", "cells"
  )
  layout_slot_map <- c(
    superRow    = "superRows",
    row         = "rows",
    superColumn = "superColumns",
    column      = "columns"
  )
  req_layout_keys <- unname(layout_slot_map)
  req_cell_keys <- c("superRow", "row", "superColumn", "column", "measures")

  if (!is.list(s) || is.data.frame(s) || !all(req_survey_keys %in% names(s))) {
    cli::cli_abort(
      c(
        "Each element of `data` must be an Approach C survey object containing: {.val {req_survey_keys}}.",
        "i" = "Legacy flat-row payloads are not supported by `/table/download`."
      )
    )
  }

  layout <- s$layout
  if (!is.list(layout) || is.data.frame(layout) || !all(req_layout_keys %in% names(layout))) {
    cli::cli_abort(
      "Survey `layout` must be an object containing keys: {.val {req_layout_keys}}."
    )
  }

  slot_names_vec <- names(layout_slot_map)
  is_active <- logical(length(slot_names_vec))
  varnames_tmp <- character(length(slot_names_vec))
  for (k_idx in seq_along(slot_names_vec)) {
    l_key <- layout_slot_map[[k_idx]]
    slot_obj <- layout[[l_key]]
    if (!is.null(slot_obj) && length(slot_obj) > 0L) {
      if (!is.list(slot_obj) || is.null(slot_obj$varname) ||
          !nzchar(trimws(as.character(slot_obj$varname)[1L]))) {
        cli::cli_abort(
          "Active layout slot {.val {l_key}} must contain a non-empty `varname`."
        )
      }
      is_active[[k_idx]] <- TRUE
      varnames_tmp[[k_idx]] <- trimws(as.character(slot_obj$varname)[1L])
    }
  }
  active_cell_slots <- unname(slot_names_vec[is_active])
  active_varnames   <- unname(varnames_tmp[is_active])

  measures_vec <- unname(as.character(unlist(s$measures, use.names = FALSE)))
  if (length(measures_vec) == 0L || anyNA(measures_vec) || any(!nzchar(measures_vec))) {
    cli::cli_abort("Survey `measures` must be a non-empty array of measure names.")
  }

  cells <- s$cells
  if (!is.list(cells) || is.data.frame(cells) || length(cells) == 0L) {
    cli::cli_abort("Survey `cells` must be a non-empty array of cell objects.")
  }

  for (c_idx in seq_along(cells)) {
    cell <- cells[[c_idx]]
    if (!is.list(cell) || is.data.frame(cell) || !all(req_cell_keys %in% names(cell)) ||
        !is.list(cell$measures)) {
      cli::cli_abort(
        "Each element of `cells` must be an object containing: {.val {req_cell_keys}}."
      )
    }
  }

  n_cells <- length(cells)

  # Validate scalar survey header fields before indexing [[1L]]. JSON
  # parsed with simplifyVector = FALSE yields a present-but-NULL/empty
  # value for `{"field": null}`, which would otherwise throw a raw,
  # uninformative base-R indexing error ("attempt to select less than one
  # element in get1index") instead of a clear validation message.
  .scalar_chr <- function(x, field) {
    if (is.null(x) || length(x) == 0L || is.na(x[[1L]]) ||
        !nzchar(trimws(as.character(x[[1L]])))) {
      cli::cli_abort(
        "Survey {.field {field}} must be a non-empty scalar string."
      )
    }
    trimws(as.character(x[[1L]]))
  }
  pip_id_val       <- .scalar_chr(s$pip_id, "pip_id")
  country_code_val <- .scalar_chr(s$country_code, "country_code")
  welfare_type_val <- .scalar_chr(s$welfare_type, "welfare_type")
  analysis_var_val <- .scalar_chr(s$analysis_var, "analysis_var")

  year_raw <- s$surveyid_year
  year_val <- if (is.null(year_raw) || length(year_raw) == 0L) {
    NA_integer_
  } else {
    suppressWarnings(as.integer(year_raw[[1L]]))
  }
  if (is.na(year_val)) {
    cli::cli_abort(
      "Survey {.field surveyid_year} must be a scalar integer-coercible value."
    )
  }

  pl_raw <- s$poverty_line
  pl_val <- if (is.null(pl_raw) || length(pl_raw) == 0L || is.na(pl_raw[[1L]])) {
    NA_real_
  } else {
    suppressWarnings(as.numeric(pl_raw[[1L]]))
  }
  if (!is.null(pl_raw) && length(pl_raw) > 0L && !is.na(pl_raw[[1L]]) && is.na(pl_val)) {
    cli::cli_abort(
      "Survey {.field poverty_line} must be a finite numeric value when present."
    )
  }

  header_cols <- c(
    "pip_id", "country_code", "surveyid_year",
    "welfare_type", "analysis_var", "poverty_line"
  )
  if (anyDuplicated(active_varnames) > 0L) {
    cli::cli_abort("Active layout slot `varname` values must be unique.")
  }
  if (anyDuplicated(measures_vec) > 0L) {
    cli::cli_abort("Survey `measures` names must be unique.")
  }
  if (length(intersect(active_varnames, header_cols)) > 0L ||
      length(intersect(measures_vec, header_cols)) > 0L ||
      length(intersect(active_varnames, measures_vec)) > 0L) {
    cli::cli_abort(
      "Active layout slot `varname` values and `measures` names must not collide with each other or with the reserved header columns: {.val {header_cols}}."
    )
  }

  cols_list <- list(
    pip_id        = rep(pip_id_val, n_cells),
    country_code  = rep(country_code_val, n_cells),
    surveyid_year = rep(year_val, n_cells),
    welfare_type  = rep(welfare_type_val, n_cells),
    analysis_var  = rep(analysis_var_val, n_cells),
    poverty_line  = rep(pl_val, n_cells)
  )

  for (i in seq_along(active_cell_slots)) {
    c_slot <- active_cell_slots[[i]]
    v_name <- active_varnames[[i]]
    cols_list[[v_name]] <- vapply(cells, function(cell) {
      slot_val <- cell[[c_slot]]
      if (!is.null(slot_val) && !is.list(slot_val)) {
        cli::cli_abort(
          "Cell active slot {.val {c_slot}} must be a `{{code, label}}` object or `null`."
        )
      }
      if (is.null(slot_val) || is.null(slot_val$label) ||
          length(slot_val$label) == 0L || is.na(slot_val$label[[1L]])) {
        NA_character_
      } else {
        as.character(slot_val$label[[1L]])
      }
    }, character(1L))
  }

  for (m in measures_vec) {
    cols_list[[m]] <- vapply(cells, function(cell) {
      m_val <- cell$measures[[m]]
      if (is.null(m_val) || length(m_val) == 0L || is.na(m_val[[1L]])) {
        NA_character_
      } else {
        as.character(m_val[[1L]])
      }
    }, character(1L))
  }

  list(
    table             = data.table::setDT(cols_list),
    active_cell_slots = active_cell_slots,
    active_varnames   = active_varnames,
    measures_vec      = measures_vec
  )
}
