# Table Response Serializer (Approach C)
#
# Formats flat table_maker() output into per-survey layout + cells[] structures.

#' Normalize a single category code value to a consistent comparison form
#'
#' Used on both the registry side (building `layout` categories) and the data
#' side (matching observed cell values against those categories), so a code
#' such as `1`, `1L`, or `"1"` is always treated identically regardless of
#' which side it came from. This prevents silent match failures from
#' floating-point formatting, scientific notation, or numeric-vs-character
#' representation differences between the registry and the flat data.table.
#'
#' @param code_val A single scalar code value (numeric, character, or list-
#'   wrapped singleton), or `NULL`/`NA`.
#'
#' @return A normalized scalar: `NULL` for missing/NA/the literal strings
#'   `"NA"`/`"null"`; an integer for whole numbers (numeric or numeric-string);
#'   otherwise a character string.
#' @keywords internal
.normalize_category_code <- function(code_val) {
  if (is.list(code_val)) {
    code_val <- unlist(code_val, use.names = FALSE)
  }
  if (length(code_val) > 0L) {
    code_val <- code_val[[1L]]
  } else {
    code_val <- NULL
  }

  if (is.null(code_val) || is.na(code_val) || identical(code_val, "NA") || identical(code_val, "null")) {
    NULL
  } else if (is.numeric(code_val)) {
    if (floor(code_val) == code_val) as.integer(code_val) else code_val
  } else if (is.character(code_val) && grepl("^-?[0-9]+$", trimws(code_val))) {
    as.integer(trimws(code_val))
  } else {
    as.character(code_val)
  }
}

#' Normalize a category code to its comparison-key string form
#'
#' Applies [.normalize_category_code()] and then coerces to character, giving
#' a single canonical string used for equality comparisons on both the
#' registry side and the observed-data side. `NULL` (missing/NA) normalizes
#' to `NA_character_` so both sides treat "no code" identically.
#'
#' @param code_val A single scalar code value, or `NULL`/`NA`.
#'
#' @return A length-1 character scalar, or `NA_character_` for missing/NA codes.
#' @keywords internal
.category_code_key <- function(code_val) {
  normalized <- .normalize_category_code(code_val)
  if (is.null(normalized)) NA_character_ else as.character(normalized)
}

#' Build table layout structure for /table API response
#'
#' Maps the 4 layout slots (`column`, `row`, `superColumn`, `superRow`)
#' to the API response layout structure (`columns`, `rows`, `superColumns`, `superRows`).
#'
#' @param slots Named list with keys `column`, `row`, `superColumn`, `superRow`.
#' @param release Character scalar release ID or NULL for current release.
#'
#' @return Named list with keys `columns`, `rows`, `superColumns`, `superRows`.
#'   Unfilled slots are `NULL`. Filled slots are lists with `varname`, `label`,
#'   and `categories`.
#' @keywords internal
build_table_layout <- function(slots, release = NULL) {
  slot_map <- c(
    column      = "columns",
    row         = "rows",
    superColumn = "superColumns",
    superRow    = "superRows"
  )

  layout <- stats::setNames(vector("list", 4L), unname(slot_map))

  active_slots <- vapply(names(slot_map), function(k) {
    v <- slots[[k]]
    !is.null(v) && nzchar(trimws(v))
  }, logical(1L))

  if (!any(active_slots)) {
    layout["columns"]      <- list(NULL)
    layout["rows"]         <- list(NULL)
    layout["superColumns"] <- list(NULL)
    layout["superRows"]    <- list(NULL)
    return(layout)
  }

  reg <- piptm_variable_registry(release)

  for (slot_key in names(slot_map)) {
    out_key <- slot_map[[slot_key]]
    varname <- slots[[slot_key]]

    if (is.null(varname) || !nzchar(trimws(varname))) {
      layout[out_key] <- list(NULL)
      next
    }

    varname <- trimws(varname)
    if (!varname %in% names(reg)) {
      cli::cli_abort(
        c(
          "Variable {.val {varname}} in slot {.val {slot_key}} not found in variable registry.",
          "i" = "Available variables: {.val {sort(names(reg))}}."
        )
      )
    }

    entry <- reg[[varname]]
    var_label <- as.character(entry$ui_label %||% varname)

    raw_cats <- entry$categories %||% list()

    norm_cats <- lapply(raw_cats, function(cat) {
      norm_code <- .normalize_category_code(cat$code)

      lbl_val <- cat$label
      if (is.list(lbl_val)) {
        lbl_val <- unlist(lbl_val, use.names = FALSE)
      }
      lbl_val <- if (length(lbl_val) > 0L) as.character(lbl_val[[1L]]) else ""

      list(
        code  = norm_code,
        label = lbl_val
      )
    })

    layout[[out_key]] <- list(
      varname    = varname,
      label      = var_label,
      categories = norm_cats
    )
  }

  layout
}

#' Resolve category object and sort index for a single slot value
#'
#' @param val Cell value in the data table.
#' @param slot_layout Layout list for this slot (or NULL if slot is unused).
#'
#' @return Named list with `cell_slot`, `cat_idx`, and `tie_key` (a stable,
#'   content-derived secondary sort key used to break ties between unmatched
#'   values, so cell order never depends on upstream row arrival order).
#' @keywords internal
.resolve_cell_slot <- function(val, slot_layout) {
  if (is.null(slot_layout)) {
    return(list(cell_slot = NULL, cat_idx = 0L, tie_key = ""))
  }

  cats <- slot_layout$categories
  val_norm <- .normalize_category_code(val)
  val_is_na <- is.null(val_norm)
  val_key <- .category_code_key(val)
  val_str <- if (!val_is_na) as.character(val_norm) else NA_character_

  if (is.null(cats) || length(cats) == 0L) {
    val_tie <- if (val_is_na) "" else val_str
    return(list(
      cell_slot = list(code = if (val_is_na) NULL else val_norm, label = as.character(val %||% "")),
      cat_idx = 999999L,
      tie_key = val_tie
    ))
  }

  matched_idx <- NA_integer_
  for (i in seq_along(cats)) {
    c_code <- cats[[i]]$code
    c_key <- .category_code_key(c_code)
    c_is_na <- is.na(c_key)
    if (val_is_na && c_is_na) {
      matched_idx <- i
      break
    } else if (!val_is_na && !c_is_na && identical(c_key, val_key)) {
      matched_idx <- i
      break
    }
  }

  if (!is.na(matched_idx)) {
    list(
      cell_slot = cats[[matched_idx]],
      cat_idx = matched_idx,
      tie_key = ""
    )
  } else if (val_is_na) {
    list(
      cell_slot = list(code = NULL, label = "Unknown"),
      cat_idx = length(cats) + 1L,
      tie_key = ""
    )
  } else {
    list(
      cell_slot = list(code = val_norm, label = val_str),
      cat_idx = length(cats) + 1L,
      tie_key = val_str
    )
  }
}

#' Reshape flat table_maker output into Approach C layout + cells structure
#'
#' Formats a flat [data.table::data.table()] from [format_table_output()] into
#' a list of per-survey objects containing `layout` and dense `cells[]`.
#'
#' @param dt A [data.table::data.table()] output from [format_table_output()].
#'   Must contain columns `pip_id`, `country_code`, `surveyid_year`,
#'   `welfare_type`, `measure`, and `value`, plus any variable named in
#'   `slots` (e.g. if `slots$column = "gender"`, a `gender` column must be
#'   present).
#' @param slots Named list with keys `column`, `row`, `superColumn`, `superRow`.
#'   Each value is either a single variable name (character scalar) or `NULL`
#'   for an unused slot.
#' @param analysis_var Character scalar analysis variable name. Taken from the
#'   validated request, not inferred from `dt` (the flat table has no
#'   `analysis_var` column).
#' @param poverty_line Numeric scalar poverty line, or `NULL`. Taken from the
#'   validated request; embedded verbatim as each survey's `poverty_line`
#'   field since `dt$poverty_line` (if present) may be `NA` for non-poverty
#'   measure rows.
#' @param measures Character vector of requested measure names in order. Each
#'   name becomes a key in every cell's `measures` object. A name with no
#'   matching `dt$measure` rows for a given cell yields `NA_character_` for
#'   that measure (serializes to JSON `null`) rather than an error.
#' @param release Optional character scalar release ID; passed through to
#'   [build_table_layout()] for registry lookup.
#'
#' @return A list of per-survey objects suitable for the `/table` API
#'   response. Returns `list()` (an empty list) if `dt` has zero rows,
#'   regardless of how many surveys were requested. Otherwise, one object per
#'   unique `pip_id`, each with header fields (`pip_id`, `country_code`,
#'   `surveyid_year`, `welfare_type`, `poverty_line`, `analysis_var`,
#'   `measures`), a `layout` block, and a `cells` array. If no slot in `slots`
#'   is filled, every survey's `cells` contains exactly one cell with
#'   `superRow`, `row`, `superColumn`, and `column` all `NULL`.
#'
#' @section Errors:
#' Aborts via [cli::cli_abort()] if: `dt` is not a `data.table`; `slots` is
#' not a list containing all four required keys; `measures` is not a
#' non-empty character vector free of `NA`/empty values; `analysis_var` is
#' not a single non-empty character value; `poverty_line` is not `NULL` or a
#' single finite numeric value; `dt` is missing a required column; or `dt` is
#' missing the column named by an active slot. Also propagates any
#' registry-resolution error from [build_table_layout()] (e.g. an unknown
#' slot variable or unresolvable `release`) without substituting a fallback
#' registry. Warns (but does not abort) via [cli::cli_warn()] if a requested
#' measure has no matching rows anywhere in `dt`; every cell then reports
#' `NA`/`null` for that measure rather than a computed value.
#'
#' @family api
#' @export
#' @examples
#' \dontrun{
#' # Requires a loaded release registry (e.g. via activate_test_registry() in
#' # tests, or a real manifest in production) so "gender" resolves to a
#' # variable entry.
#' dt <- data.table::data.table(
#'   pip_id = "COL_2010_ECH_INC_ALL",
#'   country_code = "COL",
#'   surveyid_year = 2010L,
#'   welfare_type = "INC",
#'   gender = c("male", "female"),
#'   poverty_line = NA_real_,
#'   measure = "mean",
#'   value = c("10.00", "9.00")
#' )
#' serialize_table_response(
#'   dt = dt,
#'   slots = list(column = "gender", row = NULL, superColumn = NULL, superRow = NULL),
#'   analysis_var = "welfare",
#'   poverty_line = NULL,
#'   measures = "mean"
#' )
#' }
serialize_table_response <- function(dt,
                                     slots,
                                     analysis_var,
                                     poverty_line,
                                     measures,
                                     release = NULL) {
  if (!data.table::is.data.table(dt)) {
    cli::cli_abort("{.arg dt} must be a data.table.")
  }

  slot_names <- c("column", "row", "superColumn", "superRow")
  if (!is.list(slots) || !all(slot_names %in% names(slots))) {
    cli::cli_abort("{.arg slots} must be a list containing: {.val {slot_names}}.")
  }

  if (!is.character(measures) || length(measures) == 0L || anyNA(measures) || any(!nzchar(measures))) {
    cli::cli_abort("{.arg measures} must be a non-empty character vector with no missing or empty values.")
  }

  if (!is.character(analysis_var) || length(analysis_var) != 1L || is.na(analysis_var) || !nzchar(analysis_var)) {
    cli::cli_abort("{.arg analysis_var} must be a single non-empty character value.")
  }

  if (!is.null(poverty_line)) {
    poverty_line_num <- suppressWarnings(as.numeric(poverty_line))
    if (length(poverty_line_num) != 1L || is.na(poverty_line_num)) {
      cli::cli_abort("{.arg poverty_line} must be a single numeric value or NULL.")
    }
  }

  # Build layout block
  layout <- build_table_layout(slots, release = release)

  if (nrow(dt) == 0L) {
    return(list())
  }

  # Active slots
  active_slots <- Filter(
    function(k) !is.null(slots[[k]]) && nzchar(trimws(slots[[k]])),
    slot_names
  )

  # Check required columns
  meta_cols <- c("pip_id", "country_code", "surveyid_year", "welfare_type")
  req_cols <- c(meta_cols, "measure", "value")
  missing_req <- setdiff(req_cols, names(dt))
  if (length(missing_req) > 0L) {
    cli::cli_abort(
      c(
        "{.arg dt} is missing required column{?s}: {.val {missing_req}}."
      )
    )
  }

  for (k in active_slots) {
    varname <- trimws(slots[[k]])
    if (!varname %in% names(dt)) {
      cli::cli_abort(
        c(
          "Slot {.val {k}} variable {.val {varname}} is missing from {.arg dt}."
        )
      )
    }
  }

  unmatched_measures <- setdiff(measures, unique(dt$measure))
  if (length(unmatched_measures) > 0L) {
    cli::cli_warn(
      c(
        "Requested measure{?s} {.val {unmatched_measures}} not found anywhere in {.arg dt}.",
        "i" = "Every cell will report {.code null} for {cli::qty(length(unmatched_measures))} th{?is/ese} measure{?s} rather than a computed value."
      )
    )
  }

  unique_pips <- unique(dt$pip_id)
  out_surveys <- vector("list", length(unique_pips))
  # Single grouped split by pip_id, computed once, instead of rescanning dt
  # per survey (`dt[pip_id == pid]` in a loop).
  survey_groups <- dt[, list(.rows = list(.I)), by = "pip_id"]

  for (s_idx in seq_along(unique_pips)) {
    pid <- unique_pips[[s_idx]]
    sub_dt <- dt[survey_groups$.rows[[s_idx]]]

    first_row <- sub_dt[1L]
    survey_pl <- if (!is.null(poverty_line)) poverty_line_num else NULL

    survey_obj <- list(
      pip_id        = as.character(pid),
      country_code  = as.character(first_row$country_code),
      surveyid_year = as.integer(first_row$surveyid_year),
      welfare_type  = as.character(first_row$welfare_type),
      poverty_line  = survey_pl,
      analysis_var  = as.character(analysis_var),
      measures      = I(as.character(measures)),
      layout        = layout,
      cells         = list()
    )

    if (length(active_slots) == 0L) {
      # No slots: single cell with all 4 slots NULL
      meas_obj <- stats::setNames(vector("list", length(measures)), measures)
      for (m in measures) {
        val_m <- sub_dt[measure == m, value]
        meas_obj[[m]] <- if (length(val_m) > 0L && !is.na(val_m[[1L]])) val_m[[1L]] else NA_character_
      }

      single_cell <- stats::setNames(vector("list", 5L), c("superRow", "row", "superColumn", "column", "measures"))
      single_cell["superRow"]    <- list(NULL)
      single_cell["row"]         <- list(NULL)
      single_cell["superColumn"] <- list(NULL)
      single_cell["column"]      <- list(NULL)
      single_cell["measures"]    <- list(meas_obj)

      survey_obj$cells <- list(single_cell)
      out_surveys[[s_idx]] <- survey_obj
      next
    }

    # Active slots: identify unique observed dimension combinations and their
    # matching row indices in a single grouped pass (one O(n) scan via
    # data.table's by=), rather than rescanning the full survey subset once
    # per cell (the previous O(n_cells * nrow(sub_dt)) boolean-mask pattern).
    # data.table groups preserve first-occurrence order, so `grouped`'s row
    # order matches what `unique()` on the same columns would have produced.
    dim_varnames <- unname(vapply(active_slots, function(k) trimws(slots[[k]]), character(1L)))
    grouped <- sub_dt[, list(.rows = list(.I)), by = dim_varnames]
    unique_cells_dt <- grouped[, dim_varnames, with = FALSE]

    # For each cell in unique_cells_dt, resolve slot categories and sort keys
    n_cells <- nrow(unique_cells_dt)
    cell_list <- vector("list", n_cells)
    sort_keys <- data.frame(
      superRow_rank    = integer(n_cells),
      row_rank         = integer(n_cells),
      superColumn_rank = integer(n_cells),
      column_rank      = integer(n_cells),
      superRow_tie     = character(n_cells),
      row_tie          = character(n_cells),
      superColumn_tie  = character(n_cells),
      column_tie       = character(n_cells),
      stringsAsFactors = FALSE
    )

    for (c_idx in seq_len(n_cells)) {
      c_row <- unique_cells_dt[c_idx]

      sr_val <- if ("superRow" %in% active_slots) c_row[[trimws(slots$superRow)]] else NULL
      r_val  <- if ("row" %in% active_slots) c_row[[trimws(slots$row)]] else NULL
      sc_val <- if ("superColumn" %in% active_slots) c_row[[trimws(slots$superColumn)]] else NULL
      col_val<- if ("column" %in% active_slots) c_row[[trimws(slots$column)]] else NULL

      sr_res  <- .resolve_cell_slot(sr_val, layout$superRows)
      r_res   <- .resolve_cell_slot(r_val, layout$rows)
      sc_res  <- .resolve_cell_slot(sc_val, layout$superColumns)
      col_res <- .resolve_cell_slot(col_val, layout$columns)

      sort_keys$superRow_rank[[c_idx]]    <- sr_res$cat_idx
      sort_keys$row_rank[[c_idx]]         <- r_res$cat_idx
      sort_keys$superColumn_rank[[c_idx]] <- sc_res$cat_idx
      sort_keys$column_rank[[c_idx]]      <- col_res$cat_idx
      sort_keys$superRow_tie[[c_idx]]     <- sr_res$tie_key
      sort_keys$row_tie[[c_idx]]          <- r_res$tie_key
      sort_keys$superColumn_tie[[c_idx]]  <- sc_res$tie_key
      sort_keys$column_tie[[c_idx]]       <- col_res$tie_key

      # Rows for this cell were already grouped in the single O(n) pass above;
      # no per-cell rescan of sub_dt is needed.
      cell_sub <- sub_dt[grouped$.rows[[c_idx]]]

      meas_obj <- stats::setNames(vector("list", length(measures)), measures)
      for (m in measures) {
        val_m <- cell_sub[measure == m, value]
        meas_obj[[m]] <- if (length(val_m) > 0L && !is.na(val_m[[1L]])) val_m[[1L]] else NA_character_
      }

      cell_obj <- stats::setNames(vector("list", 5L), c("superRow", "row", "superColumn", "column", "measures"))
      cell_obj["superRow"]    <- list(sr_res$cell_slot)
      cell_obj["row"]         <- list(r_res$cell_slot)
      cell_obj["superColumn"] <- list(sc_res$cell_slot)
      cell_obj["column"]      <- list(col_res$cell_slot)
      cell_obj["measures"]    <- list(meas_obj)

      cell_list[[c_idx]] <- cell_obj
    }

    # Sort cells per R10: superRows, then rows, then superColumns, then columns.
    # Each rank is paired with a content-derived tie-break key (the raw
    # unmatched value's string form) so cells with tied ranks (e.g. multiple
    # distinct out-of-registry values) sort deterministically by content
    # rather than falling back to upstream row-arrival order.
    cell_order <- order(
      sort_keys$superRow_rank,    sort_keys$superRow_tie,
      sort_keys$row_rank,         sort_keys$row_tie,
      sort_keys$superColumn_rank, sort_keys$superColumn_tie,
      sort_keys$column_rank,      sort_keys$column_tie
    )

    survey_obj$cells <- cell_list[cell_order]
    out_surveys[[s_idx]] <- survey_obj
  }

  out_surveys
}

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
#' `layout` and `cells`), missing required survey/layout/cell fields, or if
#' multiple surveys in `data` do not share identical active slot variable names
#' and measure names.
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

  ref_slot_keys <- NULL
  ref_dim_cols  <- NULL
  ref_measures  <- NULL

  survey_tables <- vector("list", length(data))

  for (s_idx in seq_along(data)) {
    s <- data[[s_idx]]
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

    active_cell_slots <- character(0L)
    active_varnames   <- character(0L)
    for (cell_slot in names(layout_slot_map)) {
      l_key <- layout_slot_map[[cell_slot]]
      slot_obj <- layout[[l_key]]
      if (!is.null(slot_obj)) {
        if (!is.list(slot_obj) || is.null(slot_obj$varname) ||
            !nzchar(trimws(as.character(slot_obj$varname)[1L]))) {
          cli::cli_abort(
            "Active layout slot {.val {l_key}} must contain a non-empty `varname`."
          )
        }
        active_cell_slots <- c(active_cell_slots, cell_slot)
        active_varnames   <- c(active_varnames, trimws(as.character(slot_obj$varname)[1L]))
      }
    }
    active_cell_slots <- unname(active_cell_slots)
    active_varnames   <- unname(active_varnames)

    measures_vec <- unname(as.character(unlist(s$measures, use.names = FALSE)))
    if (length(measures_vec) == 0L || anyNA(measures_vec) || any(!nzchar(measures_vec))) {
      cli::cli_abort("Survey `measures` must be a non-empty array of measure names.")
    }

    if (s_idx == 1L) {
      ref_slot_keys <- active_cell_slots
      ref_dim_cols  <- active_varnames
      ref_measures  <- measures_vec
    } else {
      if (!identical(active_varnames, ref_dim_cols) || !identical(active_cell_slots, ref_slot_keys)) {
        cli::cli_abort(
          "All surveys in `data` must share identical active layout slot variables."
        )
      }
      if (!identical(measures_vec, ref_measures)) {
        cli::cli_abort(
          "All surveys in `data` must share identical `measures`."
        )
      }
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
    pl_raw <- s$poverty_line
    pl_val <- if (is.null(pl_raw) || length(pl_raw) == 0L || is.na(pl_raw[[1L]])) {
      NA_real_
    } else {
      suppressWarnings(as.numeric(pl_raw[[1L]]))
    }

    cols_list <- list(
      pip_id        = rep(as.character(s$pip_id[[1L]]), n_cells),
      country_code  = rep(as.character(s$country_code[[1L]]), n_cells),
      surveyid_year = rep(as.integer(s$surveyid_year[[1L]]), n_cells),
      welfare_type  = rep(as.character(s$welfare_type[[1L]]), n_cells),
      analysis_var  = rep(as.character(s$analysis_var[[1L]]), n_cells),
      poverty_line  = rep(pl_val, n_cells)
    )

    for (i in seq_along(active_cell_slots)) {
      c_slot <- active_cell_slots[[i]]
      v_name <- active_varnames[[i]]
      cols_list[[v_name]] <- vapply(cells, function(cell) {
        slot_val <- cell[[c_slot]]
        if (is.null(slot_val) || !is.list(slot_val) || is.null(slot_val$label) ||
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

    survey_tables[[s_idx]] <- data.table::as.data.table(cols_list)
  }

  data.table::rbindlist(survey_tables, use.names = TRUE, fill = FALSE)
}
