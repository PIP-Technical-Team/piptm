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
