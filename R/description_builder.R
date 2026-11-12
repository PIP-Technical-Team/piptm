#' @importFrom data.table data.table setorder as.data.table
NULL

# Null-coalescing operator (used internally; not exported)
`%||%` <- function(x, y) if (!is.null(x)) x else y

# ── build_description_model ───────────────────────────────────────────────────

#' Build a structured table-metadata model from table parameters
#'
#' Constructs the 6-section table-metadata document model directly from the
#' request parameters plus the in-memory release manifest and variable registry.
#' This builder performs **no** Arrow/microdata I/O and never calls
#' [table_maker()]: survey inclusion/exclusion is resolved from the manifest via
#' the shared [.partition_surveys_by_dimensions()] helper, and all variable,
#' category, and measure labels come from the release registry accessors
#' (`piptm_analysis_variables()`, `piptm_filter_categories()`,
#' `piptm_layout_covariates()`, `piptm_stat_groups()`,
#' `piptm_variable_registry()`).
#'
#' The returned model is rendered to HTML or Markdown by
#' [render_description_html()] / [render_description_markdown()].
#'
#' @param pip_id Character vector of requested survey identifiers.
#' @param analysis_var Character scalar analysis variable name.
#' @param measures Character vector of requested measure keys, in request order.
#' @param poverty_line Numeric scalar/vector poverty line(s), or `NULL`.
#' @param column,row,superColumn,superRow Character scalar layout-slot
#'   covariate names, or `NULL` for an unused slot.
#' @param filter_base Named list sample filter, or `NULL`.
#' @param ppp Integer scalar PPP reference year.
#' @param pop_share_threshold Numeric scalar cell-suppression threshold in
#'   `(0, 1)`, or `NULL` to disable suppression.
#' @param release Character scalar release ID, or `NULL` for the current release.
#'
#' @return A named list of 6 sections (`overview`, `provenance`,
#'   `surveys_selected`, `filters_applied`, `layout_configuration`,
#'   `statistics_selected`), each with `visible`, `title`, and `content` fields.
#' @export
#' @examples
#' \dontrun{
#' model <- build_description_model(
#'   pip_id = "COL_2010_GEIH_INC_ALL",
#'   analysis_var = "welfare",
#'   measures = c("mean", "gini"),
#'   column = "gender"
#' )
#' model$overview
#' model$statistics_selected
#' }
build_description_model <- function(pip_id,
                                    analysis_var = "welfare",
                                    measures = "mean",
                                    poverty_line = NULL,
                                    column = NULL,
                                    row = NULL,
                                    superColumn = NULL,
                                    superRow = NULL,
                                    filter_base = NULL,
                                    ppp = 2021L,
                                    pop_share_threshold = 0.001,
                                    release = NULL) {
  # Layout slots in visual-hierarchy order, matching flatten_table_response()'s
  # column order (superRows -> rows -> superColumns -> columns).
  slots <- list(
    superRow    = superRow,
    row         = row,
    superColumn = superColumn,
    column      = column
  )
  active_slots <- slots[!vapply(slots, is.null, logical(1))]
  by <- unname(unlist(active_slots, use.names = FALSE))
  if (length(by) == 0L) by <- NULL

  # ── Shared domain validation (identical rules to table_maker()) ────────────
  param_check <- .validate_and_normalize_table_params(
    analysis_var = analysis_var,
    measures     = measures,
    poverty_line = poverty_line,
    by           = by,
    filter_base  = filter_base,
    release      = release
  )
  normalized_filter_base <- param_check$normalized_filter_base
  filter_vars            <- param_check$filter_vars

  # ── Survey partition (identical to table_maker(); no warnings here) ─────────
  partition <- .partition_surveys_by_dimensions(
    release                = release,
    pip_id                 = pip_id,
    normalized_filter_base = normalized_filter_base,
    filter_vars            = filter_vars,
    by                     = by,
    warn                   = FALSE
  )
  resolved_release <- partition$release
  included         <- partition$included
  excluded         <- partition$excluded

  # ── Registry (best-effort; degrades to raw keys) ───────────────────────────
  registry <- tryCatch(
    piptm_variable_registry(resolved_release),
    error = function(e) NULL
  )
  analysis_var_entry <- if (is.null(registry)) NULL else registry[[analysis_var]]
  analysis_var_label <- analysis_var_entry$ui_label %||% analysis_var
  analysis_var_type  <- analysis_var_entry$tm_type %||% "unknown"
  is_monetary <- analysis_var %in% c("welfare", "pov_status")

  has_filters    <- !is.null(normalized_filter_base) &&
    length(normalized_filter_base) > 0L
  has_covariates <- !is.null(by) && length(by) > 0L

  pl_values  <- .normalize_poverty_line_values(poverty_line)
  has_poverty_line <- length(pl_values) > 0L

  model <- list(
    overview = list(
      visible = TRUE,
      title = "Table Overview",
      content = .build_overview_content(
        n_measures = length(measures),
        n_surveys = nrow(included),
        by = by,
        active_slots = active_slots,
        registry = registry,
        ppp = ppp,
        is_monetary = is_monetary,
        pop_share_threshold = pop_share_threshold
      )
    ),

    provenance = list(
      visible = TRUE,
      title = "Data Provenance",
      content = list(
        release_id = resolved_release,
        ppp_year = ppp,
        poverty_line = if (has_poverty_line) {
          .format_poverty_line_display(pl_values, include_currency = TRUE,
                                       suffix = "/day")
        } else {
          NULL
        },
        generated_at = sprintf(
          "%s UTC",
          format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "UTC", usetz = FALSE)
        )
      )
    ),

    surveys_selected = list(
      visible = TRUE,
      title = "Surveys Selected",
      content = .build_surveys_content(included, excluded, resolved_release)
    ),

    filters_applied = list(
      visible = has_filters,
      title = "Sample Base Filters",
      content = if (has_filters) {
        .build_filters_content(normalized_filter_base, resolved_release)
      } else {
        NULL
      }
    ),

    layout_configuration = list(
      visible = has_covariates,
      title = "Table Layout Configuration",
      content = if (has_covariates) {
        .build_layout_content(active_slots, poverty_line, resolved_release,
                              registry)
      } else {
        NULL
      }
    ),

    statistics_selected = list(
      visible = TRUE,
      title = "Statistics Computed",
      content = .build_statistics_content(
        analysis_var = analysis_var,
        analysis_var_label = analysis_var_label,
        analysis_var_type = analysis_var_type,
        measures = measures,
        poverty_line = poverty_line,
        ppp = ppp,
        release = resolved_release
      )
    )
  )

  return(model)
}


# ── Section content builders ──────────────────────────────────────────────────

#' Build overview section content.
#' @keywords internal
.build_overview_content <- function(n_measures, n_surveys, by, active_slots,
                                     registry, ppp, is_monetary,
                                     pop_share_threshold) {
  # Disaggregation description from active slot roles.
  covariate_desc <- if (!is.null(by) && length(by) > 0L) {
    labels <- vapply(by, function(v) {
      lbl <- if (!is.null(registry) && !is.null(registry[[v]])) {
        registry[[v]]$ui_label
      } else {
        NULL
      }
      if (identical(v, "pov_status")) {
        lbl <- lbl %||% "Poverty status"
      }
      as.character(lbl %||% v)
    }, character(1))
    paste0(", with results disaggregated by ", paste(labels, collapse = " \u00d7 "))
  } else {
    ""
  }

  structure_description <- sprintf(
    "This table reports %d %s for %d selected %s%s.",
    n_measures,
    if (n_measures == 1L) "statistic" else "statistics",
    n_surveys,
    if (n_surveys == 1L) "survey" else "surveys",
    covariate_desc
  )

  ppp_note <- if (isTRUE(is_monetary)) {
    sprintf(
      "Welfare values are expressed in %d PPP international dollars per day.",
      ppp
    )
  } else {
    NULL
  }

  # Suppression rule applies only when a layout slot is active (otherwise there
  # is a single cell and no NR masking can occur).
  suppression_note <- if (!is.null(pop_share_threshold) &&
                          !is.null(by) && length(by) > 0L) {
    sprintf(
      paste0(
        "Cells whose population share is below %s%% of the table sample base ",
        "are reported as \"NR\" (not reported)."
      ),
      format(pop_share_threshold * 100, trim = TRUE, scientific = FALSE)
    )
  } else {
    NULL
  }

  out <- list(
    structure_description = structure_description,
    weighting_note = "All calculations use survey sampling weights."
  )
  if (!is.null(ppp_note)) out$ppp_note <- ppp_note
  if (!is.null(suppression_note)) out$suppression_note <- suppression_note
  out
}


#' Build surveys section content from the manifest partition.
#' @keywords internal
.build_surveys_content <- function(included, excluded, release) {
  n_loaded <- if (is.null(included)) 0L else nrow(included)
  n_excluded <- if (is.null(excluded)) 0L else nrow(excluded)

  if (n_loaded > 0L) {
    inc <- data.table::as.data.table(included)
    country_label <- if ("country_name" %in% names(inc)) {
      cn <- as.character(inc$country_name)
      cn[is.na(cn)] <- inc$country_code[is.na(cn)]
      cn
    } else {
      as.character(inc$country_code)
    }
    loaded_list <- data.table::data.table(
      pip_id = as.character(inc$pip_id),
      country_code = as.character(inc$country_code),
      country_label = country_label,
      surveyid_year = as.integer(inc$year),
      welfare_type_label = format_welfare_type(as.character(inc$welfare_type)),
      version = as.character(inc$version)
    )
    data.table::setorder(loaded_list, pip_id)
  } else {
    loaded_list <- data.table::data.table(
      pip_id = character(0), country_code = character(0),
      country_label = character(0), surveyid_year = integer(0),
      welfare_type_label = character(0), version = character(0)
    )
  }

  excluded_list <- if (n_excluded > 0L) {
    out <- data.table::as.data.table(excluded)[, .(pip_id, reason)]
    data.table::setorder(out, pip_id)
    out
  } else {
    NULL
  }

  list(
    n_loaded = n_loaded,
    n_excluded = n_excluded,
    loaded_list = loaded_list,
    excluded_list = excluded_list
  )
}


#' Build filters section content from the normalized filter_base.
#' @keywords internal
.build_filters_content <- function(normalized_filter_base, release) {
  filter_cats <- tryCatch(
    piptm_filter_categories(release),
    error = function(e) list()
  )
  # O(1) name-indexed lookup instead of an O(n) Filter() scan per requested
  # filter variable.
  cats_by_var <- stats::setNames(
    filter_cats,
    vapply(filter_cats, function(x) as.character(x$varname %||% NA_character_),
           character(1))
  )

  rows <- lapply(names(normalized_filter_base), function(varname) {
    entry <- cats_by_var[[varname]]
    if (is.null(entry)) entry <- list(label = varname)

    subcats <- entry$subcategories
    if (is.null(subcats)) subcats <- list()
    if (length(subcats) > 0L) {
      subcat_codes  <- vapply(subcats, function(s) as.character(s$code %||% ""),
                              character(1))
      subcat_labels <- vapply(subcats,
                              function(s) s$label %||% as.character(s$code %||% ""),
                              character(1))
      # Safe [-based lookup with raw-code fallback (never unsafe [[ on an atomic
      # setNames vector -- see solutions/bugs/2026-10-13-hsize-group...).
      subcat_map <- stats::setNames(subcat_labels, subcat_codes)
    } else {
      subcat_map <- character(0)
    }

    selected_codes <- normalized_filter_base[[varname]]
    selected_labels <- vapply(selected_codes, function(code) {
      code_char <- as.character(code)
      resolved <- subcat_map[code_char]
      if (length(resolved) == 0L || is.na(resolved[[1L]]) ||
          !nzchar(resolved[[1L]])) {
        code_char
      } else {
        unname(resolved[[1L]])
      }
    }, character(1))

    data.table::data.table(
      varname = varname,
      variable = as.character(entry$label %||% varname),
      selected_categories = paste(selected_labels, collapse = ", ")
    )
  })

  filters_dt <- data.table::rbindlist(rows, use.names = TRUE, fill = TRUE)

  list(
    description = paste0(
      "The weighted sample is restricted to observations that meet all of the ",
      "following criteria:"
    ),
    filters = filters_dt
  )
}


#' Build layout section content from the active slots.
#' @keywords internal
.build_layout_content <- function(active_slots, poverty_line, release,
                                   registry) {
  layout_covs <- tryCatch(
    piptm_layout_covariates(release),
    error = function(e) list()
  )
  # O(1) name-indexed lookup instead of an O(n) Filter() scan per active slot.
  covs_by_var <- stats::setNames(
    layout_covs,
    vapply(layout_covs, function(x) as.character(x$varname %||% NA_character_),
           character(1))
  )

  rows <- list()
  for (slot_name in names(active_slots)) {
    varname <- active_slots[[slot_name]]

    # pov_status requires a valid poverty line to act as a dimension.
    if (identical(varname, "pov_status")) {
      pl <- .normalize_poverty_line_values(poverty_line)
      if (length(pl) == 0L) next
      rows[[length(rows) + 1L]] <- data.table::data.table(
        slot_label = format_slot_label(slot_name),
        varname = "pov_status",
        ui_label = "Poverty status",
        n_categories = "2: Poor, Non-poor"
      )
      next
    }

    cov_entry <- covs_by_var[[varname]]
    ui_label <- as.character((cov_entry$label %||% varname))

    cat_labels <- .resolve_covariate_category_labels(varname, registry)
    n_categories <- if (length(cat_labels) > 0L) {
      sprintf("%d: %s", length(cat_labels), paste(cat_labels, collapse = ", "))
    } else {
      count <- cov_entry$n_categories %||% NA_integer_
      if (is.na(count)) "" else as.character(as.integer(count))
    }

    rows[[length(rows) + 1L]] <- data.table::data.table(
      slot_label = format_slot_label(slot_name),
      varname = as.character(varname),
      ui_label = ui_label,
      n_categories = n_categories
    )
  }

  if (length(rows) == 0L) {
    return(NULL)
  }

  list(
    description = "Table dimensions are organized as follows:",
    layout = data.table::rbindlist(rows, use.names = TRUE, fill = TRUE)
  )
}


#' Build statistics section content from the registry.
#' @keywords internal
.build_statistics_content <- function(analysis_var, analysis_var_label,
                                       analysis_var_type, measures,
                                       poverty_line, ppp, release) {
  stat_groups <- tryCatch(
    piptm_stat_groups(release),
    error = function(e) list()
  )

  measures_dt <- data.table::data.table(
    measure = character(0), measure_label = character(0),
    stat_group = character(0)
  )
  if (length(stat_groups) > 0L) {
    group_tables <- lapply(stat_groups, function(group) {
      gm <- group$measures
      if (is.null(gm) || length(gm) == 0L) return(NULL)
      data.table::data.table(
        measure = vapply(gm, function(m) m$measure %||% "unknown", character(1)),
        measure_label = vapply(
          gm, function(m) m$label %||% (m$measure %||% "unknown"), character(1)
        ),
        stat_group = group$group %||% ""
      )
    })
    group_tables <- Filter(Negate(is.null), group_tables)
    if (length(group_tables) > 0L) {
      all_measures <- data.table::rbindlist(group_tables, use.names = TRUE,
                                            fill = TRUE)
      # Keep requested measures, in request order.
      ord <- match(measures, all_measures$measure)
      keep <- ord[!is.na(ord)]
      if (length(keep) > 0L) {
        measures_dt <- all_measures[keep]
      }
    }
  }
  # Fallback rows for any requested measure not found in the registry.
  found <- measures_dt$measure
  missing <- setdiff(measures, found)
  if (length(missing) > 0L) {
    measures_dt <- data.table::rbindlist(
      list(
        measures_dt,
        data.table::data.table(
          measure = missing,
          measure_label = missing,
          stat_group = ""
        )
      ),
      use.names = TRUE, fill = TRUE
    )
    # Re-apply request order.
    measures_dt <- measures_dt[match(measures, measures_dt$measure)]
  }

  pl <- .normalize_poverty_line_values(poverty_line)
  has_poverty <- length(pl) > 0L

  list(
    analysis_var_name = analysis_var,
    analysis_var_label = analysis_var_label,
    analysis_var_type = analysis_var_type,
    poverty_line = list(
      applicable = has_poverty,
      value = if (has_poverty) .format_poverty_line_display(pl) else NULL,
      ppp_year = if (has_poverty) ppp else NULL
    ),
    measures = measures_dt
  )
}


# ── Shared formatting helpers ─────────────────────────────────────────────────

#' Resolve category labels for a covariate from the variable registry.
#' @keywords internal
.resolve_covariate_category_labels <- function(varname, registry) {
  if (is.null(registry) || !is.list(registry)) {
    return(character(0))
  }
  entry <- registry[[varname]]
  if (is.null(entry)) {
    return(character(0))
  }
  cats <- entry$categories
  if (is.null(cats) || length(cats) == 0L) {
    return(character(0))
  }
  labels <- vapply(cats, function(cat) {
    lbl <- cat$label %||% cat$code
    if (is.null(lbl)) "" else as.character(lbl)
  }, character(1))
  labels[nzchar(labels)]
}


#' Normalize poverty-line input for safe rendering.
#' @keywords internal
.normalize_poverty_line_values <- function(poverty_line) {
  if (is.null(poverty_line)) {
    return(numeric())
  }
  if (is.list(poverty_line)) {
    poverty_line <- unlist(poverty_line, use.names = FALSE)
  }
  if (length(poverty_line) == 0L) {
    return(numeric())
  }
  poverty_line <- suppressWarnings(as.numeric(poverty_line))
  poverty_line <- poverty_line[!is.na(poverty_line)]
  poverty_line
}


#' Format one or more poverty lines for display.
#' @keywords internal
.format_poverty_line_display <- function(poverty_line, include_currency = FALSE,
                                         suffix = "") {
  stopifnot(is.numeric(poverty_line))
  if (length(poverty_line) == 0L) {
    return("")
  }
  values <- sprintf("%.2f", poverty_line)
  if (include_currency) {
    values <- paste0("$", values)
  }
  if (nzchar(suffix)) {
    values <- paste0(values, suffix)
  }
  paste(values, collapse = " / ")
}


#' Format welfare type code to label.
#'
#' @param welfare_type_code Character vector: "INC" or "CON".
#' @return Character vector: "Income" or "Consumption".
#' @keywords internal
format_welfare_type <- function(welfare_type_code) {
  welfare_type_labels <- c(INC = "Income", CON = "Consumption")
  mapped <- welfare_type_labels[welfare_type_code]
  mapped[is.na(mapped)] <- welfare_type_code[is.na(mapped)]
  return(unname(mapped))
}


#' Format a layout-slot name to its display label.
#'
#' Accepts both the singular camelCase slot names used by the metadata builder
#' (`superRow`, `row`, `superColumn`, `column`) and the plural keys used
#' elsewhere (`super_rows`/`superRows`, `rows`, `super_columns`/`superColumns`,
#' `columns`).
#'
#' @param slot Character vector of slot names.
#' @return Character vector of display labels.
#' @keywords internal
format_slot_label <- function(slot) {
  slot_labels <- c(
    superRow     = "Super-rows",
    row          = "Rows",
    superColumn  = "Super-columns",
    column       = "Columns",
    super_rows   = "Super-rows",
    rows         = "Rows",
    super_columns = "Super-columns",
    columns      = "Columns",
    superRows    = "Super-rows",
    superColumns = "Super-columns"
  )
  return(unname(slot_labels[slot]))
}
