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
#' category, and measure labels come from the release registry loaded once via
#' `piptm_variable_registry()` and projected into role-specific slices
#' (`.registry_filter_categories()`, `.registry_layout_covariates()`,
#' `.registry_stat_groups()`).
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
#' @return A named list of 6 sections (`provenance`,
#'   `surveys_selected`, `filters_applied`, `layout_configuration`,
#'   `statistics_selected`, `notes`), each with `visible`, `title`, and `content` fields.
#'   For identical inputs, the only run-to-run varying field is
#'   `provenance$content$generated_at`. In `statistics_selected`,
#'   `poverty_line$value` is formatted display text and
#'   `poverty_line$value_raw` stores the raw numeric value(s).
#' @export
#' @examples
#' \dontrun{
#' model <- build_description_model(
#'   pip_id = "COL_2010_GEIH_INC_ALL",
#'   analysis_var = "welfare",
#'   measures = c("mean", "gini"),
#'   column = "gender"
#' )
#' model$provenance
#' model$statistics_selected
#' model$notes
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
    column       = column,
    row          = row,
    superColumn  = superColumn,
    superRow     = superRow,
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
  analysis_var_label <- .coerce_scalar_chr(
    if (is.null(analysis_var_entry)) NULL else analysis_var_entry$ui_label,
    default = analysis_var
  )
  analysis_var_type <- .coerce_scalar_chr(
    if (is.null(analysis_var_entry)) NULL else analysis_var_entry$tm_type,
    default = "unknown"
  )
  is_monetary <- analysis_var %in% c("welfare", "pov_status")

  # Derive all role-specific registry slices once per request to avoid repeated
  # release resolution and full-registry scans.
  filter_cats <- .registry_filter_categories(registry)
  layout_covs <- .registry_layout_covariates(registry)
  stat_groups <- .registry_stat_groups(registry)

  has_filters    <- !is.null(normalized_filter_base) &&
    length(normalized_filter_base) > 0L
  has_covariates <- !is.null(by) && length(by) > 0L

  now_utc <- Sys.time()
  citation_year <- format(now_utc, "%Y", tz = "UTC")
  accessed_date <- format(now_utc, "%Y-%m-%d", tz = "UTC")

  model <- list(
    provenance = list(
      visible = TRUE,
      title = "Data Provenance",
      content = list(
        release_id = resolved_release,
        ppp_year = ppp,
        generated_at = sprintf(
          "%s UTC",
          format(now_utc, "%Y-%m-%d %H:%M:%S", tz = "UTC", usetz = FALSE)
        ),
        citation = sprintf(
          "World Bank (%s), Poverty and Inequality Platform (version %s) [data set]. pip.worldbank.org. Accessed on %s",
          citation_year,
          resolved_release,
          accessed_date
        )
      )
    ),

    surveys_selected = list(
      visible = TRUE,
      title = "Surveys Selected",
      content = .build_surveys_content(included, excluded)
    ),

    filters_applied = list(
        visible = has_filters,
        title = "Sample Base Filters",
        content = if (has_filters) {
          .build_filters_content(normalized_filter_base, filter_cats)
        } else {
          NULL
        }
    ),

    layout_configuration = list(
        visible = has_covariates,
        title = "Table Layout Configuration",
        content = if (has_covariates) {
          .build_layout_content(active_slots, poverty_line, registry,
                                layout_covs)
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
        stat_groups = stat_groups
      )
    ),

    notes = list(
      visible = TRUE,
      title = "Notes",
      content = .build_notes_content(
        by = by,
        ppp = ppp,
        is_monetary = is_monetary,
        pop_share_threshold = pop_share_threshold
      )
    )
  )

  return(model)
}


# ── Section content builders ──────────────────────────────────────────────────

#' Build notes section content.
#'
#' @param by Character vector of active disaggregation covariates, or `NULL`.
#' @param ppp Integer scalar PPP reference year.
#' @param is_monetary Logical scalar indicating whether the analysis variable is monetary.
#' @param pop_share_threshold Numeric scalar cell-suppression threshold, or `NULL`.
#' @return Named list of note strings (`version_note`, and
#'   optional `ppp_note` and `suppression_note`).
#' @keywords internal
.build_notes_content <- function(by, ppp, is_monetary, pop_share_threshold) {
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
    version_note = paste0(
      "Survey versions report the master version (vermast, the original ",
      "survey vintage in the Datalibweb master collection) and the ",
      "alternative version (veralt, the harmonized adaptation built on ",
      "that master survey)."
    )
  )
  if (!is.null(ppp_note)) out$ppp_note <- ppp_note
  if (!is.null(suppression_note)) out$suppression_note <- suppression_note
  out
}


#' Build surveys section content from the manifest partition.
#' @keywords internal
.build_surveys_content <- function(included, excluded, release = NULL) {
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
    raw_ver <- as.character(inc$version)
    raw_ver[is.na(raw_ver)] <- ""
    has_sep <- grepl("_", raw_ver, fixed = TRUE)
    vermast <- sub("_.*$", "", raw_ver)
    veralt  <- data.table::fifelse(has_sep, sub("^[^_]*_", "", raw_ver), "")

    loaded_list <- data.table::data.table(
      pip_id = as.character(inc$pip_id),
      country_code = as.character(inc$country_code),
      country_label = country_label,
      surveyid_year = as.integer(inc$year),
      welfare_type_label = format_welfare_type(as.character(inc$welfare_type)),
      vermast = vermast,
      veralt = veralt
    )
    data.table::setorder(loaded_list, pip_id)
  } else {
    loaded_list <- data.table::data.table(
      pip_id = character(0), country_code = character(0),
      country_label = character(0), surveyid_year = integer(0),
      welfare_type_label = character(0), vermast = character(0),
      veralt = character(0)
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
.build_filters_content <- function(normalized_filter_base, filter_cats = list()) {
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
.build_layout_content <- function(active_slots, poverty_line, registry,
                                  layout_covs = list()) {
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
    count <- cov_entry$n_categories %||% NA_integer_
    n_categories <- .format_category_count_display(
      labels = cat_labels,
      count = count
    )

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
                                       poverty_line, ppp,
                                       stat_groups = list()) {

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
      value = if (has_poverty) .format_poverty_line_display(pl, include_currency = TRUE) else NULL,
      value_raw = if (has_poverty) pl else NULL,
      ppp_year = if (has_poverty) ppp else NULL
    ),
    measures = measures_dt
  )
}


# ── Shared formatting helpers ─────────────────────────────────────────────────

#' Coerce a possibly list-wrapped scalar value to a non-empty character scalar.
#' @keywords internal
.coerce_scalar_chr <- function(x, default) {
  raw <- unlist(x, use.names = FALSE)
  if (length(raw) > 0L && !is.na(raw[[1L]]) && nzchar(as.character(raw[[1L]]))) {
    as.character(raw[[1L]])
  } else {
    default
  }
}

#' Build filter-category entries from an already-loaded registry.
#' @keywords internal
.registry_filter_categories <- function(registry) {
  if (is.null(registry) || !is.list(registry) || length(registry) == 0L) {
    return(list())
  }

  out <- lapply(registry, function(entry) {
    roles <- entry$roles
    if (is.null(roles) || !"filter" %in% roles) {
      return(NULL)
    }
    subcats <- entry$categories
    if (is.null(subcats)) subcats <- list()
    subcats <- lapply(subcats, function(cat) {
      list(
        code  = as.character(cat$code),
        label = as.character(cat$label)
      )
    })

    list(
      varname = as.character(entry$varname),
      label = as.character(entry$ui_label),
      subcategories = subcats
    )
  })
  unname(Filter(Negate(is.null), out))
}

#' Build layout-covariate entries from an already-loaded registry.
#' @keywords internal
.registry_layout_covariates <- function(registry) {
  if (is.null(registry) || !is.list(registry) || length(registry) == 0L) {
    return(list())
  }

  out <- lapply(registry, function(entry) {
    roles <- entry$roles
    if (is.null(roles) || !"covariate" %in% roles) {
      return(NULL)
    }
    list(
      varname = as.character(entry$varname),
      label = as.character(entry$ui_label),
      n_categories = if (is.null(entry$n_categories)) NULL else as.integer(entry$n_categories)
    )
  })
  unname(Filter(Negate(is.null), out))
}

#' Build stat-group entries from an already-loaded registry.
#' @keywords internal
.registry_stat_groups <- function(registry) {
  if (is.null(registry) || !is.list(registry) || length(registry) == 0L) {
    return(list())
  }
  measure_spec <- registry[["measure_spec"]]
  if (is.null(measure_spec) || length(measure_spec) == 0L) {
    return(list())
  }

  out <- lapply(names(measure_spec), function(group_key) {
    group <- measure_spec[[group_key]]
    measures <- group$measures
    if (is.null(measures)) measures <- list()
    measures_out <- lapply(names(measures), function(measure_key) {
      list(
        measure = as.character(measure_key),
        label = as.character(measures[[measure_key]]$label)
      )
    })

    list(
      group = as.character(group_key),
      group_label = as.character(group$label %||% group_key),
      measures = measures_out
    )
  })

  out
}

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


#' Format category metadata as a consistent "N: labels" display string.
#' @keywords internal
.format_category_count_display <- function(labels, count = NA_integer_) {
  if (length(labels) > 0L) {
    labels <- as.character(labels)
    labels <- labels[nzchar(labels)]
    if (length(labels) > 0L) {
      return(sprintf("%d: %s", length(labels), paste(labels, collapse = ", ")))
    }
  }

  count <- suppressWarnings(as.integer(count))[1L]
  if (is.na(count) || count < 0L) {
    return("0: Unspecified")
  }
  if (count == 0L) {
    return("0: Unspecified")
  }

  synthetic <- sprintf("Category %d", seq_len(count))
  sprintf("%d: %s", count, paste(synthetic, collapse = ", "))
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
