#' Build structured description data model from metadata
#'
#' Constructs a structured description model with 9 sections, each with
#' conditional visibility logic. This model serves as an intermediate
#' representation that can be rendered to multiple formats (Markdown, HTML, JSON).
#'
#' @param description_metadata List returned by `.build_description_metadata()` in
#'   `table_maker()`. Must contain `params`, `provenance`, `surveys`,
#'   `resolved_labels`, and `execution` fields.
#' @param params Original `table_maker()` parameter list for reference.
#' @return List with 9 section keys: `overview`, `provenance`, `surveys_selected`,
#'   `filters_applied`, `statistics_selected`, `layout_configuration`,
#'   `cell_definition`, `execution_summary`, `warnings`. Each section has
#'   `visible`, `title`, and `content` fields.
#' @export
#' @importFrom data.table data.table copy fifelse
#' @examples
#' \dontrun{
#' result <- table_maker(
#'   pip_id = "COL_2010_GEIH_INC_ALL",
#'   analysis_var = "welfare",
#'   measures = c("mean", "gini"),
#'   include_metadata = TRUE
#' )
#'
#' description_model <- build_description_model(
#'   description_metadata = result$description_metadata,
#'   params = result$description_metadata$params
#' )
#'
#' description_model$overview
#' description_model$cell_definition
#' }
#' @keywords internal
build_description_model <- function(description_metadata, params) {
  # Input validation
  if (!is.list(description_metadata) || !is.list(params)) {
    cli::cli_abort(
      c(
        "{.fn build_description_model} requires list inputs.",
        "i" = "Got: {.obj_type_friendly description_metadata} and {.obj_type_friendly params}."
      )
    )
  }

  required_fields <- c("params", "provenance", "surveys", "resolved_labels", "execution")
  missing_fields <- setdiff(required_fields, names(description_metadata))
  if (length(missing_fields)) {
    cli::cli_abort(
      c(
        "{.arg description_metadata} is missing required fields.",
        "i" = "Missing: {.val {missing_fields}}"
      )
    )
  }

  structural_checks <- list(
    params = is.list,
    provenance = is.list,
    surveys = is.list,
    resolved_labels = is.list,
    execution = is.list
  )
  invalid <- names(structural_checks)[vapply(names(structural_checks), function(field) {
    field_value <- description_metadata[[field]]
    is.null(field_value) || !structural_checks[[field]](field_value)
  }, logical(1))]
  if (length(invalid)) {
    cli::cli_abort(
      c(
        "{.arg description_metadata} contains malformed fields.",
        "i" = "Invalid structure for: {.val {invalid}}"
      )
    )
  }

  meta <- description_metadata
  
  # Resolve visibility flags
  has_filters <- !is.null(params$filter_base) && length(params$filter_base) > 0
  has_covariates <- !is.null(params$by) && length(params$by) > 0
  has_warnings <- !is.null(meta$execution$warnings) && length(meta$execution$warnings) > 0
  
  # Build each section
  model <- list(
    
    # Section 1: Overview
    overview = list(
      visible = TRUE,
      title = "Table Overview",
      content = .build_overview_content(meta, params)
    ),
    
    # Section 2: Provenance
    provenance = list(
      visible = TRUE,
      title = "Data Provenance",
      content = list(
        release_id = meta$provenance$release,
        ppp_year = meta$provenance$ppp_year,
        generated_at = as.character(meta$provenance$generated_at)
      )
    ),
    
    # Section 3: Surveys Selected
    surveys_selected = list(
      visible = TRUE,
      title = "Surveys Selected",
      content = .build_surveys_content(meta)
    ),
    
    # Section 4: Filters Applied (conditional)
    filters_applied = list(
      visible = has_filters,
      title = "Sample Base Filters",
      content = if (has_filters) .build_filters_content(meta) else NULL
    ),
    
    # Section 5: Statistics Selected
    statistics_selected = list(
      visible = TRUE,
      title = "Statistics Computed",
      content = .build_statistics_content(meta, params)
    ),
    
    # Section 6: Layout Configuration (conditional)
    layout_configuration = list(
      visible = has_covariates,
      title = "Table Layout Configuration",
      content = if (has_covariates) .build_layout_content(meta) else NULL
    ),
    
    # Section 7: Cell Definition
    cell_definition = list(
      visible = TRUE,
      title = "Cell Definition",
      content = build_cell_definition(
        analysis_var = params$analysis_var,
        measures = params$measures,
        filter_base = params$filter_base,
        by = params$by,
        poverty_line = params$poverty_line,
        ppp = params$ppp,
        release = params$release,
        resolved_labels = meta$resolved_labels
      )
    ),
    
    # Section 8: Execution Summary
    execution_summary = list(
      visible = TRUE,
      title = "Execution Summary",
      content = .build_execution_content(meta)
    ),
    
    # Section 9: Warnings (conditional)
    warnings = list(
      visible = has_warnings,
      title = "Warnings",
      content = if (has_warnings) list(warnings = meta$execution$warnings) else NULL
    )
  )
  
  return(model)
}


# Internal helpers for content builders

#' Build overview section content for the description model.
#'
#' @param meta Description metadata list.
#' @param params Original table_maker() parameters.
#' @return List with textual overview details.
#' @keywords internal
.build_overview_content <- function(meta, params) {
  n_measures <- length(params$measures)
  n_surveys <- meta$execution$n_surveys_loaded
  
  # Covariate description
  covariate_desc <- if (!is.null(params$by) && length(params$by) > 0) {
    paste0(
      ", with results disaggregated by ",
      format_covariate_description(params$by, meta$resolved_labels$covariates)
    )
  } else {
    ""
  }
  
  # Structure description
  structure_desc <- sprintf(
    "This table reports %d %s for %d selected %s%s.",
    n_measures,
    if (n_measures == 1) "statistic" else "statistics",
    n_surveys,
    if (n_surveys == 1) "survey" else "surveys",
    covariate_desc
  )
  
  # PPP note
  ppp_note <- sprintf(
    "Welfare values are expressed in %d PPP international dollars per day.",
    params$ppp
  )
  
  return(list(
    structure_description = structure_desc,
    weighting_note = "All calculations use survey sampling weights.",
    ppp_note = ppp_note
  ))
}


#' Build surveys section content for the description model.
#'
#' @param meta Description metadata list.
#' @return List with loaded/excluded survey tables and counts.
#' @keywords internal
.build_surveys_content <- function(meta) {
  loaded <- meta$surveys$loaded
  excluded <- meta$surveys$excluded

  # Coerce to data.table defensively. Metadata can arrive from the API after a
  # JSON round-trip, which converts data.tables to data.frames and empty tables
  # to empty lists. NULL check required before is.data.frame().
  n_loaded_rows <- if (is.null(loaded)) 0L else if (is.data.frame(loaded)) nrow(loaded) else 0L
  n_excluded_rows <- if (is.null(excluded)) 0L else if (is.data.frame(excluded)) nrow(excluded) else 0L

  # Map welfare_type codes to labels
  loaded_list <- if (n_loaded_rows > 0) {
    data.table::data.table(
      pip_id = loaded$pip_id,
      country_code = loaded$country_code,
      surveyid_year = loaded$surveyid_year,
      welfare_type_label = format_welfare_type(loaded$welfare_type)
    )
  } else {
    data.table::data.table(
      pip_id = character(0),
      country_code = character(0),
      surveyid_year = character(0),
      welfare_type_label = character(0)
    )
  }

  excluded_list <- if (n_excluded_rows > 0) {
    data.table::as.data.table(excluded)
  } else {
    NULL
  }

  return(list(
    n_loaded = meta$execution$n_surveys_loaded,
    n_excluded = meta$execution$n_surveys_excluded,
    loaded_list = loaded_list,
    excluded_list = excluded_list
  ))
}


#' Build filters section content for the description model.
#'
#' @param meta Description metadata list.
#' @return List describing active filters, or NULL when absent.
#' @keywords internal
.build_filters_content <- function(meta) {
  filters_dt <- meta$resolved_labels$filters

  n_rows <- if (is.null(filters_dt)) 0L else if (is.data.frame(filters_dt)) nrow(filters_dt) else 0L
  if (n_rows == 0L) {
    return(NULL)
  }
  
  # P0-2: Validate schema before column access
  filters_dt <- data.table::as.data.table(filters_dt)
  required_cols <- c("ui_label", "selected_labels")
  if (!all(required_cols %in% names(filters_dt))) {
    cli::cli_abort("filters_dt missing required columns: {setdiff(required_cols, names(filters_dt))}")
  }
  
  # P0-3: Normalize list-column before vapply to handle NULL/non-character elements
  filters_dt[, selected_labels := lapply(selected_labels, function(x) {
    if (is.null(x) || length(x) == 0) return(character(0))
    as.character(x)
  })]
  
  # Format selected_labels as comma-separated strings
  filters_formatted <- data.table::data.table(
    variable = filters_dt$ui_label,
    selected_categories = vapply(
      filters_dt$selected_labels,
      function(x) if (length(x) == 0) "(unspecified)" else paste(x, collapse = ", "),
      character(1)
    )
  )
  
  return(list(
    description = "The weighted sample is restricted to observations that meet all of the following criteria:",
    filters = filters_formatted
  ))
}


#' Build statistics section content for the description model.
#'
#' @param meta Description metadata list.
#' @param params Original table_maker() parameters.
#' @return List describing measures and analysis variable metadata.
#' @keywords internal
.build_statistics_content <- function(meta, params) {
  measures_dt <- meta$resolved_labels$measures
  
  # P1-4: Validate measures_dt before column access
  if (is.null(measures_dt) || !is.data.frame(measures_dt) || nrow(measures_dt) == 0) {
    cli::cli_abort("`resolved_labels$measures` must be a non-empty data.frame")
  }
  measures_dt <- data.table::as.data.table(measures_dt)

  analysis_var_label <- .coerce_description_scalar(
    meta$resolved_labels$analysis_var$ui_label,
    fallback = as.character(params$analysis_var)
  )
  analysis_var_type <- .coerce_description_scalar(
    meta$resolved_labels$analysis_var$tm_type,
    fallback = "unknown"
  )
  
  # Poverty line applicable flag. A JSON round-trip converts NULL to a
  # length-0 list; treat length-0 or NULL as "not applicable".
  pl <- .normalize_poverty_line_values(params$poverty_line)
  has_poverty <- length(pl) > 0L
  
  return(list(
    analysis_var_label = analysis_var_label,
    analysis_var_type = analysis_var_type,
    measures = data.table::data.table(
      measure_label = measures_dt$ui_label,
      stat_group = measures_dt$stat_group,
      analysis_var_label = analysis_var_label
    ),
    poverty_line = list(
      applicable = has_poverty,
      value = if (has_poverty) .format_poverty_line_display(pl) else NULL,
      ppp_year = if (has_poverty) params$ppp else NULL
    )
  ))
}


#' Coerce a possibly list-wrapped scalar metadata field into a single string.
#'
#' @param x Scalar-like metadata field, possibly list-wrapped after JSON.
#' @param fallback Character scalar to use when `x` is NULL/empty/NA/blank.
#' @return Character scalar.
#' @keywords internal
.coerce_description_scalar <- function(x, fallback) {
  if (is.list(x)) {
    x <- unlist(x, use.names = FALSE)
  }

  if (is.null(x) || length(x) == 0L) {
    return(fallback)
  }

  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]
  if (length(x) == 0L) {
    return(fallback)
  }

  x[[1]]
}


#' Normalize poverty-line input for safe description rendering.
#'
#' @param poverty_line Numeric-like vector, possibly list-wrapped after JSON.
#' @return Numeric vector with NULL/NA/empty values removed.
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
  if (length(poverty_line) == 0L) {
    return(numeric())
  }

  poverty_line
}


#' Format one or more poverty lines for natural-language descriptions.
#'
#' @param poverty_line Numeric vector of poverty thresholds.
#' @param include_currency Logical; prefix values with `$` when TRUE.
#' @param suffix Character scalar appended to each formatted value (e.g.
#'   `"/day"`); default `""` adds no suffix.
#' @return Character scalar.
#' @keywords internal
.format_poverty_line_display <- function(poverty_line, include_currency = FALSE, suffix = "") {
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


#' Build layout section content for the description model.
#'
#' @param meta Description metadata list.
#' @return List detailing covariate slots and labels.
#' @keywords internal
.build_layout_content <- function(meta) {
  covariates_dt <- meta$resolved_labels$covariates

  # P1-5: Check NULL and is.data.frame before nrow
  if (is.null(covariates_dt) || !is.data.frame(covariates_dt) || nrow(covariates_dt) == 0) {
    return(NULL)
  }

  # Coerce to data.table: metadata can arrive from the API after a JSON
  # round-trip, which converts data.tables to plain data.frames.
  covariates_dt <- data.table::as.data.table(covariates_dt)

  # Format with slot labels
  covariates_formatted <- data.table::copy(covariates_dt)
  covariates_formatted[, slot_label := format_slot_label(slot)]

  # P1-6: Coerce to character/integer before fifelse to ensure type compatibility
  covariates_formatted[, ':='(
    varname = as.character(varname),
    ui_label = as.character(ui_label),
    n_categories = as.integer(n_categories)
  )]

  # Resolve category labels per covariate from the variable registry, so the
  # Categories column carries both the count and the labels (e.g.
  # "2: male, female") instead of just the integer count.
  release <- meta$provenance$release
  registry <- tryCatch(
    piptm_variable_registry(release),
    error = function(e) NULL
  )

  categories_field <- vapply(
    seq_len(nrow(covariates_formatted)),
    function(i) {
      varname <- covariates_formatted$varname[[i]]
      count <- covariates_formatted$n_categories[[i]]
      count_int <- if (is.na(count)) 0L else count

      # Special case: pov_status is not a registry covariate; use fixed labels.
      if (identical(varname, "pov_status")) {
        return("2: poor, non-poor")
      }

      labels <- .resolve_covariate_category_labels(varname, registry)
      if (length(labels) == 0L) {
        return(as.character(count_int))
      }

      # If the registry-resolved label count disagrees with n_categories, prefer
      # the registry length (it reflects reality of what will be displayed).
      n_labels <- length(labels)
      sprintf("%d: %s", n_labels, paste(labels, collapse = ", "))
    },
    character(1)
  )

  covariates_formatted[, n_categories := categories_field]

  return(list(
    description = "Table dimensions are organized as follows:",
    layout = covariates_formatted[, .(
      slot_label,
      varname = data.table::fifelse(is.na(varname), "", varname),
      ui_label = data.table::fifelse(is.na(ui_label), "(None)", ui_label),
      n_categories = n_categories
    )]
  ))
}


#' Resolve category labels for a covariate from the variable registry.
#'
#' Looks up `varname` in `registry` and returns its category labels in
#' registry order. Returns `character(0)` when the registry is unavailable,
#' the entry is missing, or the entry has no categories.
#'
#' @param varname Character scalar covariate variable name.
#' @param registry Named list returned by `piptm_variable_registry()`, or NULL.
#' @return Character vector of category labels (possibly empty).
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
  labels <- labels[nzchar(labels)]
  labels
}


#' Build execution summary section content for the description model.
#'
#' @param meta Description metadata list.
#' @return List with execution counts and suppression summary.
#' @keywords internal
.build_execution_content <- function(meta) {
  exec <- meta$execution
  
  suppression_summary <- if (exec$suppression$triggered) {
    sprintf(
      "Population share suppression was applied with threshold = %.3f. %d %s suppressed.",
      exec$suppression$threshold,
      exec$suppression$n_cells_suppressed,
      if (exec$suppression$n_cells_suppressed == 1) "cell was" else "cells were"
    )
  } else {
    "No suppression was applied."
  }
  
  return(list(
    n_surveys_loaded = exec$n_surveys_loaded,
    n_surveys_excluded = exec$n_surveys_excluded,
    n_filters_applied = exec$n_filters_applied,
    n_measures_computed = exec$n_measures_computed,
    suppression_summary = suppression_summary,
    suppressed_cells = if (exec$suppression$triggered) exec$suppression$suppressed_cells else NULL
  ))
}


#' Resolve a human-readable group label for a set of covariates.
#'
#' Handles the `pov_status` special case (using its resolved `ui_label`, or
#' falling back to `"Poverty status"`) identically to the general covariate
#' path. Used for both the `population_scope` group qualifier and the shares
#' table's `plain_meaning` group phrase, so the two never drift apart.
#'
#' @param by Character vector or NULL: covariate names.
#' @param covariates_dt Data frame of resolved covariate labels, or NULL.
#' @return Character scalar group label, or NULL when `by` is NULL/empty.
#' @keywords internal
.resolve_group_label <- function(by, covariates_dt) {
  if (is.null(by) || length(by) == 0) {
    return(NULL)
  }
  if ("pov_status" %in% by) {
    pov_label <- if (!is.null(covariates_dt) && "varname" %in% names(covariates_dt)) {
      vals <- covariates_dt[varname == "pov_status", ui_label]
      vals[!is.na(vals)][1]
    } else {
      NA_character_
    }
    if (is.na(pov_label) || !nzchar(pov_label)) {
      pov_label <- "Poverty status"
    }
    return(pov_label)
  }
  return(format_covariate_description(by, covariates_dt))
}


#' Build cell definition content (core algorithm)
#'
#' Generates mathematically precise population definitions that adapt to all
#' permutations of filters, analysis variables, measures, and covariates.
#'
#' @param analysis_var Character scalar: analysis variable name.
#' @param measures Character vector: measure keys.
#' @param filter_base Named list or NULL: filter conditions.
#' @param by Character vector or NULL: covariate names.
#' @param poverty_line Numeric scalar or NULL: poverty threshold.
#' @param ppp Integer scalar: PPP year.
#' @param release Character scalar: release ID.
#' @param resolved_labels List: pre-resolved labels from metadata.
#' @return List with `population_scope` (character scalar) and
#'   `measure_interpretation`. When no share measure
#'   (`pop_share`, `target_within_group_share`, `target_survey_share`) is
#'   among `measures`, `measure_interpretation` is a character vector, one
#'   sentence per measure (unchanged legacy shape). When one or more share
#'   measures are requested, `measure_interpretation` is instead a named
#'   list with: `prose` (character vector of non-share sentences, possibly
#'   length 0), `shares_table` (a `data.table` with one row per requested
#'   share measure and columns `measure`, `denominator`, `numerator`,
#'   `plain_meaning`), and `shares_footer` (a character scalar identity note,
#'   or `NULL` when fewer than 2 share measures were requested).
#' @keywords internal
build_cell_definition <- function(analysis_var, measures, filter_base, by,
                                   poverty_line, ppp, release, resolved_labels) {
  
  # P0-4: Validate filters_dt schema at function entry (only if non-NULL and has rows)
  filters_dt <- resolved_labels$filters
  if (!is.null(filters_dt) && is.data.frame(filters_dt) && nrow(filters_dt) > 0) {
    required <- c("varname", "ui_label", "selected_labels")
    if (!all(required %in% names(filters_dt))) {
      cli::cli_abort("filters_dt missing required columns: {setdiff(required, names(filters_dt))}")
    }
  }
  
  poverty_line_values <- .normalize_poverty_line_values(poverty_line)
  poverty_line_daily_text <- .format_poverty_line_display(
    poverty_line_values,
    include_currency = TRUE,
    suffix = "/day"
  )
  poverty_group_text <- if (length(poverty_line_values) <= 1L) {
    sprintf("below/above %s PPP %d", poverty_line_daily_text, ppp)
  } else {
    sprintf("below/above poverty lines %s PPP %d", poverty_line_daily_text, ppp)
  }

  # Step 1: Determine base_pop from filter_base
  if (is.null(filter_base) || length(filter_base) == 0) {
    base_pop <- "survey-weighted individuals in the selected survey"
  } else {
    # Format filter conditions
    filter_conditions <- if (!is.null(filters_dt) && nrow(filters_dt) > 0) {
      vapply(
        seq_len(nrow(filters_dt)),
        function(i) {
          labels <- filters_dt$selected_labels[[i]]
          if (is.null(labels) || length(labels) == 0) {
            raw_vals <- filter_base[[filters_dt$varname[i]]]
            labels <- if (is.null(raw_vals)) character(0) else as.character(raw_vals)
          }
          label_text <- if (length(labels)) paste(labels, collapse = ", ") else "(unspecified)"
          var_label <- filters_dt$ui_label[i]
          if (is.null(var_label) || !nzchar(var_label)) {
            var_label <- filters_dt$varname[i]
          }
          sprintf("%s is among [%s]", var_label, label_text)
        },
        character(1)
      )
    } else {
      fallback_names <- names(filter_base)
      if (is.null(fallback_names) || any(!nzchar(fallback_names))) {
        fallback_names <- sprintf("filter_%d", seq_along(filter_base))
      }
      vapply(
        seq_along(filter_base),
        function(idx) {
          varname <- fallback_names[idx]
          values <- filter_base[[idx]]
          value_text <- if (length(values)) paste(as.character(values), collapse = ", ") else "(unspecified)"
          sprintf("%s is among [%s]", varname, value_text)
        },
        character(1)
      )
    }
    filter_text <- paste(filter_conditions, collapse = " AND ")
    base_pop <- sprintf("survey-weighted individuals for whom %s", filter_text)
  }
  
  # Step 2: Layer group_qualifier from by
  group_label <- if (is.null(by) || length(by) == 0) {
    NULL
  } else {
    .resolve_group_label(by, resolved_labels$covariates)
  }
  if (is.null(by) || length(by) == 0) {
    group_qualifier <- ""
    full_pop <- base_pop
  } else {
    # Check for pov_status special case
    if ("pov_status" %in% by) {
      group_qualifier <- sprintf(
        ", within each %s group (%s)",
        group_label,
        poverty_group_text
      )
    } else {
      group_qualifier <- sprintf(", within each %s group", group_label)
    }
    full_pop <- paste0(base_pop, group_qualifier)
  }
  
  # Step 3: Build measure-specific sentences
  analysis_var_label <- .coerce_description_scalar(
    resolved_labels$analysis_var$ui_label,
    fallback = as.character(analysis_var)
  )
  analysis_var_type <- .coerce_description_scalar(
    resolved_labels$analysis_var$tm_type,
    fallback = "unknown"
  )
  unknown_tm_type <- is.null(analysis_var_type) || identical(analysis_var_type, "unknown")
  measures_dt <- resolved_labels$measures

  # Partition requested measures into shares and non-shares. Shares get a
  # structured table (denominator/numerator/plain meaning); everything else
  # keeps the existing prose path unchanged. Duplicate measures are not a
  # supported request pattern, so `setdiff()`/logical indexing on unique
  # values is acceptable here.
  measure_stat_groups <- vapply(
    measures,
    function(measure_key) {
      idx <- which(measures_dt$measure == measure_key)
      if (length(idx) == 0) return(NA_character_)
      measures_dt$stat_group[idx[1]]
    },
    character(1)
  )
  is_share_measure <- !is.na(measure_stat_groups) & measure_stat_groups == "shares"
  nonshare_measures <- measures[!is_share_measure]
  share_measures <- measures[is_share_measure]

  # Each stat_group has distinct natural-language semantics (summary stats use
  # "of/for", inequality uses "among", poverty includes thresholds). Shares
  # are handled separately below via a structured table.
  measure_sentences <- vapply(
    nonshare_measures,
    function(measure_key) {
      # Find measure label and stat_group
      idx <- which(measures_dt$measure == measure_key)
      if (length(idx) == 0) {
        return(sprintf("The %s of %s.", measure_key, analysis_var_label))
      }
      
      measure_label <- measures_dt$ui_label[idx]
      stat_group <- measures_dt$stat_group[idx]
      
      # Apply measure-specific semantics per family
      sentence <- switch(
        stat_group,
        
        "summary_statistics" = sprintf(
          "The %s of %s for %s.",
          measure_label,
          analysis_var_label,
          full_pop
        ),
        
        "inequality" = sprintf(
          "The %s of %s among %s.",
          measure_label,
          analysis_var_label,
          full_pop
        ),
        
        "poverty" = sprintf(
          "The %s at %s (PPP %d) for %s.",
          measure_label,
          poverty_line_daily_text,
          ppp,
          full_pop
        ),
        
        # Fallback for unknown stat_groups
        sprintf("The %s of %s for %s.", measure_label, analysis_var_label, full_pop)
      )
      
      return(sentence)
    },
    character(1)
  )

  # Build the structured shares table, if any share measures were requested.
  shares_table <- NULL
  shares_footer <- NULL
  if (length(share_measures) > 0) {
    cell_numerator <- sprintf("Individuals in this cell (%s)", full_pop)
    target_numerator <- sprintf("Cell members for whom %s is true", analysis_var_label)
    no_grouping <- is.null(by) || length(by) == 0

    # Plain-meaning phrasing building blocks, reused across all three share
    # templates. `filter_phrase` and `group_phrase` are built from the same
    # filter/group labels used for `base_pop`/`full_pop` and `group_label`
    # above, so wording never drifts from the rest of the section.
    has_filter <- !is.null(filter_base) && length(filter_base) > 0
    filter_phrase <- if (has_filter) {
      sprintf("all individuals for whom %s", filter_text)
    } else {
      "the total survey population"
    }
    group_phrase <- if (!no_grouping) sprintf("this %s group", group_label) else NULL

    # Canonical share-measure labels, used as a fallback when a measure isn't
    # itself requested (so its metadata row may be absent from `measures_dt`,
    # per table_maker.R's upstream filtering) and we still need to refer to
    # it by name -- e.g. target_survey_share's no-grouping clarification below
    # and the identity footer further down. Never leak a raw internal key.
    share_canonical_labels <- c(
      pop_share = "Population share",
      target_within_group_share = "Target share within cell",
      target_survey_share = "Target share in sample base"
    )
    share_label <- function(measure_key) {
      idx <- which(measures_dt$measure == measure_key)
      if (length(idx) > 0) {
        return(measures_dt$ui_label[idx[1]])
      }
      unname(share_canonical_labels[measure_key])
    }

    share_row <- function(measure_key) {
      idx <- which(measures_dt$measure == measure_key)
      measure_label <- if (length(idx) > 0) measures_dt$ui_label[idx[1]] else measure_key

      if (identical(measure_key, "pop_share")) {
        plain_meaning <- if (no_grouping) {
          "This value is always 1 when there is no grouping."
        } else {
          sprintf("Among %s, what fraction fall in %s?", filter_phrase, group_phrase)
        }
        return(data.table::data.table(
          measure = measure_label,
          denominator = base_pop,
          numerator = cell_numerator,
          plain_meaning = plain_meaning
        ))
      }

      if (identical(measure_key, "target_within_group_share")) {
        # When there is no grouping, this measure is numerically identical
        # to target_survey_share (the cell IS the filtered base), so the two
        # intentionally share the same plain-meaning text in that case.
        plain_meaning <- if (no_grouping) {
          sprintf("Among %s, what fraction have %s?", filter_phrase, analysis_var_label)
        } else {
          sprintf(
            "Among %s in %s, what fraction have %s?",
            filter_phrase, group_phrase, analysis_var_label
          )
        }
        return(data.table::data.table(
          measure = measure_label,
          denominator = cell_numerator,
          numerator = target_numerator,
          plain_meaning = plain_meaning
        ))
      }

      if (identical(measure_key, "target_survey_share")) {
        # When there is no grouping, this measure is numerically identical to
        # target_within_group_share (the cell IS the filtered base). The
        # plain-meaning question itself is worded identically for both in
        # that case, so we append a parenthetical cross-reference here (only
        # on target_survey_share) to avoid two indistinguishable rows in the
        # rendered table.
        plain_meaning <- if (no_grouping) {
          sprintf(
            "Among %s, what fraction have %s? (Equivalent to %s when no grouping is applied.)",
            filter_phrase, analysis_var_label, share_label("target_within_group_share")
          )
        } else {
          sprintf(
            "Among %s, what fraction fall in %s and have %s?",
            filter_phrase, group_phrase, analysis_var_label
          )
        }
        return(data.table::data.table(
          measure = measure_label,
          denominator = base_pop,
          numerator = target_numerator,
          plain_meaning = plain_meaning
        ))
      }

      # Fallback for unknown shares measures
      data.table::data.table(
        measure = measure_label,
        denominator = base_pop,
        numerator = cell_numerator,
        plain_meaning = sprintf("What share is represented by %s?", measure_label)
      )
    }

    shares_table <- data.table::rbindlist(lapply(share_measures, share_row))

    if (length(share_measures) >= 2) {
      # Reuses the shared `share_label()` resolver defined above (falls back
      # to canonical labels rather than leaking a raw internal measure key
      # when a referenced measure wasn't itself requested).
      shares_footer <- sprintf(
        "For cells with positive weighted population: %s = %s \u00d7 %s.",
        share_label("target_survey_share"),
        share_label("pop_share"),
        share_label("target_within_group_share")
      )
    }
  }
  
  # Step 4: Handle annotations
  notes <- character()
  if (!is.null(by) && "pov_status" %in% by) {
    notes <- c(
      notes,
      sprintf(
        if (length(poverty_line_values) <= 1L) {
          "Note: Poverty status groups are defined using a threshold of %s (PPP %d)."
        } else {
          "Note: Poverty status groups are defined using thresholds of %s (PPP %d)."
        },
        poverty_line_daily_text,
        ppp
      )
    )
  }
  if (unknown_tm_type) {
    notes <- c(
      notes,
      sprintf(
        "Note: Registry metadata for analysis variable %s was unavailable; descriptions use generic phrasing.",
        analysis_var_label
      )
    )
  }
  note <- if (length(notes)) paste(notes, collapse = "\n") else NULL
  
  # Return structured cell definition. `measure_interpretation` remains a
  # plain character vector (one sentence per measure) when no share measures
  # were requested, preserving the existing contract exactly. When one or
  # more share measures are requested, it becomes a named list of
  # `prose` (non-share sentences, possibly empty), `shares_table` (a
  # data.table with one row per share measure: measure/denominator/
  # numerator/plain_meaning), and `shares_footer` (an identity note, present
  # only when 2+ share measures were requested).
  measure_interpretation <- if (length(share_measures) == 0) {
    unname(measure_sentences)
  } else {
    list(
      prose = unname(measure_sentences),
      shares_table = shares_table,
      shares_footer = shares_footer
    )
  }

  return(list(
    population_scope = full_pop,
    measure_interpretation = measure_interpretation,
    note = note
  ))
}


#' Format welfare type code to label
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


#' Format slot name to display label
#'
#' @param slot Character vector: internal slot names.
#' @return Character vector: display labels.
#' @keywords internal
format_slot_label <- function(slot) {
  slot_labels <- c(
    columns = "Columns",
    rows = "Rows",
    super_columns = "Super Columns",
    super_rows = "Super Rows"
  )
  return(unname(slot_labels[slot]))
}


#' Format covariate description
#'
#' Joins covariate labels with "×" separator.
#'
#' @param by Character vector: covariate varnames.
#' @param covariates_dt data.table: resolved covariate labels.
#' @return Character scalar: formatted string (e.g., "Gender × Area").
#' @keywords internal
format_covariate_description <- function(by, covariates_dt) {
  if (is.null(by) || length(by) == 0) {
    return("")
  }
  if (is.null(covariates_dt) || nrow(covariates_dt) == 0) {
    return(paste(by, collapse = " × "))
  }
  
  # Match by varnames to get ui_labels
  labels <- vapply(
    by,
    function(varname) {
      idx <- which(covariates_dt$varname == varname)
      if (length(idx) > 0 && !is.na(covariates_dt$ui_label[idx[1]])) {
        return(covariates_dt$ui_label[idx[1]])
      } else {
        return(varname)  # Fallback to varname
      }
    },
    character(1)
  )
  
  return(paste(labels, collapse = " × "))
}
