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

  analysis_var_label <- meta$resolved_labels$analysis_var$ui_label
  analysis_var_type <- meta$resolved_labels$analysis_var$tm_type

  if (is.list(analysis_var_label)) {
    analysis_var_label <- unlist(analysis_var_label, use.names = FALSE)
  }
  if (is.list(analysis_var_type)) {
    analysis_var_type <- unlist(analysis_var_type, use.names = FALSE)
  }

  analysis_var_label <- if (length(analysis_var_label) == 0L) {
    as.character(params$analysis_var)
  } else {
    as.character(analysis_var_label[[1]])
  }
  analysis_var_type <- if (length(analysis_var_type) == 0L) {
    "unknown"
  } else {
    as.character(analysis_var_type[[1]])
  }
  
  # Poverty line applicable flag. A JSON round-trip converts NULL to a
  # length-0 list; treat length-0 or NULL as "not applicable".
  pl <- params$poverty_line
  has_poverty <- !is.null(pl) && length(pl) > 0L && !is.na(pl)
  
  return(list(
    analysis_var_label = analysis_var_label,
    analysis_var_type = analysis_var_type,
    measures = data.table::data.table(
      measure_label = measures_dt$ui_label,
      stat_group = measures_dt$stat_group,
      analysis_var_label = rep(analysis_var_label, nrow(measures_dt))
    ),
    poverty_line = list(
      applicable = has_poverty,
      value = if (has_poverty) params$poverty_line else NULL,
      ppp_year = if (has_poverty) params$ppp else NULL
    )
  ))
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
  
  return(list(
    description = "Table dimensions are organized as follows:",
    layout = covariates_formatted[, .(
      slot_label,
      varname = data.table::fifelse(is.na(varname), "", varname),
      ui_label = data.table::fifelse(is.na(ui_label), "(None)", ui_label),
      n_categories = data.table::fifelse(is.na(n_categories), 0L, n_categories)
    )]
  ))
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
#'   `measure_interpretation` (character vector, one entry per measure).
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
  if (is.null(by) || length(by) == 0) {
    group_qualifier <- ""
    full_pop <- base_pop
  } else {
    # Check for pov_status special case
    if ("pov_status" %in% by) {
      covariates_dt <- resolved_labels$covariates
      pov_label <- if (!is.null(covariates_dt) && "varname" %in% names(covariates_dt)) {
        vals <- covariates_dt[varname == "pov_status", ui_label]
        vals[!is.na(vals)][1]
      } else {
        NA_character_
      }
      if (is.na(pov_label) || !nzchar(pov_label)) {
        pov_label <- "Poverty status"
      }
      group_qualifier <- sprintf(
        ", within each %s group (below/above $%.2f/day PPP %d)",
                                   pov_label,
                                   poverty_line,
                                   ppp)
    } else {
      covariate_desc <- format_covariate_description(by, resolved_labels$covariates)
      group_qualifier <- sprintf(", within each %s group", covariate_desc)
    }
    full_pop <- paste0(base_pop, group_qualifier)
  }
  
  # Step 3: Build measure-specific sentences
  analysis_var_label <- resolved_labels$analysis_var$ui_label
  if (is.null(analysis_var_label) || !nzchar(analysis_var_label)) {
    analysis_var_label <- analysis_var
  }
  analysis_var_type <- resolved_labels$analysis_var$tm_type
  unknown_tm_type <- is.null(analysis_var_type) || identical(analysis_var_type, "unknown")
  measures_dt <- resolved_labels$measures
  
  # Each stat_group has distinct natural-language semantics (summary stats use
  # "of/for", inequality uses "among", poverty includes thresholds, and
  # shares distinguish between survey-level and group-level denominators).
  measure_sentences <- vapply(
    measures,
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
          "The %s at $%.2f/day (PPP %d) for %s.",
          measure_label,
          poverty_line,
          ppp,
          full_pop
        ),
        
        "shares" = {
          # Shares family has 3 distinct measures
          if (measure_key == "pop_share") {
            sprintf(
              "The share of the total weighted survey population represented by %s.",
              full_pop
            )
          } else if (measure_key == "target_within_group_share") {
            # Denominator is the group population
            if (is.null(by) || length(by) == 0) {
              # No groups -> collapses to survey-level
              sprintf(
                "The share of the total weighted survey population for whom %s is true.",
                analysis_var_label
              )
            } else {
              sprintf(
                "Within %s, the share for which %s is true.",
                full_pop,
                analysis_var_label
              )
            }
          } else if (measure_key == "target_survey_share") {
            # Numerator is filtered+grouped, denominator is total survey
            sprintf(
              "The share of the total survey-weighted population represented by %s where %s is true.",
              full_pop,
              analysis_var_label
            )
          } else {
            # Fallback for unknown shares measures
            sprintf("The %s for %s.", measure_label, full_pop)
          }
        },
        
        # Fallback for unknown stat_groups
        sprintf("The %s of %s for %s.", measure_label, analysis_var_label, full_pop)
      )
      
      return(sentence)
    },
    character(1)
  )
  
  # Step 4: Handle annotations
  notes <- character()
  if (!is.null(by) && "pov_status" %in% by) {
    notes <- c(
      notes,
      sprintf(
        "Note: Poverty status groups are defined using a threshold of $%.2f/day (PPP %d).",
        poverty_line,
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
  
  # Return structured cell definition
  return(list(
    population_scope = full_pop,
    measure_interpretation = unname(measure_sentences),
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
