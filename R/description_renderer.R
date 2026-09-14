#' Render a description model to Markdown
#'
#' Converts the structured description model produced by
#' [`build_description_model()`] into readable Markdown text. Only sections
#' marked as visible are rendered; sections with `NULL` content are skipped.
#'
#' @param model List produced by [`build_description_model()`] with 9 sections,
#'   each containing `visible`, `title`, and `content` fields.
#' @return A single character scalar containing the rendered Markdown.
#' @export
#' @examples
#' \dontrun{
#' result <- table_maker(
#'   pip_id = "COL_2010_GEIH_INC_ALL",
#'   analysis_var = "welfare",
#'   measures = c("mean", "gini"),
#'   include_metadata = TRUE
#' )
#' model <- build_description_model(
#'   description_metadata = result$description_metadata,
#'   params = result$description_metadata$params
#' )
#' cat(render_description_markdown(model))
#' }
#' @keywords internal
#' @note This file mirrors `R/description_renderer_html.R`'s dispatch
#'   structure (table / named list / char vector / scalar) for the Markdown
#'   output format. Changes to `build_description_model()`'s content shapes
#'   must be reflected in BOTH renderers, or Markdown/HTML output will
#'   silently diverge.
render_description_markdown <- function(model) {
  stopifnot(is.list(model))

  section_blocks <- character()
  for (section_name in names(model)) {
    section <- model[[section_name]]

    visible <- isTRUE(section$visible)
    content <- section$content
    if (!visible || is.null(content)) {
      next
    }

    title <- section$title %||% section_name
    rendered <- .render_section_content(content, section_name = section_name)
    if (is.null(rendered) || !nzchar(rendered)) {
      next
    }

    section_blocks <- c(
      section_blocks,
      paste0("## ", title, "\n\n", rendered)
    )
  }

  if (length(section_blocks) == 0) {
    return("")
  }
  return(paste(section_blocks, collapse = "\n\n"))
}


#' Render a single section's content to Markdown by type.
#'
#' Dispatches on the structure of `content`:
#' - an object coercible to a data.frame renders as a Markdown table;
#' - a named list renders as key-value bullets (or nested structures);
#' - a character vector renders as a numbered list (with names as labels);
#' - otherwise the value is flattened to text.
#'
#' @param content The `content` field of a description model section.
#' @return Character scalar Markdown, or `NULL` when there is nothing to render.
#' @keywords internal
.render_section_content <- function(content, section_name = NULL) {
  if (is.null(content)) {
    return(NULL)
  }

  if (inherits(content, "data.table") || is.data.frame(content)) {
    return(.render_table(content))
  }

  if (is.list(content)) {
    return(.render_named_list(content, section_name = section_name))
  }

  if (is.character(content)) {
    return(.render_char_vector(content))
  }

  return(as.character(content))
}


#' Render a data.frame/data.table to a Markdown pipe table.
#'
#' @param dt data.frame or data.table.
#' @return Character scalar Markdown table, or NULL for empty frames.
#' @keywords internal
.render_table <- function(dt) {
  if (nrow(dt) == 0 || ncol(dt) == 0) {
    return(NULL)
  }

  cols <- names(dt)
  header <- paste0("| ", paste(vapply(cols, .display_table_header_markdown, character(1)), collapse = " | "), " |")
  separator <- paste0("| ", paste(rep("---", length(cols)), collapse = " | "), " |")

  rows <- vapply(seq_len(nrow(dt)), function(i) {
    values <- vapply(cols, function(col_name) {
      .display_table_value_markdown(col_name, dt[[col_name]][[i]])
    }, character(1))
    values[is.na(values)] <- ""
    paste0("| ", paste(values, collapse = " | "), " |")
  }, character(1))

  return(paste(c(header, separator, rows), collapse = "\n"))
}


#' Render a named list to Markdown.
#'
#' Named scalar or atomic entries become `- key: value` bullets. A nested
#' data.frame value renders as a Markdown table under its key. A `description`
#' entry is rendered as a lead paragraph. Nested lists recurse.
#'
#' @param content Named list.
#' @return Character scalar Markdown, or NULL when empty.
#' @keywords internal
.render_named_list <- function(content, section_name = NULL, parent_key = NULL) {
  if (length(content) == 0) {
    return(NULL)
  }

  lines <- character()

  # Lead description paragraph, if present.
  if ("description" %in% names(content)) {
    lines <- c(lines, as.character(content[["description"]]))
    content <- content[names(content) != "description"]
  }

  if (identical(section_name, "overview") && "ppp_note" %in% names(content)) {
    lines <- c(lines, as.character(content[["ppp_note"]]))
    content <- content[names(content) != "ppp_note"]
  }

  for (key in names(content)) {
    value <- content[[key]]
    if (is.null(value)) {
      next
    }

    if (
      identical(section_name, "statistics_selected") &&
      key %in% c("analysis_var_label", "analysis_var_type")
    ) {
      next
    }

    if (
      identical(section_name, "statistics_selected") &&
      identical(key, "poverty_line") &&
      is.list(value)
    ) {
      if (!isTRUE(value$applicable)) {
        next
      }

      nested_lines <- character()
      for (nested_key in c("value", "ppp_year")) {
        nested_text <- .scalar_text(value[[nested_key]])
        if (is.null(nested_text) || is.na(nested_text) || !nzchar(nested_text)) {
          next
        }
        nested_lines <- c(
          nested_lines,
          sprintf(
            "- %s: %s",
            .display_label_markdown(section_name, nested_key, parent_key = "poverty_line"),
            nested_text
          )
        )
      }

      if (length(nested_lines) > 0) {
        lines <- c(
          lines,
          .printed_nested(
            .display_label_markdown(section_name, key, parent_key = parent_key),
            paste(nested_lines, collapse = "\n")
          )
        )
      }
      next
    }

    if (inherits(value, "data.table") || is.data.frame(value)) {
      rendered <- .render_table(value)
      if (!is.null(rendered)) {
        lines <- c(lines, paste0("**", .display_label_markdown(section_name, key, parent_key = parent_key), "**"), rendered)
      }
    } else if (
      identical(section_name, "cell_definition") &&
      identical(key, "measure_interpretation") &&
      is.list(value) &&
      !is.data.frame(value)
    ) {
      # Share measures: render non-share prose first, then the structured
      # shares table, then the identity footer (if present). Character-
      # vector `measure_interpretation` (no shares requested) falls through
      # to the existing branch below unchanged.
      rendered <- .render_measure_interpretation_shares(value)
      if (!is.null(rendered)) {
        lines <- c(lines, paste0("**", .display_label_markdown(section_name, key, parent_key = parent_key), "**"), rendered)
      }
    } else if (is.list(value)) {
      rendered <- .render_named_list(value, section_name = section_name, parent_key = key)
      if (!is.null(rendered)) {
        lines <- c(lines, .printed_nested(.display_label_markdown(section_name, key, parent_key = parent_key), rendered))
      }
    } else if (is.character(value) && length(value) > 1) {
      rendered <- .render_char_vector(value)
      if (!is.null(rendered)) {
        lines <- c(lines, .printed_nested(.display_label_markdown(section_name, key, parent_key = parent_key), rendered))
      }
    } else if (is.null(dim(value))) {
      text <- .scalar_text(value)
      if (!is.null(text)) {
        lines <- c(lines, sprintf("- %s: %s", .display_label_markdown(section_name, key, parent_key = parent_key), text))
      }
    }
  }

  if (length(lines) == 0) {
    return(NULL)
  }
  return(paste(lines, collapse = "\n"))
}


#' Render the structured shares payload of `cell_definition$measure_interpretation`.
#'
#' Emits non-share prose (numbered list, matching the legacy character-vector
#' rendering), then the shares table, then the identity footer.
#'
#' @param value List with `prose`, `shares_table`, `shares_footer`.
#' @return Character scalar Markdown, or NULL when there is nothing to render.
#' @keywords internal
.render_measure_interpretation_shares <- function(value) {
  blocks <- character()

  prose_rendered <- .render_char_vector(value$prose)
  if (!is.null(prose_rendered)) {
    blocks <- c(blocks, prose_rendered)
  }

  table_rendered <- .render_table(value$shares_table)
  if (!is.null(table_rendered)) {
    blocks <- c(blocks, table_rendered)
  }

  if (!is.null(value$shares_footer) && nzchar(value$shares_footer)) {
    blocks <- c(blocks, paste0("*", value$shares_footer, "*"))
  }

  if (length(blocks) == 0) {
    return(NULL)
  }
  return(paste(blocks, collapse = "\n\n"))
}


#' Render a character vector as a numbered list.
#'
#' Names, when present, are used as inline labels. Otherwise entries are
#' numbered sequentially.
#'
#' @param x Character vector.
#' @return Markdown numbered list, or NULL when empty.
#' @keywords internal
.render_char_vector <- function(x) {
  if (is.null(x) || length(x) == 0) {
    return(NULL)
  }
  x <- x[!is.na(x) & nzchar(x)]
  if (length(x) == 0) {
    return(NULL)
  }

  x_names <- names(x)
  lines <- seq_along(x)
  paste(vapply(lines, function(i) {
    label <- if (!is.null(x_names) && !is.na(x_names[i]) && nzchar(x_names[i])) {
      sprintf("%s. **%s:** %s", i, x_names[i], x[i])
    } else {
      sprintf("%s. %s", i, x[i])
    }
    label
  }, character(1)), collapse = "\n")
}


#' Format a single scalar value for bullet lines.
#'
#' @param value Scalar value.
#' @return Character scalar, or NA when the value is empty.
#' @keywords internal
.scalar_text <- function(value) {
  if (is.na(value)) {
    return(NA_character_)
  }
  if (is.logical(value)) {
    return(if (value) "Yes" else "No")
  }
  if (is.numeric(value)) {
    return(format(value, digits = 6))
  }
  return(as.character(value))
}

#' Flatten a nested rendering into a sub-bullet paragraph.
#'
#' @param key Label for the nested list.
#' @param rendered Inner Markdown text.
#' @return Markdown lines.
#' @keywords internal
.printed_nested <- function(key, rendered) {
  paste0("- ", key, ":\n", paste0("  ", strsplit(rendered, "\n")[[1]], collapse = "\n"))
}


#' Map internal Markdown keys to display labels aligned with HTML output.
#'
#' @param section_name Top-level description section key.
#' @param key Field key to label.
#' @param parent_key Optional nested parent key.
#' @return Display label.
#' @keywords internal
.display_label_markdown <- function(section_name, key, parent_key = NULL) {
  if (identical(parent_key, "poverty_line")) {
    if (identical(key, "value")) return("Value")
    if (identical(key, "ppp_year")) return("PPP year")
    if (identical(key, "applicable")) return("Applicable")
  }

  if (identical(section_name, "overview")) {
    if (identical(key, "structure_description")) return("Description")
    if (identical(key, "weighting_note")) return("Population note")
    if (identical(key, "ppp_note")) return("PPP note")
  }

  if (identical(section_name, "provenance")) {
    if (identical(key, "release_id")) return("Release")
    if (identical(key, "ppp_year")) return("PPP year")
    if (identical(key, "generated_at")) return("Generated at")
  }

  if (identical(section_name, "surveys_selected")) {
    if (identical(key, "n_loaded")) return("Surveys loaded")
    if (identical(key, "n_excluded")) return("Surveys excluded")
    if (identical(key, "loaded_list")) return("Included surveys")
    if (identical(key, "excluded_list")) return("Excluded surveys")
  }

  if (identical(section_name, "statistics_selected")) {
    if (identical(key, "analysis_var_label")) return("Analysis variable")
    if (identical(key, "analysis_var_type")) return("Analysis variable type")
    if (identical(key, "measures")) return("Selected measures")
    if (identical(key, "poverty_line")) return("Poverty line")
  }

  if (identical(section_name, "filters_applied")) {
    if (identical(key, "filters")) return("Filter criteria")
  }

  if (identical(section_name, "layout_configuration")) {
    if (identical(key, "layout")) return("Layout dimensions")
  }

  if (identical(section_name, "cell_definition")) {
    if (identical(key, "population_scope")) return("Population scope")
    if (identical(key, "measure_interpretation")) return("Measure interpretation")
    if (identical(key, "note")) return("Note")
  }

  if (identical(section_name, "warnings") && identical(key, "warnings")) {
    return("Warnings")
  }

  if (identical(key, "release_id")) return("Release")
  if (identical(key, "ppp_year")) return("PPP year")
  if (identical(key, "generated_at")) return("Generated at")
  if (identical(key, "n_surveys_loaded")) return("Surveys loaded")
  if (identical(key, "n_surveys_excluded")) return("Surveys excluded")
  if (identical(key, "n_filters_applied")) return("Filters applied")
  if (identical(key, "n_measures_computed")) return("Measures computed")
  if (identical(key, "suppression_summary")) return("Suppression")
  if (identical(key, "suppressed_cells")) return("Suppressed cells")

  label <- gsub("_", " ", key, fixed = TRUE)
  if (!nzchar(label)) {
    return(key)
  }
  paste0(toupper(substr(label, 1, 1)), substring(label, 2))
}


#' Map internal table column names to display labels for Markdown headers.
#'
#' @param col_name Internal column name.
#' @return Display label.
#' @keywords internal
.display_table_header_markdown <- function(col_name) {
  mapping <- c(
    pip_id = "Survey identifier",
    country_code = "Country",
    surveyid_year = "Survey year",
    welfare_type_label = "Welfare type",
    measure_label = "Measure",
    stat_group = "Group",
    analysis_var_label = "Variable of analysis",
    slot_label = "Dimension",
    varname = "Variable",
    ui_label = "Label",
    n_categories = "Categories",
    measure = "Measure",
    denominator = "Denominator",
    numerator = "Numerator",
    plain_meaning = "Plain meaning"
  )

  if (col_name %in% names(mapping)) {
    return(unname(mapping[[col_name]]))
  }
  col_name
}


#' Format table cell values for Markdown display aligned with HTML output.
#'
#' @param col_name Internal column name.
#' @param value Scalar cell value.
#' @return Character scalar.
#' @keywords internal
.display_table_value_markdown <- function(col_name, value) {
  if (is.null(value) || length(value) == 0) {
    return("")
  }
  if (is.na(value)) {
    return("")
  }

  if (identical(col_name, "stat_group")) {
    mapping <- c(
      summary_statistics = "Summary statistics",
      inequality = "Inequality",
      poverty = "Poverty",
      shares = "Shares"
    )
    key <- as.character(value)
    if (key %in% names(mapping)) {
      return(unname(mapping[[key]]))
    }
  }

  as.character(value)
}
