#' Render a description model to HTML
#'
#' Converts the structured description model produced by
#' [`build_description_model()`] into readable HTML. Only sections marked as
#' visible are rendered; sections with `NULL` content are skipped.
#'
#' The renderer applies context-aware display labels for internal model keys
#' (for example, `structure_description` -> "Description") without modifying
#' the model schema itself.
#'
#' @param model List produced by [`build_description_model()`] with 9 sections,
#'   each containing `visible`, `title`, and `content` fields.
#' @return A single character scalar containing rendered HTML.
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
#' cat(render_description_html(model))
#' }
#' @keywords internal
render_description_html <- function(model) {
  stopifnot(is.list(model))

  sections <- list()
  for (section_name in names(model)) {
    section <- model[[section_name]]

    visible <- isTRUE(section$visible)
    content <- section$content
    if (!visible || is.null(content)) {
      next
    }

    title <- section$title %||% section_name
    rendered <- .render_section_content_html(content, section_name = section_name)
    if (is.null(rendered) || !nzchar(rendered)) {
      next
    }

    sections[[length(sections) + 1L]] <- list(title = title, rendered = rendered)
  }

  if (length(sections) == 0) {
    return("")
  }

  section_blocks <- vapply(seq_along(sections), function(i) {
    section <- sections[[i]]
    border_style <- if (i < length(sections)) {
      " border-bottom: 1px solid #e4e7eb;"
    } else {
      ""
    }

    paste0(
      "<section style=\"margin: 0 0 22px 0; padding: 0 0 16px 0;",
      border_style,
      "\">",
      "<h2 style=\"margin: 0 0 10px 0; font-size: 20px; font-weight: 700; color: #102a43;\">",
      .html_escape(section$title),
      "</h2>",
      section$rendered,
      "</section>"
    )
  }, character(1))

  return(
    paste0(
      "<div style=\"max-width: 980px; margin: 0 auto; background: #ffffff; border: 1px solid #d9e2ec; border-radius: 10px; padding: 24px 28px; box-shadow: 0 1px 3px rgba(16, 24, 40, 0.08);\">",
      paste(section_blocks, collapse = ""),
      "</div>"
    )
  )
}


#' Render a single section's content to HTML by type.
#'
#' @param content The `content` field of a description model section.
#' @param section_name Section key used for context-aware display labels.
#' @param parent_key Optional parent key for nested rendering context.
#' @return Character scalar HTML, or `NULL` when nothing to render.
#' @keywords internal
.render_section_content_html <- function(content, section_name, parent_key = NULL) {
  if (is.null(content)) {
    return(NULL)
  }

  if (inherits(content, "data.table") || is.data.frame(content)) {
    return(.render_table_html(content))
  }

  if (is.list(content)) {
    return(.render_named_list_html(content, section_name, parent_key = parent_key))
  }

  if (is.character(content)) {
    return(.render_char_vector_html(content))
  }

  text <- .scalar_text_html(content)
  if (is.null(text)) {
    return(NULL)
  }
  return(
    paste0(
      "<p style=\"margin: 0 0 10px 0; color: #334e68;\">",
      .html_escape(text),
      "</p>"
    )
  )
}


#' Render a data.frame/data.table to HTML table.
#'
#' @param dt data.frame or data.table.
#' @return Character scalar HTML table, or NULL for empty frames.
#' @keywords internal
.render_table_html <- function(dt) {
  if (nrow(dt) == 0 || ncol(dt) == 0) {
    return(NULL)
  }

  cols <- names(dt)
  th <- paste0(
    "<th style=\"text-align: left; padding: 8px 10px; border: 1px solid #d9e2ec; background: #f0f4f8; color: #102a43; font-weight: 700;\">",
    .html_escape(vapply(cols, .display_table_header_html, character(1))),
    "</th>",
    collapse = ""
  )

  rows <- vapply(seq_len(nrow(dt)), function(i) {
    values <- vapply(seq_along(cols), function(j) {
      .display_table_value_html(cols[[j]], dt[[cols[[j]]]][[i]])
    }, character(1))
    tds <- paste0(
      "<td style=\"padding: 8px 10px; border: 1px solid #d9e2ec;\">",
      .html_escape(values),
      "</td>",
      collapse = ""
    )
    paste0("<tr>", tds, "</tr>")
  }, character(1))

  return(
    paste0(
      "<table style=\"width: 100%; border-collapse: collapse; margin: 0; font-size: 14px; background: #ffffff;\">",
      "<thead><tr>", th, "</tr></thead>",
      "<tbody>", paste(rows, collapse = ""), "</tbody>",
      "</table>"
    )
  )
}


#' Map internal table cell values to readable display text.
#'
#' @param col_name Internal table column name.
#' @param value Cell value.
#' @return Display-ready text.
#' @keywords internal
.display_table_value_html <- function(col_name, value) {
  if (length(value) == 0 || is.null(value) || is.na(value)) {
    return("")
  }

  text <- as.character(value)

  if (identical(col_name, "stat_group")) {
    stat_group_map <- c(
      summary_statistics = "Summary statistics",
      poverty = "Poverty",
      inequality = "Inequality",
      welfare = "Welfare"
    )

    if (text %in% names(stat_group_map)) {
      return(unname(stat_group_map[[text]]))
    }

    text <- gsub("_", " ", text, fixed = TRUE)
    return(paste0(toupper(substr(text, 1, 1)), substring(text, 2)))
  }

  return(text)
}


#' Render a named list to HTML.
#'
#' @param content Named list.
#' @param section_name Section key used for context-aware display labels.
#' @param parent_key Optional parent key for nested rendering context.
#' @return Character scalar HTML, or NULL when empty.
#' @keywords internal
.render_named_list_html <- function(content, section_name, parent_key = NULL) {
  if (length(content) == 0) {
    return(NULL)
  }

  blocks <- character()
  list_items <- character()

  flush_list_items <- function() {
    if (length(list_items) > 0) {
      blocks <<- c(
        blocks,
        paste0(
          "<ul style=\"margin: 0 0 10px 0; padding-left: 20px;\">",
          paste(list_items, collapse = ""),
          "</ul>"
        )
      )
      list_items <<- character()
    }
  }

  if ("description" %in% names(content)) {
    description_text <- .scalar_text_html(content[["description"]])
    if (!is.null(description_text)) {
      blocks <- c(
        blocks,
        paste0(
          "<p style=\"margin: 0 0 10px 0; color: #334e68;\">",
          .html_escape(description_text),
          "</p>"
        )
      )
    }
    content <- content[names(content) != "description"]
  }

  if (identical(section_name, "overview") && "ppp_note" %in% names(content)) {
    ppp_note_text <- .scalar_text_html(content[["ppp_note"]])
    if (!is.null(ppp_note_text)) {
      blocks <- c(
        blocks,
        paste0(
          "<p style=\"margin: 0 0 10px 0; color: #334e68;\">",
          .html_escape(ppp_note_text),
          "</p>"
        )
      )
    }
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

      poverty_items <- character()
      for (nested_key in c("value", "ppp_year")) {
        nested_value <- value[[nested_key]]
        nested_text <- .scalar_text_html(nested_value)
        if (is.null(nested_text) || is.na(nested_text) || !nzchar(nested_text)) {
          next
        }
        poverty_items <- c(
          poverty_items,
          paste0(
            "<li style=\"margin: 0 0 6px 0;\"><b style=\"color: #334e68;\">",
            .html_escape(.display_label_html(section_name, nested_key, parent_key = "poverty_line")),
            ":</b> ",
            .html_escape(nested_text),
            "</li>"
          )
        )
      }

      if (length(poverty_items) > 0) {
        list_items <- c(
          list_items,
          paste0(
            "<li style=\"margin: 0 0 7px 0;\"><b style=\"color: #243b53;\">",
            .html_escape(.display_label_html(section_name, key, parent_key = parent_key)),
            ":</b>",
            "<ul style=\"margin: 6px 0 0 0; padding-left: 20px;\">",
            paste(poverty_items, collapse = ""),
            "</ul></li>"
          )
        )
      }
      next
    }

    if (inherits(value, "data.table") || is.data.frame(value)) {
      flush_list_items()
      rendered <- .render_table_html(value)
      if (!is.null(rendered)) {
        blocks <- c(
          blocks,
          paste0(
            "<div style=\"margin: 6px 0 6px 0; font-size: 13px; font-weight: 700; color: #486581; text-transform: uppercase; letter-spacing: 0.04em;\">",
            .html_escape(.display_label_html(section_name, key, parent_key = parent_key)),
            "</div>",
            rendered
          )
        )
      }
    } else if (is.list(value)) {
      rendered <- .render_named_list_html(value, section_name, parent_key = key)
      if (!is.null(rendered) && nzchar(rendered)) {
        list_items <- c(
          list_items,
          paste0(
            "<li style=\"margin: 0 0 7px 0;\"><b style=\"color: #243b53;\">",
            .html_escape(.display_label_html(section_name, key, parent_key = parent_key)),
            ":</b>",
            "<div style=\"margin-top: 6px;\">",
            rendered,
            "</div></li>"
          )
        )
      }
    } else if (is.character(value) && length(value) > 1) {
      rendered <- .render_char_vector_html(value)
      if (!is.null(rendered) && nzchar(rendered)) {
        list_items <- c(
          list_items,
          paste0(
            "<li style=\"margin: 0 0 7px 0;\"><b style=\"color: #243b53;\">",
            .html_escape(.display_label_html(section_name, key, parent_key = parent_key)),
            ":</b>",
            "<div style=\"margin-top: 6px;\">",
            rendered,
            "</div></li>"
          )
        )
      }
    } else if (is.null(dim(value))) {
      text <- .scalar_text_html(value)
      if (!is.null(text) && !is.na(text)) {
        list_items <- c(
          list_items,
          paste0(
            "<li style=\"margin: 0 0 7px 0;\"><b style=\"color: #243b53;\">",
            .html_escape(.display_label_html(section_name, key, parent_key = parent_key)),
            ":</b> ",
            .html_escape(text),
            "</li>"
          )
        )
      }
    }
  }

  flush_list_items()

  if (length(blocks) == 0) {
    return(NULL)
  }

  return(paste(blocks, collapse = ""))
}


#' Map internal table column names to display labels for HTML headers.
#'
#' @param col_name Internal column name.
#' @return Display label.
#' @keywords internal
.display_table_header_html <- function(col_name) {
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
    n_categories = "Categories"
  )

  if (col_name %in% names(mapping)) {
    return(unname(mapping[[col_name]]))
  }
  return(col_name)
}


#' Render a character vector as ordered HTML list.
#'
#' @param x Character vector.
#' @return Character scalar HTML, or NULL when empty.
#' @keywords internal
.render_char_vector_html <- function(x) {
  if (is.null(x) || length(x) == 0) {
    return(NULL)
  }

  x <- x[!is.na(x) & nzchar(x)]
  if (length(x) == 0) {
    return(NULL)
  }

  x_names <- names(x)
  items <- vapply(seq_along(x), function(i) {
    label <- if (!is.null(x_names) && !is.na(x_names[i]) && nzchar(x_names[i])) {
      paste0(
        "<b style=\"color: #243b53;\">",
        .html_escape(x_names[i]),
        ":</b> "
      )
    } else {
      ""
    }

    paste0(
      "<li style=\"margin: 0 0 8px 0;\">",
      label,
      .html_escape(x[i]),
      "</li>"
    )
  }, character(1))

  return(
    paste0(
      "<ol style=\"margin: 0; padding-left: 22px;\">",
      paste(items, collapse = ""),
      "</ol>"
    )
  )
}


#' Format scalar values for HTML display.
#'
#' @param value Scalar value.
#' @return Character scalar or NULL when empty.
#' @keywords internal
.scalar_text_html <- function(value) {
  if (is.null(value) || length(value) == 0) {
    return(NULL)
  }
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


#' Escape text for safe HTML insertion.
#'
#' @param x Character vector.
#' @return Character vector with HTML entities escaped.
#' @keywords internal
.html_escape <- function(x) {
  x <- as.character(x)
  x <- gsub("&", "&amp;", x, fixed = TRUE)
  x <- gsub("<", "&lt;", x, fixed = TRUE)
  x <- gsub(">", "&gt;", x, fixed = TRUE)
  x <- gsub('"', "&quot;", x, fixed = TRUE)
  x <- gsub("'", "&#39;", x, fixed = TRUE)
  return(x)
}


#' Map internal keys to display labels in HTML rendering context.
#'
#' @param section_name Top-level description section key.
#' @param key Field key to label.
#' @param parent_key Optional nested parent key.
#' @return Display label.
#' @keywords internal
.display_label_html <- function(section_name, key, parent_key = NULL) {
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
    if (identical(key, "analysis_var_type")) return("Variable type")
    if (identical(key, "measures")) return("Measures")
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

  if (identical(section_name, "execution_summary")) {
    if (identical(key, "n_surveys_loaded")) return("Surveys loaded")
    if (identical(key, "n_surveys_excluded")) return("Surveys excluded")
    if (identical(key, "n_filters_applied")) return("Filters applied")
    if (identical(key, "n_measures_computed")) return("Measures computed")
    if (identical(key, "suppression_summary")) return("Suppression summary")
    if (identical(key, "suppressed_cells")) return("Suppressed cells")
  }

  if (identical(section_name, "warnings") && identical(key, "warnings")) {
    return("Warnings")
  }

  label <- gsub("_", " ", key, fixed = TRUE)
  if (!nzchar(label)) {
    return(key)
  }
  return(paste0(toupper(substr(label, 1, 1)), substring(label, 2)))
}
