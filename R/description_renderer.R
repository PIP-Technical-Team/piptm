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
    rendered <- .render_section_content(content)
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
.render_section_content <- function(content) {
  if (is.null(content)) {
    return(NULL)
  }

  if (inherits(content, "data.table") || is.data.frame(content)) {
    return(.render_table(content))
  }

  if (is.list(content)) {
    return(.render_named_list(content))
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
  header <- paste0("| ", paste(cols, collapse = " | "), " |")
  separator <- paste0("| ", paste(rep("---", length(cols)), collapse = " | "), " |")

  rows <- vapply(seq_len(nrow(dt)), function(i) {
    values <- vapply(dt[i, , drop = TRUE], as.character, character(1))
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
.render_named_list <- function(content) {
  if (length(content) == 0) {
    return(NULL)
  }

  lines <- character()

  # Lead description paragraph, if present.
  if ("description" %in% names(content)) {
    lines <- c(lines, as.character(content[["description"]]))
    content <- content[names(content) != "description"]
  }

  for (key in names(content)) {
    value <- content[[key]]
    if (is.null(value)) {
      next
    }

    if (inherits(value, "data.table") || is.data.frame(value)) {
      rendered <- .render_table(value)
      if (!is.null(rendered)) {
        lines <- c(lines, paste0("**", key, "**"), rendered)
      }
    } else if (is.list(value)) {
      rendered <- .render_named_list(value)
      if (!is.null(rendered)) {
        lines <- c(lines, .printed_nested(key, rendered))
      }
    } else if (is.character(value) && length(value) > 1) {
      rendered <- .render_char_vector(value)
      if (!is.null(rendered)) {
        lines <- c(lines, .printed_nested(key, rendered))
      }
    } else if (is.null(dim(value))) {
      text <- .scalar_text(value)
      if (!is.null(text)) {
        lines <- c(lines, sprintf("- %s: %s", key, text))
      }
    }
  }

  if (length(lines) == 0) {
    return(NULL)
  }
  return(paste(lines, collapse = "\n"))
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