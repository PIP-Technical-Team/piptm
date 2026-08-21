# Description model and markdown renderer for table results
#
# Provides functions to build a structured description model from a
# table_result (with_meta = TRUE output) and render it to markdown.
#
# Architecture:
#   table_maker(with_meta = TRUE) → table_result
#   build_description_model(table_result) → description_model
#   render_description_markdown(description_model) → markdown string
#
# The description_model is the reusable, testable intermediate representation.
# Markdown is one renderer; HTML/JSON renderers can be added later.

#' @importFrom cli cli_abort
NULL

# ── build_description_model ───────────────────────────────────────────────────

#' Build a structured description model from a table result
#'
#' Takes the `with_meta = TRUE` return from [table_maker()] and produces a
#' structured list describing the table's contents. The model is an
#' intermediate representation that can be rendered to markdown, HTML, or JSON.
#'
#' @param table_result A list with elements `data`, `specification`, `execution`,
#'   `provenance`, and `warnings`, as returned by `table_maker(with_meta = TRUE)`.
#'
#' @return A named list with sections: `metadata`, `surveys`, `sample`,
#'   `statistics`, `cell_definition`, `suppression`, and optionally `poverty_line`,
#'   `layout`, `excluded_surveys`, and `warnings`.
#'
#' @keywords internal
#' @export
build_description_model <- function(table_result) {

  spec <- table_result$specification
  exec <- table_result$execution
  prov <- table_result$provenance

  # ── metadata ──────────────────────────────────────────────────────────────
  metadata <- list(
    release         = prov$release,
    package_version = prov$package_version,
    ppp             = spec$ppp
  )

  # ── surveys ───────────────────────────────────────────────────────────────
  surveys <- list(
    included = exec$included_surveys
  )

  if (nrow(exec$excluded_surveys) > 0L) {
    surveys$excluded_surveys <- exec$excluded_surveys
  }

  # ── sample ────────────────────────────────────────────────────────────────
  if (!is.null(spec$filter_base) && length(spec$filter_base) > 0L) {
    sample <- list(
      filters_applied = TRUE,
      variables = spec$filter_base
    )
  } else {
    sample <- list(
      filters_applied = FALSE,
      text = "The full survey sample was used (no filters applied)."
    )
  }

  # ── statistics ────────────────────────────────────────────────────────────
  statistics <- list(
    analysis_var = spec$analysis_var,
    measures     = spec$measures
  )

  # ── poverty_line (conditional) ────────────────────────────────────────────
  # Only included when poverty measures are requested

  # ── layout (conditional) ──────────────────────────────────────────────────
  # Only included when by is non-NULL

  # ── cell_definition ───────────────────────────────────────────────────────
  # Always included — generated from layout + statistics

  # ── suppression ───────────────────────────────────────────────────────────
  suppression <- exec$suppression

  # ── warnings (conditional) ────────────────────────────────────────────────
  # Only included when non-empty

  # ── Build model ───────────────────────────────────────────────────────────
  model <- list(
    metadata        = metadata,
    surveys         = surveys,
    sample          = sample,
    statistics      = statistics,
    cell_definition = NULL,  # populated below
    suppression     = suppression
  )

  # Add poverty_line if present
  if (!is.null(spec$poverty_line)) {
    model$poverty_line <- list(
      value = spec$poverty_line,
      ppp   = spec$ppp
    )
  }

  # Add layout if by is present
  if (!is.null(spec$by) && length(spec$by) > 0L) {
    model$layout <- list(variables = spec$by)
  }

  # Add excluded surveys if any
  if (nrow(exec$excluded_surveys) > 0L) {
    model$surveys$excluded_surveys <- exec$excluded_surveys
  }

  # Add warnings if any
  if (length(table_result$warnings) > 0L) {
    model$warnings <- table_result$warnings
  }

  # ── Generate cell definition ──────────────────────────────────────────────
  model$cell_definition <- .generate_cell_definition(spec)

  model
}


# ── render_description_markdown ───────────────────────────────────────────────

#' Render a description model to markdown
#'
#' Takes a description model (from [build_description_model()]) and produces
#' a markdown string suitable for display or PDF conversion.
#'
#' @param model A description model list from [build_description_model()].
#'
#' @return A single character string containing the full markdown document.
#'
#' @keywords internal
#' @export
render_description_markdown <- function(model) {
  parts <- character(0L)

  # ── Header ────────────────────────────────────────────────────────────────
  parts <- c(parts, "# Table Description")
  parts <- c(parts, "")

  # ── Metadata line ─────────────────────────────────────────────────────────
  meta_line <- paste0(
    "**Release:** ", model$metadata$release,
    " \\u00b7 **PPP year:** ", model$metadata$ppp
  )
  parts <- c(parts, meta_line)
  parts <- c(parts, "")

  # ── Surveys ───────────────────────────────────────────────────────────────
  parts <- c(parts, "## Surveys Analyzed")
  parts <- c(parts, "")

  # Survey table
  parts <- c(parts, "| Country | Year | Welfare type | Survey ID |")
  parts <- c(parts, "|---------|------|--------------|-----------|")
  for (i in seq_len(nrow(model$surveys$included))) {
    row <- model$surveys$included[i]
    wt <- if (row$welfare_type == "INC") "Income" else "Consumption"
    parts <- c(parts, paste0("| ", row$country_code, " | ", row$surveyid_year, " | ", wt, " | ", row$pip_id, " |"))
  }
  parts <- c(parts, "")

  # Excluded surveys (if any)
  if (!is.null(model$surveys$excluded_surveys) && nrow(model$surveys$excluded_surveys) > 0L) {
    parts <- c(parts, "### Excluded Surveys")
    parts <- c(parts, "")
    parts <- c(parts, "| Survey ID | Reason |")
    parts <- c(parts, "|-----------|--------|")
    for (i in seq_len(nrow(model$surveys$excluded_surveys))) {
      row <- model$surveys$excluded_surveys[i]
      parts <- c(parts, paste0("| ", row$pip_id, " | ", row$reason, " |"))
    }
    parts <- c(parts, "")
    n_excl <- nrow(model$surveys$excluded_surveys)
    n_total <- nrow(model$surveys$included) + n_excl
    parts <- c(parts, paste0(n_excl, " of ", n_total, " requested survey", if (n_total > 1) "s" else "", " were excluded."))
    parts <- c(parts, "")
  }

  # ── Sample Base ───────────────────────────────────────────────────────────
  parts <- c(parts, "## Sample Base")
  parts <- c(parts, "")

  if (model$sample$filters_applied) {
    parts <- c(parts, "Filtered to:")
    for (var in model$sample$variables) {
      kept_text <- paste(var$kept, collapse = ", ")
      parts <- c(parts, paste0("- **", var$label, ":** ", kept_text))
    }
  } else {
    parts <- c(parts, model$sample$text)
  }
  parts <- c(parts, "")

  # ── Statistics ────────────────────────────────────────────────────────────
  parts <- c(parts, "## Statistics")
  parts <- c(parts, "")
  parts <- c(parts, paste0("- **Analysis variable:** ", model$statistics$analysis_var$label))

  # Poverty line (if present)
  if (!is.null(model$poverty_line)) {
    parts <- c(parts, paste0("- **Poverty line:** $", model$poverty_line$value, " per day (PPP ", model$poverty_line$ppp, ")"))
  }

  measure_labels <- vapply(model$statistics$measures, function(m) m$label, character(1L))
  parts <- c(parts, paste0("- **Measures:** ", paste(measure_labels, collapse = ", ")))
  parts <- c(parts, "")

  # ── Layout (if present) ──────────────────────────────────────────────────
  if (!is.null(model$layout)) {
    parts <- c(parts, "## Table Structure")
    parts <- c(parts, "")

    # Determine role labels
    role_order <- c("super_rows", "rows", "super_columns", "columns")
    role_labels <- c(super_rows = "Super Rows", rows = "Rows", super_columns = "Super Columns", columns = "Columns")

    # For now, assign roles based on order: last = columns, second-to-last = rows, etc.
    n_vars <- length(model$layout$variables)
    roles <- character(n_vars)
    if (n_vars >= 1L) roles[n_vars] <- "columns"
    if (n_vars >= 2L) roles[n_vars - 1L] <- "rows"
    if (n_vars >= 3L) roles[n_vars - 2L] <- "super_columns"
    if (n_vars >= 4L) roles[n_vars - 3L] <- "super_rows"

    for (i in seq_len(n_vars)) {
      var <- model$layout$variables[[i]]
      role_label <- role_labels[roles[i]]
      cat_text <- if (!is.null(var$categories) && length(var$categories) > 0L) {
        cat_labels <- vapply(var$categories, function(c) c$label, character(1L))
        paste0(" (", length(cat_labels), " categories: ", paste(cat_labels, collapse = ", "), ")")
      } else {
        ""
      }
      parts <- c(parts, paste0("- **", role_label, ":** ", var$label, cat_text))
    }
    parts <- c(parts, "")
  }

  # ── Cell Definition ───────────────────────────────────────────────────────
  parts <- c(parts, "## Cell Definition")
  parts <- c(parts, "")
  parts <- c(parts, model$cell_definition)
  parts <- c(parts, "")

  # ── Suppression ───────────────────────────────────────────────────────────
  parts <- c(parts, "## Suppression")
  parts <- c(parts, "")

  if (!is.null(model$suppression$threshold)) {
    threshold <- model$suppression$threshold
    n_suppressed <- model$suppression$n_suppressed_cells
    if (n_suppressed > 0L) {
      parts <- c(parts, paste0("Cells with a population share below ", threshold * 100, "% are suppressed. ", n_suppressed, " cell", if (n_suppressed > 1) "s were" else " was", " suppressed in this table."))
    } else {
      parts <- c(parts, paste0("Cells with a population share below ", threshold * 100, "% are suppressed (non-share measures removed). No cells were suppressed in this table."))
    }
  } else {
    parts <- c(parts, "Suppression is disabled.")
  }
  parts <- c(parts, "")

  # ── Warnings (if any) ────────────────────────────────────────────────────
  if (!is.null(model$warnings) && length(model$warnings) > 0L) {
    parts <- c(parts, "## Warnings")
    parts <- c(parts, "")
    for (w in model$warnings) {
      parts <- c(parts, paste0("- ", w))
    }
    parts <- c(parts, "")
  }

  paste(parts, collapse = "\n")
}


# ── build_table_description ──────────────────────────────────────────────────

#' Generate a markdown description of a table result
#'
#' Convenience wrapper that runs [table_maker()] with metadata capture, builds
#' a [build_description_model()], and renders it to markdown via
#' [render_description_markdown()].
#'
#' @inheritParams table_maker
#'
#' @return A single character string containing the full markdown description
#'   document.
#'
#' @family api
#' @export
#' @examples
#' \dontrun{
#' set_manifest_dir("//server/manifests")
#' set_arrow_root("//server/pip/arrow")
#'
#' md <- build_table_description(
#'   pip_id       = "COL_2010_GEIH_INC_ALL",
#'   analysis_var = "welfare",
#'   measures     = c("mean", "gini"),
#'   by           = c("gender", "area")
#' )
#' cat(md)
#' }
build_table_description <- function(pip_id        = NULL,
                                    analysis_var,
                                    measures,
                                    poverty_line  = NULL,
                                    by            = NULL,
                                    filter_base   = NULL,
                                    ppp           = 2021L,
                                    release       = NULL,
                                    pop_share_threshold = 0.01) {

  result <- table_maker(
    pip_id              = pip_id,
    analysis_var        = analysis_var,
    measures            = measures,
    poverty_line        = poverty_line,
    by                  = by,
    filter_base         = filter_base,
    ppp                 = ppp,
    release             = release,
    pop_share_threshold = pop_share_threshold,
    with_meta           = TRUE
  )

  model <- build_description_model(result)
  render_description_markdown(model)
}


# ── Internal helpers ──────────────────────────────────────────────────────────

#' Generate cell definition sentence from specification
#'
#' @param spec The specification list from table_result.
#' @return A character string describing what each cell represents.
#' @keywords internal
.generate_cell_definition <- function(spec) {
  measure_labels <- vapply(spec$measures, function(m) m$label, character(1L))
  measures_text <- paste(measure_labels, collapse = ", ")
  av_label <- spec$analysis_var$label

  if (is.null(spec$by) || length(spec$by) == 0L) {
    # No disaggregation
    base_text <- paste0(
      "Statistics are computed for the full survey sample."
    )
  } else {
    var_labels <- vapply(spec$by, function(v) v$label, character(1L))
    if (length(var_labels) == 1L) {
      combo_text <- var_labels
    } else if (length(var_labels) == 2L) {
      combo_text <- paste(var_labels, collapse = " and ")
    } else {
      combo_text <- paste(
        paste(var_labels[-length(var_labels)], collapse = ", "),
        "and",
        var_labels[length(var_labels)]
      )
    }
    base_text <- paste0(
      "Each cell represents one combination of ", combo_text, ". ",
      "Statistics are calculated for the weighted population in that subgroup."
    )
  }

  # Add poverty line if present
  if (!is.null(spec$poverty_line)) {
    base_text <- paste0(
      base_text,
      " using a poverty line of $", spec$poverty_line, " per day (PPP ", spec$ppp, ")."
    )
  }

  base_text
}
