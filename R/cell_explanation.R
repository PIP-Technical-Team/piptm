# Page 2 cell-level natural-language explanation builder
#
# Reads curated prose fragments from `inst/extdata/tm_cell_prose.yaml` (cached
# in `.piptm_env$cell_prose`) and composes the three-section cell explanation
# (`who_is_in_table`, `what_a_cell_is`, `what_is_shown`, `notes`) for
# `POST /cell-definition` without calling `table_maker()` or touching Arrow
# microdata.

#' Load and cache `inst/extdata/tm_cell_prose.yaml`
#'
#' @return Named list parsed from `tm_cell_prose.yaml`.
#' @keywords internal
.get_cell_prose <- function() {
  if (!is.null(.piptm_env$cell_prose)) {
    return(.piptm_env$cell_prose)
  }

  if (!requireNamespace("yaml", quietly = TRUE)) {
    cli::cli_abort(
      c(
        "Package {.pkg yaml} is required to load cell explanation prose.",
        "i" = "Install it with {.code install.packages('yaml')}."
      )
    )
  }

  prose_path <- system.file("extdata", "tm_cell_prose.yaml", package = "piptm")
  if (!nzchar(prose_path) || !file.exists(prose_path)) {
    cli::cli_abort("Could not find {.file inst/extdata/tm_cell_prose.yaml}.")
  }

  parsed <- yaml::read_yaml(prose_path)
  .piptm_env$cell_prose <- parsed
  parsed
}


#' Format a fallback category label when registry or prose is missing
#'
#' @param code Character or numeric category code.
#' @return Character scalar fallback label.
#' @keywords internal
.fallback_cat_label <- function(code) {
  code_chr <- trimws(as.character(code)[[1L]])
  if (grepl("^[0-9]+$", code_chr)) {
    return(paste("code", code_chr))
  }
  if (grepl("^[0-9]+([-+\u2013][0-9]*)+$", code_chr)) {
    return(code_chr)
  }
  clean <- gsub("_", " ", code_chr, fixed = TRUE)
  paste0(toupper(substr(clean, 1L, 1L)), substring(clean, 2L))
}


#' Join character vector with Oxford comma
#'
#' @param items Character vector.
#' @param conj Conjunction (`"and"` or `"or"`).
#' @return Character scalar.
#' @keywords internal
.join_natural <- function(items, conj = "and") {
  items <- as.character(items)
  n <- length(items)
  if (n == 0L) return("")
  if (n == 1L) return(items[[1L]])
  if (n == 2L) return(paste0(items[[1L]], " ", conj, " ", items[[2L]]))
  paste0(paste(items[-n], collapse = ", "), ", ", conj, " ", items[[n]])
}


#' Join multiple category phrases for a single filter variable without repeating
#' shared leading prepositions or trailing nouns
#'
#' @param phrases Character vector of category phrases for one variable.
#' @return Character scalar.
#' @keywords internal
.join_var_phrases <- function(phrases) {
  phrases <- as.character(phrases)
  if (length(phrases) <= 1L) return(phrases)

  prefixes <- c(
    "aged ",
    "with ",
    "living in ",
    "working in ",
    "working as ",
    "in households of ",
    "in welfare quintile ",
    "who are "
  )
  suffixes <- c(
    " secondary education",
    " primary education",
    " education",
    " areas"
  )

  matched_prefix <- ""
  for (p in prefixes) {
    if (all(startsWith(phrases, p))) {
      matched_prefix <- p
      break
    }
  }

  if (nzchar(matched_prefix)) {
    cores <- substring(phrases, nchar(matched_prefix) + 1L)
    matched_suffix <- ""
    for (s in suffixes) {
      if (all(endsWith(cores, s)) && all(nchar(cores) > nchar(s))) {
        matched_suffix <- s
        break
      }
    }
    if (nzchar(matched_suffix)) {
      cores <- substr(cores, 1L, nchar(cores) - nchar(matched_suffix))
    }
    return(paste0(matched_prefix, .join_natural(cores, conj = "or"), matched_suffix))
  }

  .join_natural(phrases, conj = "or")
}


#' Convert a trailing filter phrase into a "who also ..." clause for the 2nd+
#' filter variable when a registry is present
#'
#' @param phrase Character scalar phrase (e.g. `"with incomplete or general secondary education"`).
#' @return Character scalar (e.g. `"who also have incomplete or general secondary education"`).
#' @keywords internal
.to_who_also_clause <- function(phrase) {
  if (startsWith(phrase, "with ")) {
    return(paste0("who also have ", substring(phrase, 6L)))
  }
  if (startsWith(phrase, "without ")) {
    return(paste0("who also lack ", substring(phrase, 9L)))
  }
  if (startsWith(phrase, "who are ")) {
    return(paste0("who are also ", substring(phrase, 9L)))
  }
  if (startsWith(phrase, "living in ")) {
    return(paste0("who also live in ", substring(phrase, 11L)))
  }
  if (startsWith(phrase, "working in ")) {
    return(paste0("who also work in ", substring(phrase, 12L)))
  }
  if (startsWith(phrase, "working as ")) {
    return(paste0("who also work as ", substring(phrase, 12L)))
  }
  if (startsWith(phrase, "aged ") || startsWith(phrase, "in ") || startsWith(phrase, "outside ")) {
    return(paste0("who are also ", phrase))
  }
  paste0("and ", phrase)
}


#' Capitalize the first character of a string
#'
#' @param x Character scalar.
#' @return Character scalar.
#' @keywords internal
.cap_first <- function(x) {
  if (is.null(x) || !nzchar(x)) return(x)
  paste0(toupper(substr(x, 1L, 1L)), substring(x, 2L))
}


#' Look up prose metadata for a variable and category code
#'
#' When `reg` is `NULL` (registry unavailable), follows R5 strictly: every
#' category label degrades to the raw code (`"code 1"`, etc.) without reading
#' category phrases from `tm_cell_prose.yaml`, while still using the variable's
#' human `slot_noun` when available so raw variable names like `"educat4"` are
#' not shown to the user.
#'
#' @param varname Character scalar variable name.
#' @param code    Category code (character or integer).
#' @param reg     Loaded release variable registry list, or `NULL`.
#' @param prose   Parsed `tm_cell_prose.yaml` list.
#' @return Named list with `code`, `label`, `slot_noun`, `noun`, `adj`,
#'   `phrase`, and `filter_noun`.
#' @keywords internal
.lookup_cat_prose <- function(varname, code, reg, prose) {
  code_chr  <- as.character(code)[[1L]]
  reg_entry <- if (!is.null(reg)) reg[[varname]] else NULL
  v_prose   <- if (!is.null(prose$variables)) prose$variables[[varname]] else NULL

  slot_noun <- if (!is.null(v_prose$slot_noun) && nzchar(v_prose$slot_noun)) {
    as.character(v_prose$slot_noun)[[1L]]
  } else if (!is.null(reg_entry$ui_label) && nzchar(as.character(reg_entry$ui_label)[[1L]])) {
    tolower(as.character(reg_entry$ui_label)[[1L]])
  } else {
    gsub("_", " ", varname, fixed = TRUE)
  }

  # R5: When registry is unavailable, degrade category label strictly to raw code
  if (is.null(reg)) {
    display_label <- .fallback_cat_label(code_chr)
    return(list(
      code        = code_chr,
      label       = display_label,
      slot_noun   = slot_noun,
      noun        = NULL,
      adj         = NULL,
      phrase      = sprintf("whose %s is %s", slot_noun, display_label),
      filter_noun = sprintf("people whose %s is %s", slot_noun, display_label)
    ))
  }

  # Look up category label in release registry
  reg_label <- NULL
  if (!is.null(reg_entry$categories) && length(reg_entry$categories) > 0L) {
    for (cat_item in reg_entry$categories) {
      c_code <- as.character(cat_item$code)[[1L]]
      c_lbl  <- as.character(cat_item$label)[[1L]]
      if (identical(tolower(c_code), tolower(code_chr)) ||
          identical(tolower(c_lbl), tolower(code_chr))) {
        reg_label <- c_lbl
        code_chr  <- c_code
        break
      }
    }
  }

  display_label <- reg_label %||% .fallback_cat_label(code_chr)

  if (!is.null(reg_label) && !is.null(v_prose)) {
    noun_val <- if (!is.null(v_prose$nouns) && !is.null(v_prose$nouns[[code_chr]])) {
      as.character(v_prose$nouns[[code_chr]])[[1L]]
    } else {
      NULL
    }

    adj_val <- if (!is.null(v_prose$adjectives) && !is.null(v_prose$adjectives[[code_chr]])) {
      as.character(v_prose$adjectives[[code_chr]])[[1L]]
    } else {
      NULL
    }

    phrase_val <- if (!is.null(v_prose$overrides) && !is.null(v_prose$overrides[[code_chr]])) {
      as.character(v_prose$overrides[[code_chr]])[[1L]]
    } else if (!is.null(v_prose$phrase_frame) && nzchar(v_prose$phrase_frame)) {
      pf <- as.character(v_prose$phrase_frame)[[1L]]
      pf <- gsub("{label_lower}", tolower(reg_label), pf, fixed = TRUE)
      pf <- gsub("{label}",       reg_label,          pf, fixed = TRUE)
      pf
    } else {
      sprintf("whose %s is %s", slot_noun, display_label)
    }

    filter_noun_val <- if (!is.null(noun_val) && nzchar(noun_val)) {
      noun_val
    } else {
      paste("people", phrase_val)
    }

    return(list(
      code        = code_chr,
      label       = display_label,
      slot_noun   = slot_noun,
      noun        = noun_val,
      adj         = adj_val,
      phrase      = phrase_val,
      filter_noun = filter_noun_val
    ))
  }

  # Runtime fallback when variable or category is absent from registry/prose
  list(
    code        = code_chr,
    label       = display_label,
    slot_noun   = slot_noun,
    noun        = NULL,
    adj         = NULL,
    phrase      = sprintf("whose %s is %s", slot_noun, display_label),
    filter_noun = sprintf("people whose %s is %s", slot_noun, display_label)
  )
}


#' Pick the first category code for a layout slot variable deterministically
#'
#' @param varname Character scalar variable name.
#' @param reg     Loaded release variable registry list, or `NULL`.
#' @return Character scalar category code.
#' @keywords internal
.first_slot_code <- function(varname, reg) {
  reg_entry <- if (!is.null(reg)) reg[[varname]] else NULL
  if (!is.null(reg_entry$categories) && length(reg_entry$categories) > 0L) {
    return(as.character(reg_entry$categories[[1L]]$code)[[1L]])
  }
  "1"
}


#' Format poverty line phrase
#'
#' @param poverty_line Numeric vector or NULL.
#' @param ppp          Integer scalar PPP year or NULL.
#' @return Named list with `short` (e.g. `"$2.15 a day"`) and `full` (e.g.
#'   `"$2.15 a day (2021 PPP)"`).
#' @keywords internal
.format_pov_line <- function(poverty_line, ppp) {
  if (is.null(poverty_line) || length(poverty_line) == 0L) {
    short <- "the poverty line"
    full  <- if (!is.null(ppp)) sprintf("the poverty line (%s PPP)", as.character(ppp)[[1L]]) else short
    return(list(short = short, full = full))
  }
  vals <- vapply(poverty_line, function(x) {
    paste0("$", format(as.numeric(x), trim = TRUE, scientific = FALSE))
  }, character(1))
  joined <- .join_natural(vals, conj = "or")
  short <- paste0(joined, " a day")
  full  <- if (!is.null(ppp)) sprintf("%s (%s PPP)", short, as.character(ppp)[[1L]]) else short
  list(short = short, full = full)
}


#' Build the natural-language cell explanation for a Page 2 configuration
#'
#' Pure function of the Page 2 configuration parameters, the release variable
#' registry (if loaded), and `inst/extdata/tm_cell_prose.yaml`. Never calls
#' [table_maker()], never loads Arrow microdata, and never emits sample sizes
#' or computed data values.
#'
#' @param analysis_var        Character scalar analysis variable name.
#' @param measures            Character vector of measure names.
#' @param poverty_line        Optional numeric poverty line(s).
#' @param column              Optional variable assigned to the Columns slot.
#' @param row                 Optional variable assigned to the Rows slot.
#' @param superColumn         Optional variable assigned to the Super-columns slot.
#' @param superRow            Optional variable assigned to the Super-rows slot.
#' @param filter_base         Optional named list of sample-base filter codes.
#' @param ppp                 Optional integer PPP reference year (default `2021L`).
#' @param pop_share_threshold Optional numeric cell-suppression threshold in
#'   `(0, 1)`, or `NULL` when suppression is disabled.
#' @param welfare_type        Optional `"INC"` or `"CON"` so welfare prose says
#'   `"daily income"` or `"daily consumption"` instead of `"daily welfare"`.
#' @param release             Optional character scalar release ID.
#'
#' @return A named list with elements `schema_version`, `who_is_in_table`,
#'   `what_a_cell_is`, `what_is_shown`, `notes`, and `warnings`.
#' @export
build_cell_explanation <- function(analysis_var,
                                   measures,
                                   poverty_line = NULL,
                                   column = NULL,
                                   row = NULL,
                                   superColumn = NULL,
                                   superRow = NULL,
                                   filter_base = NULL,
                                   ppp = 2021L,
                                   pop_share_threshold = 0.001,
                                   welfare_type = NULL,
                                   release = NULL) {
  prose <- .get_cell_prose()
  warnings_out <- character(0)

  # ── 1. Resolve release & variable registry (R5) ────────────────────────────
  rel_resolved <- if (is.null(release) || !nzchar(trimws(as.character(release)[[1L]]))) {
    tryCatch(piptm_current_release(), error = function(e) NULL)
  } else {
    trimws(as.character(release)[[1L]])
  }

  reg <- tryCatch(piptm_variable_registry(rel_resolved), error = function(e) NULL)
  if (is.null(reg)) {
    rel_label <- if (!is.null(rel_resolved) && nzchar(rel_resolved)) rel_resolved else "unresolved"
    warnings_out <- c(
      warnings_out,
      sprintf("Variable registry unavailable for release %s; using fallback labels.", rel_label)
    )
  }

  # ── 2. Classify measures & analysis variable ───────────────────────────────
  measures <- as.character(measures)
  share_keys <- c("pop_share", "target_within_group_share", "target_survey_share")
  any_shares <- any(measures %in% share_keys)
  n_shares   <- sum(measures %in% share_keys)

  wt_norm <- if (!is.null(welfare_type) && nzchar(trimws(as.character(welfare_type)[[1L]]))) {
    toupper(trimws(as.character(welfare_type)[[1L]]))
  } else {
    NULL
  }

  avar_prose <- prose$analysis_vars[[analysis_var]]
  if (!is.null(avar_prose)) {
    is_monetary <- isTRUE(avar_prose$monetary)
    var_noun <- if (identical(wt_norm, "INC") && !is.null(avar_prose$noun_inc)) {
      avar_prose$noun_inc
    } else if (identical(wt_norm, "CON") && !is.null(avar_prose$noun_con)) {
      avar_prose$noun_con
    } else {
      avar_prose$noun_default %||% analysis_var
    }
    short_var_noun <- if (identical(wt_norm, "INC") && !is.null(avar_prose$short_noun_inc)) {
      avar_prose$short_noun_inc
    } else if (identical(wt_norm, "CON") && !is.null(avar_prose$short_noun_con)) {
      avar_prose$short_noun_con
    } else {
      avar_prose$short_noun_default %||% var_noun
    }
  } else {
    is_monetary    <- identical(analysis_var, "welfare") || identical(analysis_var, "pov_status")
    var_noun       <- gsub("_", " ", analysis_var, fixed = TRUE)
    short_var_noun <- var_noun
  }

  ppp_suffix <- if (is_monetary && !is.null(ppp)) {
    sprintf(" (%s PPP dollars)", as.character(ppp)[[1L]])
  } else {
    ""
  }
  pov_line_fmt <- .format_pov_line(poverty_line, ppp)

  # ── 3. Filter base & Section 1: who_is_in_table (R4, R6) ───────────────────
  has_filter <- !is.null(filter_base) && length(filter_base) > 0L
  is_short_filter <- has_filter && length(filter_base) == 1L && length(filter_base[[1L]]) <= 2L

  filter_phrases_trailing <- character(0)
  if (!has_filter) {
    table_pop_short <- "everyone in the survey"
    table_pop_denom <- "everyone in the survey"
    who_text <- if (any_shares) {
      "Everyone in the survey. No filter is applied. That group is the 100% for the table."
    } else {
      "Everyone in the survey. No filter is applied."
    }
  } else {
    f_vars <- names(filter_base)
    var_phrases <- character(length(f_vars))
    first_var_noun_only <- NULL

    for (i in seq_along(f_vars)) {
      fv <- f_vars[[i]]
      fcodes <- as.character(unlist(filter_base[[i]], use.names = FALSE))
      cat_infos <- lapply(fcodes, function(cd) .lookup_cat_prose(fv, cd, reg, prose))
      fphrases <- vapply(cat_infos, function(ci) ci$phrase, character(1))
      joined_phrase <- .join_var_phrases(fphrases)
      var_phrases[[i]] <- joined_phrase

      if (i == 1L && length(f_vars) == 1L) {
        all_have_noun <- all(vapply(cat_infos, function(ci) !is.null(ci$noun) && nzchar(ci$noun), logical(1)))
        if (all_have_noun) {
          fnouns <- vapply(cat_infos, function(ci) ci$noun, character(1))
          first_var_noun_only <- .join_natural(fnouns, conj = "or")
        }
      }

      if (is_short_filter) {
        filter_phrases_trailing <- c(filter_phrases_trailing, joined_phrase)
      }
    }

    # Combine different filter variables: when registry is loaded, connect 2nd+
    # filter variables with "who also ..." ("People aged 15-24 or 25-64 who also
    # have incomplete or general secondary education"); when fallback ("whose ..."),
    # join with "and".
    if (!is.null(first_var_noun_only)) {
      full_filter_group <- first_var_noun_only
    } else if (length(var_phrases) == 1L) {
      full_filter_group <- paste("people", var_phrases[[1L]])
    } else if (all(startsWith(var_phrases, "whose "))) {
      full_filter_group <- paste("people", .join_natural(var_phrases, conj = "and"))
    } else {
      tail_clauses <- vapply(var_phrases[-1L], .to_who_also_clause, character(1))
      full_filter_group <- paste(c("people", var_phrases[[1L]], .join_natural(tail_clauses, conj = "and")), collapse = " ")
    }

    if (is_short_filter) {
      table_pop_short <- full_filter_group
      table_pop_denom <- sprintf("all %s in the survey", full_filter_group)
    } else {
      table_pop_short <- "everyone in the table (the filtered group above)"
      table_pop_denom <- "everyone in the table (the filtered group above)"
    }

    if (any_shares && is_short_filter) {
      who_text <- sprintf(
        "All %s in the survey. That group is the 100%% for the table.",
        full_filter_group
      )
    } else if (any_shares) {
      who_text <- sprintf(
        "%s. Anyone outside this group is excluded before the table is built. That group is the 100%% for the table.",
        .cap_first(full_filter_group)
      )
    } else {
      who_text <- sprintf(
        "%s. Anyone outside this group is excluded before the table is built.",
        .cap_first(full_filter_group)
      )
    }
  }

  who_is_in_table <- list(
    title    = "Who is in the table",
    filtered = has_filter,
    text     = who_text
  )

  # ── 4. Active layout slots & Section 2: what_a_cell_is (R6) ────────────────
  display_slots <- list(
    list(slot = "rows",         arg = row,         role_label = "row"),
    list(slot = "columns",      arg = column,      role_label = "column"),
    list(slot = "superRows",    arg = superRow,    role_label = "row group"),
    list(slot = "superColumns", arg = superColumn, role_label = "column group")
  )
  active_display <- Filter(function(s) !is.null(s$arg) && nzchar(trimws(as.character(s$arg)[[1L]])), display_slots)

  if (length(active_display) == 0L) {
    what_a_cell_is <- list(
      title              = "What a cell is",
      dimensions         = list(),
      structure_text     = sprintf(
        "No row or column variables are selected, so the table has a single cell covering %s.",
        table_pop_short
      ),
      example_cell_label = table_pop_short,
      example_text       = NULL
    )
    cell_group <- table_pop_short
  } else {
    slot_resolved <- lapply(active_display, function(s) {
      vname <- trimws(as.character(s$arg)[[1L]])
      fcode <- .first_slot_code(vname, reg)
      cinfo <- .lookup_cat_prose(vname, fcode, reg, prose)
      list(
        slot             = s$slot,
        role_label       = s$role_label,
        varname          = vname,
        slot_noun        = cinfo$slot_noun,
        example_category = cinfo$label,
        cat_info         = cinfo
      )
    })

    dimensions_out <- lapply(slot_resolved, function(sr) {
      list(
        slot             = sr$slot,
        varname          = sr$varname,
        label            = sr$slot_noun,
        example_category = sr$example_category
      )
    })

    if (length(slot_resolved) == 1L) {
      sr1 <- slot_resolved[[1L]]
      structure_text <- sprintf(
        "Each cell is one %s group (%s).",
        sr1$slot_noun,
        sr1$role_label
      )
    } else {
      dim_phrases <- vapply(slot_resolved, function(sr) {
        sprintf("%s (%s)", sr$slot_noun, sr$role_label)
      }, character(1))
      structure_text <- sprintf(
        "Each cell is one combination of %s.",
        .join_natural(dim_phrases, conj = "and")
      )
    }

    active_varnames <- vapply(slot_resolved, function(sr) sr$varname, character(1))
    if ("pov_status" %in% active_varnames && !is.null(poverty_line)) {
      structure_text <- paste0(
        substr(structure_text, 1L, nchar(structure_text) - 1L),
        sprintf(", where \"poor\" means living on less than %s.", pov_line_fmt$full)
      )
    }

    # Head-noun precedence order (R6): column > row > superColumn > superRow
    precedence_order <- c("columns", "rows", "superColumns", "superRows")
    head_slot <- NULL
    head_noun <- "people"
    for (p_slot in precedence_order) {
      idx <- match(p_slot, vapply(slot_resolved, function(sr) sr$slot, character(1)))
      if (!is.na(idx)) {
        cand_noun <- slot_resolved[[idx]]$cat_info$noun
        if (!is.null(cand_noun) && nzchar(cand_noun)) {
          head_slot <- p_slot
          head_noun <- cand_noun
          break
        }
      }
    }

    # Pre-nominal adjectives (e.g. "non-poor men") vs trailing phrases
    pre_modifiers <- character(0)
    trailing      <- character(0)
    for (sr in slot_resolved) {
      if (!identical(sr$slot, head_slot)) {
        if (!is.null(sr$cat_info$adj) && nzchar(sr$cat_info$adj)) {
          pre_modifiers <- c(pre_modifiers, sr$cat_info$adj)
        } else {
          trailing <- c(trailing, sr$cat_info$phrase)
        }
      }
    }

    # Weave short filter phrase into cell_group ONLY when no share measures are requested
    if (!any_shares && is_short_filter && !(names(filter_base)[[1L]] %in% active_varnames)) {
      trailing <- c(trailing, filter_phrases_trailing)
    }

    head_with_mods <- paste(c(pre_modifiers, head_noun), collapse = " ")

    # When all trailing clauses are "whose ..." fallback clauses, join with commas and "and"
    if (length(trailing) > 1L && all(startsWith(trailing, "whose "))) {
      cell_group <- paste(head_with_mods, .join_natural(trailing, conj = "and"))
    } else {
      cell_group <- paste(c(head_with_mods, trailing), collapse = " ")
    }

    example_text <- if (has_filter && (!is_short_filter || any_shares)) {
      sprintf("For example, the first cell is %s \u2014 within the filtered group above.", cell_group)
    } else {
      sprintf("For example, the first cell is %s.", cell_group)
    }

    what_a_cell_is <- list(
      title              = "What a cell is",
      dimensions         = dimensions_out,
      structure_text     = structure_text,
      example_cell_label = cell_group,
      example_text       = example_text
    )
  }

  # ── 5. Section 3: what_is_shown (R7) ───────────────────────────────────────
  target_info <- prose$targets[[analysis_var]]
  target_predicate <- if (!is.null(target_info$predicate)) {
    as.character(target_info$predicate)[[1L]]
  } else {
    sprintf("have %s", var_noun)
  }
  target_modifier <- if (!is.null(target_info$modifier) && nzchar(as.character(target_info$modifier)[[1L]])) {
    as.character(target_info$modifier)[[1L]]
  } else {
    NULL
  }
  target_rate_noun <- if (!is.null(target_info$rate_noun)) {
    as.character(target_info$rate_noun)[[1L]]
  } else {
    sprintf("rate of %s", var_noun)
  }

  target_cell_group <- if (length(active_display) == 0L) {
    base_group_for_target <- if (!has_filter) "people in the survey" else table_pop_short
    if (!is.null(target_modifier)) {
      paste(target_modifier, base_group_for_target)
    } else {
      paste(base_group_for_target, "who", target_predicate)
    }
  } else if (!is.null(target_modifier)) {
    paste(target_modifier, cell_group)
  } else {
    paste(cell_group, "who", target_predicate)
  }

  both_target_shares <- all(c("target_within_group_share", "target_survey_share") %in% measures)

  measures_out <- lapply(measures, function(mkey) {
    m_prose <- prose$measures[[mkey]]
    m_label <- if (!is.null(m_prose$label)) as.character(m_prose$label)[[1L]] else mkey

    if (mkey %in% share_keys) {
      if (length(active_display) == 0L) {
        if (identical(mkey, "pop_share")) {
          counts_val <- sprintf("%s (everyone in the single cell)", table_pop_short)
          out_of_val <- sprintf("%s (everyone in the table)", table_pop_denom)
          text_val   <- sprintf("Out of %s, the percentage in the single cell (always 100%%).", table_pop_denom)
          total_val  <- "Always 100% because the table has a single cell."
        } else if (identical(mkey, "target_within_group_share")) {
          counts_val <- target_cell_group
          out_of_val <- sprintf("%s (everyone in the single cell)", table_pop_denom)
          text_val   <- sprintf("Out of %s, the percentage who %s.", table_pop_denom, target_predicate)
          total_val  <- if (both_target_shares) {
            sprintf(
              "With no row or column breakdown, Target share within cell and Target share in sample base collapse to the same overall %s of %s.",
              target_rate_noun,
              table_pop_short
            )
          } else {
            sprintf(
              "With no row or column breakdown, the single cell equals the overall %s of %s.",
              target_rate_noun,
              table_pop_short
            )
          }
        } else {
          counts_val <- target_cell_group
          out_of_val <- sprintf("%s (everyone in the table)", table_pop_denom)
          text_val   <- sprintf("Out of %s, the percentage who %s.", table_pop_denom, target_predicate)
          total_val  <- if (both_target_shares) {
            sprintf(
              "With no row or column breakdown, Target share in sample base and Target share within cell collapse to the same overall %s of %s.",
              target_rate_noun,
              table_pop_short
            )
          } else {
            sprintf(
              "With no row or column breakdown, the single cell equals the overall %s of %s.",
              target_rate_noun,
              table_pop_short
            )
          }
        }
      } else if (identical(mkey, "pop_share")) {
        counts_val <- sprintf("%s (everyone in the cell)", cell_group)
        out_of_val <- sprintf("%s (everyone in the table)", table_pop_denom)
        text_val   <- sprintf("Out of %s, the percentage who are %s.", table_pop_denom, cell_group)
        total_val  <- "All cells in the table add up to 100%."
      } else if (identical(mkey, "target_within_group_share")) {
        counts_val <- target_cell_group
        out_of_val <- sprintf("%s (everyone in that one cell)", cell_group)
        text_val   <- sprintf("Out of %s, the percentage who %s.", cell_group, target_predicate)
        total_val  <- sprintf(
          "Does not add up to 100%% (each cell is its own %s).",
          target_rate_noun
        )
      } else {
        counts_val <- target_cell_group
        out_of_val <- sprintf("%s (everyone in the table)", table_pop_denom)
        text_val   <- sprintf("Out of %s, the percentage who are %s.", table_pop_denom, target_cell_group)
        total_val  <- sprintf(
          "All cells together add up to the overall %s of %s.",
          target_rate_noun,
          table_pop_short
        )
      }

      return(list(
        key        = mkey,
        label      = m_label,
        counts     = counts_val,
        out_of     = out_of_val,
        text       = text_val,
        total_note = total_val
      ))
    }

    # Non-share measure: render template
    tmpl <- if (!is.null(m_prose$template)) {
      as.character(m_prose$template)[[1L]]
    } else {
      paste0(.cap_first(m_label), " of {var_noun} for {cell_group}{ppp_suffix}.")
    }

    txt <- tmpl
    txt <- gsub("{Var_noun}",       .cap_first(var_noun),       txt, fixed = TRUE)
    txt <- gsub("{var_noun}",       var_noun,                   txt, fixed = TRUE)
    txt <- gsub("{Short_var_noun}", .cap_first(short_var_noun), txt, fixed = TRUE)
    txt <- gsub("{short_var_noun}", short_var_noun,             txt, fixed = TRUE)
    txt <- gsub("{cell_group}",     cell_group,                 txt, fixed = TRUE)
    txt <- gsub("{ppp_suffix}",     ppp_suffix,                 txt, fixed = TRUE)
    txt <- gsub("{pov_line_text}",  pov_line_fmt$full,          txt, fixed = TRUE)
    txt <- gsub("{pov_line_short}", pov_line_fmt$short,         txt, fixed = TRUE)
    txt <- .cap_first(txt)

    list(
      key        = mkey,
      label      = m_label,
      counts     = NULL,
      out_of     = NULL,
      text       = txt,
      total_note = NULL
    )
  })

  what_is_shown_intro <- if (n_shares >= 2L) {
    "The cell shows multiple percentages. Notice how they count the same or different groups against two different 100% totals (either the cell or the whole table):"
  } else {
    NULL
  }

  what_is_shown <- list(
    title    = "What is shown in that cell",
    intro    = what_is_shown_intro,
    measures = measures_out
  )

  # ── 6. Notes (R8) ──────────────────────────────────────────────────────────
  notes_out <- character(0)

  if (length(active_display) > 0L) {
    active_varnames <- vapply(slot_resolved, function(sr) sr$varname, character(1))

    # R8(a): pov_status in a layout slot alongside a non-poverty measure
    poverty_measures <- c("headcount", "poverty_gap", "poverty_severity", "severity", "watts", "npoor", "pop_poverty")
    has_non_pov_measure <- any(!measures %in% poverty_measures)
    if ("pov_status" %in% active_varnames && has_non_pov_measure) {
      pov_idx <- match("pov_status", active_varnames)
      pov_role <- slot_resolved[[pov_idx]]$role_label
      notes_out <- c(
        notes_out,
        sprintf(
          "The %s line is only used to split the %ss into poor and non-poor. It does not change how the statistic itself is calculated.",
          pov_line_fmt$short,
          pov_role
        )
      )
    }

    # R8(b): filter variable also used in a layout slot
    if (has_filter) {
      overlap_vars <- intersect(names(filter_base), active_varnames)
      for (ov in overlap_vars) {
        ov_idx <- match(ov, active_varnames)
        ov_role <- slot_resolved[[ov_idx]]$role_label
        ov_lbl  <- .cap_first(slot_resolved[[ov_idx]]$slot_noun)
        notes_out <- c(
          notes_out,
          sprintf(
            "%s is used both as a sample filter and as a %s. Categories excluded by the filter will be empty.",
            ov_lbl,
            ov_role
          )
        )
      }
    }

    # R8(c): NR suppression note (parity with table_maker.R:903-915: active slots > 0 && !is.null(pop_share_threshold))
    if (!is.null(pop_share_threshold) && is.numeric(pop_share_threshold) &&
        length(pop_share_threshold) == 1L && is.finite(pop_share_threshold) &&
        pop_share_threshold > 0 && pop_share_threshold < 1) {
      pct_str <- format(pop_share_threshold * 100, trim = TRUE, scientific = FALSE)
      notes_out <- c(
        notes_out,
        sprintf(
          "Cells covering less than %s%% of the table are shown as \"NR\" instead of a number.",
          pct_str
        )
      )
    }
  }

  list(
    schema_version  = as.character(prose$schema_version %||% "1.0")[[1L]],
    who_is_in_table = who_is_in_table,
    what_a_cell_is  = what_a_cell_is,
    what_is_shown   = what_is_shown,
    notes           = as.list(notes_out),
    warnings        = as.list(warnings_out)
  )
}
