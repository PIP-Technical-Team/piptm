#' @importFrom data.table is.data.table data.table setcolorder set fsetdiff
#' @importFrom cli cli_abort cli_warn
#' @importFrom collapse GRP
NULL

# ── Internal helpers ─────────────────────────────────────────────────────────

# Null-coalescing operator (used internally; not exported)
`%||%` <- function(x, y) if (!is.null(x)) x else y

# ── pip_lookup ────────────────────────────────────────────────────────────────

#' Resolve survey triplets to pip_ids via the manifest
#'
#' Translates human-friendly survey identifiers (country code, year, welfare
#' type) into the canonical `pip_id` strings stored in the manifest.  This is
#' the recommended entry point for ad-hoc R usage when the full `pip_id` string
#' is not known.
#'
#' The function performs an inner join against the manifest for the specified
#' release.  Unmatched triplets emit a [cli::cli_warn()] listing the
#' unresolved combinations but do not stop execution.
#'
#' @param country_code Character vector of ISO3 country codes
#'   (e.g. `c("COL", "BOL")`).
#' @param year Integer vector of survey years (same length as `country_code`).
#' @param welfare_type Character vector of welfare types (`"INC"` or `"CON"`,
#'   same length as `country_code`).
#' @param release Character scalar release ID (e.g. `"20260206"`). Defaults
#'   to the current release as returned by [piptm_current_release()].
#'
#' @return Character vector of matching `pip_id` strings (one per matched
#'   triplet, in the order returned by the manifest join).  May be shorter
#'   than the input if some triplets are unmatched.
#'
#' @family api
#' @export
#' @examples
#' \dontrun{
#' set_manifest_dir("//server/manifests")
#' ids <- pip_lookup(
#'   country_code = c("COL", "BOL"),
#'   year         = c(2010L, 2000L),
#'   welfare_type = c("INC", "INC")
#' )
#' }
pip_lookup <- function(country_code, year, welfare_type, release = NULL) {

  # ── Guard: equal-length vectors ────────────────────────────────────────────
  n <- length(country_code)
  if (length(year) != n || length(welfare_type) != n) {
    cli_abort(
      c(
        "{.arg country_code}, {.arg year}, and {.arg welfare_type} must all have the same length.",
        "i" = "Lengths: {.arg country_code} = {n}, {.arg year} = {length(year)}, {.arg welfare_type} = {length(welfare_type)}."
      )
    )
  }

  # ── Build query table ───────────────────────────────────────────────────────
  query <- data.table(
    country_code = country_code,
    year         = as.integer(year),
    welfare_type = welfare_type
  )

  # ── Join against manifest ───────────────────────────────────────────────────
  mf      <- piptm_manifest(release)
  matched <- mf[query, on = .(country_code, year, welfare_type), nomatch = NULL]

  # ── Warn about unmatched triplets ───────────────────────────────────────────
  if (nrow(matched) < n) {
    unmatched <- data.table::fsetdiff(
      query,
      matched[, .(country_code, year, welfare_type)]
    )
    labels <- paste(
      unmatched$country_code,
      unmatched$year,
      unmatched$welfare_type,
      sep = "-"
    )
    cli_warn(
      c(
        "{nrow(unmatched)} survey triplet{?s} not found in manifest for release {.val {mf$release[[1L]] %||% release}}:",
        "i" = "{.val {labels}}"
      )
    )
  }

  matched$pip_id
}

# ── .build_description_metadata ───────────────────────────────────────────────

#' Build description metadata for table_maker results
#'
#' Internal helper that assembles execution metadata, provenance, and resolved
#' labels for use by the `/description` endpoint.
#'
#' @param params Named list of function parameters echoed from table_maker()
#' @param result data.table result from table_maker()
#' @param release Character scalar release ID
#' @param excluded_surveys data.table with pip_id and reason columns
#' @param suppressed_cells data.table of suppressed cells, or NULL
#' @param captured_warnings Character vector of warning messages
#'
#' @return Named list conforming to spec §2.1 schema
#' @keywords internal
.build_description_metadata <- function(params, result, release, 
                                       excluded_surveys, suppressed_cells, 
                                       captured_warnings) {
  # ── Provenance ────────────────────────────────────────────────────────────
  provenance <- list(
    release = release,
    ppp_year = params$ppp,
    generated_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S UTC", tz = "UTC")
  )
  
  # ── Surveys ───────────────────────────────────────────────────────────────
  # Extract loaded surveys from result
  loaded_pip_ids <- unique(result$pip_id)
  manifest <- piptm_manifest(release)
  loaded_surveys <- manifest[pip_id %chin% loaded_pip_ids, 
                             .(pip_id, country_code, country_name, 
                               surveyid_year, welfare_type)]
  
  # ── Resolved Labels ───────────────────────────────────────────────────────
  # Analysis variable
  reg <- piptm_variable_registry(release)
  analysis_var_entry <- reg[[params$analysis_var]]
  if (is.null(analysis_var_entry)) {
    analysis_var_info <- list(
      varname = params$analysis_var,
      ui_label = params$analysis_var,
      tm_type = "unknown"
    )
  } else {
    analysis_var_info <- list(
      varname = analysis_var_entry$varname,
      ui_label = analysis_var_entry$ui_label,
      tm_type = analysis_var_entry$tm_type
    )
  }
  
  # Measures
  stat_groups <- piptm_stat_groups(release)
  measures_dt <- data.table::rbindlist(lapply(stat_groups, function(group) {
    group_name <- group$group
    group_measures <- group$measures
    data.table::rbindlist(lapply(group_measures, function(m) {
      data.table::data.table(
        measure = m$measure,
        ui_label = m$label,
        stat_group = group_name
      )
    }), use.names = TRUE, fill = TRUE)
  }), use.names = TRUE, fill = TRUE)
  
  # Filter to requested measures only
  measures_dt <- measures_dt[measure %chin% params$measures]
  
  # Filters
  filters_dt <- NULL
  if (!is.null(params$filter_base)) {
    filter_cats <- piptm_filter_categories(release)
    filters_list <- lapply(names(params$filter_base), function(varname) {
      # Find the filter category entry
      filter_entry <- Filter(function(x) x$varname == varname, filter_cats)[[1]]
      if (is.null(filter_entry)) {
        return(NULL)
      }
      
      selected_codes <- params$filter_base[[varname]]
      # Find labels for selected codes
      subcats <- filter_entry$subcategories
      selected_labels <- vapply(selected_codes, function(code) {
        subcat <- Filter(function(s) s$code == as.character(code), subcats)
        if (length(subcat) > 0) subcat[[1]]$label else as.character(code)
      }, character(1))
      
      list(
        varname = varname,
        ui_label = filter_entry$label,
        selected_codes = list(selected_codes),
        selected_labels = list(selected_labels)
      )
    })
    filters_list <- Filter(Negate(is.null), filters_list)
    if (length(filters_list) > 0) {
      filters_dt <- data.table::rbindlist(filters_list, use.names = TRUE, fill = TRUE)
    }
  }
  
  # Covariates
  covariates_dt <- data.table::data.table(
    slot = character(0),
    varname = character(0),
    ui_label = character(0),
    n_categories = integer(0)
  )
  if (!is.null(params$by) && length(params$by) > 0) {
    layout_covs <- piptm_layout_covariates(release)
    # For now, assume all covariates are in "rows" slot (simplified)
    # Full implementation would need layout information from params
    covariates_list <- lapply(params$by, function(varname) {
      cov_entry <- Filter(function(x) x$varname == varname, layout_covs)
      if (length(cov_entry) > 0) {
        cov_entry <- cov_entry[[1]]
        list(
          slot = "rows",  # Simplified assumption
          varname = cov_entry$varname,
          ui_label = cov_entry$label,
          n_categories = cov_entry$n_categories %||% NA_integer_
        )
      } else if (varname == "pov_status") {
        # Special case for pov_status
        list(
          slot = "rows",
          varname = "pov_status",
          ui_label = "Poverty Status",
          n_categories = 2L
        )
      } else {
        NULL
      }
    })
    covariates_list <- Filter(Negate(is.null), covariates_list)
    if (length(covariates_list) > 0) {
      covariates_dt <- data.table::rbindlist(covariates_list, use.names = TRUE, fill = TRUE)
    }
  }
  
  # ── Execution ─────────────────────────────────────────────────────────────
  n_surveys_loaded <- length(loaded_pip_ids)
  n_surveys_excluded <- nrow(excluded_surveys)
  n_filters_applied <- if (is.null(params$filter_base)) 0L else length(params$filter_base)
  n_measures_computed <- length(params$measures)
  
  # Suppression
  suppression_triggered <- !is.null(suppressed_cells) && nrow(suppressed_cells) > 0
  suppression <- list(
    triggered = suppression_triggered,
    threshold = params$pop_share_threshold,
    n_cells_suppressed = if (suppression_triggered) nrow(suppressed_cells) else 0L,
    suppressed_cells = suppressed_cells
  )
  
  # Warnings
  warnings <- if (length(captured_warnings) > 0) captured_warnings else NULL
  
  # ── Assemble ──────────────────────────────────────────────────────────────
  list(
    params = params,
    provenance = provenance,
    surveys = list(
      loaded = loaded_surveys,
      excluded = excluded_surveys
    ),
    resolved_labels = list(
      analysis_var = analysis_var_info,
      measures = measures_dt,
      filters = filters_dt,
      covariates = covariates_dt
    ),
    execution = list(
      n_surveys_loaded = n_surveys_loaded,
      n_surveys_excluded = n_surveys_excluded,
      n_filters_applied = n_filters_applied,
      n_measures_computed = n_measures_computed,
      suppression = suppression,
      warnings = warnings
    )
  )
}

# ── table_maker ───────────────────────────────────────────────────────────────

#' Compute cross-tabulated welfare, inequality, and poverty measures
#'
#' Top-level API function that orchestrates the full pipeline:
#' manifest lookup → survey loading → pre-processing → per-survey computation
#' → long-format output.
#'
#' Requests are survey-first: callers provide one or more `pip_id` values plus
#' an explicit `analysis_var` that determines routing.
#'
#' `pov_status` is treated as a derived variable (never loaded from Parquet).
#' When `"pov_status"` appears in `by`, it is computed as
#' `as.integer(welfare < poverty_line)` after survey loading.
#'
#' @param pip_id Character vector of survey identifiers (manifest primary key,
#'   e.g. `"COL_2010_GEIH_INC_ALL"`). Must be provided and non-empty.
#' @param analysis_var Character scalar analysis variable name.
#' @param measures Non-empty character vector of measure names drawn from
#'   the internal measure registry (validated by [.classify_measures()]).
#' @param poverty_line Positive numeric scalar poverty line, or `NULL`.
#'   Required when any poverty-family measure is requested or when
#'   `"pov_status"` is included in `by`.
#' @param by Character vector of disaggregation dimension names, or `NULL` for
#'   aggregate results. Values must be valid covariates from
#'   [piptm_layout_covariates()] for the selected release. At most 4
#'   dimensions are allowed.
#' @param filter_base Named list of sample-base filters, or `NULL`.
#'   Each name is a variable and each value is an integer vector of allowed
#'   codes (AND across variables, IN within variable). Example:
#'   `list(age_group = c(1L, 2L), gender = 0L)`.
#' @param ppp          Integer scalar PPP year (e.g. `2017L`), or `NULL`.
#'   Passed through to [load_surveys()]. Selects which `welfare_ppp_*` column
#'   to use as `welfare` for surveys written with the deflated-data schema.
#'   When `NULL` (default), the manifest `ppp_sort` default is used.
#' @param release Character scalar release ID (e.g. `"20260206"`). Defaults
#'   to the current release as returned by [piptm_current_release()].
#' @param pop_share_threshold Numeric scalar in (0, 1) or `NULL`.  When
#'   non-NULL, cells whose population share falls below this threshold have all
#'   non-share measures suppressed (dropped from output), regardless of whether
#'   `"pop_share"` was requested.  If `"pop_share"` is not among the requested
#'   `measures`, it is computed internally and used solely for threshold
#'   evaluation — it does not appear in the output.  Share-family measures
#'   (`pop_share`, `target_within_group_share`, `target_survey_share`) are
#'   always retained so callers can inspect the distribution of suppressed
#'   groups.  Has no effect when `by = NULL` (aggregate mode: each cell is a
#'   full survey with pop_share = 1.0).  Default: `0.01` (1%).
#' @param include_metadata Logical scalar (default `FALSE`). When `FALSE`, returns
#'   a [data.table::data.table()] directly (default behavior). When `TRUE`, returns
#'   a list with two elements: `data` (the data.table) and `description_metadata`
#'   (a structured list containing execution logs, provenance, and resolved labels
#'   for use by the `/description` endpoint).
#'
#' @return When `include_metadata = FALSE` (default): A [data.table::data.table()] 
#'   in **long format** with columns:
#' \describe{
#'   \item{`pip_id`}{Survey identifier.}
#'   \item{`country_code`}{ISO3 country code.}
#'   \item{`surveyid_year`}{Survey year.}
#'   \item{`welfare_type`}{`"INC"` or `"CON"`.}
#'   \item{`[by cols]`}{One column per element of `by` (if non-NULL).
#'     Surveys missing any requested dimension are excluded before loading;
#'     every row in the result is guaranteed to have non-`NA` values for all
#'     breakdown columns.}
#'   \item{`poverty_line`}{Poverty threshold; populated for poverty-family
#'     rows and `NA` for non-poverty rows.}
#'   \item{`measure`}{Measure name (e.g. `"headcount"`, `"gini"`).}
#'   \item{`value`}{Computed statistic.}
#'   \item{`population`}{Total weighted population in the group.  For surveys
#'     where a requested dimension is absent (partial-match), this reflects the
#'     weighted count of respondents in the non-`NA` grouping cells only; rows
#'     with `NA` in the missing dimension still carry the correct weighted count
#'     for their observed group.}
#' }
#'   When `include_metadata = TRUE`: A list with elements:
#' \describe{
#'   \item{`data`}{The [data.table::data.table()] described above.}
#'   \item{`description_metadata`}{A named list containing:
#'     \describe{
#'       \item{`params`}{Echo of all function arguments.}
#'       \item{`provenance`}{Release ID, PPP year, and generation timestamp.}
#'       \item{`surveys`}{Data tables of loaded and excluded surveys.}
#'       \item{`resolved_labels`}{UI labels for analysis_var, measures, filters, and covariates.}
#'       \item{`execution`}{Execution logs including warnings and suppression events.}
#'     }
#'   }
#' }
#' \describe{
#'   \item{`pip_id`}{Survey identifier.}
#'   \item{`country_code`}{ISO3 country code.}
#'   \item{`surveyid_year`}{Survey year.}
#'   \item{`welfare_type`}{`"INC"` or `"CON"`.}
#'   \item{`[by cols]`}{One column per element of `by` (if non-NULL).
#'     Surveys missing any requested dimension are excluded before loading;
#'     every row in the result is guaranteed to have non-`NA` values for all
#'     breakdown columns.}
#'   \item{`poverty_line`}{Poverty threshold; populated for poverty-family
#'     rows and `NA` for non-poverty rows.}
#'   \item{`measure`}{Measure name (e.g. `"headcount"`, `"gini"`).}
#'   \item{`value`}{Computed statistic.}
#'   \item{`population`}{Total weighted population in the group.  For surveys
#'     where a requested dimension is absent (partial-match), this reflects the
#'     weighted count of respondents in the non-`NA` grouping cells only; rows
#'     with `NA` in the missing dimension still carry the correct weighted count
#'     for their observed group.}
#' }
#'
#' @family api
#' @export
#' @examples
#' \dontrun{
#' set_manifest_dir("//server/manifests")
#' set_arrow_root("//server/pip/arrow")
#'
#' # Welfare analysis
#' result <- table_maker(
#'   pip_id        = "COL_2010_GEIH_INC_ALL",
#'   analysis_var  = "welfare",
#'   measures      = c("headcount", "gini", "mean"),
#'   poverty_line  = 2.15,
#'   by            = c("gender", "area")
#' )
#'
#' # Poverty-status analysis
#' result <- table_maker(
#'   pip_id        = "COL_2010_GEIH_INC_ALL",
#'   analysis_var  = "pov_status",
#'   measures      = c("headcount", "poverty_gap"),
#'   poverty_line  = 2.15
#' )
#' }
table_maker <- function(pip_id        = NULL,
                        analysis_var,
                        measures,
                        poverty_line  = NULL,
                        by            = NULL,
                        filter_base   = NULL,
                        ppp           = 2021L,
                        release       = NULL,
                        pop_share_threshold = 0.01,
                        include_metadata = FALSE) {

  # ── 0. Validate metadata parameter ─────────────────────────────────────────
  if (!is.logical(include_metadata) || length(include_metadata) != 1L) {
    cli_abort("{.arg include_metadata} must be a logical scalar (TRUE or FALSE).")
  }

  # ── 1. Resolve survey identifiers ──────────────────────────────────────────
  if (is.null(pip_id)) {
    cli_abort("{.arg pip_id} must be provided.")
  }

  if (length(pip_id) == 0L) {
    cli_abort("No surveys to process: {.arg pip_id} is empty after resolution.")
  }

  # ── 2. Validate computation parameters ─────────────────────────────────────
  families <- .classify_measures(measures)
  .validate_poverty_lines(poverty_line, names(families))
  by_validate <- by
  if (!is.null(by_validate)) {
    by_validate <- setdiff(by_validate, "pov_status")
    if (length(by_validate) == 0L) by_validate <- NULL
  }
  .validate_by(by_validate, release = release)

  # ── 2b. Validate and normalize sample-base filters ─────────────────────────
  normalized_filter_base <- NULL
  filter_vars <- character(0L)
  if (!is.null(filter_base)) {
    if (is.data.frame(filter_base)) {
      filter_base <- as.list(filter_base)
    }

    if (!is.list(filter_base)) {
      cli_abort("{.arg filter_base} must be NULL or a named list.")
    }
    if (length(filter_base) == 0L) {
      cli_abort("{.arg filter_base} must be NULL or a non-empty named list.")
    }

    filter_vars <- names(filter_base)
    if (is.null(filter_vars) || anyNA(filter_vars) || any(!nzchar(filter_vars))) {
      cli_abort("{.arg filter_base} must have non-empty variable names.")
    }

    allowed_filter_vars <- unique(
      vapply(piptm_filter_categories(), `[[`, character(1), "varname")
    )

    invalid_vars <- setdiff(filter_vars, allowed_filter_vars)
    if (length(invalid_vars) > 0L) {
      cli_abort(
        c(
          "Invalid {.arg filter_base} variable{?s}: {.val {invalid_vars}}.",
          "i" = "Allowed variables: {.val {allowed_filter_vars}}"
        )
      )
    }

    normalized_filter_base <- setNames(
      lapply(filter_vars, function(varname) {
        vals <- unlist(filter_base[[varname]], use.names = FALSE)
        if (length(vals) == 0L) {
          cli_abort(
            "{.arg filter_base} variable {.val {varname}} must include at least one value."
          )
        }
        suppressWarnings(vals_int <- as.integer(vals))
        if (anyNA(vals_int)) {
          cli_abort(
            c(
              "{.arg filter_base} variable {.val {varname}} has non-integer value{?s}.",
              "i" = "Values must be integer codes stored in Parquet."
            )
          )
        }
        unique(vals_int)
      }),
      filter_vars
    )
  }

  # ── 2. Manifest lookup ──────────────────────────────────────────────────────
  mf      <- piptm_manifest(release)
  .ids    <- pip_id  # local copy avoids data.table column-name ambiguity
  entries <- mf[pip_id %chin% .ids]

  if (nrow(entries) == 0L) {
    cli_abort(
      c(
        "No matching surveys found in manifest for release {.val {mf$release[[1L]] %||% piptm_current_release()}}.",
        "i" = "Requested pip_id{?s}: {.val {(.ids)}}"
      )
    )
  }

  # Warn about pip_ids that exist in the request but not in the manifest
  missing_ids <- setdiff(pip_id, entries$pip_id)
  if (length(missing_ids)) {
    cli_warn(
      c(
        "{length(missing_ids)} pip_id{?s} not found in manifest:",
        "i" = "{.val {missing_ids}}"
      )
    )
  }

  # ── Initialize execution log trackers ──────────────────────────────────────
  # Only used when include_metadata = TRUE
  excluded_surveys <- data.table::data.table(
    pip_id = character(0),
    reason = character(0)
  )
  captured_warnings <- character()
  suppressed_cells <- NULL

  # ── 2b. Filter-base manifest pre-filter (before loading) ──────────────────
  if (!is.null(normalized_filter_base)) {
    overlap_fb <- vapply(
      entries$dimensions,
      function(d) length(intersect(filter_vars, d)),
      integer(1L)
    )

    full_filter_idx <- overlap_fb == length(filter_vars)
    dropped_filter_idx <- !full_filter_idx

    if (any(dropped_filter_idx)) {
      dropped_entries <- entries[dropped_filter_idx]
      dropped_info <- vapply(seq_len(nrow(dropped_entries)), function(i) {
        have <- dropped_entries$dimensions[[i]]
        miss <- setdiff(filter_vars, have)
        if (length(miss) == length(filter_vars)) {
          paste0(dropped_entries$pip_id[[i]], ": no filter_base dimensions")
        } else {
          paste0(dropped_entries$pip_id[[i]], ": missing ", paste(miss, collapse = ", "))
        }
      }, character(1L))

      # Capture exclusions for metadata
      if (include_metadata) {
        excluded_surveys <- data.table::rbindlist(list(
          excluded_surveys,
          data.table::data.table(
            pip_id = dropped_entries$pip_id,
            reason = dropped_info
          )
        ), use.names = TRUE, fill = TRUE)
      }

      cli_warn(
        c(
          "Excluding {length(dropped_info)} survey{?s} that lack all required {.arg filter_base} dimensions ({.val {filter_vars}}):",
          "i" = "{dropped_info}"
        )
      )

      entries <- entries[full_filter_idx]
    }

    if (nrow(entries) == 0L) {
      cli_abort(
        c(
          "All requested surveys were excluded: none have all required {.arg filter_base} dimensions ({.val {filter_vars}}).",
          "i" = "Check {.fn piptm_manifest} for available dimensions per survey."
        )
      )
    }
  }

  # ── 3. Dimension pre-filter (before loading) ────────────────────────────────
  # For each entry, compute the overlap count between user's `by` and the
  # survey's available dimensions (manifest list column).
  # Keep: ≥1 overlap (full or partial match)
  # Drop: 0 overlap — warn with pip_id list
  # Partial: keep but warn — missing dims filled with NA in per-survey loop
  if (!is.null(by)) {
    by_check <- by[!by %in% "pov_status"]

    if (length(by_check) == 0L) {
      by_check <- NULL
    }

    if (!is.null(by_check)) {
      overlap <- vapply(
        entries$dimensions,
        function(d) length(intersect(by_check, d)),
        integer(1L)
      )

      # A survey must carry ALL requested dimensions; partial matches are
      # excluded before loading so no NA-fill is ever needed downstream.
      full_idx <- overlap == length(by_check)
      dropped_idx <- !full_idx

      if (any(dropped_idx)) {
        dropped_entries <- entries[dropped_idx]
        dropped_info <- vapply(seq_len(nrow(dropped_entries)), function(i) {
          have <- dropped_entries$dimensions[[i]]
          miss <- setdiff(by_check, have)
          if (length(miss) == length(by_check)) {
            paste0(dropped_entries$pip_id[[i]], ": no requested dimensions")
          } else {
            paste0(dropped_entries$pip_id[[i]], ": missing ", paste(miss, collapse = ", "))
          }
        }, character(1L))

        # Capture exclusions for metadata
        if (include_metadata) {
          excluded_surveys <- data.table::rbindlist(list(
            excluded_surveys,
            data.table::data.table(
              pip_id = dropped_entries$pip_id,
              reason = dropped_info
            )
          ), use.names = TRUE, fill = TRUE)
        }

        cli_warn(
          c(
            "Excluding {length(dropped_info)} survey{?s} that lack all requested dimensions ({.val {by_check}}):",
            "i" = "{dropped_info}"
          )
        )
        entries <- entries[full_idx]
      }

      if (nrow(entries) == 0L) {
        cli_abort(
          c(
            "All requested surveys were excluded: none have all of the requested dimensions ({.val {by_check}}).",
            "i" = "Check {.fn piptm_manifest} for available dimensions per survey."
          )
        )
      }
    }
  }

  # ── 4. Load ─────────────────────────────────────────────────────────────────
  # Column pruning: request only the columns needed for computation.
  # "welfare" is the logical name; load_surveys() translates it to the physical
  # PPP column internally. country_code / surveyid_year / welfare_type are
  # needed for the metadata join in Step 8.
  needs_welfare <- is.null(analysis_var) ||
    analysis_var %in% c("welfare", "pov_status") ||
    (!is.null(by) && "pov_status" %in% by)

  needed_cols <- unique(c(
    "pip_id", "country_code", "surveyid_year", "welfare_type",
    "weight",
    if (needs_welfare) "welfare",
    if (!is.null(analysis_var) && !analysis_var %in% c("welfare", "pov_status")) analysis_var,
    by[!by %in% "pov_status"],
    filter_vars   # NULL is silently dropped by c()
  ))
  dt <- load_surveys(entries,
                     ppp = ppp,
                     cols = needed_cols,
                     filter_base = normalized_filter_base,
                     release = release)

  if (nrow(dt) == 0L) {
    cli_abort(
      c(
        "{.fn load_surveys} returned no rows for the requested surveys.",
        "i" = "Check the Arrow repository path and partition keys."
      )
    )
  }

  # Guard: assert load_surveys() attached the expected metadata columns so that
  # downstream sdt$country_code[[1L]] etc. fail at the right place with a clear
  # message if the contract changes.
  required_meta <- c("pip_id", "country_code", "surveyid_year", "welfare_type")
  missing_meta  <- setdiff(required_meta, names(dt))
  if (length(missing_meta)) {
    cli_abort(
      c(
        "{.fn load_surveys} result is missing expected metadata columns.",
        "i" = "Missing: {.val {missing_meta}}"
      )
    )
  }

  # ── 5. Derived covariates ───────────────────────────────────────────────────
  if (!is.null(by) && "pov_status" %in% by) {
    if (is.null(poverty_line) || !is.numeric(poverty_line) ||
        length(poverty_line) != 1L || !is.finite(poverty_line) || poverty_line <= 0) {
      cli_abort(
        c(
          "{.arg poverty_line} is required when {.val pov_status} is used as a disaggregation dimension.",
          "i" = "Provide a single positive numeric scalar."
        )
      )
    }
    dt[, pov_status := as.integer(welfare < poverty_line)]
  }

  # ── 7. Batch compute ────────────────────────────────────────────────────────
  # Single grouped call across all surveys (Approach B). compute_measures()
  # uses GRP(c("pip_id", by)) internally, paying the overhead once instead
  # of once per survey.
  result <- compute_measures(
    dt,
    measures = measures,
    analysis_var = analysis_var,
    poverty_line = poverty_line,
    by = by,
    release = release
  )

  # ── 8. Attach survey metadata ────────────────────────────────────────────────
  # country_code, surveyid_year, welfare_type are attached via a keyed join
  # from a pip_id → metadata lookup extracted from the loaded data.
  meta <- unique(dt[, .(pip_id, country_code, surveyid_year, welfare_type)])
  if (data.table::uniqueN(meta, by = "pip_id") != nrow(meta)) {
    dups <- meta[duplicated(meta, by = "pip_id"), pip_id]
    cli_abort(
      c(
        "{.fn load_surveys} returned inconsistent metadata for {length(dups)} pip_id{?s}.",
        "i" = "Each pip_id must map to exactly one country_code / surveyid_year / welfare_type.",
        "i" = "Affected: {.val {dups}}"
      ),
      call = NULL
    )
  }
  result <- meta[result, on = "pip_id"]

  # ── 9. Reorder columns ──────────────────────────────────────────────────────
  meta_cols <- c("pip_id", "country_code", "surveyid_year", "welfare_type")
  dim_cols  <- if (!is.null(by)) by else character(0L)
  tail_cols <- c("poverty_line", "measure", "value", "population")
  col_order <- c(meta_cols, dim_cols, tail_cols)
  # Only reorder columns that are actually present
  col_order <- intersect(col_order, names(result))
  data.table::setcolorder(result, col_order)

  # ── 10. Pop-share threshold suppression ─────────────────────────────────────
  # The threshold is a data-quality safeguard: cells whose population share
  # falls below `pop_share_threshold` have all non-share measures suppressed
  # (dropped from output).  Activation is unconditional — if "pop_share" was
  # not requested by the caller it is computed internally for evaluation only
  # and never appended to the output.
  if (!is.null(pop_share_threshold)) {
    # Validate threshold unconditionally — not contingent on requested measures.
    if (!is.numeric(pop_share_threshold) || length(pop_share_threshold) != 1L ||
        !is.finite(pop_share_threshold) || pop_share_threshold <= 0 ||
        pop_share_threshold >= 1) {
      cli_abort(
        c(
          "{.arg pop_share_threshold} must be a single numeric value in (0, 1), or {.code NULL} to disable.",
          "i" = "Got: {.val {pop_share_threshold}}."
        )
      )
    }

    # Suppression is only meaningful when `by` is non-NULL.  With by = NULL
    # every "cell" is a full survey (pop_share = 1.0 always), so the threshold
    # can never trigger.
    if (length(dim_cols) > 0L) {
      caller_requested_pop_share <- "pop_share" %in% measures

      # Obtain pop_share per cell.  If pop_share was not requested by the
      # caller, compute it internally using the same batch grouping as
      # compute_measures(); used only for threshold evaluation, never output.
      if (caller_requested_pop_share) {
        pop_rows <- result[measure == "pop_share"]
      } else {
        batch_by_ps <- c("pip_id", dim_cols)
        grp_ps      <- collapse::GRP(dt, by = batch_by_ps)
        pop_rows    <- compute_shares(dt, by = batch_by_ps,
                                      measures = "pop_share", grp = grp_ps)
      }

      cell_keys  <- c("pip_id", dim_cols)
      suppressed <- pop_rows[value < pop_share_threshold]

      # Store suppressed for metadata capture
      if (include_metadata && nrow(suppressed) > 0L) {
        suppressed_cells <- suppressed
      }

      if (nrow(suppressed) > 0L) {
        # Retain ALL shares-family measures for below-threshold cells so
        # callers can inspect the population distribution of suppressed groups.
        share_measures <- names(Filter(function(f) f == "shares",
                                       .MEASURE_REGISTRY))

        sup_labels <- vapply(seq_len(nrow(suppressed)), function(i) {
          dims <- paste(
            vapply(cell_keys, function(k)
              paste0(k, "=", suppressed[[k]][[i]]), character(1L)),
            collapse = ", "
          )
          paste0(dims, " (pop_share=", round(suppressed[["value"]][[i]], 4L), ")")
        }, character(1L))

        cli_warn(
          c(
            "Suppressing non-share measures for {nrow(suppressed)} cell{?s} with pop_share < {pop_share_threshold}:",
            "i" = "{sup_labels}"
          )
        )

        # Anti-join: for suppressed cells, drop all non-share measure rows.
        sup_keys <- suppressed[, ..cell_keys]
        result[, .suppress := FALSE]
        idx <- result[sup_keys, on = cell_keys, which = TRUE, nomatch = NULL]
        result[idx, .suppress := !(measure %chin% share_measures)]
        result <- result[(.suppress) == FALSE][, .suppress := NULL]
      }
    }
  }

  # ── 11. Return ──────────────────────────────────────────────────────────────
  if (include_metadata) {
    # Return list with data and description_metadata
    metadata <- .build_description_metadata(
      params = list(
        pip_id = pip_id,
        analysis_var = analysis_var,
        measures = measures,
        poverty_line = poverty_line,
        by = by,
        filter_base = filter_base,
        ppp = ppp,
        release = release %||% piptm_current_release(),
        pop_share_threshold = pop_share_threshold
      ),
      result = result,
      release = release %||% piptm_current_release(),
      excluded_surveys = excluded_surveys,
      suppressed_cells = suppressed_cells,
      captured_warnings = captured_warnings
    )
    
    return(list(
      data = result,
      description_metadata = metadata
    ))
  }

  result[]
}

