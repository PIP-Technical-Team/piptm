#' @importFrom data.table is.data.table data.table setcolorder set fsetdiff %chin%
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
    piptm_abort(
      c(
        "{.arg country_code}, {.arg year}, and {.arg welfare_type} must all have the same length.",
        "i" = "Lengths: {.arg country_code} = {n}, {.arg year} = {length(year)}, {.arg welfare_type} = {length(welfare_type)}."
      ),
      user_message = "Country, year, and welfare type selections must have the same number of entries."
    )
  }

  if (!is.integer(year)) {
    if (!is.numeric(year)) {
      piptm_abort(
        "{.arg year} must be an integer vector of survey years (e.g. 2010L).",
        user_message = .PIPTM_SURVEY_YEAR_USER_ERROR
      )
    }
    if (any(is.na(year))) {
      piptm_abort(
        "{.arg year} cannot contain NA values.",
        user_message = .PIPTM_SURVEY_YEAR_USER_ERROR
      )
    }
    fractional_years <- year != floor(year)
    if (any(fractional_years)) {
      piptm_abort(
        c(
          "{.arg year} must contain whole-number years (no fractional values).",
          "i" = "Offending value{?s}: {.val {unique(year[fractional_years])}}"
        ),
        user_message = .PIPTM_SURVEY_YEAR_USER_ERROR
      )
    }
    year <- as.integer(year)
  }

  # ── Build query table ───────────────────────────────────────────────────────
  query <- data.table(
    country_code = country_code,
    year         = year,
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
#' @param ppp          Integer scalar PPP year (e.g. `2017L`). Defaults to
#'   `2021L`, the current PPP baseline. Passed through to [load_surveys()] and
#'   selects which `welfare_ppp_*` column to use as `welfare` for deflated-data
#'   surveys. Set to `NULL` to fall back to the manifest `ppp_sort` default.
#' @param release Character scalar release ID (e.g. `"20260206"`). Defaults
#'   to the current release as returned by [piptm_current_release()].
#' @param pop_share_threshold Numeric scalar in (0, 1) or `NULL`.  When
#'   non-NULL, cells whose population share falls below this threshold have all
#'   measures masked with `value = NA_real_` and `flag = "NR"`, regardless of whether
#'   `"pop_share"` was requested. Cells whose population share cannot be computed
#'   (`NA`, e.g. a zero-denominator or all-`NA` group) are also masked as `"NR"`,
#'   in addition to cells below the threshold. If `"pop_share"` is not among the
#'   requested `measures`, it is computed internally and used solely for threshold
#'   evaluation — it does not appear in the output. Has no effect when `by = NULL`
#'   (aggregate mode: each cell is a full survey with pop_share = 1.0).
#'   Default: `0.001` (0.1%).
#'
#' @return A [data.table::data.table()] in **long format** with columns:
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
#'   \item{`value`}{Computed statistic. `NA_real_` when `flag == "NR"` (masked).}
#'   \item{`flag`}{Character suppression flag. `"NR"` identifies cells masked
#'     because their population share is below `pop_share_threshold` (or could
#'     not be computed); otherwise the value is `NA`.}
#' }
#'
#' @section Downstream formatting:
#' Suppressed cells are returned as raw `value = NA_real_` with
#' `flag = "NR"` — `table_maker()` itself never coerces `value` to character
#' and never distinguishes masked `NA` from genuinely missing `NA` beyond the
#' `flag` column. Any caller that displays results to end users, or otherwise
#' needs to tell "masked" apart from "not computed", must pass this table
#' through [format_table_output()] (or equivalent logic) before use; treating
#' `value == NA` alone as ordinary missing data will silently conflate masked
#' and unmasked-missing cells.
#'
#' @seealso [format_table_output()] for converting this table's raw
#'   `value`/`flag` columns into the formatted, `flag`-free display
#'   representation used by the `/table` API.
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
                        pop_share_threshold = 0.001) {

  # ── 1. Resolve survey identifiers ──────────────────────────────────────────
  if (is.null(pip_id)) {
    cli_abort("{.arg pip_id} must be provided.")
  }

  if (length(pip_id) == 0L) {
    piptm_abort(
      "No surveys to process: {.arg pip_id} is empty after resolution.",
      user_message = "No surveys matched your selection. Please select at least one available country and survey year."
    )
  }

  # ── 2. Validate and normalize computation parameters ───────────────────────
  # Domain validation (measure families, poverty-line coherence, release-scoped
  # covariates, and filter_base schema/codes) is shared with
  # build_description_model() via .validate_and_normalize_table_params() so the
  # computation and metadata paths can never diverge.
  param_check <- .validate_and_normalize_table_params(
    analysis_var = analysis_var,
    measures     = measures,
    poverty_line = poverty_line,
    by           = by,
    filter_base  = filter_base,
    release      = release
  )
  families               <- param_check$families
  by_validate            <- param_check$by_validate
  normalized_filter_base <- param_check$normalized_filter_base
  filter_vars            <- param_check$filter_vars

  # ── 2b. Manifest lookup + two-stage survey partition ───────────────────────
  # Resolve the release once and partition requested surveys into included vs.
  # excluded using the shared helper (also consumed by the metadata builder).
  # warn = TRUE preserves the exact exclusion/partial-match warnings this path
  # has always emitted.
  if (is.null(release)) {
    release <- piptm_current_release()
  }
  partition <- .partition_surveys_by_dimensions(
    release                = release,
    pip_id                 = pip_id,
    normalized_filter_base = normalized_filter_base,
    filter_vars            = filter_vars,
    by                     = by,
    warn                   = TRUE
  )
  release <- partition$release
  entries <- partition$included

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

  # ── 4. Load surveys & compute ─────────────────────────────────────────────
  compute_core <- function() {
    dt <- load_surveys(entries,
                       ppp = ppp,
                       cols = needed_cols,
                       filter_base = normalized_filter_base,
                       release = release)

    if (nrow(dt) == 0L) {
      piptm_abort(
        c(
          "{.fn load_surveys} returned no rows for the requested surveys.",
          "i" = "Check the Arrow repository path and partition keys."
        ),
        user_message = "No survey observations matched the selected criteria."
      )
    }

    # Guard metadata columns
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

    pip_metadata <- unique(dt[, .(pip_id, country_code, surveyid_year, welfare_type)])
    if (data.table::uniqueN(pip_metadata, by = "pip_id") != nrow(pip_metadata)) {
      dups <- pip_metadata[duplicated(pip_metadata, by = "pip_id"), pip_id]
      cli_abort(
        c(
          "{.fn load_surveys} returned inconsistent metadata for {length(dups)} pip_id{?s}.",
          "i" = "Each pip_id must map to exactly one country_code / surveyid_year / welfare_type.",
          "i" = "Affected: {.val {dups}}"
        ),
        call = NULL
      )
    }
    if (!is.character(pip_metadata$pip_id)) {
      pip_metadata[, pip_id := as.character(pip_id)]
    }
    if (!is.character(pip_metadata$country_code)) {
      pip_metadata[, country_code := as.character(country_code)]
    }
    if (!is.integer(pip_metadata$surveyid_year)) {
      if (!is.numeric(pip_metadata$surveyid_year)) {
        cli_abort("surveyid_year metadata must be numeric/integer")
      }
      pip_metadata[, surveyid_year := as.integer(round(pip_metadata$surveyid_year))]
    }
    if (!is.character(pip_metadata$welfare_type)) {
      pip_metadata[, welfare_type := as.character(welfare_type)]
    }

    # Derived covariates
    # poverty_line coherence for a pov_status breakdown is enforced earlier by
    # the shared .validate_and_normalize_table_params() (step 2), so by the
    # time compute_core() runs, poverty_line is already known valid here.
    if (!is.null(by) && "pov_status" %in% by) {
      dt[, pov_status := as.integer(welfare < poverty_line)]
    }

    result <- compute_measures(
      dt,
      measures = measures,
      analysis_var = analysis_var,
      poverty_line = poverty_line,
      by = by,
      release = release
    )

    result <- pip_metadata[result, on = "pip_id"]

    meta_cols <- c("pip_id", "country_code", "surveyid_year", "welfare_type")
    dim_cols  <- if (!is.null(by)) by else character(0L)
    cell_keys <- c("pip_id", dim_cols)

    # Capture the per-cell `population` already computed by compute_measures()
    # before it is dropped from the output below. When `pop_share` is not
    # among the requested measures, this lets threshold evaluation reuse
    # these values via a cheap lookup/join instead of re-aggregating the raw
    # microdata a second time through compute_shares()/GRP().
    pop_lookup <- NULL
    if (length(dim_cols) > 0L && "population" %in% names(result)) {
      pop_lookup <- unique(result[, c(cell_keys, "population"), with = FALSE], by = cell_keys)
    }

    if (!"flag" %in% names(result)) {
      result[, flag := NA_character_]
    }
    result[, population := NULL]
    tail_cols <- c("poverty_line", "measure", "value", "flag")
    col_order <- intersect(c(meta_cols, dim_cols, tail_cols), names(result))
    data.table::setcolorder(result, col_order)

    if (!is.null(pop_share_threshold)) {
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

      if (length(dim_cols) > 0L) {
        caller_requested_pop_share <- "pop_share" %in% measures
        if (caller_requested_pop_share) {
          pop_rows <- result[measure == "pop_share"]
        } else if (!is.null(pop_lookup)) {
          # Reuse the per-cell population captured above: pop_share is this
          # cell's population divided by its survey's total population
          # (summed across all cells sharing the same pip_id) — identical
          # math to .shares_setup()'s denom_survey computation, without a
          # second GRP()/fsum() pass over the raw microdata.
          denom_lookup <- pop_lookup[, list(denom_survey = collapse::fsum(population)), by = "pip_id"]
          pop_rows <- pop_lookup[denom_lookup, on = "pip_id"]
          pop_rows[, value := population / denom_survey]
          pop_rows[, c("population", "denom_survey") := NULL]
        } else {
          batch_by_ps <- c("pip_id", dim_cols)
          grp_ps      <- collapse::GRP(dt, by = batch_by_ps)
          pop_rows    <- compute_shares(dt, by = batch_by_ps,
                                        measures = "pop_share", grp = grp_ps)
        }

        suppressed <- pop_rows[is.na(value) | value < pop_share_threshold]

        if (nrow(suppressed) > 0L) {
          # Vectorized label construction (no row-wise vapply): build one
          # "key=value" character vector per cell_keys column, paste them
          # together column-wise, then append the rounded pop_share. This
          # keeps the whole labeling step O(rows) in vectorized operations
          # instead of a nested per-row loop, which matters when many small
          # cells are masked across a large batch.
          key_parts  <- lapply(cell_keys, function(k) paste0(k, "=", suppressed[[k]]))
          dims_vec   <- do.call(paste, c(key_parts, list(sep = ", ")))
          sup_labels <- paste0(dims_vec, " (pop_share=", round(suppressed[["value"]], 4L), ")")

          # Cap the number of labels shown in the warning; this is a
          # diagnostic message, not a data structure, so truncating it for
          # very large suppressed sets keeps the warning readable without
          # affecting which cells are actually masked.
          max_labels <- 20L
          if (length(sup_labels) > max_labels) {
            sup_labels <- c(
              sup_labels[seq_len(max_labels)],
              sprintf("... and %d more", length(sup_labels) - max_labels)
            )
          }

          cli_warn(
            c(
              "Reporting {nrow(suppressed)} cell{?s} as NR (population share < {pop_share_threshold}):",
              "i" = "{sup_labels}"
            )
          )

          sup_keys <- suppressed[, ..cell_keys]
          idx <- result[sup_keys, on = cell_keys, which = TRUE, nomatch = NULL]
          result[idx, `:=`(value = NA_real_, flag = "NR")]
        }
      }
    }

    result
  }

  result <- compute_core()

  # ── 11. Return ──────────────────────────────────────────────────────────────
  return(result[])
}
