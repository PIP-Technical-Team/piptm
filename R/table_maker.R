#' @importFrom data.table is.data.table data.table setcolorder rbindlist set fsetdiff
#' @importFrom cli cli_abort cli_warn
NULL

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

# ── table_maker ───────────────────────────────────────────────────────────────

#' Compute cross-tabulated welfare, inequality, and poverty measures
#'
#' Top-level API function that orchestrates the full pipeline:
#' manifest lookup → survey loading → pre-processing → per-survey computation
#' → long-format output.
#'
#' **Two equivalent input patterns are supported:**
#'
#' *Pattern 1 — pip_id (primary, used by the API layer):*
#' ```r
#' table_maker(
#'   pip_id   = c("COL_2010_GEIH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
#'   measures = c("headcount", "gini"),
#'   poverty_lines = 2.15
#' )
#' ```
#'
#' *Pattern 2 — triplet fallback (ad-hoc R console use):*
#' ```r
#' table_maker(
#'   country_code = c("COL", "BOL"),
#'   year         = c(2010L, 2000L),
#'   welfare_type = c("INC", "INC"),
#'   measures     = c("headcount", "gini"),
#'   poverty_lines = 2.15
#' )
#' ```
#'
#' When triplets are provided, [pip_lookup()] resolves them to `pip_id`s
#' internally.  If both `pip_id` and triplets are supplied, `pip_id` takes
#' precedence and the triplets are silently ignored.
#'
#' @param pip_id Character vector of survey identifiers (manifest primary key,
#'   e.g. `"COL_2010_GEIH_INC_ALL"`). Either this **or** the three triplet
#'   params below must be supplied.
#' @param country_code Character vector of ISO3 country codes (triplet fallback).
#' @param year Integer vector of survey years (triplet fallback, same length as
#'   `country_code`).
#' @param welfare_type Character vector of welfare types — `"INC"` or `"CON"`
#'   (triplet fallback, same length as `country_code`).
#' @param measures Non-empty character vector of measure names drawn from
#'   [pip_measures()].
#' @param poverty_lines Positive numeric vector of poverty line values, or
#'   `NULL`. Required when any poverty-family measure (`headcount`,
#'   `poverty_gap`, `severity`, `watts`, `pop_poverty`) is requested.
#' @param by Character vector of disaggregation dimension names, or `NULL` for
#'   aggregate results.  Valid values: `"gender"`, `"area"`, `"educat4"`,
#'   `"educat5"`, `"educat7"`, `"age"`.  At most 4 dimensions; at most one
#'   education column.
#' @param release Character scalar release ID (e.g. `"20260206"`). Defaults
#'   to the current release as returned by [piptm_current_release()].
#'
#' @return A [data.table::data.table()] in **long format** with columns:
#' \describe{
#'   \item{`pip_id`}{Survey identifier.}
#'   \item{`country_code`}{ISO3 country code.}
#'   \item{`surveyid_year`}{Survey year.}
#'   \item{`welfare_type`}{`"INC"` or `"CON"`.}
#'   \item{`[by cols]`}{One column per element of `by` (if non-NULL);
#'     `NA` when the dimension is absent for a survey (partial-match).}
#'   \item{`poverty_line`}{Poverty threshold; `NA` for non-poverty measures.}
#'   \item{`measure`}{Measure name (e.g. `"headcount"`, `"gini"`).}
#'   \item{`value`}{Computed statistic.}
#'   \item{`population`}{Total weighted population in the group.}
#' }
#'
#' @family api
#' @export
#' @examples
#' \dontrun{
#' set_manifest_dir("//server/manifests")
#' set_arrow_root("//server/pip/arrow")
#'
#' # Via pip_id
#' result <- table_maker(
#'   pip_id        = "COL_2010_GEIH_INC_ALL",
#'   measures      = c("headcount", "gini", "mean"),
#'   poverty_lines = c(2.15, 3.65),
#'   by            = c("gender", "area")
#' )
#'
#' # Via triplets
#' result <- table_maker(
#'   country_code  = "COL",
#'   year          = 2010L,
#'   welfare_type  = "INC",
#'   measures      = c("headcount", "gini", "mean"),
#'   poverty_lines = c(2.15, 3.65),
#'   by            = c("gender", "area")
#' )
#' }
table_maker <- function(pip_id        = NULL,
                        country_code  = NULL,
                        year          = NULL,
                        welfare_type  = NULL,
                        measures,
                        poverty_lines = NULL,
                        by            = NULL,
                        release       = NULL) {

  # ── 0. Resolve survey identifiers ──────────────────────────────────────────
  # pip_id takes precedence. Triplets used only when pip_id is NULL.
  if (is.null(pip_id)) {
    if (is.null(country_code) || is.null(year) || is.null(welfare_type)) {
      cli_abort(
        c(
          "Provide either {.arg pip_id} or all of {.arg country_code}, {.arg year}, and {.arg welfare_type}.",
          "i" = "Use {.fn pip_lookup} to translate triplets to pip_ids manually."
        )
      )
    }
    pip_id <- pip_lookup(country_code, year, welfare_type, release)
  }

  if (length(pip_id) == 0L) {
    cli_abort("No surveys to process: {.arg pip_id} is empty after resolution.")
  }

  # ── 1. Validate computation parameters ─────────────────────────────────────
  families <- .classify_measures(measures)
  .validate_poverty_lines(poverty_lines, names(families))
  .validate_by(by)

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

  # ── 3. Dimension pre-filter (before loading) ────────────────────────────────
  # For each entry, compute the overlap count between user's `by` and the
  # survey's available dimensions (manifest list column).
  # Keep: ≥1 overlap (full or partial match)
  # Drop: 0 overlap — warn with pip_id list
  # Partial: keep but warn — missing dims filled with NA in per-survey loop
  if (!is.null(by)) {
    # `age` maps to `age_group` after binning; check original `age` in manifest
    by_check <- if ("age" %in% by) c(setdiff(by, "age"), "age") else by

    overlap <- vapply(
      entries$dimensions,
      function(d) length(intersect(by_check, d)),
      integer(1L)
    )

    zero_idx <- overlap == 0L
    if (any(zero_idx)) {
      dropped_ids <- entries$pip_id[zero_idx]
      cli_warn(
        c(
          "Excluding {length(dropped_ids)} survey{?s} with none of the requested dimensions ({.val {by}}):",
          "i" = "{.val {dropped_ids}}"
        )
      )
      entries <- entries[!zero_idx]
      overlap  <- overlap[!zero_idx]
    }

    if (nrow(entries) == 0L) {
      cli_abort(
        c(
          "All requested surveys were excluded: none have any of the requested dimensions ({.val {by}}).",
          "i" = "Check {.fn piptm_manifest} for available dimensions per survey."
        )
      )
    }

    partial_idx <- overlap > 0L & overlap < length(by_check)
    if (any(partial_idx)) {
      partial_entries <- entries[partial_idx]
      missing_info <- vapply(seq_len(nrow(partial_entries)), function(i) {
        miss <- setdiff(by_check, partial_entries$dimensions[[i]])
        paste0(partial_entries$pip_id[[i]], ": ", paste(miss, collapse = ", "))
      }, character(1L))
      cli_warn(
        c(
          "{sum(partial_idx)} survey{?s} {?is/are} missing some requested dimensions.",
          "i" = "Results will have {.val NA} for missing dimensions.",
          "i" = "{missing_info}"
        )
      )
    }
  }

  # ── 4. Load ─────────────────────────────────────────────────────────────────
  dt <- load_surveys(entries, release = release)

  # ── 5. Age binning ──────────────────────────────────────────────────────────
  # .bin_age adds an `age_group` column; we replace the raw `age` column with
  # the binned factor so that compute_measures() can group by "age" as usual.
  # After rbindlist we rename "age" -> "age_group" in the result.
  age_was_binned <- FALSE
  if (!is.null(by) && "age" %in% by) {
    .bin_age(dt)
    dt[, age := age_group]   # overwrite raw ages with binned factor
    dt[, age_group := NULL]  # drop the helper column
    age_was_binned <- TRUE
  }

  # ── 6. Per-survey compute ───────────────────────────────────────────────────
  survey_ids <- unique(dt$pip_id)
  results <- lapply(survey_ids, function(pid) {
    sdt <- dt[pip_id == pid]

    # 6a. Fill missing dimension columns with NA_character_ so that
    #     compute_measures() always receives the full `by` vector.
    #     GRP naturally produces a single NA group for missing dimensions.
    if (!is.null(by)) {
      for (d in setdiff(by, names(sdt))) {
        data.table::set(sdt, j = d, value = NA_character_)
      }
    }

    res <- compute_measures(sdt, measures, poverty_lines, by)

    # Attach survey metadata AFTER computation (compute_measures does not
    # need nor use these columns)
    res[, pip_id        := pid]
    res[, country_code  := sdt$country_code[[1L]]]
    res[, surveyid_year := sdt$surveyid_year[[1L]]]
    res[, welfare_type  := sdt$welfare_type[[1L]]]
    res
  })

  result <- data.table::rbindlist(results, fill = TRUE)

  # ── 7. Reorder columns ──────────────────────────────────────────────────────
  # When age was binned, rename the "age" column to "age_group" in the output.
  if (age_was_binned) {
    data.table::setnames(result, "age", "age_group")
    by <- c(setdiff(by, "age"), "age_group")
  }
  meta_cols <- c("pip_id", "country_code", "surveyid_year", "welfare_type")
  dim_cols  <- if (!is.null(by)) by else character(0L)
  tail_cols <- c("poverty_line", "measure", "value", "population")
  col_order <- c(meta_cols, dim_cols, tail_cols)
  # Only reorder columns that are actually present
  col_order <- intersect(col_order, names(result))
  data.table::setcolorder(result, col_order)

  result[]
}

# ── Internal helper ───────────────────────────────────────────────────────────

# Null-coalescing operator (used internally; not exported)
`%||%` <- function(x, y) if (!is.null(x)) x else y
