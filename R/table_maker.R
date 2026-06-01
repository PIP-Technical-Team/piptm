#' @importFrom data.table is.data.table data.table setcolorder set fsetdiff
#' @importFrom cli cli_abort cli_warn
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
#' @param ppp          Integer scalar PPP year (e.g. `2017L`), or `NULL`.
#'   Passed through to [load_surveys()]. Selects which `welfare_ppp_*` column
#'   to use as `welfare` for surveys written with the deflated-data schema.
#'   When `NULL` (default), the manifest `ppp_sort` default is used.
#' @param release Character scalar release ID (e.g. `"20260206"`). Defaults
#'   to the current release as returned by [piptm_current_release()].
#' @param pop_share_threshold Numeric scalar in (0, 1) or `NULL`.  When
#'   non-NULL and `"pop_share"` is among the requested `measures`, cells whose
#'   population share falls below this threshold have all non-share measures
#'   suppressed (dropped from output).  `pop_share` and `obs_share` rows are
#'   always retained.  Default: `0.01` (1%).
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
#'   \item{`poverty_line`}{Poverty threshold; `NA` for non-poverty measures.}
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
                        ppp           = 2021L,
                        release       = NULL,
                        pop_share_threshold = 0.01) {

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
    # The manifest stores the pre-binning dimension name "age" (not "age_group"),
    # so we can intersect directly against `by` without any remapping.
    by_check <- by

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
      cli_warn(
        c(
          "Excluding {length(dropped_info)} survey{?s} that lack all requested dimensions ({.val {by}}):",
          "i" = "{dropped_info}"
        )
      )
      entries <- entries[full_idx]
    }

    if (nrow(entries) == 0L) {
      cli_abort(
        c(
          "All requested surveys were excluded: none have all of the requested dimensions ({.val {by}}).",
          "i" = "Check {.fn piptm_manifest} for available dimensions per survey."
        )
      )
    }
  }

  # ── 4. Load ─────────────────────────────────────────────────────────────────
  # Column pruning: request only the columns needed for computation.
  # "welfare" is the logical name; load_surveys() translates it to the physical
  # PPP column internally. country_code / surveyid_year / welfare_type are
  # needed for the metadata join in Step 8.
  needed_cols <- unique(c(
    "pip_id", "country_code", "surveyid_year", "welfare_type",
    "welfare", "weight",
    by   # NULL is silently dropped by c()
  ))
  dt <- load_surveys(entries, ppp = ppp, cols = needed_cols, release = release)

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

  # ── 6. (Dimension NA-fill removed) ────────────────────────────────────────
  # Surveys that do not carry all requested `by` dimensions are excluded in
  # Step 3 above, so every row in `dt` is guaranteed to have all dimension
  # columns present. No NA-fill is needed.

  # ── 7. Batch compute ────────────────────────────────────────────────────────
  # Single grouped call across all surveys (Approach B). compute_measures()
  # uses GRP(c("pip_id", by)) internally, paying the overhead once instead
  # of once per survey.
  result <- compute_measures(dt, measures, poverty_lines, by)

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

  # ── 10. Pop-share threshold suppression ─────────────────────────────────────
  # When pop_share is requested and threshold is set, drop non-share measures
  # for cells whose pop_share falls below the threshold.
  if (!is.null(pop_share_threshold) && "pop_share" %in% measures) {
    # Validate threshold
    if (!is.numeric(pop_share_threshold) || length(pop_share_threshold) != 1L ||
        !is.finite(pop_share_threshold) || pop_share_threshold <= 0 || pop_share_threshold >= 1) {
      cli_abort(
        c(
          "{.arg pop_share_threshold} must be a single numeric value in (0, 1), or {.code NULL} to disable.",
          "i" = "Got: {.val {pop_share_threshold}}."
        )
      )
    }

    # Identify cell keys (pip_id + dimension columns)
    cell_keys <- c("pip_id", dim_cols)

    # Extract pop_share rows and find below-threshold cells
    pop_rows <- result[measure == "pop_share"]
    suppressed <- pop_rows[value < pop_share_threshold]

    if (nrow(suppressed) > 0L) {
      # Build warning message
      sup_labels <- vapply(seq_len(nrow(suppressed)), function(i) {
        dims <- paste(
          vapply(cell_keys, function(k) paste0(k, "=", suppressed[[k]][[i]]), character(1L)),
          collapse = ", "
        )
        paste0(dims, " (pop_share=", round(suppressed[["value"]][[i]], 4L), ")")
      }, character(1L))

      cli_warn(
        c(
          "Suppressing measures for {nrow(suppressed)} cell{?s} with pop_share below {pop_share_threshold}:",
          "i" = "{sup_labels}"
        )
      )

      # Anti-join: drop rows for suppressed cells where measure is not a share
      share_measures <- c("pop_share", "obs_share")
      sup_keys <- suppressed[, ..cell_keys]
      result[, .suppress := FALSE]
      idx <- result[sup_keys, on = cell_keys, which = TRUE, nomatch = NULL]
      result[idx, .suppress := !(measure %chin% share_measures)]
      result <- result[(.suppress) == FALSE][, .suppress := NULL]
    }
  }

  result[]
}

