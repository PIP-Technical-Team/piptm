# Survey Microdata Loading Functions
#
# Plan: .cg-docs/plans/2026-04-03-manifest-generation-version-partition.md
#       (Step 5)
#
# Provides functions for loading PIP survey microdata from the shared Arrow
# repository. The manifest is the authoritative source for the `version`
# partition key, ensuring reproducibility across releases.
#
# Partition structure (4-level Hive):
#   <arrow_root>/country_code=<cc>/surveyid_year=<yr>/welfare_type=<wt>/version=<ver>/
#
# Both loading functions use the same path-based backend (.build_parquet_paths):
#   1. Construct the exact Hive leaf directory path from the four partition keys.
#   2. Discover *.parquet files in that directory (non-recursive).
#   3. Open via open_dataset(files, format = "parquet") |> collect().
# This avoids a global repository scan and is ~19x faster than filtering on
# data columns.
#
# Internal helpers
# ----------------
#   .build_parquet_paths()   — construct leaf paths + discover parquet files
#
# Exported functions
# ------------------
#   load_survey_microdata()  — load a single survey by (country, year, welfare_type)
#   load_surveys()           — batch-load via a manifest data.table subset

# ---------------------------------------------------------------------------
# .build_parquet_paths()  — shared internal helper
# ---------------------------------------------------------------------------

#' Construct the Hive leaf directory path and discover Parquet files
#'
#' Given the four partition keys for one survey, builds the exact leaf
#' directory path under `arrow_root` and returns all `.parquet` files found
#' directly in that directory (non-recursive).  Errors if no files are found.
#'
#' @param arrow_root   Character scalar. Root of the shared Arrow repository.
#' @param country_code Character scalar. ISO3 country code.
#' @param year         Integer scalar. Survey year.
#' @param welfare_type Character scalar. `"INC"` or `"CON"`.
#' @param version      Character scalar. Version string (e.g. `"v01_v04"`).
#'
#' @return Character vector of absolute `.parquet` file paths.
#' @keywords internal
.build_parquet_paths <- function(arrow_root, country_code, year,
                                  welfare_type, version) {
  leaf <- file.path(
    arrow_root,
    paste0("country_code=",  country_code),
    paste0("surveyid_year=", year),
    paste0("welfare_type=",  welfare_type),
    paste0("version=",       version)
  )
  files <- list.files(leaf, pattern = "\\.parquet$",
                      full.names = TRUE, recursive = FALSE)
  if (length(files) == 0L) {
    cli::cli_abort(
      c(
        "No Parquet files found for {.val {country_code}} / {year} / {.val {welfare_type}} / version {.val {version}}.",
        "i" = "Expected partition path: {.path {leaf}}",
        "i" = "Arrow root: {.path {arrow_root}}"
      )
    )
  }
  files
}

# ---------------------------------------------------------------------------
# load_survey_microdata()
# ---------------------------------------------------------------------------

#' Load microdata for a single survey from the shared Arrow repository
#'
#' Looks up the manifest for `release` (defaulting to the current release),
#' finds the entry matching `country_code`, `year`, and `welfare_type`,
#' extracts the `version` partition key, then constructs the exact Hive leaf
#' directory path and loads only the Parquet files in that directory.
#'
#' For surveys written with the deflated-data schema (multiple `welfare_ppp_*`
#' columns), the `ppp` argument controls which welfare column is selected and
#' returned as `welfare`. For legacy surveys with a single `welfare` column,
#' the `ppp` argument has no effect.
#'
#' The returned `data.table` has a `"dimensions"` attribute: a character
#' vector of the available breakdown dimension columns recorded in the
#' manifest entry (e.g. `c("area", "gender", "age")`).
#'
#' @param country_code Character scalar ISO3 country code (e.g. `"COL"`).
#' @param year         Integer scalar survey year (e.g. `2010L`).
#' @param welfare_type Character scalar welfare type (`"INC"` or `"CON"`).
#' @param ppp          Integer scalar PPP year (e.g. `2017L`), or `NULL`.
#'   When non-NULL, selects the `welfare_ppp_<ppp>` column and renames it to
#'   `welfare`. When `NULL` (default), the manifest's `ppp_sort` field is used
#'   as the default PPP for new-schema surveys; errors if `ppp_sort` is `NA`.
#'   Has no effect for legacy surveys whose manifest entry has no `welfare_vars`.
#' @param release      Character scalar release ID (e.g. `"20260206"`).
#'   Defaults to [piptm_current_release()].
#'
#' @return A `data.table` of survey microdata with a single `welfare` column
#'   and attribute `"dimensions"`.
#'
#' @seealso [load_surveys()], [piptm_manifest()], [set_arrow_root()]
#' @importFrom arrow open_dataset
#' @importFrom dplyr collect
#' @importFrom data.table as.data.table setattr is.data.table setnames
#' @importFrom cli cli_abort
#' @export
#' @examples
#' \dontrun{
#' set_manifest_dir("//server/manifests")
#' set_arrow_root("//server/pip/arrow")
#' dt <- load_survey_microdata("COL", 2010L, "INC", ppp = 2017L)
#' attr(dt, "dimensions")
#' }
load_survey_microdata <- function(country_code,
                                  year,
                                  welfare_type,
                                  ppp     = NULL,
                                  release = NULL) {

  stopifnot(
    is.character(country_code), length(country_code) == 1L, !is.na(country_code),
    is.numeric(year),           length(year) == 1L,          !is.na(year),
    is.character(welfare_type), length(welfare_type) == 1L,  !is.na(welfare_type)
  )

  year <- as.integer(year)

  # --- 1. Resolve release and manifest ----------------------------------------
  if (is.null(release)) {
    release <- piptm_current_release()
    if (is.null(release)) {
      cli::cli_abort(
        c(
          "No current release is set.",
          "i" = "Call {.fn set_manifest_dir} to load manifests, or pass {.arg release} explicitly."
        )
      )
    }
  }

  mf <- piptm_manifest(release)

  # --- 2. Filter manifest to the requested survey -----------------------------
  # Rename scalars before filtering to avoid data.table's scoping rules:
  # inside [.data.table, bare names resolve to columns first, so a column
  # named `country_code` would shadow the function argument of the same name,
  # making `country_code == country_code` always TRUE and returning all rows.
  # Using distinct local names (prefixed with `.`) sidesteps the collision.
  .cc  <- country_code
  .yr  <- year
  .wt  <- welfare_type

  entry <- mf[
    country_code == .cc &
    year         == .yr &
    welfare_type == .wt
  ]

  if (nrow(entry) == 0L) {
    cli::cli_abort(
      c(
        "No manifest entry found for {.val {country_code}} / {year} / {.val {welfare_type}} in release {.val {release}}.",
        "i" = "Check {.fn piptm_manifest} for available surveys in this release."
      )
    )
  }

  if (nrow(entry) > 1L) {
    cli::cli_abort(
      c(
        "Multiple manifest entries found for {.val {country_code}} / {year} / {.val {welfare_type}} in release {.val {release}}.",
        "i" = "This indicates a corrupt manifest. Expected exactly one match."
      )
    )
  }

  version      <- entry$version[[1L]]
  dimensions   <- entry$dimensions[[1L]]
  welfare_vars <- entry$welfare_vars[[1L]]
  ppp_sort_val <- entry$ppp_sort[[1L]]

  # --- 3. Resolve Arrow root --------------------------------------------------
  arrow_root <- piptm_arrow_root()
  if (is.null(arrow_root)) {
    cli::cli_abort(
      c(
        "Arrow root is not configured.",
        "i" = "Call {.fn set_arrow_root} to set the path to the Arrow repository."
      )
    )
  }

  # --- 4. Load data via path-based Parquet discovery -------------------------
  # .build_parquet_paths() constructs the exact Hive leaf directory path from
  # the four partition keys and discovers *.parquet files non-recursively.
  # This avoids opening the entire repository and is ~19x faster than
  # open_dataset(arrow_root) |> filter(...) on data columns.
  parquet_files <- .build_parquet_paths(
    arrow_root    = arrow_root,
    country_code  = country_code,
    year          = year,
    welfare_type  = welfare_type,
    version       = version
  )

  dt <- arrow::open_dataset(parquet_files, format = "parquet") |>
    dplyr::collect() |>
    data.table::as.data.table()

  if (nrow(dt) == 0L) {
    cli::cli_abort(
      c(
        "Arrow query returned 0 rows for {.val {country_code}} / {year} / {.val {welfare_type}} / version {.val {version}}.",
        "i" = "The Parquet partition may be missing from the Arrow repository.",
        "i" = "Arrow root: {.path {arrow_root}}"
      )
    )
  }

  # --- 5. PPP welfare column selection (new-schema surveys only) --------------
  # New-schema surveys have multiple welfare_ppp_* columns (welfare_vars != "").
  # Legacy surveys have a single `welfare` column — skip selection.
  if (length(welfare_vars) > 0L) {
    # Helper: find welfare column by PPP year prefix.
    # Column names follow the pattern welfare_ppp_<year>_<vermast>_<veralt>.
    # Prefix matching: ppp = 2017 matches "welfare_ppp_2017_01_02" etc.
    find_ppp_col <- function(wv, year_val) {
      prefix <- paste0("welfare_ppp_", year_val)
      wv[wv == prefix | startsWith(wv, paste0(prefix, "_"))]
    }

    if (!is.null(ppp)) {
      candidates <- find_ppp_col(welfare_vars, ppp)
      if (length(candidates) == 0L) {
        available_ppp <- unique(sub(
          "^welfare_ppp_([0-9]+).*", "\\1",
          welfare_vars[grepl("^welfare_ppp_", welfare_vars)]
        ))
        cli::cli_abort(
          c(
            "PPP {.val {ppp}} not available for {.val {country_code}} / {year} / {.val {welfare_type}}.",
            "i" = "Available PPPs: {.val {available_ppp}}"
          )
        )
      }
      target_col <- candidates[[1L]]
    } else {
      if (is.na(ppp_sort_val)) {
        cli::cli_abort(
          c(
            "No default PPP available for {.val {country_code}} / {year} / {.val {welfare_type}}.",
            "i" = "The manifest entry has no {.field ppp_sort}. Pass {.arg ppp} explicitly."
          )
        )
      }
      candidates <- find_ppp_col(welfare_vars, ppp_sort_val)
      if (length(candidates) == 0L) {
        cli::cli_abort(
          c(
            "Default PPP year {.val {ppp_sort_val}} not found in the manifest welfare columns.",
            "i" = "Available welfare columns: {.val {welfare_vars}}"
          )
        )
      }
      target_col <- candidates[[1L]]
    }
    welfare_data_cols <- intersect(welfare_vars, names(dt))
    data.table::setnames(dt, target_col, "welfare")
    drop_cols <- setdiff(welfare_data_cols, target_col)
    if (length(drop_cols) > 0L) dt[, (drop_cols) := NULL]
  }

  # --- 6. Attach manifest metadata as attributes ------------------------------
  data.table::setattr(dt, "dimensions", dimensions)
  data.table::setattr(dt, "pip_id",     entry$pip_id[[1L]])
  data.table::setattr(dt, "release",    release)

  dt[]
}

# ---------------------------------------------------------------------------
# load_surveys()
# ---------------------------------------------------------------------------

#' Batch-load microdata for multiple surveys from the shared Arrow repository
#'
#' Accepts a subset of a manifest `data.table` (as returned by
#' [piptm_manifest()], possibly filtered by the caller) and loads all
#' matching surveys by opening their exact Hive partition directories directly.
#' This avoids a global dataset scan and is significantly faster than filtering
#' on data columns.
#'
#' The returned `data.table` contains all surveys combined (row-bound). A
#' `pip_id` column (already present in the Parquet files) identifies each row's
#' survey. A `"release"` attribute records the release ID.
#'
#' For surveys written with the deflated-data schema (multiple `welfare_ppp_*`
#' columns), the `ppp` argument selects a single welfare column uniformly
#' across all surveys and renames it to `welfare`. Legacy surveys (no
#' `welfare_vars` in manifest entry) are loaded as-is.
#'
#' @param entries_dt A `data.table` with at least the columns `country_code`,
#'   `year`, `welfare_type`, and `version` (i.e. a subset of what
#'   [piptm_manifest()] returns). These four columns map directly to the Hive
#'   partition keys and are used to construct exact leaf directory paths.
#' @param ppp        Integer scalar PPP year (e.g. `2017L`), or `NULL`.
#'   Applied uniformly to all new-schema surveys in `entries_dt`.  When
#'   `NULL` (default), `ppp_sort` from each entry's manifest row is used; if
#'   entries have inconsistent `ppp_sort` values, an error is raised asking
#'   the caller to supply `ppp` explicitly.  Has no effect for legacy surveys.
#' @param release Character scalar release ID. Used only for error messages and
#'   to attach as an attribute on the result. Defaults to [piptm_current_release()].
#'
#' @return A `data.table` of combined survey microdata with a single `welfare`
#'   column and attribute `"release"`. Contains all rows from matching surveys.
#'
#' @seealso [load_survey_microdata()], [piptm_manifest()]
#' @export
#' @examples
#' \dontrun{
#' set_manifest_dir("//server/manifests")
#' set_arrow_root("//server/pip/arrow")
#' colombia <- piptm_manifest()[country_code == "COL"]
#' dt <- load_surveys(colombia, ppp = 2017L)
#' }
load_surveys <- function(entries_dt, ppp = NULL, release = NULL) {

  stopifnot(
    data.table::is.data.table(entries_dt),
    all(c("country_code", "year", "welfare_type", "version") %in% names(entries_dt))
  )

  if (nrow(entries_dt) == 0L) {
    cli::cli_abort(
      c(
        "{.arg entries_dt} has 0 rows — nothing to load.",
        "i" = "Pass a non-empty subset of {.fn piptm_manifest}."
      )
    )
  }

  if (is.null(release)) {
    release <- piptm_current_release()
  }

  # --- Resolve Arrow root -----------------------------------------------------
  arrow_root <- piptm_arrow_root()
  if (is.null(arrow_root)) {
    cli::cli_abort(
      c(
        "Arrow root is not configured.",
        "i" = "Call {.fn set_arrow_root} to set the path to the Arrow repository."
      )
    )
  }

  # --- Build exact partition paths and collect Parquet files -----------------
  # Call .build_parquet_paths() per survey row so any missing partition
  # directory raises an error immediately (no silent partial-miss).
  parquet_files <- unlist(lapply(seq_len(nrow(entries_dt)), function(i) {
    .build_parquet_paths(
      arrow_root    = arrow_root,
      country_code  = entries_dt$country_code[[i]],
      year          = entries_dt$year[[i]],
      welfare_type  = entries_dt$welfare_type[[i]],
      version       = entries_dt$version[[i]]
    )
  }))

  # --- Load data from exact Parquet files -------------------------------------
  dt <- arrow::open_dataset(parquet_files, format = "parquet") |>
    dplyr::collect() |>
    data.table::as.data.table()

  if (nrow(dt) == 0L) {
    cli::cli_abort(
      c(
        "Arrow query returned 0 rows for the requested surveys.",
        "i" = "Arrow root: {.path {arrow_root}}"
      )
    )
  }

  # --- Integrity check: verify only requested surveys were loaded ------------
  # pip_id is stored in every Parquet file and encodes the exact survey tuple.
  # An unexpected pip_id indicates path construction error or partition
  # contamination.
  loaded_ids   <- unique(dt$pip_id)
  unexpected   <- setdiff(loaded_ids, entries_dt$pip_id)
  if (length(unexpected) > 0L) {
    cli::cli_abort(
      c(
        "Loaded unexpected survey(s): {.val {unexpected}}.",
        "i" = "This may indicate partition path contamination or a manifest mismatch.",
        "i" = "Arrow root: {.path {arrow_root}}"
      )
    )
  }

  # --- PPP welfare column selection (new-schema surveys only) ----------------
  # New-schema entries carry non-empty welfare_vars in entries_dt.
  # Legacy entries (welfare_vars absent or empty) are skipped.
  has_welfare_vars <- "welfare_vars" %in% names(entries_dt)
  if (has_welfare_vars) {
    new_schema_mask <- vapply(
      entries_dt$welfare_vars,
      function(wv) length(wv) > 0L,
      logical(1L)
    )
  } else {
    new_schema_mask <- rep(FALSE, nrow(entries_dt))
  }

  # Helper: find welfare column(s) matching PPP year by prefix.
  find_ppp_col_wv <- function(wv, year_val) {
    prefix <- paste0("welfare_ppp_", year_val)
    wv[wv == prefix | startsWith(wv, paste0(prefix, "_"))]
  }

  if (any(new_schema_mask)) {
    if (!is.null(ppp)) {
      effective_year <- ppp
    } else {
      # ppp = NULL: determine uniform ppp_sort from new-schema entries
      if (!"ppp_sort" %in% names(entries_dt)) {
        cli::cli_abort(
          c(
            "Cannot determine default PPP: {.field ppp_sort} column absent from {.arg entries_dt}.",
            "i" = "Pass {.arg ppp} explicitly."
          )
        )
      }
      ppp_sorts <- entries_dt$ppp_sort[new_schema_mask]
      ppp_sorts <- ppp_sorts[!is.na(ppp_sorts)]
      if (length(ppp_sorts) == 0L) {
        cli::cli_abort(
          c(
            "No default PPP available: all new-schema manifest entries have {.field ppp_sort = NA}.",
            "i" = "Pass {.arg ppp} explicitly."
          )
        )
      }
      unique_sorts <- unique(ppp_sorts)
      if (length(unique_sorts) > 1L) {
        cli::cli_abort(
          c(
            "New-schema surveys have different {.field ppp_sort} values: {.val {unique_sorts}}.",
            "i" = "Pass {.arg ppp} explicitly to use a uniform PPP across all surveys."
          )
        )
      }
      effective_year <- unique_sorts[[1L]]
    }

    # Validate all new-schema entries have a column matching this PPP year
    bad_mask <- new_schema_mask & !vapply(
      entries_dt$welfare_vars,
      function(wv) length(find_ppp_col_wv(wv, effective_year)) > 0L,
      logical(1L)
    )
    if (any(bad_mask)) {
      bad_ids <- entries_dt$pip_id[bad_mask]
      cli::cli_abort(
        c(
          "PPP {.val {effective_year}} not available in all surveys.",
          "i" = "Missing matching welfare column in: {.val {bad_ids}}"
        )
      )
    }

    # Determine the actual target column name from the first new-schema entry.
    # All surveys in a release are expected to share the same full column name
    # for a given PPP year (same version suffix). Error if they diverge.
    first_idx <- which(new_schema_mask)[[1L]]
    target_cols <- unique(vapply(
      which(new_schema_mask),
      function(i) find_ppp_col_wv(entries_dt$welfare_vars[[i]], effective_year)[[1L]],
      character(1L)
    ))
    if (length(target_cols) > 1L) {
      cli::cli_abort(
        c(
          "Surveys have different full column names for PPP year {.val {effective_year}}:",
          "i" = "{.val {target_cols}}",
          "i" = "This can occur when surveys come from different pipeline versions. Load surveys separately."
        )
      )
    }
    target_col <- target_cols[[1L]]

    # Use union of welfare_vars from all new-schema entries to identify welfare
    # data columns in the loaded data. This avoids broad grep patterns that
    # would incorrectly match `welfare_type` (a partition key).
    all_welfare_vars <- unique(unlist(entries_dt$welfare_vars[new_schema_mask]))
    welfare_data_cols <- intersect(all_welfare_vars, names(dt))
    if (target_col %in% welfare_data_cols) {
      data.table::setnames(dt, target_col, "welfare")
      drop_cols <- setdiff(welfare_data_cols, target_col)
      if (length(drop_cols) > 0L) dt[, (drop_cols) := NULL]
    } else {
      cli::cli_abort(
        c(
          "Expected welfare column {.val {target_col}} not found in loaded data.",
          "i" = "Columns present: {.val {names(dt)}}"
        )
      )
    }
  }

  data.table::setattr(dt, "release", release)

  dt[]
}
