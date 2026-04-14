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
#' The returned `data.table` has a `"dimensions"` attribute: a character
#' vector of the available breakdown dimension columns recorded in the
#' manifest entry (e.g. `c("area", "gender", "age")`).
#'
#' @param country_code Character scalar ISO3 country code (e.g. `"COL"`).
#' @param year         Integer scalar survey year (e.g. `2010L`).
#' @param welfare_type Character scalar welfare type (`"INC"` or `"CON"`).
#' @param release      Character scalar release ID (e.g. `"20260206"`).
#'   Defaults to [piptm_current_release()].
#'
#' @return A `data.table` of survey microdata with attribute `"dimensions"`.
#'
#' @seealso [load_surveys()], [piptm_manifest()], [set_arrow_root()]
#' @importFrom arrow open_dataset
#' @importFrom dplyr collect
#' @importFrom data.table as.data.table setattr is.data.table
#' @importFrom cli cli_abort
#' @export
#' @examples
#' \dontrun{
#' set_manifest_dir("//server/manifests")
#' set_arrow_root("//server/pip/arrow")
#' dt <- load_survey_microdata("COL", 2010L, "INC")
#' attr(dt, "dimensions")
#' }
load_survey_microdata <- function(country_code,
                                  year,
                                  welfare_type,
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

  version    <- entry$version[[1L]]
  dimensions <- entry$dimensions[[1L]]

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

  # --- 5. Attach manifest metadata as attributes ------------------------------
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
#' @param entries_dt A `data.table` with at least the columns `country_code`,
#'   `year`, `welfare_type`, and `version` (i.e. a subset of what
#'   [piptm_manifest()] returns). These four columns map directly to the Hive
#'   partition keys and are used to construct exact leaf directory paths.
#' @param release Character scalar release ID. Used only for error messages and
#'   to attach as an attribute on the result. Defaults to [piptm_current_release()].
#'
#' @return A `data.table` of combined survey microdata with attribute
#'   `"release"`. Contains all rows from matching surveys.
#'
#' @seealso [load_survey_microdata()], [piptm_manifest()]
#' @export
#' @examples
#' \dontrun{
#' set_manifest_dir("//server/manifests")
#' set_arrow_root("//server/pip/arrow")
#' colombia <- piptm_manifest()[country_code == "COL"]
#' dt <- load_surveys(colombia)
#' }
load_surveys <- function(entries_dt, release = NULL) {

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

  data.table::setattr(dt, "release", release)

  dt[]
}
