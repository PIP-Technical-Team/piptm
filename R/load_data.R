# Survey Microdata Loading Functions
#
# Plan: .cg-docs/plans/2026-04-03-manifest-generation-version-partition.md
#       (Step 5)
#
# Provides functions for loading PIP survey microdata from the shared Arrow
# repository using partition pushdown via open_dataset() |> filter(). The
# manifest is used as the authoritative lookup for the `version` partition
# key, ensuring reproducibility across releases.
#
# Partition structure (4-level Hive):
#   <arrow_root>/country=<cc>/year=<yr>/welfare_type=<wt>/version=<ver>/
#
# Note: the Parquet column is `surveyid_year` (integer); the Arrow Hive
# partition directory is `year=<yr>`. Arrow exposes the directory value as
# the partition column, and the Parquet column `surveyid_year` carries the
# same integer inside each file. Filtering on `surveyid_year` inside
# open_dataset() applies partition pruning because Arrow recognises `year` as
# the hive key.  We therefore filter on `surveyid_year` to match the schema.
#
# Exported functions
# ------------------
#   load_survey_microdata()  — load a single survey by (release, country, year, welfare_type)
#   load_surveys()           — batch load via a manifest data.table subset

# ---------------------------------------------------------------------------
# load_survey_microdata()
# ---------------------------------------------------------------------------

#' Load microdata for a single survey from the shared Arrow repository
#'
#' Looks up the manifest for `release` (defaulting to the current release),
#' finds the entry matching `country_code`, `year`, and `welfare_type`,
#' extracts the `version` partition key, then opens the shared Arrow dataset
#' and applies a partition-pushdown filter to retrieve only the matching rows.
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
#' @importFrom dplyr filter collect
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

  # --- 4. Load data via partition pushdown ------------------------------------
  cc  <- country_code
  yr  <- year
  wt  <- welfare_type
  ver <- version

  dt <- arrow::open_dataset(arrow_root) |>
    dplyr::filter(
      country_code  == cc,
      surveyid_year == yr,
      welfare_type  == wt,
      version       == ver
    ) |>
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
#' matching surveys in a single `open_dataset() |> filter()` call using `%in%`
#' filters on the four partition keys.
#'
#' The returned `data.table` contains all surveys combined (row-bound). A
#' `pip_id` column (already present in the Parquet files) identifies each row's
#' survey. A `"release"` attribute records the release ID.
#'
#' @param entries_dt A `data.table` with at least the columns `country_code`,
#'   `year`, `welfare_type`, `version`, and `pip_id` (i.e. a subset of what
#'   [piptm_manifest()] returns). `pip_id` is used as the exact-tuple filter
#'   key against the Parquet data to avoid Cartesian over-fetching.
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
    all(c("country_code", "year", "welfare_type", "version", "pip_id") %in% names(entries_dt))
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

  # --- Build exact-tuple filter -----------------------------------------------
  # Independent %in% filters on each partition key are incorrect: they form a
  # Cartesian product and can match surveys not present in entries_dt.
  # Example: requesting COL/2010/INC and ARG/2004/INC would also pass
  # COL/2004/INC through the filter if it happens to exist in the repository.
  #
  # The fix: filter on pip_id, which encodes the exact (cc/yr/wt/ver) tuple
  # and is stored as a column in every Parquet file.
  pip_ids <- unique(entries_dt$pip_id)

  # --- Load data via pip_id filter --------------------------------------------
  dt <- arrow::open_dataset(arrow_root) |>
    dplyr::filter(pip_id %in% pip_ids) |>
    dplyr::collect() |>
    data.table::as.data.table()

  if (nrow(dt) == 0L) {
    cli::cli_abort(
      c(
        "Arrow query returned 0 rows for the requested surveys.",
        "i" = "The Parquet partitions may be missing from the Arrow repository.",
        "i" = "Arrow root: {.path {arrow_root}}"
      )
    )
  }

  data.table::setattr(dt, "release", release)

  dt[]
}
