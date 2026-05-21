# Shared fixture builders for {piptm} tests
#
# Loaded automatically by testthat before any test file. Provides canonical,
# single-source-of-truth helpers for writing Parquet fixtures and manifest
# JSON files used across test-load-data.R and test-table-maker.R.
#
# If the manifest schema gains a new required field, update write_fixture_parquet_tm
# and write_fixture_manifest_tm here — one change covers all test files.

library(data.table)
library(arrow)
library(jsonlite)

# ---------------------------------------------------------------------------
# write_fixture_parquet_tm()
# ---------------------------------------------------------------------------

#' Write one Parquet fixture file into a 4-level Hive partition tree.
#'
#' The more flexible variant used by table_maker tests: supports optional
#' `welfare` / `weight` vectors and additional dimension columns.
#'
#' @return Invisibly returns `pip_id`.
write_fixture_parquet_tm <- function(arrow_root,
                                     country_code,
                                     year,
                                     welfare_type,
                                     version,
                                     pip_id,
                                     survey_acronym = NULL,
                                     n_rows  = 10L,
                                     welfare = NULL,
                                     weight  = NULL,
                                     extra_cols = character(0L)) {

  dir_path <- file.path(
    arrow_root,
    paste0("country_code=",  country_code),
    paste0("surveyid_year=", year),
    paste0("welfare_type=",  welfare_type),
    paste0("version=",       version)
  )
  dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)

  if (is.null(welfare)) welfare <- seq(1, by = 1, length.out = n_rows)
  if (is.null(weight))  weight  <- rep(1.0, n_rows)

  dt <- data.table(
    country_code   = country_code,
    surveyid_year  = as.integer(year),
    welfare_type   = welfare_type,
    version        = version,
    pip_id         = pip_id,
    welfare        = as.numeric(welfare),
    weight         = as.numeric(weight)
  )

  for (col in extra_cols) {
    if (col == "gender") dt[, gender := factor(
      rep_len(c("male", "female"), n_rows), levels = c("male", "female"))]
    if (col == "area")   dt[, area   := factor(
      rep(c("urban", "urban", "rural", "rural"), length.out = n_rows),
      levels = c("urban", "rural"))]
    if (col == "age")    dt[, age    := as.integer(seq(10L, by = 10L, length.out = n_rows))]
    if (col == "educat4") dt[, educat4 := factor(
      rep_len(c("Primary", "Secondary", "Tertiary (complete or incomplete)", "No education"), n_rows))]
  }

  arrow::write_parquet(dt, file.path(dir_path, "data.parquet"))
  invisible(pip_id)
}

# ---------------------------------------------------------------------------
# write_fixture_manifest_tm()
# ---------------------------------------------------------------------------

#' Write a fixture manifest JSON for table_maker tests.
#'
#' @return Invisibly returns the path to the written manifest file.
write_fixture_manifest_tm <- function(manifest_dir, release, entries,
                                      set_current = TRUE) {
  manifest <- list(
    release      = release,
    generated_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    entries      = entries
  )
  fname <- file.path(manifest_dir, paste0("manifest_", release, ".json"))
  jsonlite::write_json(manifest, fname, auto_unbox = TRUE, pretty = TRUE)
  if (set_current) {
    jsonlite::write_json(
      list(current_release = release),
      file.path(manifest_dir, "current_release.json"),
      auto_unbox = TRUE
    )
  }
  invisible(fname)
}
