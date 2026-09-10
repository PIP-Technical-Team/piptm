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
    welfare_ppp_2021_01_02 = as.numeric(welfare),
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

# ---------------------------------------------------------------------------
# activate_test_registry()
# ---------------------------------------------------------------------------

#' Activate a minimal variable registry for tests
#'
#' Seeds `.piptm_env$registries[[release]]` with analysis variables and
#' common covariates used across tests (`gender`, `area`, `age`, `wquintile`).
#' Also sets `.piptm_env$current_release` to `release`.
#'
#' @param release Character scalar release ID. If `NULL`, uses current release
#'   when available, otherwise falls back to `"TEST_RELEASE"`.
#' @param .local_envir Environment used by `withr::defer()` for restoration.
#'
#' @return Invisibly returns the activated release ID.
activate_test_registry <- function(release = NULL, .local_envir = parent.frame()) {
  env <- get(".piptm_env", envir = asNamespace("piptm"))

  if (is.null(release)) {
    if (!is.null(env$current_release) && nzchar(env$current_release)) {
      release <- env$current_release
    } else {
      release <- "TEST_RELEASE"
    }
  }

  old_reg <- env$registries
  old_rel <- env$current_release

  registry <- list(
    welfare = list(
      varname = "welfare",
      ui_label = "Welfare",
      tm_type = "welfare",
      roles = c("analysis_var"),
      stat_groups = c("summary_statistics", "inequality"),
      n_categories = NULL,
      categories = NULL
    ),
    pov_status = list(
      varname = "pov_status",
      ui_label = "Poverty status",
      tm_type = "poverty",
      roles = c("analysis_var", "covariate"),
      stat_groups = c("poverty"),
      n_categories = 2L,
      categories = NULL
    ),
    gender = list(
      varname = "gender",
      ui_label = "Gender",
      tm_type = "categorical",
      roles = c("covariate", "filter"),
      stat_groups = character(0L),
      n_categories = 2L,
      categories = list(
        list(code = "male", label = "Male"),
        list(code = "female", label = "Female")
      )
    ),
    area = list(
      varname = "area",
      ui_label = "Area",
      tm_type = "categorical",
      roles = c("covariate", "filter"),
      stat_groups = character(0L),
      n_categories = 2L,
      categories = list(
        list(code = "urban", label = "Urban"),
        list(code = "rural", label = "Rural")
      )
    ),
    age = list(
      varname = "age",
      ui_label = "Age",
      tm_type = "continuous",
      roles = c("covariate", "filter"),
      stat_groups = character(0L),
      n_categories = NULL,
      categories = NULL
    ),
    wquintile = list(
      varname = "wquintile",
      ui_label = "Welfare quintile",
      tm_type = "categorical",
      roles = c("covariate", "filter"),
      stat_groups = character(0L),
      n_categories = 5L,
      categories = list(
        list(code = "1", label = "Q1"),
        list(code = "2", label = "Q2"),
        list(code = "3", label = "Q3"),
        list(code = "4", label = "Q4"),
        list(code = "5", label = "Q5")
      )
    )
  )

  if (is.null(env$registries)) {
    env$registries <- list()
  }
  env$registries[[release]] <- registry
  env$current_release <- release

  withr::defer({
    env$registries <- old_reg
    env$current_release <- old_rel
  }, envir = .local_envir)

  invisible(release)
}
