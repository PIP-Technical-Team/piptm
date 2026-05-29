# Tests for load_data.R
#
# Strategy: write fixture Parquet files in a temp 4-level Hive partition
# directory, write a fixture manifest JSON, load via load_survey_microdata()
# and load_surveys(), and verify data integrity and attributes.

library(data.table)
library(arrow)

# ---------------------------------------------------------------------------
# Helpers — fixture builders
# ---------------------------------------------------------------------------

#' Write one Parquet file for a survey into a 4-level Hive partition tree.
#' Returns the path to the written file.
write_fixture_parquet <- function(arrow_root,
                                  country_code,
                                  year,
                                  welfare_type,
                                  version,
                                  pip_id,
                                  survey_acronym,
                                  n_rows = 5L,
                                  extra_cols = character(0L)) {

  dir_path <- file.path(
    arrow_root,
    paste0("country_code=",  country_code),
    paste0("surveyid_year=", year),
    paste0("welfare_type=",  welfare_type),
    paste0("version=",       version)
  )
  dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)

  dt <- data.table(
    country_code   = rep(country_code,   n_rows),
    surveyid_year  = rep(as.integer(year), n_rows),
    welfare_type   = rep(welfare_type,   n_rows),
    version        = rep(version,        n_rows),
    pip_id         = rep(pip_id,         n_rows),
    survey_acronym = rep(survey_acronym, n_rows),
    welfare_ppp_2021_01_02 = seq(100, by = 50, length.out = n_rows),
    weight         = rep(1.0,           n_rows)
  )

  # Append extra dimension columns (e.g. "gender", "area")
  for (col in extra_cols) {
    if (col == "gender") dt[, gender := factor(c("male", "female", "male", "female", "male")[seq_len(n_rows)], levels = c("male", "female"))]
    if (col == "area")   dt[, area   := factor(c("urban", "rural",  "urban", "rural",  "urban")[seq_len(n_rows)], levels = c("urban", "rural"))]
    if (col == "age")    dt[, age    := as.integer(seq(20L, by = 5L, length.out = n_rows))]
  }

  out_file <- file.path(dir_path, "data.parquet")
  arrow::write_parquet(dt, out_file)
  invisible(out_file)
}

#' Write a fixture manifest JSON with one or more entries.
write_fixture_manifest <- function(manifest_dir,
                                   release,
                                   entries,
                                   set_current = FALSE) {

  manifest <- list(
    release      = release,
    generated_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    entries      = entries
  )

  fname <- file.path(manifest_dir, paste0("manifest_", release, ".json"))
  jsonlite::write_json(manifest, fname, auto_unbox = TRUE, pretty = TRUE)

  if (set_current) {
    pointer <- list(current_release = release)
    jsonlite::write_json(
      pointer,
      file.path(manifest_dir, "current_release.json"),
      auto_unbox = TRUE
    )
  }

  invisible(fname)
}

# ---------------------------------------------------------------------------
# Shared fixture setup (re-used across multiple test blocks via local())
# ---------------------------------------------------------------------------

make_fixtures <- function(env = parent.frame()) {
  tmp_arrow    <- withr::local_tempdir(.local_envir = env)
  tmp_manifest <- withr::local_tempdir(.local_envir = env)

  # Survey 1: COL 2010 INC v01_v02, with gender + area dimensions
  write_fixture_parquet(
    arrow_root    = tmp_arrow,
    country_code  = "COL",
    year          = 2010L,
    welfare_type  = "INC",
    version       = "v01_v02",
    pip_id        = "COL_2010_ECH_INC_ALL",
    survey_acronym = "ECH",
    extra_cols    = c("gender", "area")
  )

  # Survey 2: BOL 2015 CON v01_v01, no dimensions
  write_fixture_parquet(
    arrow_root    = tmp_arrow,
    country_code  = "BOL",
    year          = 2015L,
    welfare_type  = "CON",
    version       = "v01_v01",
    pip_id        = "BOL_2015_EH_CON_ALL",
    survey_acronym = "EH",
    extra_cols    = character(0L)
  )

  # Survey 3: COL 2015 INC v02_v01, with age dimension
  write_fixture_parquet(
    arrow_root    = tmp_arrow,
    country_code  = "COL",
    year          = 2015L,
    welfare_type  = "INC",
    version       = "v02_v01",
    pip_id        = "COL_2015_ECH_INC_ALL",
    survey_acronym = "ECH",
    extra_cols    = c("age")
  )

  entries_list <- list(
    list(
      pip_id         = "COL_2010_ECH_INC_ALL",
      survey_id      = "COL_2010_ECH_v01_M_v02_A_GMD_ALL",
      country_code   = "COL",
      year           = 2010L,
      welfare_type   = "INC",
      version        = "v01_v02",
      survey_acronym = "ECH",
      module         = "ALL",
      dimensions     = list("gender", "area"),
      welfare_vars   = list("welfare_ppp_2021_01_02"),
      ppp_sort       = 2021L
    ),
    list(
      pip_id         = "BOL_2015_EH_CON_ALL",
      survey_id      = "BOL_2015_EH_v01_M_v01_A_GMD_ALL",
      country_code   = "BOL",
      year           = 2015L,
      welfare_type   = "CON",
      version        = "v01_v01",
      survey_acronym = "EH",
      module         = "ALL",
      dimensions     = list(),
      welfare_vars   = list("welfare_ppp_2021_01_02"),
      ppp_sort       = 2021L
    ),
    list(
      pip_id         = "COL_2015_ECH_INC_ALL",
      survey_id      = "COL_2015_ECH_v02_M_v01_A_GMD_ALL",
      country_code   = "COL",
      year           = 2015L,
      welfare_type   = "INC",
      version        = "v02_v01",
      survey_acronym = "ECH",
      module         = "ALL",
      dimensions     = list("age"),
      welfare_vars   = list("welfare_ppp_2021_01_02"),
      ppp_sort       = 2021L
    )
  )

  write_fixture_manifest(
    manifest_dir = tmp_manifest,
    release      = "20260206",
    entries      = entries_list,
    set_current  = TRUE
  )

  list(
    tmp_arrow    = tmp_arrow,
    tmp_manifest = tmp_manifest
  )
}

# ---------------------------------------------------------------------------
# load_survey_microdata() — single survey
# ---------------------------------------------------------------------------

test_that("load_survey_microdata() returns correct data for a single survey", {
  fx <- make_fixtures()

  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer({
    env <- getNamespace("piptm")$.piptm_env
    env$arrow_root    <- NULL
    env$manifest_dir  <- NULL
    env$manifests     <- list()
    env$current_release <- NULL
  })

  dt <- piptm::load_survey_microdata("COL", 2010L, "INC")

  expect_s3_class(dt, "data.table")
  expect_equal(nrow(dt), 5L)
  expect_true("welfare" %in% names(dt))
  expect_true("weight"  %in% names(dt))
  expect_identical(unique(dt$country_code), "COL")
  expect_identical(unique(dt$surveyid_year), 2010L)
  expect_identical(unique(dt$welfare_type),  "INC")
  expect_identical(unique(dt$version),       "v01_v02")
})

test_that("load_survey_microdata() attaches dimensions attribute from manifest", {
  fx <- make_fixtures()

  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer({
    env <- getNamespace("piptm")$.piptm_env
    env$arrow_root    <- NULL
    env$manifest_dir  <- NULL
    env$manifests     <- list()
    env$current_release <- NULL
  })

  dt <- piptm::load_survey_microdata("COL", 2010L, "INC")
  expect_identical(attr(dt, "dimensions"), c("gender", "area"))
})

test_that("load_survey_microdata() attaches pip_id attribute", {
  fx <- make_fixtures()

  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer({
    env <- getNamespace("piptm")$.piptm_env
    env$arrow_root    <- NULL
    env$manifest_dir  <- NULL
    env$manifests     <- list()
    env$current_release <- NULL
  })

  dt <- piptm::load_survey_microdata("COL", 2010L, "INC")
  expect_identical(attr(dt, "pip_id"), "COL_2010_ECH_INC_ALL")
})

test_that("load_survey_microdata() attaches release attribute", {
  fx <- make_fixtures()

  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer({
    env <- getNamespace("piptm")$.piptm_env
    env$arrow_root    <- NULL
    env$manifest_dir  <- NULL
    env$manifests     <- list()
    env$current_release <- NULL
  })

  dt <- piptm::load_survey_microdata("COL", 2010L, "INC")
  expect_identical(attr(dt, "release"), "20260206")
})

test_that("load_survey_microdata() respects release parameter", {
  fx <- make_fixtures()

  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer({
    env <- getNamespace("piptm")$.piptm_env
    env$arrow_root    <- NULL
    env$manifest_dir  <- NULL
    env$manifests     <- list()
    env$current_release <- NULL
  })

  dt <- piptm::load_survey_microdata("COL", 2010L, "INC", release = "20260206")
  expect_equal(nrow(dt), 5L)
})

test_that("load_survey_microdata() dimensions = [] gives empty character attribute", {
  fx <- make_fixtures()

  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer({
    env <- getNamespace("piptm")$.piptm_env
    env$arrow_root    <- NULL
    env$manifest_dir  <- NULL
    env$manifests     <- list()
    env$current_release <- NULL
  })

  dt <- piptm::load_survey_microdata("BOL", 2015L, "CON")
  expect_equal(length(attr(dt, "dimensions")), 0L)
})

test_that("load_survey_microdata() errors on non-existent release", {
  fx <- make_fixtures()

  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer({
    env <- getNamespace("piptm")$.piptm_env
    env$arrow_root    <- NULL
    env$manifest_dir  <- NULL
    env$manifests     <- list()
    env$current_release <- NULL
  })

  expect_error(
    piptm::load_survey_microdata("COL", 2010L, "INC", release = "99991231"),
    regexp = "not found"
  )
})

test_that("load_survey_microdata() errors when survey not in manifest", {
  fx <- make_fixtures()

  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer({
    env <- getNamespace("piptm")$.piptm_env
    env$arrow_root    <- NULL
    env$manifest_dir  <- NULL
    env$manifests     <- list()
    env$current_release <- NULL
  })

  expect_error(
    piptm::load_survey_microdata("ZZZ", 2099L, "INC"),
    regexp = "No manifest entry found"
  )
})

test_that("load_survey_microdata() errors when arrow_root is not configured", {
  fx <- make_fixtures()

  piptm::set_manifest_dir(fx$tmp_manifest)
  env <- getNamespace("piptm")$.piptm_env
  old_root <- env$arrow_root
  env$arrow_root <- NULL
  withr::defer({
    env$arrow_root    <- old_root
    env$manifest_dir  <- NULL
    env$manifests     <- list()
    env$current_release <- NULL
  })

  expect_error(
    piptm::load_survey_microdata("COL", 2010L, "INC"),
    regexp = "Arrow root is not configured"
  )
})

test_that("load_survey_microdata() errors when no current release and release = NULL", {
  fx <- make_fixtures()

  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  env <- getNamespace("piptm")$.piptm_env
  old_cr <- env$current_release
  env$current_release <- NULL
  withr::defer({
    env$current_release <- old_cr
    env$arrow_root      <- NULL
    env$manifest_dir    <- NULL
    env$manifests       <- list()
  })

  expect_error(
    piptm::load_survey_microdata("COL", 2010L, "INC"),
    regexp = "No current release"
  )
})

# ---------------------------------------------------------------------------
# load_surveys() — batch load
# ---------------------------------------------------------------------------

test_that("load_surveys() returns combined data for multiple surveys", {
  fx <- make_fixtures()

  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer({
    env <- getNamespace("piptm")$.piptm_env
    env$arrow_root    <- NULL
    env$manifest_dir  <- NULL
    env$manifests     <- list()
    env$current_release <- NULL
  })

  mf  <- piptm::piptm_manifest()
  col <- mf[mf$country_code == "COL"]
  dt  <- piptm::load_surveys(col)

  expect_s3_class(dt, "data.table")
  # Two COL surveys: 5 + 5 rows
  expect_equal(nrow(dt), 10L)
  expect_true(all(dt$country_code == "COL"))
})

test_that("load_surveys() attaches release attribute", {
  fx <- make_fixtures()

  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer({
    env <- getNamespace("piptm")$.piptm_env
    env$arrow_root    <- NULL
    env$manifest_dir  <- NULL
    env$manifests     <- list()
    env$current_release <- NULL
  })

  mf <- piptm::piptm_manifest()
  dt <- piptm::load_surveys(mf)
  expect_identical(attr(dt, "release"), "20260206")
})

test_that("load_surveys() loads all 3 fixture surveys (15 rows total)", {
  fx <- make_fixtures()

  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer({
    env <- getNamespace("piptm")$.piptm_env
    env$arrow_root    <- NULL
    env$manifest_dir  <- NULL
    env$manifests     <- list()
    env$current_release <- NULL
  })

  mf <- piptm::piptm_manifest()
  dt <- piptm::load_surveys(mf)
  expect_equal(nrow(dt), 15L)
})

test_that("load_surveys() errors on 0-row entries_dt", {
  fx <- make_fixtures()

  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer({
    env <- getNamespace("piptm")$.piptm_env
    env$arrow_root    <- NULL
    env$manifest_dir  <- NULL
    env$manifests     <- list()
    env$current_release <- NULL
  })

  empty_dt <- piptm::piptm_manifest()[piptm::piptm_manifest()$country_code == "ZZZ"]

  expect_error(
    piptm::load_surveys(empty_dt),
    regexp = "0 rows"
  )
})

test_that("load_surveys() errors when arrow_root not configured", {
  fx <- make_fixtures()

  piptm::set_manifest_dir(fx$tmp_manifest)
  env <- getNamespace("piptm")$.piptm_env
  old_root <- env$arrow_root
  env$arrow_root <- NULL
  withr::defer({
    env$arrow_root    <- old_root
    env$manifest_dir  <- NULL
    env$manifests     <- list()
    env$current_release <- NULL
  })

  mf <- piptm::piptm_manifest()

  expect_error(
    piptm::load_surveys(mf),
    regexp = "Arrow root is not configured"
  )
})

# ---------------------------------------------------------------------------
# Regression tests for fixed bugs
# ---------------------------------------------------------------------------

# Bug 1: load_survey_microdata() manifest filter — parameter names collided
# with data.table column names of the same name, causing the filter predicate
# to compare each column to itself (always TRUE) and return all manifest rows.
test_that("load_survey_microdata() filters manifest to the correct single row", {
  fx <- make_fixtures()

  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer({
    env <- getNamespace("piptm")$.piptm_env
    env$arrow_root      <- NULL
    env$manifest_dir    <- NULL
    env$manifests       <- list()
    env$current_release <- NULL
  })

  # Three surveys in the manifest (COL/2010/INC, BOL/2015/CON, COL/2015/INC).
  # Requesting BOL/2015/CON must return exactly 5 rows from BOL, not all rows
  # from all three surveys.
  dt <- piptm::load_survey_microdata("BOL", 2015L, "CON")

  expect_s3_class(dt, "data.table")
  expect_equal(nrow(dt), 5L)
  expect_identical(unique(dt$country_code), "BOL")
  expect_identical(unique(dt$welfare_type),  "CON")
  expect_identical(unique(dt$version),       "v01_v01")
})

# Bug 2: load_surveys() Cartesian over-fetch — independent %in% filters on
# each partition key produced a cross-product, matching surveys not in the
# requested entries_dt when partition key values were shared across surveys.
#
# Scenario: COL/2010/INC (v01_v02) and ARG/2004/INC (v03_v01).
# Shared keys after collecting unique values per column:
#   country_code %in% c("COL", "ARG")
#   year         %in% c(2010, 2004)
#   welfare_type %in% c("INC")
#   version      %in% c("v01_v02", "v03_v01")
# This would also match a hypothetical COL/2004/INC/v03_v01 row if present.
# The fix filters on pip_id (exact tuple) instead.
test_that("load_surveys() fetches exactly the requested surveys — COL/2010/INC + ARG/2004/INC", {

  tmp_arrow    <- withr::local_tempdir()
  tmp_manifest <- withr::local_tempdir()

  # COL 2010 INC v01_v02
  write_fixture_parquet(
    arrow_root     = tmp_arrow,
    country_code   = "COL",
    year           = 2010L,
    welfare_type   = "INC",
    version        = "v01_v02",
    pip_id         = "COL_2010_ECH_INC_ALL",
    survey_acronym = "ECH"
  )
  # ARG 2004 INC v03_v01
  write_fixture_parquet(
    arrow_root     = tmp_arrow,
    country_code   = "ARG",
    year           = 2004L,
    welfare_type   = "INC",
    version        = "v03_v01",
    pip_id         = "ARG_2004_EPH_INC_ALL",
    survey_acronym = "EPH"
  )
  # Decoy: COL 2004 INC v03_v01 — shares year=2004 with ARG and version=v03_v01.
  # The old %in% filter would have fetched this; the new pip_id filter must not.
  write_fixture_parquet(
    arrow_root     = tmp_arrow,
    country_code   = "COL",
    year           = 2004L,
    welfare_type   = "INC",
    version        = "v03_v01",
    pip_id         = "COL_2004_ECH_INC_ALL",
    survey_acronym = "ECH"
  )

  entries_list <- list(
    list(
      pip_id         = "COL_2010_ECH_INC_ALL",
      survey_id      = "COL_2010_ECH_v01_M_v02_A_GMD_ALL",
      country_code   = "COL", year = 2010L, welfare_type = "INC",
      version        = "v01_v02", survey_acronym = "ECH",
      module = "ALL", dimensions = list("gender", "area"),
      welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L
    ),
    list(
      pip_id         = "ARG_2004_EPH_INC_ALL",
      survey_id      = "ARG_2004_EPH_v03_M_v01_A_GMD_ALL",
      country_code   = "ARG", year = 2004L, welfare_type = "INC",
      version        = "v03_v01", survey_acronym = "EPH",
      module = "ALL", dimensions = list(),
      welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L
    )
    # Note: decoy COL_2004_ECH_INC_ALL is intentionally absent from manifest.
  )

  write_fixture_manifest(
    manifest_dir = tmp_manifest,
    release      = "20260206",
    entries      = entries_list,
    set_current  = TRUE
  )

  piptm::set_manifest_dir(tmp_manifest)
  piptm::set_arrow_root(tmp_arrow)
  withr::defer({
    env <- getNamespace("piptm")$.piptm_env
    env$arrow_root      <- NULL
    env$manifest_dir    <- NULL
    env$manifests       <- list()
    env$current_release <- NULL
  })

  mf <- piptm::piptm_manifest()
  dt <- piptm::load_surveys(mf)  # request both COL/2010/INC and ARG/2004/INC; default ppp=2021

  # 5 rows each = 10 total; the decoy COL/2004/INC must not appear.
  expect_s3_class(dt, "data.table")
  expect_equal(nrow(dt), 10L)
  expect_true(all(dt$pip_id %in% c("COL_2010_ECH_INC_ALL", "ARG_2004_EPH_INC_ALL")))
  expect_false("COL_2004_ECH_INC_ALL" %in% dt$pip_id)
})

# ---------------------------------------------------------------------------
# Regression: partial path failure — manifest entry exists but Parquet files
# are missing from disk for one survey
# ---------------------------------------------------------------------------

# Bug: load_surveys() with some missing partition directories silently skipped
# those surveys and returned fewer rows than expected rather than erroring.
# Fix: the pip_id integrity check now catches any loaded surveys that were not
# in entries_dt (contamination) AND the .build_parquet_paths() helper errors
# when no Parquet files are found for a given survey.
test_that("load_surveys() errors when a manifest entry has no Parquet files on disk", {

  tmp_arrow    <- withr::local_tempdir()
  tmp_manifest <- withr::local_tempdir()

  # Write Parquet only for survey 1 — survey 2 is manifest-only (missing files)
  write_fixture_parquet(
    arrow_root     = tmp_arrow,
    country_code   = "COL",
    year           = 2010L,
    welfare_type   = "INC",
    version        = "v01_v02",
    pip_id         = "COL_2010_ECH_INC_ALL",
    survey_acronym = "ECH"
  )
  # Survey 2: manifest entry exists but NO Parquet directory written

  entries_list <- list(
    list(
      pip_id         = "COL_2010_ECH_INC_ALL",
      survey_id      = "COL_2010_ECH_v01_M_v02_A_GMD_ALL",
      country_code   = "COL", year = 2010L, welfare_type = "INC",
      version        = "v01_v02", survey_acronym = "ECH",
      module = "ALL", dimensions = list(),
      welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L
    ),
    list(
      pip_id         = "BOL_2015_EH_CON_ALL",
      survey_id      = "BOL_2015_EH_v01_M_v01_A_GMD_ALL",
      country_code   = "BOL", year = 2015L, welfare_type = "CON",
      version        = "v01_v01", survey_acronym = "EH",
      module = "ALL", dimensions = list(),
      welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L
    )
    # BOL/2015/CON partition directory was never written to tmp_arrow
  )

  write_fixture_manifest(
    manifest_dir = tmp_manifest,
    release      = "20260206",
    entries      = entries_list,
    set_current  = TRUE
  )

  piptm::set_manifest_dir(tmp_manifest)
  piptm::set_arrow_root(tmp_arrow)
  withr::defer({
    env <- getNamespace("piptm")$.piptm_env
    env$arrow_root      <- NULL
    env$manifest_dir    <- NULL
    env$manifests       <- list()
    env$current_release <- NULL
  })

  mf <- piptm::piptm_manifest()

  # Must error — the BOL partition path has no Parquet files
  expect_error(
    piptm::load_surveys(mf),
    regexp = "No Parquet files found"
  )
})

# ---------------------------------------------------------------------------
# PPP welfare column selection — new-schema surveys
# ---------------------------------------------------------------------------

#' Write a multi-welfare Parquet fixture (new deflated-data schema).
#' Returns list(arrow_root, manifest_dir) with env reset deferred to `env`.
make_ppp_fixtures <- function(env = parent.frame()) {
  tmp_arrow    <- withr::local_tempdir(.local_envir = env)
  tmp_manifest <- withr::local_tempdir(.local_envir = env)

  dir_path <- file.path(
    tmp_arrow,
    "country_code=COL", "surveyid_year=2010",
    "welfare_type=INC", "version=v01_v02"
  )
  dir.create(dir_path, recursive = TRUE)
  dt <- data.table::data.table(
    country_code   = "COL",
    surveyid_year  = 2010L,
    welfare_type   = "INC",
    version        = "v01_v02",
    pip_id         = "COL_2010_ECH_INC_ALL",
    survey_acronym = "ECH",
    welfare_lcu            = c(500, 600, 700, 800, 900),
    welfare_ppp_2021_01_02 = c(1.5, 2.0, 2.5, 3.0, 3.5),
    welfare_ppp_2017_01_02 = c(1.2, 1.6, 2.0, 2.4, 2.8),
    welfare_ppp_2011_01_01 = c(2.1, 2.8, 3.5, 4.2, 4.9),
    weight         = rep(1.0, 5L)
  )
  arrow::write_parquet(dt, file.path(dir_path, "data.parquet"))

  entries_list <- list(list(
    pip_id         = "COL_2010_ECH_INC_ALL",
    survey_id      = "COL_2010_ECH_v01_M_v02_A_GMD_ALL",
    country_code   = "COL",
    year           = 2010L,
    welfare_type   = "INC",
    version        = "v01_v02",
    survey_acronym = "ECH",
    module         = "ALL",
    dimensions     = list("gender"),
    welfare_vars   = list("welfare_lcu", "welfare_ppp_2021_01_02", "welfare_ppp_2017_01_02", "welfare_ppp_2011_01_01"),
    ppp_sort       = 2021L
  ))
  write_fixture_manifest(tmp_manifest, "20260206", entries_list, set_current = TRUE)

  list(tmp_arrow = tmp_arrow, tmp_manifest = tmp_manifest)
}

reset_load_env <- function() {
  env <- getNamespace("piptm")$.piptm_env
  env$arrow_root      <- NULL
  env$manifest_dir    <- NULL
  env$manifests       <- list()
  env$current_release <- NULL
}

test_that("load_survey_microdata() with ppp selects and renames the correct welfare column", {
  fx <- make_ppp_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_load_env())

  # Default ppp = 2021 — should load welfare_ppp_2021_01_02
  dt <- piptm::load_survey_microdata("COL", 2010L, "INC")

  expect_true("welfare" %in% names(dt))
  expect_false("welfare_ppp_2021_01_02" %in% names(dt))
  expect_false("welfare_ppp_2017_01_02" %in% names(dt))
  expect_false("welfare_ppp_2011_01_01" %in% names(dt))
  expect_false("welfare_lcu"            %in% names(dt))
  expect_equal(dt$welfare, c(1.5, 2.0, 2.5, 3.0, 3.5))
})

test_that("load_survey_microdata() with explicit ppp=2017 selects the 2017 welfare column", {
  fx <- make_ppp_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_load_env())

  dt <- piptm::load_survey_microdata("COL", 2010L, "INC", ppp = 2017L)

  expect_true("welfare" %in% names(dt))
  expect_false("welfare_ppp_2021_01_02" %in% names(dt))
  expect_false("welfare_ppp_2017_01_02" %in% names(dt))
  expect_false("welfare_ppp_2011_01_01" %in% names(dt))
  expect_false("welfare_lcu"            %in% names(dt))
  expect_equal(dt$welfare, c(1.2, 1.6, 2.0, 2.4, 2.8))
})

test_that("load_survey_microdata() default ppp=2021 loads welfare_ppp_2021_01_02", {
  fx <- make_ppp_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_load_env())

  # No ppp argument — defaults to 2021L
  dt <- piptm::load_survey_microdata("COL", 2010L, "INC")

  expect_true("welfare" %in% names(dt))
  expect_equal(dt$welfare, c(1.5, 2.0, 2.5, 3.0, 3.5))
})

test_that("load_survey_microdata() errors when requested ppp not in welfare_vars", {
  fx <- make_ppp_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_load_env())

  expect_error(
    piptm::load_survey_microdata("COL", 2010L, "INC", ppp = 2005L),
    regexp = "not available"
  )
})

test_that("load_survey_microdata() with ppp=NULL errors because no welfare_ppp_NA column exists", {
  tmp_arrow    <- withr::local_tempdir()
  tmp_manifest <- withr::local_tempdir()

  dir_path <- file.path(
    tmp_arrow,
    "country_code=COL", "surveyid_year=2010",
    "welfare_type=INC",  "version=v01_v02"
  )
  dir.create(dir_path, recursive = TRUE)
  dt <- data.table::data.table(
    country_code  = "COL", surveyid_year = 2010L,
    welfare_type  = "INC", version = "v01_v02",
    pip_id        = "COL_2010_ECH_INC_ALL", survey_acronym = "ECH",
    welfare_ppp_2017_01_02 = c(1.5, 2.0, 2.5),
    weight = rep(1.0, 3L)
  )
  arrow::write_parquet(dt, file.path(dir_path, "data.parquet"))

  entries <- list(list(
    pip_id = "COL_2010_ECH_INC_ALL", survey_id = "S",
    country_code = "COL", year = 2010L, welfare_type = "INC",
    version = "v01_v02", survey_acronym = "ECH", module = "ALL",
    dimensions = list(),
    welfare_vars = list("welfare_ppp_2017_01_02"),
    ppp_sort = NA_integer_   # no default set
  ))
  write_fixture_manifest(tmp_manifest, "20260206", entries, set_current = TRUE)

  piptm::set_manifest_dir(tmp_manifest)
  piptm::set_arrow_root(tmp_arrow)
  withr::defer(reset_load_env())

  # ppp=NULL is passed explicitly; find_welfare_col(welfare_vars, NA) returns
  # empty → "not available" error
  expect_error(
    piptm::load_survey_microdata("COL", 2010L, "INC", ppp = NULL),
    regexp = "not available"
  )
})

test_that("load_survey_microdata() errors on legacy survey (empty welfare_vars)", {
  tmp_arrow    <- withr::local_tempdir()
  tmp_manifest <- withr::local_tempdir()

  # Write a legacy-format Parquet with a single plain `welfare` column
  dir_path <- file.path(tmp_arrow,
    "country_code=COL", "surveyid_year=2010", "welfare_type=INC", "version=v01_v01")
  dir.create(dir_path, recursive = TRUE)
  arrow::write_parquet(
    data.table::data.table(
      country_code = "COL", surveyid_year = 2010L, welfare_type = "INC",
      version = "v01_v01", pip_id = "COL_2010_ECH_INC_ALL",
      survey_acronym = "ECH", welfare = c(100, 150, 200, 250, 300),
      weight = rep(1.0, 5L)
    ),
    file.path(dir_path, "data.parquet")
  )

  # Manifest entry with no welfare_vars and no ppp_sort
  write_fixture_manifest(tmp_manifest, "20260206", list(list(
    pip_id = "COL_2010_ECH_INC_ALL", survey_id = "S",
    country_code = "COL", year = 2010L, welfare_type = "INC",
    version = "v01_v01", survey_acronym = "ECH", module = "ALL",
    dimensions = list()
    # No welfare_vars / ppp_sort → legacy survey → must now error
  )), set_current = TRUE)

  piptm::set_manifest_dir(tmp_manifest)
  piptm::set_arrow_root(tmp_arrow)
  withr::defer(reset_load_env())

  # Legacy schema: welfare_vars is empty, so no welfare_ppp_2021 column
  # exists — must error at PPP resolution
  expect_error(
    piptm::load_survey_microdata("COL", 2010L, "INC"),
    regexp = "not available"
  )
})

# ---------------------------------------------------------------------------
# load_survey_microdata() — cols parameter (P3.3)
# ---------------------------------------------------------------------------

test_that("load_survey_microdata() cols subsets columns before collect", {
  fx <- make_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_load_env())

  # COL/2010 fixture has: country_code, surveyid_year, welfare_type, version,
  # pip_id, survey_acronym, welfare, weight, gender, area
  dt <- piptm::load_survey_microdata("COL", 2010L, "INC",
                                     cols = c("welfare", "weight", "gender"))

  expect_true(all(c("welfare", "weight", "gender", "pip_id") %in% names(dt)))
  expect_false("survey_acronym" %in% names(dt))
  expect_false("area"           %in% names(dt))
  expect_false("version"        %in% names(dt))
  expect_equal(nrow(dt), 5L)
})

test_that("load_survey_microdata() cols always auto-includes welfare, weight, pip_id", {
  fx <- make_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_load_env())

  # Request only gender — welfare, weight, pip_id must still appear
  dt <- piptm::load_survey_microdata("COL", 2010L, "INC", cols = c("gender"))

  expect_true("welfare" %in% names(dt))
  expect_true("weight"  %in% names(dt))
  expect_true("pip_id"  %in% names(dt))
  expect_true("gender"  %in% names(dt))
})

test_that("load_survey_microdata() cols with new-schema survey translates 'welfare'", {
  fx <- make_ppp_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_load_env())

  dt <- piptm::load_survey_microdata("COL", 2010L, "INC", ppp = 2017L,
                                     cols = c("welfare", "weight", "pip_id"))

  expect_true("welfare" %in% names(dt))
  expect_false("welfare_ppp_2021_01_02" %in% names(dt))
  expect_false("welfare_ppp_2017_01_02" %in% names(dt))
  expect_false("welfare_ppp_2011_01_01" %in% names(dt))
  expect_setequal(names(dt), c("welfare", "weight", "pip_id"))
  expect_equal(dt$welfare, c(1.2, 1.6, 2.0, 2.4, 2.8))
})

test_that("load_survey_microdata() cols errors on non-character input", {
  fx <- make_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_load_env())

  expect_error(
    piptm::load_survey_microdata("COL", 2010L, "INC", cols = 1:3),
    regexp = "non-empty character vector"
  )
})

test_that("load_survey_microdata() cols=NULL loads all columns (backward compat)", {
  fx <- make_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_load_env())

  dt_null <- piptm::load_survey_microdata("COL", 2010L, "INC", cols = NULL)
  dt_def  <- piptm::load_survey_microdata("COL", 2010L, "INC")

  expect_equal(names(dt_null), names(dt_def))
  expect_equal(nrow(dt_null),  nrow(dt_def))
})

test_that("load_survey_microdata() cols with default ppp=2021 translates 'welfare' correctly", {
  fx <- make_ppp_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_load_env())

  # Default ppp=2021; welfare_ppp_2021_01_02 = c(1.5, 2.0, 2.5, 3.0, 3.5)
  dt <- piptm::load_survey_microdata("COL", 2010L, "INC",
                                     cols = c("welfare", "weight", "pip_id"))

  expect_true("welfare" %in% names(dt))
  expect_setequal(names(dt), c("welfare", "weight", "pip_id"))
  expect_equal(sort(dt$welfare), c(1.5, 2.0, 2.5, 3.0, 3.5))
})

test_that("load_surveys() with explicit ppp selects correct welfare column across all surveys", {
  tmp_arrow    <- withr::local_tempdir()
  tmp_manifest <- withr::local_tempdir()

  for (cc in c("COL", "BOL")) {
    dp <- file.path(
      tmp_arrow,
      paste0("country_code=", cc), "surveyid_year=2010",
      "welfare_type=INC", "version=v01_v01"
    )
    dir.create(dp, recursive = TRUE)
    dt_i <- data.table::data.table(
      country_code   = cc, surveyid_year = 2010L,
      welfare_type   = "INC", version = "v01_v01",
      pip_id         = paste0(cc, "_2010_ECH_INC_ALL"), survey_acronym = "ECH",
      welfare_lcu              = c(500, 600, 700),
      welfare_ppp_2021_01_02   = c(1.0, 2.0, 3.0),
      welfare_ppp_2017_01_02   = c(0.8, 1.6, 2.4),
      welfare_ppp_2011_01_01   = c(1.5, 2.5, 3.5),
      weight         = rep(1.0, 3L)
    )
    arrow::write_parquet(dt_i, file.path(dp, "data.parquet"))
  }

  entries_list <- lapply(c("COL", "BOL"), function(cc) {
    list(
      pip_id = paste0(cc, "_2010_ECH_INC_ALL"), survey_id = "S",
      country_code = cc, year = 2010L, welfare_type = "INC",
      version = "v01_v01", survey_acronym = "ECH", module = "ALL",
      dimensions = list(),
      welfare_vars = list("welfare_lcu", "welfare_ppp_2021_01_02", "welfare_ppp_2017_01_02",
                          "welfare_ppp_2011_01_01"),
      ppp_sort = 2021L
    )
  })
  write_fixture_manifest(tmp_manifest, "20260206", entries_list, set_current = TRUE)

  piptm::set_manifest_dir(tmp_manifest)
  piptm::set_arrow_root(tmp_arrow)
  withr::defer(reset_load_env())

  mf <- piptm::piptm_manifest()

  # Default ppp = 2021
  dt_default <- piptm::load_surveys(mf)
  expect_true("welfare" %in% names(dt_default))
  expect_false(any(grepl("^welfare_ppp_|^welfare_lcu", names(dt_default))))
  expect_equal(nrow(dt_default), 6L)
  expect_true(all(dt_default$welfare %in% c(1.0, 2.0, 3.0)))

  # Explicit ppp = 2017
  dt_2017 <- piptm::load_surveys(mf, ppp = 2017L)
  expect_equal(nrow(dt_2017), 6L)
  expect_true(all(dt_2017$welfare %in% c(0.8, 1.6, 2.4)))
})

test_that("load_surveys() default ppp=2021 loads welfare_ppp_2021 columns", {
  tmp_arrow    <- withr::local_tempdir()
  tmp_manifest <- withr::local_tempdir()

  for (cc in c("COL", "BOL")) {
    dp <- file.path(
      tmp_arrow, paste0("country_code=", cc), "surveyid_year=2010",
      "welfare_type=INC", "version=v01_v01"
    )
    dir.create(dp, recursive = TRUE)
    dt_i <- data.table::data.table(
      country_code = cc, surveyid_year = 2010L, welfare_type = "INC",
      version = "v01_v01", pip_id = paste0(cc, "_2010_ECH_INC_ALL"),
      survey_acronym = "ECH",
      welfare_ppp_2021_01_02 = c(1.0, 2.0),
      weight = rep(1.0, 2L)
    )
    arrow::write_parquet(dt_i, file.path(dp, "data.parquet"))
  }

  entries_list <- lapply(c("COL", "BOL"), function(cc) {
    list(
      pip_id = paste0(cc, "_2010_ECH_INC_ALL"), survey_id = "S",
      country_code = cc, year = 2010L, welfare_type = "INC",
      version = "v01_v01", survey_acronym = "ECH", module = "ALL",
      dimensions = list(),
      welfare_vars = list("welfare_ppp_2021_01_02"),
      ppp_sort = 2021L
    )
  })
  write_fixture_manifest(tmp_manifest, "20260206", entries_list, set_current = TRUE)

  piptm::set_manifest_dir(tmp_manifest)
  piptm::set_arrow_root(tmp_arrow)
  withr::defer(reset_load_env())

  mf <- piptm::piptm_manifest()
  dt <- piptm::load_surveys(mf)  # default ppp = 2021

  expect_true("welfare" %in% names(dt))
  expect_equal(nrow(dt), 4L)
})

test_that("load_surveys() warns and skips surveys missing the requested PPP welfare column", {
  tmp_arrow    <- withr::local_tempdir()
  tmp_manifest <- withr::local_tempdir()

  # COL has both 2021 and 2017 columns; BOL has only 2017
  welfare_by_cc <- list(
    COL = list(cols = c("welfare_ppp_2021_01_02", "welfare_ppp_2017_01_02"), vals = list(c(1.0, 2.0), c(0.8, 1.6))),
    BOL = list(cols = c("welfare_ppp_2017_01_02"),                            vals = list(c(0.5, 1.0)))
  )
  for (cc in c("COL", "BOL")) {
    dp <- file.path(
      tmp_arrow, paste0("country_code=", cc), "surveyid_year=2010",
      "welfare_type=INC", "version=v01_v01"
    )
    dir.create(dp, recursive = TRUE)
    info <- welfare_by_cc[[cc]]
    dt_i <- data.table::data.table(
      country_code = cc, surveyid_year = 2010L, welfare_type = "INC",
      version = "v01_v01", pip_id = paste0(cc, "_2010_ECH_INC_ALL"),
      survey_acronym = "ECH", weight = rep(1.0, 2L)
    )
    for (k in seq_along(info$cols)) dt_i[, (info$cols[[k]]) := info$vals[[k]]]
    arrow::write_parquet(dt_i, file.path(dp, "data.parquet"))
  }

  entries_list <- list(
    list(pip_id = "COL_2010_ECH_INC_ALL", survey_id = "S",
         country_code = "COL", year = 2010L, welfare_type = "INC",
         version = "v01_v01", survey_acronym = "ECH", module = "ALL",
         dimensions = list(),
         welfare_vars = list("welfare_ppp_2021_01_02", "welfare_ppp_2017_01_02"),
         ppp_sort = 2021L),
    list(pip_id = "BOL_2010_ECH_INC_ALL", survey_id = "S",
         country_code = "BOL", year = 2010L, welfare_type = "INC",
         version = "v01_v01", survey_acronym = "ECH", module = "ALL",
         dimensions = list(),
         welfare_vars = list("welfare_ppp_2017_01_02"),
         ppp_sort = 2017L)
  )
  write_fixture_manifest(tmp_manifest, "20260206", entries_list, set_current = TRUE)

  piptm::set_manifest_dir(tmp_manifest)
  piptm::set_arrow_root(tmp_arrow)
  withr::defer(reset_load_env())

  mf <- piptm::piptm_manifest()

  # ppp=2021: BOL lacks 2021 column → warn and skip; only COL returned
  dt <- NULL
  expect_warning(
    {dt <- piptm::load_surveys(mf, ppp = 2021L)},
    regexp = "skipping"
  )
  expect_s3_class(dt, "data.table")
  expect_equal(nrow(dt), 2L)  # only COL's 2 rows
  expect_true(all(dt$pip_id == "COL_2010_ECH_INC_ALL"))
  expect_equal(sort(dt$welfare), c(1.0, 2.0))
})

test_that("load_surveys() errors when ALL surveys lack the requested PPP column", {
  tmp_arrow    <- withr::local_tempdir()
  tmp_manifest <- withr::local_tempdir()

  for (cc in c("COL", "BOL")) {
    dp <- file.path(
      tmp_arrow, paste0("country_code=", cc), "surveyid_year=2010",
      "welfare_type=INC", "version=v01_v01"
    )
    dir.create(dp, recursive = TRUE)
    dt_i <- data.table::data.table(
      country_code = cc, surveyid_year = 2010L, welfare_type = "INC",
      version = "v01_v01", pip_id = paste0(cc, "_2010_ECH_INC_ALL"),
      survey_acronym = "ECH", welfare_ppp_2017_01_02 = c(1.0, 2.0),
      weight = rep(1.0, 2L)
    )
    arrow::write_parquet(dt_i, file.path(dp, "data.parquet"))
  }

  entries_list <- lapply(c("COL", "BOL"), function(cc) {
    list(pip_id = paste0(cc, "_2010_ECH_INC_ALL"), survey_id = "S",
         country_code = cc, year = 2010L, welfare_type = "INC",
         version = "v01_v01", survey_acronym = "ECH", module = "ALL",
         dimensions = list(), welfare_vars = list("welfare_ppp_2017_01_02"),
         ppp_sort = 2017L)
  })
  write_fixture_manifest(tmp_manifest, "20260206", entries_list, set_current = TRUE)

  piptm::set_manifest_dir(tmp_manifest)
  piptm::set_arrow_root(tmp_arrow)
  withr::defer(reset_load_env())

  mf <- piptm::piptm_manifest()

  # ppp=2021: neither survey has the 2021 column → error after warning
  expect_error(
    suppressWarnings(piptm::load_surveys(mf, ppp = 2021L)),
    regexp = "No surveys remain"
  )
})

test_that("load_surveys() warns when only some surveys lack the requested ppp column, returns rest", {
  tmp_arrow    <- withr::local_tempdir()
  tmp_manifest <- withr::local_tempdir()

  # COL has ppp 2021; BOL has only ppp 2011 → requesting ppp=2021 should
  # skip BOL with a warning and return only COL
  welfare_by_cc <- list(
    COL = list(col = "welfare_ppp_2021_01_02", val = c(1.0, 2.0)),
    BOL = list(col = "welfare_ppp_2011_01_01", val = c(1.5, 2.5))
  )
  for (cc in c("COL", "BOL")) {
    dp <- file.path(
      tmp_arrow, paste0("country_code=", cc), "surveyid_year=2010",
      "welfare_type=INC", "version=v01_v01"
    )
    dir.create(dp, recursive = TRUE)
    info <- welfare_by_cc[[cc]]
    dt_i <- data.table::data.table(
      country_code = cc, surveyid_year = 2010L, welfare_type = "INC",
      version = "v01_v01", pip_id = paste0(cc, "_2010_ECH_INC_ALL"),
      survey_acronym = "ECH", weight = rep(1.0, 2L)
    )
    dt_i[, (info$col) := info$val]
    arrow::write_parquet(dt_i, file.path(dp, "data.parquet"))
  }

  entries_list <- lapply(c("COL", "BOL"), function(cc) {
    info <- welfare_by_cc[[cc]]
    list(
      pip_id = paste0(cc, "_2010_ECH_INC_ALL"), survey_id = "S",
      country_code = cc, year = 2010L, welfare_type = "INC",
      version = "v01_v01", survey_acronym = "ECH", module = "ALL",
      dimensions = list(),
      welfare_vars = list(info$col),
      ppp_sort = NA_integer_
    )
  })
  write_fixture_manifest(tmp_manifest, "20260206", entries_list, set_current = TRUE)

  piptm::set_manifest_dir(tmp_manifest)
  piptm::set_arrow_root(tmp_arrow)
  withr::defer(reset_load_env())

  mf <- piptm::piptm_manifest()

  # BOL is skipped with warning; only COL (2 rows) returned
  dt <- NULL
  expect_warning(
    {dt <- piptm::load_surveys(mf, ppp = 2021L)},
    regexp = "skipping"
  )
  expect_s3_class(dt, "data.table")
  expect_equal(nrow(dt), 2L)
  expect_true(all(dt$pip_id == "COL_2010_ECH_INC_ALL"))
})

# ---------------------------------------------------------------------------
# cols parameter — column pruning
# ---------------------------------------------------------------------------

# P3.1: cols type validation
test_that("load_surveys() errors when cols is a non-character vector", {
  fx <- make_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_load_env())

  mf <- piptm::piptm_manifest()
  expect_error(
    piptm::load_surveys(mf, cols = 1:3),
    regexp = "non-empty character vector"
  )
})

test_that("load_surveys() errors when cols is an empty character vector", {
  fx <- make_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_load_env())

  mf <- piptm::piptm_manifest()
  expect_error(
    piptm::load_surveys(mf, cols = character(0L)),
    regexp = "non-empty character vector"
  )
})

test_that("load_surveys() cols=NULL loads all columns (no-op, backward compat)", {  fx <- make_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_load_env())

  mf <- piptm::piptm_manifest()
  col <- mf[mf$country_code == "COL" & mf$year == 2010L]
  dt_all  <- piptm::load_surveys(col, cols = NULL)
  dt_none <- piptm::load_surveys(col)

  expect_equal(names(dt_all), names(dt_none))
  expect_equal(nrow(dt_all), nrow(dt_none))
})

test_that("load_surveys() cols subset returns only requested columns", {
  fx <- make_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_load_env())

  mf  <- piptm::piptm_manifest()
  col <- mf[mf$country_code == "COL" & mf$year == 2010L]
  # The COL/2010 fixture has columns: country_code, surveyid_year, welfare_type,
  # version, pip_id, survey_acronym, welfare, weight, gender, area
  requested <- c("welfare", "weight", "pip_id", "gender")
  dt <- piptm::load_surveys(col, cols = requested)

  # pip_id is always included (integrity check) + the 3 we explicitly requested
  expect_true(all(c("welfare", "weight", "pip_id", "gender") %in% names(dt)))
  # Columns NOT in requested (and not pip_id) must be absent
  expect_false("survey_acronym" %in% names(dt))
  expect_false("area"           %in% names(dt))
  expect_false("version"        %in% names(dt))
})

test_that("load_surveys() cols with partial-match survey omits absent dimension silently", {
  fx <- make_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_load_env())

  # COL/2010 has gender+area; BOL/2015 has neither.
  # Requesting both should succeed — BOL rows simply won't have those cols loaded.
  mf <- piptm::piptm_manifest()
  dt <- piptm::load_surveys(
    mf,
    cols = c("pip_id", "welfare", "weight", "gender", "area")
  )
  expect_s3_class(dt, "data.table")
  # Must include all 3 surveys (15 rows total)
  expect_equal(nrow(dt), 15L)
  # gender / area present (loaded from COL surveys; BOL rows will be NA/absent
  # in the schema-unified result or just absent if BOL files have no such cols)
  expect_true("pip_id"  %in% names(dt))
  expect_true("welfare" %in% names(dt))
  expect_true("weight"  %in% names(dt))
  # gender/area absent from BOL — Arrow unified schema fills them as NA
  expect_true("gender" %in% names(dt))
  expect_true("area"   %in% names(dt))
  bol_rows <- dt[pip_id == "BOL_2015_EH_CON_ALL"]
  expect_true(all(is.na(bol_rows$gender)))
  expect_true(all(is.na(bol_rows$area)))
})

test_that("load_surveys() cols with new-schema survey translates 'welfare' to PPP column", {
  fx <- make_ppp_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_load_env())

  mf <- piptm::piptm_manifest()
  # Default ppp = 2021 — should load welfare_ppp_2021_01_02
  dt <- piptm::load_surveys(mf, cols = c("welfare", "weight", "pip_id"))

  expect_true("welfare" %in% names(dt))
  expect_false("welfare_ppp_2021_01_02" %in% names(dt))
  expect_false("welfare_ppp_2017_01_02" %in% names(dt))
  expect_false("welfare_ppp_2011_01_01" %in% names(dt))
  expect_false("welfare_lcu"            %in% names(dt))
  expect_setequal(names(dt), c("welfare", "weight", "pip_id"))
})

# P2.1: weight auto-inclusion — weight must be present even when not in cols
test_that("load_surveys() cols always includes weight even when not requested", {
  fx <- make_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_load_env())

  mf  <- piptm::piptm_manifest()
  col <- mf[mf$country_code == "COL" & mf$year == 2010L]
  # Deliberately omit "weight" and "welfare" from cols — they must still appear
  dt  <- piptm::load_surveys(col, cols = c("pip_id", "gender"))

  expect_true("weight"  %in% names(dt))
  expect_true("welfare" %in% names(dt))
  expect_true("pip_id"  %in% names(dt))
  # Columns not in the requested set and not auto-included must be absent
  expect_false("survey_acronym" %in% names(dt))
  expect_false("version"        %in% names(dt))
})


