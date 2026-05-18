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
    welfare        = seq(100, by = 50, length.out = n_rows),
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
      dimensions     = list("gender", "area")
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
      dimensions     = list()
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
      dimensions     = list("age")
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
      module = "ALL", dimensions = list("gender", "area")
    ),
    list(
      pip_id         = "ARG_2004_EPH_INC_ALL",
      survey_id      = "ARG_2004_EPH_v03_M_v01_A_GMD_ALL",
      country_code   = "ARG", year = 2004L, welfare_type = "INC",
      version        = "v03_v01", survey_acronym = "EPH",
      module = "ALL", dimensions = list()
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
  dt <- piptm::load_surveys(mf)  # request both COL/2010/INC and ARG/2004/INC

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
      module = "ALL", dimensions = list()
    ),
    list(
      pip_id         = "BOL_2015_EH_CON_ALL",
      survey_id      = "BOL_2015_EH_v01_M_v01_A_GMD_ALL",
      country_code   = "BOL", year = 2015L, welfare_type = "CON",
      version        = "v01_v01", survey_acronym = "EH",
      module = "ALL", dimensions = list()
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
    welfare_lcu          = c(500, 600, 700, 800, 900),
    welfare_ppp_2017_01_02 = c(1.5, 2.0, 2.5, 3.0, 3.5),
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
    welfare_vars   = list("welfare_lcu", "welfare_ppp_2017_01_02", "welfare_ppp_2011_01_01"),
    ppp_sort       = 2017L
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

  dt <- piptm::load_survey_microdata("COL", 2010L, "INC", ppp = 2017L)

  expect_true("welfare" %in% names(dt))
  expect_false("welfare_ppp_2017_01_02" %in% names(dt))
  expect_false("welfare_ppp_2011_01_01" %in% names(dt))
  expect_false("welfare_lcu"            %in% names(dt))
  expect_equal(dt$welfare, c(1.5, 2.0, 2.5, 3.0, 3.5))
})

test_that("load_survey_microdata() ppp=NULL uses ppp_sort from manifest", {
  fx <- make_ppp_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_load_env())

  dt <- piptm::load_survey_microdata("COL", 2010L, "INC", ppp = NULL)

  # ppp_sort = 2017 → should load welfare_ppp_2017_01_02
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

test_that("load_survey_microdata() with ppp=NULL and ppp_sort=NA errors informatively", {
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

  expect_error(
    piptm::load_survey_microdata("COL", 2010L, "INC"),
    regexp = "No default PPP"
  )
})

test_that("load_survey_microdata() legacy survey (welfare_vars empty) loads as-is", {
  fx <- make_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_load_env())

  # Legacy Parquet has single `welfare` column; manifest has welfare_vars=character(0)
  dt <- piptm::load_survey_microdata("COL", 2010L, "INC")
  expect_true("welfare" %in% names(dt))
  expect_equal(nrow(dt), 5L)
})

test_that("load_surveys() with ppp selects correct welfare column across all surveys", {
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
      welfare_ppp_2017_01_02   = c(1.0, 2.0, 3.0),
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
      welfare_vars = list("welfare_lcu", "welfare_ppp_2017_01_02", "welfare_ppp_2011_01_01"),
      ppp_sort = 2017L
    )
  })
  write_fixture_manifest(tmp_manifest, "20260206", entries_list, set_current = TRUE)

  piptm::set_manifest_dir(tmp_manifest)
  piptm::set_arrow_root(tmp_arrow)
  withr::defer(reset_load_env())

  mf <- piptm::piptm_manifest()
  dt <- piptm::load_surveys(mf, ppp = 2017L)

  expect_true("welfare" %in% names(dt))
  expect_false(any(grepl("^welfare_ppp_|^welfare_lcu", names(dt))))
  expect_equal(nrow(dt), 6L)
  expect_true(all(dt$welfare %in% c(1.0, 2.0, 3.0)))
})

test_that("load_surveys() ppp=NULL uses uniform ppp_sort across surveys", {
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
      welfare_ppp_2017_01_02 = c(1.0, 2.0),
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
      welfare_vars = list("welfare_ppp_2017_01_02"),
      ppp_sort = 2017L  # same for both
    )
  })
  write_fixture_manifest(tmp_manifest, "20260206", entries_list, set_current = TRUE)

  piptm::set_manifest_dir(tmp_manifest)
  piptm::set_arrow_root(tmp_arrow)
  withr::defer(reset_load_env())

  mf <- piptm::piptm_manifest()
  dt <- piptm::load_surveys(mf, ppp = NULL)

  expect_true("welfare" %in% names(dt))
  expect_equal(nrow(dt), 4L)
})

test_that("load_surveys() errors when surveys have inconsistent ppp_sort and ppp=NULL", {
  tmp_arrow    <- withr::local_tempdir()
  tmp_manifest <- withr::local_tempdir()

  ppp_sorts <- c(COL = 2017L, BOL = 2011L)
  for (cc in c("COL", "BOL")) {
    dp <- file.path(
      tmp_arrow, paste0("country_code=", cc), "surveyid_year=2010",
      "welfare_type=INC", "version=v01_v01"
    )
    dir.create(dp, recursive = TRUE)
    ps <- ppp_sorts[[cc]]
    col_nm <- paste0("welfare_ppp_", ps, "_01_01")
    dt_i <- data.table::data.table(
      country_code = cc, surveyid_year = 2010L, welfare_type = "INC",
      version = "v01_v01", pip_id = paste0(cc, "_2010_ECH_INC_ALL"),
      survey_acronym = "ECH",
      weight = rep(1.0, 2L)
    )
    dt_i[, (col_nm) := c(1.0, 2.0)]
    arrow::write_parquet(dt_i, file.path(dp, "data.parquet"))
  }

  entries_list <- lapply(c("COL", "BOL"), function(cc) {
    ps <- ppp_sorts[[cc]]
    col_nm <- paste0("welfare_ppp_", ps, "_01_01")
    list(
      pip_id = paste0(cc, "_2010_ECH_INC_ALL"), survey_id = "S",
      country_code = cc, year = 2010L, welfare_type = "INC",
      version = "v01_v01", survey_acronym = "ECH", module = "ALL",
      dimensions = list(),
      welfare_vars = list(col_nm),
      ppp_sort = ps
    )
  })
  write_fixture_manifest(tmp_manifest, "20260206", entries_list, set_current = TRUE)

  piptm::set_manifest_dir(tmp_manifest)
  piptm::set_arrow_root(tmp_arrow)
  withr::defer(reset_load_env())

  mf <- piptm::piptm_manifest()
  expect_error(
    piptm::load_surveys(mf, ppp = NULL),
    regexp = "different.*ppp_sort|ppp_sort.*different"
  )
})

test_that("load_surveys() errors when a survey lacks the requested ppp column", {
  tmp_arrow    <- withr::local_tempdir()
  tmp_manifest <- withr::local_tempdir()

  # COL has ppp 2017; BOL has only ppp 2011 → requesting ppp=2017 must error
  welfare_by_cc <- list(
    COL = list(col = "welfare_ppp_2017_01_02", val = c(1.0, 2.0)),
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
  expect_error(
    piptm::load_surveys(mf, ppp = 2017L),
    regexp = "not available"
  )
})
