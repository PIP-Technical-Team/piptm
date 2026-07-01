library(data.table)
library(arrow)
library(jsonlite)

reset_piptm_env_v2 <- function() {
  env <- getNamespace("piptm")$.piptm_env
  env$arrow_root <- NULL
  env$manifest_dir <- NULL
  env$manifests <- list()
  env$current_release <- NULL
}

make_tm_fixtures_v2 <- function(env = parent.frame()) {
  tmp_arrow <- withr::local_tempdir(.local_envir = env)
  tmp_manifest <- withr::local_tempdir(.local_envir = env)

  write_fixture_parquet_tm(
    arrow_root = tmp_arrow,
    country_code = "COL",
    year = 2010L,
    welfare_type = "INC",
    version = "v01_v01",
    pip_id = "COL_2010_ECH_INC_ALL",
    extra_cols = c("gender", "area")
  )

  write_fixture_parquet_tm(
    arrow_root = tmp_arrow,
    country_code = "BOL",
    year = 2000L,
    welfare_type = "INC",
    version = "v01_v01",
    pip_id = "BOL_2000_ECH_INC_ALL"
  )

  write_fixture_parquet_tm(
    arrow_root = tmp_arrow,
    country_code = "COL",
    year = 2015L,
    welfare_type = "INC",
    version = "v01_v01",
    pip_id = "COL_2015_ECH_INC_ALL",
    extra_cols = c("age")
  )

  entries <- list(
    list(
      pip_id = "COL_2010_ECH_INC_ALL", survey_id = "S1", country_code = "COL",
      year = 2010L, welfare_type = "INC", version = "v01_v01",
      survey_acronym = "ECH", module = "ALL",
      dimensions = list("gender", "area"),
      welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L
    ),
    list(
      pip_id = "BOL_2000_ECH_INC_ALL", survey_id = "S2", country_code = "BOL",
      year = 2000L, welfare_type = "INC", version = "v01_v01",
      survey_acronym = "ECH", module = "ALL",
      dimensions = list(),
      welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L
    ),
    list(
      pip_id = "COL_2015_ECH_INC_ALL", survey_id = "S3", country_code = "COL",
      year = 2015L, welfare_type = "INC", version = "v01_v01",
      survey_acronym = "ECH", module = "ALL",
      dimensions = list("age"),
      welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L
    )
  )

  write_fixture_manifest_tm(tmp_manifest, "20260401_TEST", entries)

  list(tmp_arrow = tmp_arrow, tmp_manifest = tmp_manifest)
}

activate_tm_fixtures_v2 <- function(fx) {
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
}

test_that("table_maker v2 requires analysis_var", {
  fx <- make_tm_fixtures_v2()
  activate_tm_fixtures_v2(fx)
  withr::defer(reset_piptm_env_v2())

  expect_error(
    piptm::table_maker(
      pip_id = "COL_2010_ECH_INC_ALL",
      measures = "mean"
    ),
    "analysis_var"
  )
})

test_that("table_maker v2 welfare analysis works", {
  fx <- make_tm_fixtures_v2()
  activate_tm_fixtures_v2(fx)
  withr::defer(reset_piptm_env_v2())

  res <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = c("mean", "gini")
  )

  expect_setequal(unique(res$measure), c("mean", "gini"))
  expect_true(all(is.na(res$poverty_line)))
})

test_that("table_maker v2 poverty analysis works with scalar poverty_line", {
  fx <- make_tm_fixtures_v2()
  activate_tm_fixtures_v2(fx)
  withr::defer(reset_piptm_env_v2())

  res <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "pov_status",
    measures = c("headcount", "poverty_gap"),
    poverty_line = 5
  )

  expect_setequal(unique(res$measure), c("headcount", "poverty_gap"))
  expect_equal(unique(res$poverty_line), 5)
})

test_that("table_maker v2 requires poverty_line when by includes pov_status", {
  fx <- make_tm_fixtures_v2()
  activate_tm_fixtures_v2(fx)
  withr::defer(reset_piptm_env_v2())

  expect_error(
    piptm::table_maker(
      pip_id = "COL_2010_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = "mean",
      by = "pov_status"
    ),
    "poverty_line"
  )
})

test_that("table_maker v2 populates poverty_line when by includes pov_status", {
  fx <- make_tm_fixtures_v2()
  activate_tm_fixtures_v2(fx)
  withr::defer(reset_piptm_env_v2())

  res <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean",
    by = "pov_status",
    poverty_line = 5
  )

  expect_true(all(!is.na(res$poverty_line)))
  expect_equal(unique(res$poverty_line), 5)
})

test_that("table_maker v2 excludes zero-overlap survey for by dimensions", {
  fx <- make_tm_fixtures_v2()
  activate_tm_fixtures_v2(fx)
  withr::defer(reset_piptm_env_v2())

  expect_warning(
    res <- piptm::table_maker(
      pip_id = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
      analysis_var = "welfare",
      measures = "mean",
      by = "gender"
    ),
    "Excluding"
  )

  expect_setequal(unique(res$pip_id), "COL_2010_ECH_INC_ALL")
})

test_that("table_maker v2 output serializes", {
  fx <- make_tm_fixtures_v2()
  activate_tm_fixtures_v2(fx)
  withr::defer(reset_piptm_env_v2())

  res <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = c("mean", "gini")
  )

  txt <- jsonlite::toJSON(res, na = "null", auto_unbox = TRUE)
  parsed <- jsonlite::fromJSON(txt)

  expect_true(is.data.frame(parsed))
  expect_true(all(c("measure", "value", "population") %in% names(parsed)))
})
