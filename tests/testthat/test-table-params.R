# Tests for the shared table-parameter helpers extracted from table_maker():
#   .validate_and_normalize_table_params() and
#   .partition_surveys_by_dimensions()
#
# These helpers are consumed by both table_maker() and build_description_model()
# so the two can never disagree on domain validation or on which surveys are
# included/excluded. See plan
# .cg-docs/plans/2026-11-12-metadata-rework-and-table-maker-decoupling.md (R1, R3).

library(data.table)
library(arrow)
library(jsonlite)

# Self-contained manifest/arrow fixtures mirroring test-table-maker-v2.R:
#   COL_2010 has gender + area, BOL_2000 has no dimensions, COL_2015 has age.
# make_tm_fixtures_v2() is defined inside test-table-maker-v2.R (not a shared
# helper-*.R), so this file builds its own equivalent via the shared
# write_fixture_* helpers in helper-fixtures.R.
make_tm_fixtures_v2 <- function(env = parent.frame()) {
  tmp_arrow <- withr::local_tempdir(.local_envir = env)
  tmp_manifest <- withr::local_tempdir(.local_envir = env)

  write_fixture_parquet_tm(
    arrow_root = tmp_arrow, country_code = "COL", year = 2010L,
    welfare_type = "INC", version = "v01_v01",
    pip_id = "COL_2010_ECH_INC_ALL", extra_cols = c("gender", "area")
  )
  write_fixture_parquet_tm(
    arrow_root = tmp_arrow, country_code = "BOL", year = 2000L,
    welfare_type = "INC", version = "v01_v01",
    pip_id = "BOL_2000_ECH_INC_ALL"
  )
  write_fixture_parquet_tm(
    arrow_root = tmp_arrow, country_code = "COL", year = 2015L,
    welfare_type = "INC", version = "v01_v01",
    pip_id = "COL_2015_ECH_INC_ALL", extra_cols = c("age")
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
reset_piptm_env_v2 <- reset_piptm_env_tm

test_that(".validate_and_normalize_table_params classifies and normalizes", {
  fx <- make_tm_fixtures_v2()
  activate_tm_fixtures_v2(fx)
  withr::defer(reset_piptm_env_v2())

  out <- piptm:::.validate_and_normalize_table_params(
    analysis_var = "welfare",
    measures = c("mean", "gini"),
    poverty_line = NULL,
    by = "gender",
    filter_base = list(gender = 1L),
    release = "20260401_TEST"
  )

  expect_type(out, "list")
  expect_named(
    out,
    c("families", "by_validate", "normalized_filter_base", "filter_vars"),
    ignore.order = TRUE
  )
  # filter_base normalized to integer vector keyed by varname
  expect_identical(out$filter_vars, "gender")
  expect_identical(out$normalized_filter_base$gender, 1L)
})

test_that(".validate_and_normalize_table_params drops pov_status from by_validate", {
  fx <- make_tm_fixtures_v2()
  activate_tm_fixtures_v2(fx)
  withr::defer(reset_piptm_env_v2())

  out <- piptm:::.validate_and_normalize_table_params(
    analysis_var = "welfare",
    measures = "mean",
    poverty_line = 5,
    by = "pov_status",
    filter_base = NULL,
    release = "20260401_TEST"
  )
  expect_null(out$by_validate)
})

test_that(".validate_and_normalize_table_params rejects poverty measure without poverty_line", {
  fx <- make_tm_fixtures_v2()
  activate_tm_fixtures_v2(fx)
  withr::defer(reset_piptm_env_v2())

  expect_error(
    piptm:::.validate_and_normalize_table_params(
      analysis_var = "welfare",
      measures = "headcount",
      poverty_line = NULL,
      by = NULL,
      filter_base = NULL,
      release = "20260401_TEST"
    ),
    "poverty"
  )
})

test_that(".validate_and_normalize_table_params rejects unknown filter_base variable", {
  fx <- make_tm_fixtures_v2()
  activate_tm_fixtures_v2(fx)
  withr::defer(reset_piptm_env_v2())

  expect_error(
    piptm:::.validate_and_normalize_table_params(
      analysis_var = "welfare",
      measures = "mean",
      poverty_line = NULL,
      by = NULL,
      filter_base = list(not_a_real_var = 1L),
      release = "20260401_TEST"
    ),
    regexp = "filter_base|not_a_real_var|Invalid"
  )
})

test_that(".validate_and_normalize_table_params rejects non-integer filter_base code", {
  fx <- make_tm_fixtures_v2()
  activate_tm_fixtures_v2(fx)
  withr::defer(reset_piptm_env_v2())

  expect_error(
    piptm:::.validate_and_normalize_table_params(
      analysis_var = "welfare",
      measures = "mean",
      poverty_line = NULL,
      by = NULL,
      filter_base = list(gender = "not_an_int"),
      release = "20260401_TEST"
    )
  )
})

test_that(".partition_surveys_by_dimensions includes surveys carrying all by dimensions", {
  fx <- make_tm_fixtures_v2()
  activate_tm_fixtures_v2(fx)
  withr::defer(reset_piptm_env_v2())

  part <- piptm:::.partition_surveys_by_dimensions(
    release = "20260401_TEST",
    pip_id = c("COL_2010_ECH_INC_ALL"),
    normalized_filter_base = NULL,
    filter_vars = character(0L),
    by = "gender",
    warn = FALSE
  )

  expect_setequal(part$included$pip_id, "COL_2010_ECH_INC_ALL")
  expect_equal(nrow(part$excluded), 0L)
})

test_that(".partition_surveys_by_dimensions excludes zero-overlap survey with clean reason", {
  fx <- make_tm_fixtures_v2()
  activate_tm_fixtures_v2(fx)
  withr::defer(reset_piptm_env_v2())

  part <- piptm:::.partition_surveys_by_dimensions(
    release = "20260401_TEST",
    pip_id = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
    normalized_filter_base = NULL,
    filter_vars = character(0L),
    by = "gender",
    warn = FALSE
  )

  expect_setequal(part$included$pip_id, "COL_2010_ECH_INC_ALL")
  expect_setequal(part$excluded$pip_id, "BOL_2000_ECH_INC_ALL")
  # reason column must NOT duplicate the pip_id prefix
  expect_false(any(grepl("BOL_2000_ECH_INC_ALL:", part$excluded$reason, fixed = TRUE)))
  expect_true(any(grepl("no requested dimensions", part$excluded$reason)))
})

test_that(".partition_surveys_by_dimensions does not exclude on pov_status", {
  fx <- make_tm_fixtures_v2()
  activate_tm_fixtures_v2(fx)
  withr::defer(reset_piptm_env_v2())

  part <- piptm:::.partition_surveys_by_dimensions(
    release = "20260401_TEST",
    pip_id = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
    normalized_filter_base = NULL,
    filter_vars = character(0L),
    by = "pov_status",
    warn = FALSE
  )

  # pov_status is computed dynamically; neither survey is excluded for it
  expect_setequal(
    part$included$pip_id,
    c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL")
  )
  expect_equal(nrow(part$excluded), 0L)
})

test_that(".partition_surveys_by_dimensions warns on partial pip_id manifest match", {
  fx <- make_tm_fixtures_v2()
  activate_tm_fixtures_v2(fx)
  withr::defer(reset_piptm_env_v2())

  expect_warning(
    part <- piptm:::.partition_surveys_by_dimensions(
      release = "20260401_TEST",
      pip_id = c("COL_2010_ECH_INC_ALL", "XXX_9999_NONE_INC_ALL"),
      normalized_filter_base = NULL,
      filter_vars = character(0L),
      by = NULL,
      warn = TRUE
    ),
    "not found in manifest"
  )
  expect_setequal(part$included$pip_id, "COL_2010_ECH_INC_ALL")
})

test_that(".partition_surveys_by_dimensions aborts when all surveys fail by stage", {
  fx <- make_tm_fixtures_v2()
  activate_tm_fixtures_v2(fx)
  withr::defer(reset_piptm_env_v2())

  expect_error(
    piptm:::.partition_surveys_by_dimensions(
      release = "20260401_TEST",
      pip_id = "BOL_2000_ECH_INC_ALL",
      normalized_filter_base = NULL,
      filter_vars = character(0L),
      by = "gender",
      warn = FALSE
    )
  )
})

test_that("partition helper agrees with table_maker included/excluded surveys", {
  fx <- make_tm_fixtures_v2()
  activate_tm_fixtures_v2(fx)
  withr::defer(reset_piptm_env_v2())

  # table_maker drops BOL_2000 (no gender dimension) and keeps COL_2010.
  suppressWarnings(
    res <- piptm::table_maker(
      pip_id = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
      analysis_var = "welfare",
      measures = "mean",
      by = "gender"
    )
  )

  part <- piptm:::.partition_surveys_by_dimensions(
    release = "20260401_TEST",
    pip_id = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
    normalized_filter_base = NULL,
    filter_vars = character(0L),
    by = "gender",
    warn = FALSE
  )

  expect_setequal(unique(res$pip_id), part$included$pip_id)
})
