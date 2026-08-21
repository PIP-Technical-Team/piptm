library(data.table)
library(arrow)

# ── Tests for build_table_description ─────────────────────────────────────────

test_that("build_table_description returns a character string", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  md <- piptm::build_table_description(
    pip_id       = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures     = c("mean", "gini")
  )

  expect_true(is.character(md))
  expect_true(nchar(md) > 0L)
})

test_that("build_table_description includes key sections", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  md <- piptm::build_table_description(
    pip_id       = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures     = c("mean", "gini"),
    by           = c("gender", "area")
  )

  expect_true(grepl("# Table Description", md))
  expect_true(grepl("## Surveys Analyzed", md))
  expect_true(grepl("## Statistics", md))
  expect_true(grepl("## Cell Definition", md))
  expect_true(grepl("## Suppression", md))
})

test_that("build_table_description works with by = NULL (aggregate)", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  md <- piptm::build_table_description(
    pip_id       = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures     = c("mean"),
    by           = NULL
  )

  expect_true(is.character(md))
  expect_true(grepl("full survey sample", md, ignore.case = TRUE))
  expect_false(grepl("Table Structure", md))
})

test_that("build_table_description passes through all table_maker params", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  md <- piptm::build_table_description(
    pip_id              = "COL_2010_ECH_INC_ALL",
    analysis_var        = "welfare",
    measures            = c("mean"),
    ppp                 = 2021L,
    pop_share_threshold = 0.01
  )

  expect_true(is.character(md))
  expect_true(grepl("2021", md))
})
