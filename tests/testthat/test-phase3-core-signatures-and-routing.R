library(data.table)

test_that("phase3: table_maker() signature uses analysis_var + poverty_line", {
  f <- formals(piptm::table_maker)

  expect_true("analysis_var" %in% names(f))
  expect_true("poverty_line" %in% names(f))
  expect_false("target_variable" %in% names(f))
  expect_false("poverty_lines" %in% names(f))
})

test_that("phase3: compute_measures() signature uses analysis_var + poverty_line", {
  f <- formals(piptm:::compute_measures)

  expect_true("analysis_var" %in% names(f))
  expect_true("poverty_line" %in% names(f))
  expect_false("target_variable" %in% names(f))
  expect_false("poverty_lines" %in% names(f))
})

test_that("phase3: compute_measures() routes pov_status to poverty family", {
  dt <- data.table(
    pip_id = "X",
    welfare = c(1, 2, 3),
    weight = c(1, 1, 1)
  )

  res <- piptm:::compute_measures(
    dt,
    measures = "headcount",
    analysis_var = "pov_status",
    poverty_line = 2.15
  )

  expect_true("headcount" %in% res$measure)
  expect_true(all(!is.na(res[measure == "headcount"]$poverty_line)))
})

test_that("phase3: compute_measures() uses analysis_var for shares", {
  activate_test_registry("20260206")
  dt <- data.table(
    pip_id = rep("X", 4),
    welfare = c(1, 2, 3, 4),
    weight = c(1, 1, 1, 1),
    female = c(1L, 0L, 1L, 0L),
    area = c("u", "u", "r", "r")
  )

  res <- piptm:::compute_measures(
    dt,
    measures = c("pop_share", "target_survey_share"),
    analysis_var = "female",
    by = "area"
  )

  expect_setequal(unique(res$measure), c("pop_share", "target_survey_share"))
})

test_that("phase3: compute_measures() requires welfare when analysis_var is pov_status", {
  dt <- data.table(
    pip_id = "X",
    weight = c(1, 1, 1)
  )

  expect_error(
    piptm:::compute_measures(
      dt,
      measures = "headcount",
      analysis_var = "pov_status",
      poverty_line = 2.15
    ),
    "Required column"
  )
})
