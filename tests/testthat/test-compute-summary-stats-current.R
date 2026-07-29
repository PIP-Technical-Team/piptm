library(testthat)
library(data.table)

make_stats_dt <- function() {
  data.table(
    welfare = as.numeric(1:5),
    weight = rep(1, 5),
    gender = factor(c("f", "m", "f", "m", "f"), levels = c("f", "m"))
  )
}

test_that("compute_summary_stats validates required columns", {
  dt <- data.table(weight = c(1, 1, 1))
  expect_error(piptm:::compute_summary_stats(dt, measures = "mean"), "Required column")
})

test_that("compute_summary_stats computes known ungrouped values", {
  dt <- make_stats_dt()
  out <- piptm:::compute_summary_stats(dt, measures = c("mean", "median", "min", "max", "nobs", "sum"))
  expect_equal(out[measure == "mean", value], 3)
  expect_equal(out[measure == "median", value], 3)
  expect_equal(out[measure == "min", value], 1)
  expect_equal(out[measure == "max", value], 5)
  expect_equal(out[measure == "nobs", value], 5)
  expect_equal(out[measure == "sum", value], 15)
})

test_that("compute_summary_stats supports grouping", {
  dt <- make_stats_dt()
  out <- piptm:::compute_summary_stats(dt, by = "gender", measures = c("mean", "nobs"))
  expect_true("gender" %in% names(out))
  expect_equal(out[gender == "f" & measure == "nobs", value], 3)
  expect_equal(out[gender == "m" & measure == "nobs", value], 2)
})

test_that("compute_summary_stats supports alternate target_variable", {
  dt <- data.table(x = c(2, 4, 6), weight = c(1, 1, 1))
  out <- piptm:::compute_summary_stats(dt, measures = c("mean", "max"), target_variable = "x")
  expect_equal(out[measure == "mean", value], 4)
  expect_equal(out[measure == "max", value], 6)
})

test_that("compute_summary_stats returns population column", {
  dt <- make_stats_dt()
  out <- piptm:::compute_summary_stats(dt, by = "gender", measures = "mean")
  expect_true("population" %in% names(out))
  expect_equal(out[gender == "f", population][1], 3)
  expect_equal(out[gender == "m", population][1], 2)
})

test_that("compute_measures handles shares in multi-survey batch", {
  activate_test_registry()
  dt <- data.table(
    pip_id = rep(c("A", "B"), each = 6L),
    welfare = as.numeric(rep(1:6, 2L)),
    weight = rep(1.0, 12L),
    gender = factor(rep(c("m", "f"), 6L))
  )
  out <- piptm:::compute_measures(dt, measures = "pop_share", by = "gender")
  expect_true(all(out[measure == "pop_share", value] == 0.5))
})
