library(testthat)
library(data.table)

make_phase1_dt <- function() {
  data.table(
    pip_id = rep("TST_2020", 6L),
    welfare = c(1, 2, 3, 4, 5, 6),
    weight = c(1, 1, 1, 1, 1, 1),
    gender = factor(c("f", "f", "m", "m", "f", "m")),
    female = c(1L, 1L, 0L, 0L, 1L, 0L)
  )
}

test_that(".classify_measures maps summary measures to summary_stats family", {
  out <- piptm:::.classify_measures(c("mean", "gini"))
  expect_true("summary_stats" %in% names(out))
  expect_equal(out$summary_stats, "mean")
  expect_equal(out$inequality, "gini")
})

test_that("compute_measures keeps summary_stats and shares outputs together", {
  activate_test_registry("20260206")
  dt <- make_phase1_dt()

  res <- compute_measures(
    dt,
    measures = c("mean", "pop_share"),
    analysis_var = "female",
    by = "gender"
  )

  expect_setequal(unique(res$measure), c("mean", "pop_share"))
  expect_equal(nrow(res[measure == "mean"]), 2L)
  expect_equal(nrow(res[measure == "pop_share"]), 2L)
})

test_that("compute_shares supports mixed share measures in one call", {
  dt <- make_phase1_dt()

  res <- compute_shares(
    dt,
    by = "gender",
    measures = c("pop_share", "target_survey_share"),
    target_variable = "female"
  )

  expect_setequal(unique(res$measure), c("pop_share", "target_survey_share"))
  expect_equal(nrow(res), 4L)
})

test_that("compute_shares computes each share measure with correct denominator", {
  dt <- data.table(
    pip_id = rep("TST_2020", 4L),
    weight = c(2, 1, 1, 1),
    area = c("rural", "rural", "rural", "urban"),
    female = c(1L, 0L, 1L, 1L)
  )

  res <- compute_shares(
    dt,
    by = "area",
    measures = c("pop_share", "target_within_group_share", "target_survey_share"),
    target_variable = "female"
  )

  pop <- res[measure == "pop_share"][order(area)]
  expect_equal(pop$value, c(0.8, 0.2), tolerance = 1e-12)
  expect_equal(sum(pop$value), 1, tolerance = 1e-12)

  within <- res[measure == "target_within_group_share"][order(area)]
  expect_equal(within$value, c(0.75, 1.0), tolerance = 1e-12)

  survey <- res[measure == "target_survey_share"][order(area)]
  expect_equal(survey$value, c(0.6, 0.2), tolerance = 1e-12)
  expect_equal(sum(survey$value), 0.8, tolerance = 1e-12)
})

test_that("compute_measures shares use survey-specific denominators in batched mode", {
  activate_test_registry("20260206")
  dt <- data.table(
    pip_id = c("S1", "S1", "S1", "S2", "S2"),
    welfare = c(1, 2, 3, 1, 2),
    weight = c(2, 1, 1, 3, 1),
    area = c("urban", "urban", "rural", "urban", "rural"),
    female = c(1L, 0L, 1L, 1L, 0L)
  )

  res <- compute_measures(
    dt,
    measures = c("pop_share", "target_survey_share"),
    analysis_var = "female",
    by = "area"
  )

  pop <- res[measure == "pop_share"]
  pop_sum <- pop[, .(total = sum(value)), by = .(pip_id)]
  expect_equal(pop_sum[order(pip_id)]$total, c(1, 1), tolerance = 1e-12)

  survey <- res[measure == "target_survey_share"]
  survey_sum <- survey[, .(total = sum(value)), by = .(pip_id)]
  expect_equal(survey_sum[order(pip_id)]$total, c(0.75, 0.75), tolerance = 1e-12)
})


test_that("compute_shares: zero-weight cell yields NA within-share, justifying the footer qualification", {
  # "rural" cell has zero total weight; "urban" is non-degenerate.
  dt <- data.table(
    pip_id = rep("TST_ZERO", 4L),
    weight = c(0, 0, 1, 1),
    area = c("rural", "rural", "urban", "urban"),
    female = c(1L, 0L, 1L, 0L)
  )

  res <- compute_shares(
    dt,
    by = "area",
    measures = c("pop_share", "target_within_group_share", "target_survey_share"),
    target_variable = "female"
  )

  pop_rural <- res[measure == "pop_share" & area == "rural"]$value
  within_rural <- res[measure == "target_within_group_share" & area == "rural"]$value
  survey_rural <- res[measure == "target_survey_share" & area == "rural"]$value

  expect_equal(pop_rural, 0, tolerance = 1e-12)
  expect_true(is.na(within_rural))
  expect_equal(survey_rural, 0, tolerance = 1e-12)

  # The identity pop_share * target_within_group_share == target_survey_share
  # does NOT hold for a zero-weight cell: 0 * NA is NA, not 0. This is why
  # the description footer must be qualified to "cells with positive
  # weighted population" rather than stated unconditionally.
  expect_true(is.na(pop_rural * within_rural))
  expect_false(isTRUE(all.equal(pop_rural * within_rural, survey_rural)))

  # The non-degenerate "urban" cell satisfies the identity exactly.
  pop_urban <- res[measure == "pop_share" & area == "urban"]$value
  within_urban <- res[measure == "target_within_group_share" & area == "urban"]$value
  survey_urban <- res[measure == "target_survey_share" & area == "urban"]$value
  expect_equal(pop_urban * within_urban, survey_urban, tolerance = 1e-12)
})
