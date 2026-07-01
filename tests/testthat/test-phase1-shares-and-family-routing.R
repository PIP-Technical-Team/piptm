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
