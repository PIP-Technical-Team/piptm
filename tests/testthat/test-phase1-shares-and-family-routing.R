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
    target_variable = "female",
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
