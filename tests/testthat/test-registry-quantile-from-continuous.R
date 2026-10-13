library(testthat)

test_that(".build_registry_entry supports factor + quantile_from_continuous", {
  spec_var <- list(
    ui_label = "Welfare quintile",
    roles = c("filter", "covariate"),
    stat_groups = list(),
    categories = list(
      list(code = "1", label = "Q1 (bottom 20%)"),
      list(code = "2", label = "Q2"),
      list(code = "3", label = "Q3"),
      list(code = "4", label = "Q4"),
      list(code = "5", label = "Q5 (top 20%)")
    )
  )

  pip_vars <- list(
    wquintile = list(
      type = "factor",
      recode_type = "quantile_from_continuous",
      mapping = list(
        "1" = "quintile1",
        "2" = "quintile2",
        "3" = "quintile3",
        "4" = "quintile4",
        "5" = "quintile5"
      )
    )
  )

  entry <- piptm:::.build_registry_entry(
    varname = "wquintile",
    spec_var = spec_var,
    pip_vars = pip_vars
  )

  expect_identical(entry$tm_type, "categorical")
  expect_identical(entry$n_categories, 5L)
  expect_identical(
    vapply(entry$categories, `[[`, character(1), "code"),
    c("1", "2", "3", "4", "5")
  )
})
