library(testthat)

test_that(".build_registry_entry prefers recode_spec mapping for binned variables", {
  spec_var <- list(
    ui_label = "Household size",
    roles = c("filter", "covariate"),
    stat_groups = list(),
    categories = list(
      list(code = "1", label = "1 person"),
      list(code = "2-3", label = "2 to 3 persons"),
      list(code = "4-6", label = "4 to 6 persons"),
      list(code = "7+", label = "7 or more persons")
    )
  )

  pip_vars <- list(
    hsize_group = list(
      type = "factor",
      recode_type = "binned_from_continuous",
      mapping = list(
        "1" = "1 person",
        "2" = "2-3 persons",
        "3" = "4-6 persons",
        "4" = "7+ persons"
      )
    )
  )

  entry <- piptm:::.build_registry_entry(
    varname = "hsize_group",
    spec_var = spec_var,
    pip_vars = pip_vars
  )

  expect_identical(entry$tm_type, "categorical")
  expect_identical(entry$n_categories, 4L)
  expect_identical(
    vapply(entry$categories, `[[`, character(1), "code"),
    c("1", "2", "3", "4")
  )
})
