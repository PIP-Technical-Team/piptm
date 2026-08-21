# ── Tests ─────────────────────────────────────────────────────────────────────

test_that("build_description_model returns a list with expected sections", {
  result <- make_mock_table_result()
  model <- piptm:::build_description_model(result)

  expect_true(is.list(model))
  expect_true("metadata" %in% names(model))
  expect_true("surveys" %in% names(model))
  expect_true("sample" %in% names(model))
  expect_true("statistics" %in% names(model))
  expect_true("cell_definition" %in% names(model))
  expect_true("suppression" %in% names(model))
})

test_that("build_description_model includes poverty_line when present", {
  result <- make_mock_table_result(
    analysis_var = "pov_status",
    measures = c("headcount", "poverty_gap"),
    poverty_line = 2.15
  )
  model <- piptm:::build_description_model(result)

  expect_true("poverty_line" %in% names(model))
  expect_equal(model$poverty_line$value, 2.15)
})

test_that("build_description_model excludes poverty_line when absent", {
  result <- make_mock_table_result(poverty_line = NULL)
  model <- piptm:::build_description_model(result)

  expect_false("poverty_line" %in% names(model))
})

test_that("build_description_model includes layout when by is present", {
  result <- make_mock_table_result(by = c("gender", "area"))
  model <- piptm:::build_description_model(result)

  expect_true("layout" %in% names(model))
  expect_equal(length(model$layout$variables), 2)
})

test_that("build_description_model excludes layout when by is NULL", {
  result <- make_mock_table_result(by = NULL)
  model <- piptm:::build_description_model(result)

  expect_false("layout" %in% names(model))
})

test_that("build_description_model includes excluded surveys when present", {
  excluded <- data.table::data.table(
    pip_id = "BOL_2000_ECH_INC_ALL",
    reason = "Missing dimensions: gender"
  )
  result <- make_mock_table_result(excluded = excluded)
  model <- piptm:::build_description_model(result)

  expect_true("excluded_surveys" %in% names(model$surveys))
  expect_equal(nrow(model$surveys$excluded_surveys), 1)
})

test_that("build_description_model excludes excluded surveys section when empty", {
  result <- make_mock_table_result()
  model <- piptm:::build_description_model(result)

  expect_false("excluded_surveys" %in% names(model$surveys))
})

test_that("build_description_model includes filters when present", {
  result <- make_mock_table_result(filter_base = list(gender = c(0L)))
  model <- piptm:::build_description_model(result)

  expect_true(model$sample$filters_applied)
  expect_equal(length(model$sample$variables), 1)
})

test_that("build_description_model reports full sample when no filters", {
  result <- make_mock_table_result(filter_base = NULL)
  model <- piptm:::build_description_model(result)

  expect_false(model$sample$filters_applied)
  expect_equal(model$sample$text, "The full survey sample was used (no filters applied).")
})
