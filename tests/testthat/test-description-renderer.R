# ── Tests for render_description_markdown ──────────────────────────────────────

test_that("render_description_markdown produces valid markdown", {
  result <- make_mock_table_result()
  model <- piptm:::build_description_model(result)
  md <- piptm:::render_description_markdown(model)

  expect_true(is.character(md))
  expect_true(nchar(md) > 0)
  expect_true(grepl("# Table Description", md))
  expect_true(grepl("## Surveys Analyzed", md))
  expect_true(grepl("## Sample Base", md))
  expect_true(grepl("## Statistics", md))
  expect_true(grepl("## Cell Definition", md))
  expect_true(grepl("## Suppression", md))
})

test_that("render_description_markdown includes survey table", {
  result <- make_mock_table_result(pip_ids = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"))
  model <- piptm:::build_description_model(result)
  md <- piptm:::render_description_markdown(model)

  expect_true(grepl("COL_2010_ECH_INC_ALL", md))
  expect_true(grepl("BOL_2000_ECH_INC_ALL", md))
  expect_true(grepl("Colombia", md) || grepl("COL", md))
})

test_that("render_description_markdown includes excluded surveys when present", {
  excluded <- data.table::data.table(
    pip_id = "BOL_2000_ECH_INC_ALL",
    reason = "Missing dimensions: gender"
  )
  result <- make_mock_table_result(excluded = excluded)
  model <- piptm:::build_description_model(result)
  md <- piptm:::render_description_markdown(model)

  expect_true(grepl("Excluded Surveys", md))
  expect_true(grepl("BOL_2000_ECH_INC_ALL", md))
  expect_true(grepl("Missing dimensions: gender", md))
})

test_that("render_description_markdown excludes excluded surveys section when empty", {
  result <- make_mock_table_result()
  model <- piptm:::build_description_model(result)
  md <- piptm:::render_description_markdown(model)

  expect_false(grepl("Excluded Surveys", md))
})

test_that("render_description_markdown includes poverty line when present", {
  result <- make_mock_table_result(
    analysis_var = "pov_status",
    measures = c("headcount", "poverty_gap"),
    poverty_line = 2.15
  )
  model <- piptm:::build_description_model(result)
  md <- piptm:::render_description_markdown(model)

  expect_true(grepl("Poverty line", md))
  expect_true(grepl("2.15", md))
})

test_that("render_description_markdown includes layout when present", {
  result <- make_mock_table_result(by = c("gender", "area"))
  model <- piptm:::build_description_model(result)
  md <- piptm:::render_description_markdown(model)

  expect_true(grepl("Table Structure", md))
  expect_true(grepl("Rows", md))
  expect_true(grepl("Columns", md))
})

test_that("render_description_markdown excludes layout when by is NULL", {
  result <- make_mock_table_result(by = NULL)
  model <- piptm:::build_description_model(result)
  md <- piptm:::render_description_markdown(model)

  expect_false(grepl("Table Structure", md))
})

test_that("render_description_markdown includes filters when present", {
  result <- make_mock_table_result(filter_base = list(gender = c(0L)))
  model <- piptm:::build_description_model(result)
  md <- piptm:::render_description_markdown(model)

  expect_true(grepl("Filtered to", md))
  expect_true(grepl("gender", md, ignore.case = TRUE))
})

test_that("render_description_markdown reports full sample when no filters", {
  result <- make_mock_table_result(filter_base = NULL)
  model <- piptm:::build_description_model(result)
  md <- piptm:::render_description_markdown(model)

  expect_true(grepl("full survey sample", md, ignore.case = TRUE))
})

test_that("render_description_markdown cell definition matches simple scenario", {
  result <- make_mock_table_result(by = c("gender", "area"))
  model <- piptm:::build_description_model(result)
  md <- piptm:::render_description_markdown(model)

  expect_true(grepl("Each cell represents one combination", md))
  expect_true(grepl("Gender", md))
  expect_true(grepl("Area", md))
})

test_that("render_description_markdown cell definition for no dimensions", {
  result <- make_mock_table_result(by = NULL)
  model <- piptm:::build_description_model(result)
  md <- piptm:::render_description_markdown(model)

  expect_true(grepl("full survey sample", md, ignore.case = TRUE))
})

test_that("render_description_markdown includes suppression info", {
  result <- make_mock_table_result(pop_share_threshold = 0.01, n_suppressed = 3)
  model <- piptm:::build_description_model(result)
  md <- piptm:::render_description_markdown(model)

  expect_true(grepl("Suppression", md))
  expect_true(grepl("3 cells", md) || grepl("3 cell was", md))
})

test_that("render_description_markdown shows no suppression when none occurred", {
  result <- make_mock_table_result(pop_share_threshold = 0.01, n_suppressed = 0)
  model <- piptm:::build_description_model(result)
  md <- piptm:::render_description_markdown(model)

  expect_true(grepl("No cells were suppressed", md))
})
