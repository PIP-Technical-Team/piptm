# Test: Description helper functions

test_that("format_welfare_type() maps codes to labels correctly", {
  expect_equal(format_welfare_type("INC"), "Income")
  expect_equal(format_welfare_type("CON"), "Consumption")
  expect_equal(format_welfare_type(c("INC", "CON")), c("Income", "Consumption"))
})


test_that("format_welfare_type() falls back to input for unknown codes", {
  result <- format_welfare_type("UNKNOWN")
  expect_equal(result, "UNKNOWN")
  mixed <- format_welfare_type(c("INC", "UNKNOWN", "CON"))
  expect_equal(mixed, c("Income", "UNKNOWN", "Consumption"))
})


test_that("format_slot_label() maps singular camelCase slot names", {
  expect_equal(format_slot_label("column"), "Columns")
  expect_equal(format_slot_label("row"), "Rows")
  expect_equal(format_slot_label("superColumn"), "Super-columns")
  expect_equal(format_slot_label("superRow"), "Super-rows")
})


test_that("format_slot_label() maps plural keys", {
  expect_equal(format_slot_label("columns"), "Columns")
  expect_equal(format_slot_label("rows"), "Rows")
  expect_equal(format_slot_label("super_columns"), "Super-columns")
  expect_equal(format_slot_label("super_rows"), "Super-rows")
  expect_equal(format_slot_label("superColumns"), "Super-columns")
  expect_equal(format_slot_label("superRows"), "Super-rows")
})


test_that("format_slot_label() handles vector input in visual-hierarchy order", {
  slots <- c("superRow", "row", "superColumn", "column")
  result <- format_slot_label(slots)
  expect_equal(result, c("Super-rows", "Rows", "Super-columns", "Columns"))
})
