library(data.table)

test_that("format_table_output validates inputs", {
  expect_error(format_table_output(list(a = 1)), "data.table")
  
  # Missing required columns
  dt_bad1 <- data.table(measure = "gini", value = 0.5)
  expect_error(format_table_output(dt_bad1), "flag")

  dt_bad2 <- data.table(flag = NA_character_, value = 0.5)
  expect_error(format_table_output(dt_bad2), "measure")

  dt_bad3 <- data.table(measure = "gini", flag = NA_character_)
  expect_error(format_table_output(dt_bad3), "value")
})

test_that("format_table_output does not mutate input table", {
  dt <- data.table(
    pip_id  = "COL_2010_ECH_INC_ALL",
    measure = c("gini", "mean"),
    value   = c(0.45, 12.5),
    flag    = c(NA_character_, NA_character_)
  )
  orig_names <- copy(names(dt))
  orig_vals  <- copy(dt$value)

  res <- format_table_output(dt)

  # Input unchanged
  expect_equal(names(dt), orig_names)
  expect_equal(dt$value, orig_vals)
  expect_type(dt$value, "double")
  expect_true("flag" %in% names(dt))

  # Output transformed
  expect_false("flag" %in% names(res))
  expect_type(res$value, "character")
})

test_that("format_table_output formats percentage-scale measures with x100 and 2 decimals", {
  pct_meas <- c(
    "gini", "headcount", "poverty_gap", "severity",
    "pop_share", "target_within_group_share", "target_survey_share"
  )
  dt <- data.table(
    measure = pct_meas,
    value   = c(0.4567, 0.067, 0.01234, 0.0, 0.005, 0.1, 0.999),
    flag    = rep(NA_character_, length(pct_meas))
  )

  res <- format_table_output(dt)

  expect_equal(res$value, c(
    "45.67", "6.70", "1.23", "0.00", "0.50", "10.00", "99.90"
  ))
})

test_that("format_table_output formats counts as whole numbers with 0 decimals", {
  dt <- data.table(
    measure = c("pop_poverty", "nobs"),
    value   = c(123456.7, 5000),
    flag    = c(NA_character_, NA_character_)
  )

  res <- format_table_output(dt)

  expect_equal(res$value, c("123457", "5000"))
})

test_that("format_table_output formats other registered measures without x100 and with 2 decimals", {
  other_meas <- c(
    "mean", "median", "sd", "var", "min", "max",
    "p10", "p25", "p75", "p90", "sum", "mld", "watts"
  )
  dt <- data.table(
    measure = other_meas,
    value   = c(15.2, 10.0, 3.456, 12.345, 1.0, 100.555,
                2.1, 4.5, 20.0, 35.5, 12345.678, 0.352, 0.146),
    flag    = rep(NA_character_, length(other_meas))
  )

  res <- format_table_output(dt)

  expect_equal(res$value, c(
    "15.20", "10.00", "3.46", "12.35", "1.00", "100.56",
    "2.10", "4.50", "20.00", "35.50", "12345.68", "0.35", "0.15"
  ))
})

test_that("format_table_output sets value = 'NR' for masked rows", {
  dt <- data.table(
    measure = c("gini", "mean", "pop_poverty"),
    value   = c(NA_real_, NA_real_, NA_real_),
    flag    = c("NR", "NR", "NR")
  )

  res <- format_table_output(dt)

  expect_equal(res$value, c("NR", "NR", "NR"))
})

test_that("format_table_output pins exact behavior at the x.xx5 rounding boundary", {
  # Floating-point representation can make a half-way decimal value round
  # either direction once formatC() sees its actual binary value. These
  # assertions pin the *currently observed* behavior (not an idealized
  # "correct" rounding rule) so a future change to the rounding engine
  # (e.g. switching formatC() for round()/sprintf()/scales::number()) is
  # caught rather than silently shifting displayed values at the boundary.
  dt_other <- data.table(
    measure = c("mean", "sd", "median"),
    value   = c(0.145, 1.005, 2.675),
    flag    = rep(NA_character_, 3L)
  )
  res_other <- format_table_output(dt_other)
  expect_equal(res_other$value, c("0.14", "1.00", "2.67"))

  # Percentage-family measures are scaled by 100 before the same 2-decimal
  # formatC() rounding is applied; this pins a boundary case post-scaling.
  dt_pct <- data.table(
    measure = "gini",
    value   = 0.12345,
    flag    = NA_character_
  )
  res_pct <- format_table_output(dt_pct)
  expect_equal(res_pct$value, "12.35")
})

test_that("format_table_output converts NA, NaN, Inf, -Inf to NA_character_", {
  dt <- data.table(
    measure = c("mean", "gini", "sd", "mld"),
    value   = c(NA_real_, NaN, Inf, -Inf),
    flag    = rep(NA_character_, 4L)
  )

  res <- format_table_output(dt)

  expect_true(all(is.na(res$value)))
  expect_type(res$value, "character")
})

test_that("format_table_output handles negative values and large numbers without scientific notation", {
  dt <- data.table(
    measure = c("mean", "sum", "gini"),
    value   = c(-5.25, 9876543210.5, -0.015),
    flag    = rep(NA_character_, 3L)
  )

  res <- format_table_output(dt)

  expect_equal(res$value, c("-5.25", "9876543210.50", "-1.50"))
  expect_false(any(grepl("e", res$value, ignore.case = TRUE)))
})

test_that("format_table_output handles empty tables", {
  dt <- data.table(
    pip_id  = character(),
    measure = character(),
    value   = numeric(),
    flag    = character()
  )

  res <- format_table_output(dt)

  expect_equal(nrow(res), 0L)
  expect_type(res$value, "character")
  expect_false("flag" %in% names(res))
})

test_that(".PCT_MEASURES stays a subset of .MEASURE_REGISTRY (registry sync guard)", {
  pct_measures <- piptm:::.PCT_MEASURES
  registry     <- piptm:::.MEASURE_REGISTRY

  # Every percentage-scaled measure name must be a real registered measure;
  # a typo or a stale entry here would silently exclude or misclassify a
  # measure without any test ever failing.
  expect_true(all(pct_measures %in% names(registry)))
})

test_that("format_table_output falls through to the generic 2-decimal path for an unregistered/future measure name", {
  dt <- data.table(
    measure = "custom_future_measure",
    value   = 12.345,
    flag    = NA_character_
  )

  res <- format_table_output(dt)

  # Not in .PCT_MEASURES (no x100 scaling) and not in the whole-number set,
  # so it must land in the generic 2-decimal formatting bucket rather than
  # silently producing NA or an empty string.
  expect_equal(res$value, "12.35")
})
