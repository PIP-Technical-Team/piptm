library(testthat)
library(data.table)

# ── Fixture helpers ────────────────────────────────────────────────────────────

# Five-row dataset used for most scalar tests.
# welfare = 1:5, weight = 1 each, poverty_line = 3.5
# Poor individuals: welfare 1, 2, 3  (strictly < 3.5)
#
# Hand-computed expected values at poverty_line = 3.5:
#   headcount   = 3 / 5                                    = 0.6
#   gap_i       = c(2.5, 1.5, 0.5, 0, 0) / 3.5
#   poverty_gap = (2.5 + 1.5 + 0.5) / 3.5 / 5             = 4.5 / 3.5 / 5
#                                                           = 0.2571429
#   severity    = (2.5^2 + 1.5^2 + 0.5^2) / 3.5^2 / 5    = (6.25+2.25+0.25) / 12.25 / 5
#                                                           = 8.75 / 12.25 / 5 = 0.1428571
#   watts       = (log(3.5/1) + log(3.5/2) + log(3.5/3)) / 5
#               = (1.252763 + 0.559616 + 0.154151) / 5    = 1.966530 / 5 = 0.3933060
#   pop_poverty = 3  (weighted count; weight = 1 each)
#   population  = 5

make_five_dt <- function() {
  data.table(
    welfare = as.numeric(1:5),
    weight  = rep(1.0, 5L)
  )
}

# ══════════════════════════════════════════════════════════════════════════════
# headcount
# ══════════════════════════════════════════════════════════════════════════════

test_that("headcount: 3 of 5 poor with equal weights → 0.6", {
  dt  <- make_five_dt()
  res <- compute_poverty(dt, poverty_lines = 3.5, measures = "headcount")

  expect_equal(res[measure == "headcount", value], 0.6, tolerance = 1e-9)
})

test_that("headcount: all poor → 1.0", {
  dt  <- data.table(welfare = c(1, 2), weight = c(1, 1))
  res <- compute_poverty(dt, poverty_lines = 10, measures = "headcount")

  expect_equal(res[measure == "headcount", value], 1.0, tolerance = 1e-9)
})

test_that("headcount: none poor → 0.0", {
  dt  <- data.table(welfare = c(5, 6), weight = c(1, 1))
  res <- compute_poverty(dt, poverty_lines = 3.5, measures = "headcount")

  expect_equal(res[measure == "headcount", value], 0.0, tolerance = 1e-9)
})

# ══════════════════════════════════════════════════════════════════════════════
# poverty_gap
# ══════════════════════════════════════════════════════════════════════════════

test_that("poverty_gap: hand-computed value", {
  dt       <- make_five_dt()
  expected <- (2.5 + 1.5 + 0.5) / 3.5 / 5
  res      <- compute_poverty(dt, poverty_lines = 3.5, measures = "poverty_gap")

  expect_equal(res[measure == "poverty_gap", value], expected, tolerance = 1e-7)
})

# ══════════════════════════════════════════════════════════════════════════════
# severity
# ══════════════════════════════════════════════════════════════════════════════

test_that("severity: hand-computed value", {
  dt       <- make_five_dt()
  gaps     <- c(2.5, 1.5, 0.5) / 3.5
  expected <- sum(gaps^2) / 5
  res      <- compute_poverty(dt, poverty_lines = 3.5, measures = "severity")

  expect_equal(res[measure == "severity", value], expected, tolerance = 1e-7)
})

# ══════════════════════════════════════════════════════════════════════════════
# watts
# ══════════════════════════════════════════════════════════════════════════════

test_that("watts: hand-computed log-ratio sum", {
  dt       <- make_five_dt()
  expected <- (log(3.5 / 1) + log(3.5 / 2) + log(3.5 / 3)) / 5
  res      <- compute_poverty(dt, poverty_lines = 3.5, measures = "watts")

  expect_equal(res[measure == "watts", value], expected, tolerance = 1e-7)
})

test_that("watts: zero-welfare row contributes 0 (not Inf)", {
  # welfare = 0 is poor (below any positive pl) but contributes 0 to Watts
  dt <- data.table(
    welfare = c(0, 2, 4),
    weight  = c(1, 1, 1)
  )
  # Expected: only welfare=2 is poor AND > 0 → log(3/2); welfare=0 contributes 0
  expected <- log(3 / 2) / 3
  res      <- compute_poverty(dt, poverty_lines = 3, measures = "watts")

  expect_equal(res[measure == "watts", value], expected, tolerance = 1e-7)
  expect_true(is.finite(res[measure == "watts", value]))
})

# ══════════════════════════════════════════════════════════════════════════════
# pop_poverty
# ══════════════════════════════════════════════════════════════════════════════

test_that("pop_poverty: weighted count of poor", {
  dt  <- make_five_dt()
  res <- compute_poverty(dt, poverty_lines = 3.5, measures = "pop_poverty")

  expect_equal(res[measure == "pop_poverty", value], 3.0, tolerance = 1e-9)
})

test_that("pop_poverty: respects non-uniform weights", {
  dt <- data.table(
    welfare = c(1, 2, 5),
    weight  = c(2, 3, 4)   # poor: welfare 1 (w=2) and 2 (w=3)
  )
  res <- compute_poverty(dt, poverty_lines = 3, measures = "pop_poverty")

  expect_equal(res[measure == "pop_poverty", value], 5.0, tolerance = 1e-9)
})

# ══════════════════════════════════════════════════════════════════════════════
# population column
# ══════════════════════════════════════════════════════════════════════════════

test_that("population column equals total weighted sum", {
  dt  <- data.table(welfare = c(1, 2, 3), weight = c(1, 2, 3))
  res <- compute_poverty(dt, poverty_lines = 2, measures = "headcount")

  expect_equal(res$population, 6.0, tolerance = 1e-9)
})

# ══════════════════════════════════════════════════════════════════════════════
# multiple poverty lines
# ══════════════════════════════════════════════════════════════════════════════

test_that("multiple poverty lines produce one row per line", {
  dt  <- make_five_dt()
  res <- compute_poverty(dt, poverty_lines = c(2, 3.5), measures = "headcount")

  expect_equal(nrow(res), 2L)
  expect_setequal(res$poverty_line, c(2, 3.5))

  # At pl=2: welfare 1 is poor → headcount = 1/5 = 0.2
  expect_equal(res[poverty_line == 2,   value], 0.2, tolerance = 1e-9)
  # At pl=3.5: welfare 1,2,3 are poor → headcount = 3/5 = 0.6
  expect_equal(res[poverty_line == 3.5, value], 0.6, tolerance = 1e-9)
})

# ══════════════════════════════════════════════════════════════════════════════
# grouped computation
# ══════════════════════════════════════════════════════════════════════════════

test_that("grouped by gender: correct per-group headcount", {
  # male:   welfare [1, 3], weight [1, 1] → poverty_line = 2: 1 poor, headcount = 0.5
  # female: welfare [2, 4], weight [1, 1] → poverty_line = 2: 0 poor (2 is NOT < 2),
  #                                                            headcount = 0.0
  dt <- data.table(
    welfare = c(1, 3, 2, 4),
    weight  = rep(1.0, 4L),
    gender  = factor(c("male", "male", "female", "female"),
                     levels = c("male", "female"))
  )
  res <- compute_poverty(dt, poverty_lines = 2, by = "gender",
                         measures = "headcount")

  expect_equal(
    res[gender == "male"   & measure == "headcount", value], 0.5,
    tolerance = 1e-9
  )
  expect_equal(
    res[gender == "female" & measure == "headcount", value], 0.0,
    tolerance = 1e-9
  )
})

test_that("grouped: population reflects group size, not total", {
  dt <- data.table(
    welfare = c(1, 3, 2, 4),
    weight  = rep(1.0, 4L),
    gender  = factor(c("male", "male", "female", "female"),
                     levels = c("male", "female"))
  )
  res <- compute_poverty(dt, poverty_lines = 2, by = "gender",
                         measures = "headcount")

  expect_equal(res[gender == "male",   population], 2.0, tolerance = 1e-9)
  expect_equal(res[gender == "female", population], 2.0, tolerance = 1e-9)
})

# ══════════════════════════════════════════════════════════════════════════════
# multiple dimensions (cross-tabulation)
# ══════════════════════════════════════════════════════════════════════════════

test_that("two dimensions produce full cross-tabulation rows", {
  dt <- data.table(
    welfare = c(1, 5, 2, 6),
    weight  = rep(1.0, 4L),
    gender  = factor(c("male", "male", "female", "female"),
                     levels = c("male", "female")),
    area    = factor(c("urban", "rural", "urban", "rural"),
                     levels = c("urban", "rural"))
  )
  res <- compute_poverty(dt, poverty_lines = 3, by = c("gender", "area"),
                         measures = "headcount")

  # One row per (gender × area × poverty_line × measure) = 4 combos present
  expect_equal(nrow(res), 4L)

  # male/urban: welfare=1 < 3 → headcount = 1/1 = 1.0
  expect_equal(
    res[gender == "male" & area == "urban" & measure == "headcount", value],
    1.0, tolerance = 1e-9
  )
  # female/urban: welfare=2 < 3 → headcount = 1/1 = 1.0
  expect_equal(
    res[gender == "female" & area == "urban" & measure == "headcount", value],
    1.0, tolerance = 1e-9
  )
  # male/rural: welfare=5 >= 3 → headcount = 0.0
  expect_equal(
    res[gender == "male" & area == "rural" & measure == "headcount", value],
    0.0, tolerance = 1e-9
  )
})

# ══════════════════════════════════════════════════════════════════════════════
# measures subset
# ══════════════════════════════════════════════════════════════════════════════

test_that("requesting only 'headcount' returns only headcount rows", {
  dt  <- make_five_dt()
  res <- compute_poverty(dt, poverty_lines = 3.5, measures = "headcount")

  expect_equal(unique(res$measure), "headcount")
})

test_that("measures = NULL computes all 5 poverty measures", {
  dt  <- make_five_dt()
  res <- compute_poverty(dt, poverty_lines = 3.5)

  expect_setequal(
    res$measure,
    c("headcount", "poverty_gap", "severity", "watts", "pop_poverty")
  )
})

# ══════════════════════════════════════════════════════════════════════════════
# output format
# ══════════════════════════════════════════════════════════════════════════════

test_that("output is a data.table in long format with correct columns", {
  dt  <- make_five_dt()
  res <- compute_poverty(dt, poverty_lines = 3.5, measures = "headcount")

  expect_s3_class(res, "data.table")
  expect_true(all(c("poverty_line", "measure", "value", "population") %in%
                    names(res)))
  expect_type(res$poverty_line, "double")
  expect_type(res$measure,      "character")
  expect_type(res$value,        "double")
  expect_type(res$population,   "double")
})

test_that("grouped output has dimension columns before measure/value", {
  dt <- data.table(
    welfare = c(1, 4),
    weight  = c(1, 1),
    area    = factor(c("urban", "rural"), levels = c("urban", "rural"))
  )
  res <- compute_poverty(dt, poverty_lines = 2, by = "area",
                         measures = "headcount")

  expect_true("area" %in% names(res))
})

# ── grp parameter ─────────────────────────────────────────────────────────────

test_that("grp argument: pre-built GRP gives same result as internal GRP", {
  dt <- data.table(
    welfare = c(1, 3, 2, 4),
    weight  = rep(1.0, 4L),
    gender  = factor(c("male", "male", "female", "female"),
                     levels = c("male", "female"))
  )
  grp_ext  <- collapse::GRP(dt, by = "gender")
  res_int  <- compute_poverty(dt, poverty_lines = 2, by = "gender",
                              measures = "headcount")
  res_ext  <- compute_poverty(dt, poverty_lines = 2, by = "gender",
                              measures = "headcount", grp = grp_ext)
  expect_equal(res_int, res_ext)
})

test_that("grp argument: grp is ignored when by = NULL", {
  dt      <- make_five_dt()
  # Build a GRP using a by column — it will be ignored because by = NULL
  grp_ext <- collapse::GRP(dt, by = "welfare")
  # Should not error and should give the same result as without grp
  res_with    <- compute_poverty(dt, poverty_lines = 3.5, measures = "headcount",
                                 grp = grp_ext)
  res_without <- compute_poverty(dt, poverty_lines = 3.5, measures = "headcount")
  expect_equal(res_with, res_without)
})
