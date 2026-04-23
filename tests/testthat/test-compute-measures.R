library(data.table)

# ── Fixture helpers ──────────────────────────────────────────────────────────

make_single_dt <- function(n = 10L, dims = character(0L)) {
  dt <- data.table(
    pip_id  = "TST_2020_CON",
    welfare = seq(1, by = 1, length.out = n),
    weight  = rep(1.0, n)
  )
  if ("gender" %in% dims) {
    dt[, gender := factor(
      rep(c("male", "female"), length.out = n),
      levels = c("male", "female")
    )]
  }
  if ("area" %in% dims) {
    dt[, area := factor(
      rep(c("urban", "rural"), length.out = n),
      levels = c("urban", "rural")
    )]
  }
  dt
}

# All 18 canonical measure names
ALL_MEASURES <- names(unlist(piptm:::.MEASURE_REGISTRY, use.names = TRUE))
ALL_PL       <- c(3.5, 6.5)

# ── 1. Multi-survey guard ────────────────────────────────────────────────────

test_that("multi-survey guard errors when dt contains 2 pip_ids", {
  dt <- data.table(
    pip_id  = c("A", "B"),
    welfare = c(1, 2),
    weight  = c(1, 1)
  )
  expect_error(
    compute_measures(dt, measures = "mean"),
    class = "rlang_error"
  )
})

test_that("multi-survey guard error message names the found pip_ids", {
  dt <- data.table(
    pip_id  = c("SUR_2001", "SUR_2002"),
    welfare = c(1, 2),
    weight  = c(1, 1)
  )
  expect_error(
    compute_measures(dt, measures = "mean"),
    regexp = "SUR_2001|SUR_2002"
  )
})

# ── 2. All 18 measures — aggregate ──────────────────────────────────────────

test_that("all 18 measures, 1 poverty line, no by → 18 rows", {
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt, measures = ALL_MEASURES, poverty_lines = 5.5)
  expect_equal(nrow(res), 18L)
})

test_that("all 18 measures, 1 poverty line → 18 unique measure names", {
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt, measures = ALL_MEASURES, poverty_lines = 5.5)
  expect_setequal(res$measure, ALL_MEASURES)
})

test_that("all 18 measures, 2 poverty lines → poverty rows duplicated per line", {
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt, measures = ALL_MEASURES, poverty_lines = ALL_PL)
  # 5 poverty measures × 2 lines = 10; 2 ineq + 11 welfare = 13
  expect_equal(nrow(res), 23L)
})

# ── 3. Only poverty measures ─────────────────────────────────────────────────

test_that("only poverty measures → no inequality or welfare rows", {
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt, measures = c("headcount", "poverty_gap"),
                          poverty_lines = 5.0)
  expect_true(all(res$measure %in% c("headcount", "poverty_gap")))
  expect_false(any(res$measure %in% c("gini", "mld", "mean")))
})

test_that("only poverty measures → all rows have non-NA poverty_line", {
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt, measures = c("headcount", "severity"),
                          poverty_lines = 3.0)
  expect_true(all(!is.na(res$poverty_line)))
})

# ── 4. Only inequality + welfare (no poverty) ────────────────────────────────

test_that("inequality + welfare only → poverty_line is NA for all rows", {
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt, measures = c("gini", "mean", "median"))
  expect_true(all(is.na(res$poverty_line)))
})

test_that("inequality + welfare, 13 measures → 13 rows aggregate", {
  ineq_welf <- ALL_MEASURES[ALL_MEASURES %in%
    c("gini", "mld",
      "mean", "median", "sd", "var", "min", "max", "nobs",
      "p10", "p25", "p75", "p90")]
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt, measures = ineq_welf)
  expect_equal(nrow(res), 13L)
})

# ── 5. Mixed poverty + inequality + welfare ──────────────────────────────────

test_that("mixed request → poverty rows have poverty_line, others NA", {
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt,
                          measures      = c("headcount", "gini", "mean"),
                          poverty_lines = 4.0)
  poverty_rows  <- res[measure == "headcount"]
  non_pov_rows  <- res[measure %in% c("gini", "mean")]
  expect_true(all(!is.na(poverty_rows$poverty_line)))
  expect_true(all(is.na(non_pov_rows$poverty_line)))
})

# ── 6. by = NULL — aggregate row counts ─────────────────────────────────────

test_that("by = NULL → 1 row per measure per poverty line (no group cols)", {
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt, measures = c("headcount", "gini", "mean"),
                          poverty_lines = 3.5)
  # 1 PL × 1 HC + 1 gini + 1 mean = 3 rows
  expect_equal(nrow(res), 3L)
  # No by columns beyond fixed schema
  expect_false("gender" %in% names(res))
  expect_false("area"   %in% names(res))
})

# ── 7. by = "gender" — grouped row counts ───────────────────────────────────

test_that("by = 'gender' (2 groups) → row count per measure × group", {
  dt  <- make_single_dt(10L, dims = "gender")
  res <- compute_measures(dt,
                          measures      = c("headcount", "gini", "mean"),
                          poverty_lines = 3.5,
                          by            = "gender")
  # headcount: 1 PL × 2 groups = 2; gini: 2; mean: 2 → 6 rows
  expect_equal(nrow(res), 6L)
  expect_true("gender" %in% names(res))
})

# ── 8. Poverty lines interact only with poverty measures ─────────────────────

test_that("poverty line count × poverty measure count is correct", {
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt,
                          measures      = c("headcount", "severity", "mean"),
                          poverty_lines = c(2.0, 4.0, 6.0))
  pov_rows   <- res[measure %in% c("headcount", "severity")]
  welf_rows  <- res[measure == "mean"]
  # 2 poverty measures × 3 PL = 6 poverty rows
  expect_equal(nrow(pov_rows),  6L)
  # 1 welfare measure, no PL dimension = 1 row
  expect_equal(nrow(welf_rows), 1L)
})

# ── 9. Output column names and types ────────────────────────────────────────

test_that("output has required columns: measure, value, population", {
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt, measures = c("mean", "gini"),
                          poverty_lines = NULL)
  expect_true(all(c("measure", "value", "population") %in% names(res)))
})

test_that("measure column is character (not factor)", {
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt, measures = c("mean", "gini"))
  expect_type(res$measure, "character")
})

test_that("value and population columns are numeric", {
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt, measures = c("mean", "gini"))
  expect_type(res$value,      "double")
  expect_type(res$population, "double")
})

test_that("poverty_line column is numeric (not integer)", {
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt, measures = c("headcount", "mean"),
                          poverty_lines = 3.5)
  expect_type(res$poverty_line, "double")
})

# ── 10. Poverty measures without poverty_lines → error ──────────────────────

test_that("poverty measures without poverty_lines errors", {
  dt <- make_single_dt(10L)
  expect_error(
    compute_measures(dt, measures = "headcount", poverty_lines = NULL),
    class = "rlang_error"
  )
})
