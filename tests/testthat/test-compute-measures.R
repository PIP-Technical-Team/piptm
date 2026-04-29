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

make_multi_dt <- function(n_surveys = 3L, n_rows = 10L, dims = character(0L)) {
  rbindlist(lapply(seq_len(n_surveys), function(i) {
    sdt <- data.table(
      pip_id  = sprintf("TST_2020_S%d", i),
      welfare = seq(i, by = 1, length.out = n_rows),
      weight  = rep(1.0, n_rows)
    )
    if ("gender" %in% dims) {
      sdt[, gender := factor(
        rep(c("male", "female"), length.out = n_rows),
        levels = c("male", "female")
      )]
    }
    sdt
  }))
}


ALL_MEASURES <- names(pip_measures())
ALL_PL       <- c(3.5, 6.5)

# Derived counts — update automatically if the registry grows.
n_all  <- length(ALL_MEASURES)             # 18: 5 pov + 2 ineq + 11 welfare
n_pov  <- sum(pip_measures() == "poverty") # 5

# ── 1. Multi-survey capability ──────────────────────────────────────────────

test_that("multi-survey: output always contains pip_id column", {
  dt  <- make_multi_dt(3L)
  res <- compute_measures(dt, measures = "mean")
  expect_true("pip_id" %in% names(res))
})

test_that("multi-survey: output has one row per survey per measure (by = NULL)", {
  dt  <- make_multi_dt(3L)
  res <- compute_measures(dt, measures = c("mean", "gini"))
  # 3 surveys × 2 measures = 6 rows
  expect_equal(nrow(res), 6L)
  expect_equal(uniqueN(res$pip_id), 3L)
})

test_that("multi-survey values match per-survey lapply approach", {
  dt      <- make_multi_dt(3L)
  meas    <- c("mean", "gini", "headcount")
  pl      <- 3.5

  # Reference: original per-survey lapply approach
  ref <- rbindlist(lapply(unique(dt$pip_id), function(pid) {
    sdt <- dt[pip_id == pid]
    res <- rbindlist(list(
      compute_welfare(sdt,  by = NULL, measures = "mean"),
      compute_inequality(sdt, by = NULL, measures = "gini"),
      compute_poverty(sdt,  poverty_lines = pl, by = NULL, measures = "headcount")
    ), fill = TRUE)
    res[, pip_id := pid]
    res
  }), fill = TRUE)

  # New multi-survey call
  result <- compute_measures(dt, measures = meas, poverty_lines = pl)

  # Compare values after normalising to common sort order
  ref[is.na(poverty_line), poverty_line := NA_real_]
  key <- c("pip_id", "measure")
  setkeyv(ref,    key)
  setkeyv(result, key)

  expect_equal(
    result[, .(pip_id, measure, value)],
    ref[,    .(pip_id, measure, value)],
    tolerance = 1e-10
  )
})

# ── 2. All 18 measures — aggregate ──────────────────────────────────────────

test_that("all 18 measures, 1 poverty line, no by → n_all rows", {
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt, measures = ALL_MEASURES, poverty_lines = 5.5)
  expect_equal(nrow(res), n_all)
})

test_that("all 18 measures, 1 poverty line → 18 unique measure names", {
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt, measures = ALL_MEASURES, poverty_lines = 5.5)
  expect_setequal(res$measure, ALL_MEASURES)
})

test_that("all 18 measures, 2 poverty lines → poverty rows duplicated per line", {
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt, measures = ALL_MEASURES, poverty_lines = ALL_PL)
  # n_pov measures × 2 lines = 2*n_pov; remaining n_all-n_pov measures = 1 row each
  expect_equal(nrow(res), n_all + n_pov)
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

test_that("inequality + welfare only → poverty_line column present and all NA", {
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt, measures = c("gini", "mean", "median"))
  # Column must exist (guaranteed by compute_measures regardless of families run)
  expect_true("poverty_line" %in% names(res))
  # all(is.na(NULL)) is vacuously TRUE — use sum() for a diagnostic failure message
  expect_equal(sum(!is.na(res$poverty_line)), 0L)
})

test_that("inequality + welfare, n_all-n_pov measures → n_all-n_pov rows aggregate", {
  # Build dynamically so the test stays correct if the registry grows.
  ineq_welf <- ALL_MEASURES[pip_measures()[ALL_MEASURES] != "poverty"]
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt, measures = ineq_welf)
  expect_equal(nrow(res), n_all - n_pov)
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
  expect_equal(sum(!is.na(non_pov_rows$poverty_line)), 0L)
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

# ── 11. Numerical correctness — dispatch must not corrupt values ─────────────

test_that("welfare mean via compute_measures matches compute_welfare directly", {
  dt     <- make_single_dt(10L)
  direct <- compute_welfare(dt, measures = "mean")$value
  via_cm <- compute_measures(dt, measures = "mean")$value
  expect_equal(via_cm, direct)
})

test_that("gini via compute_measures matches compute_inequality directly", {
  dt     <- make_single_dt(10L)
  direct <- compute_inequality(dt, measures = "gini")$value
  via_cm <- compute_measures(dt, measures = "gini")$value
  expect_equal(via_cm, direct)
})

test_that("headcount via compute_measures matches compute_poverty directly", {
  dt     <- make_single_dt(10L)
  direct <- compute_poverty(dt, poverty_lines = 5.5, measures = "headcount")$value
  via_cm <- compute_measures(dt, measures = "headcount", poverty_lines = 5.5)$value
  expect_equal(via_cm, direct)
})

# ── 12. Duplicate measure names → no duplicate output rows ───────────────────

test_that("duplicate measure names produce no duplicate rows", {
  dt  <- make_single_dt(10L)
  res <- compute_measures(dt, measures = c("mean", "mean", "gini"))
  expect_equal(nrow(res), 2L)
})
