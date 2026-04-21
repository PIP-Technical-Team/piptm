library(data.table)

# ── Fixture helpers ────────────────────────────────────────────────────────────

# Five-row dataset: welfare = 1:5, equal weights.
#
# Hand-computed expected values (collapse defaults, equal weights):
#   mean   = 3
#   median = 3
#   sd     = sqrt(2.5) ≈ 1.581139    (sample SD, n-1 denominator)
#   var    = 2.5                      (sample variance)
#   min    = 1
#   max    = 5
#   nobs   = 5
#   p10    = 1.4   (collapse::fnth linear interpolation)
#   p25    = 2
#   p75    = 4
#   p90    = 4.6

make_five_dt <- function() {
  data.table(welfare = as.numeric(1:5), weight = rep(1.0, 5L))
}

# ══════════════════════════════════════════════════════════════════════════════
# Input validation (s5-P1.1)
# ══════════════════════════════════════════════════════════════════════════════

test_that("missing welfare column produces informative error", {
  dt <- data.table(weight = rep(1, 3), x = 1:3)
  expect_error(compute_welfare(dt, measures = "mean"), "welfare")
})

test_that("missing weight column produces informative error", {
  dt <- data.table(welfare = as.numeric(1:3))
  expect_error(compute_welfare(dt, measures = "mean"), "weight")
})

# ══════════════════════════════════════════════════════════════════════════════
# mean
# ══════════════════════════════════════════════════════════════════════════════

test_that("mean: equal weights → arithmetic mean", {
  res <- compute_welfare(make_five_dt(), measures = "mean")
  expect_equal(res[measure == "mean", value], 3, tolerance = 1e-9)
})

test_that("mean: unequal weights shift result", {
  dt  <- data.table(welfare = c(1, 3), weight = c(3, 1))
  res <- compute_welfare(dt, measures = "mean")
  # wmean = (1*3 + 3*1) / (3+1) = 6/4 = 1.5
  expect_equal(res[measure == "mean", value], 1.5, tolerance = 1e-9)
})

# ══════════════════════════════════════════════════════════════════════════════
# median
# ══════════════════════════════════════════════════════════════════════════════

test_that("median: equal weights → middle value", {
  res <- compute_welfare(make_five_dt(), measures = "median")
  expect_equal(res[measure == "median", value], 3, tolerance = 1e-9)
})

test_that("median: unequal weights shift result", {
  # welfare = c(1,2,3), weight = c(10,1,1) → weighted median is 1
  dt  <- data.table(welfare = c(1, 2, 3), weight = c(10, 1, 1))
  res <- compute_welfare(dt, measures = "median")
  expect_equal(res[measure == "median", value], 1, tolerance = 1e-9)
})

# ══════════════════════════════════════════════════════════════════════════════
# sd and var
# ══════════════════════════════════════════════════════════════════════════════

test_that("sd: known 5-point distribution", {
  res <- compute_welfare(make_five_dt(), measures = "sd")
  expect_equal(res[measure == "sd", value], sqrt(2.5), tolerance = 1e-9)
})

test_that("var: known 5-point distribution", {
  res <- compute_welfare(make_five_dt(), measures = "var")
  expect_equal(res[measure == "var", value], 2.5, tolerance = 1e-9)
})

test_that("sd^2 == var (equal weights)", {
  dt   <- make_five_dt()
  sd_v <- compute_welfare(dt, measures = "sd")[measure == "sd",   value]
  va_v <- compute_welfare(dt, measures = "var")[measure == "var", value]
  expect_equal(sd_v^2, va_v, tolerance = 1e-9)
})

test_that("sd^2 == var (unequal weights)", {
  dt   <- data.table(welfare = as.numeric(1:6), weight = c(2, 1, 3, 1, 2, 1))
  sd_v <- compute_welfare(dt, measures = "sd")[measure == "sd",   value]
  va_v <- compute_welfare(dt, measures = "var")[measure == "var", value]
  expect_equal(sd_v^2, va_v, tolerance = 1e-9)
})

test_that("sd: perfect equality → 0", {
  dt  <- data.table(welfare = c(4, 4, 4), weight = rep(1, 3))
  res <- compute_welfare(dt, measures = "sd")
  expect_equal(res[measure == "sd", value], 0, tolerance = 1e-9)
})

# ══════════════════════════════════════════════════════════════════════════════
# min and max
# ══════════════════════════════════════════════════════════════════════════════

test_that("min: returns observed minimum", {
  res <- compute_welfare(make_five_dt(), measures = "min")
  expect_equal(res[measure == "min", value], 1)
})

test_that("max: returns observed maximum", {
  res <- compute_welfare(make_five_dt(), measures = "max")
  expect_equal(res[measure == "max", value], 5)
})

test_that("min/max: single observation", {
  dt  <- data.table(welfare = 7.5, weight = 1)
  res <- compute_welfare(dt, measures = c("min", "max"))
  expect_equal(res[measure == "min", value], 7.5)
  expect_equal(res[measure == "max", value], 7.5)
})

# ══════════════════════════════════════════════════════════════════════════════
# nobs
# ══════════════════════════════════════════════════════════════════════════════

test_that("nobs: unweighted row count", {
  res <- compute_welfare(make_five_dt(), measures = "nobs")
  expect_equal(res[measure == "nobs", value], 5)
})

test_that("nobs: unaffected by weights", {
  # weights vary but row count is still 3
  dt  <- data.table(welfare = c(1, 2, 3), weight = c(100, 1, 1))
  res <- compute_welfare(dt, measures = "nobs")
  expect_equal(res[measure == "nobs", value], 3)
})

# ══════════════════════════════════════════════════════════════════════════════
# Percentiles (p10, p25, p75, p90)
# ══════════════════════════════════════════════════════════════════════════════

test_that("percentiles: known values for 1:5 equal weights", {
  res <- compute_welfare(make_five_dt(), measures = c("p10", "p25", "p75", "p90"))
  # Hand-verified via collapse::fnth with equal weights
  expect_equal(res[measure == "p10", value], 1.4,  tolerance = 1e-9)
  expect_equal(res[measure == "p25", value], 2.0,  tolerance = 1e-9)
  expect_equal(res[measure == "p75", value], 4.0,  tolerance = 1e-9)
  expect_equal(res[measure == "p90", value], 4.6,  tolerance = 1e-9)
})

test_that("weighted p25 differs from unweighted when weights concentrate mass", {
  # Very high weight on the first observation pulls p25 toward 1
  dt_equal   <- data.table(welfare = as.numeric(1:5), weight = rep(1, 5))
  dt_skewed  <- data.table(welfare = as.numeric(1:5), weight = c(4, 1, 1, 1, 1))
  p25_equal  <- compute_welfare(dt_equal,  measures = "p25")[measure == "p25", value]
  p25_skewed <- compute_welfare(dt_skewed, measures = "p25")[measure == "p25", value]
  expect_false(isTRUE(all.equal(p25_equal, p25_skewed)))
  expect_lt(p25_skewed, p25_equal)   # heavier weight on 1 pulls p25 down
})

# ══════════════════════════════════════════════════════════════════════════════
# Grouped computation
# ══════════════════════════════════════════════════════════════════════════════

test_that("grouped mean and median: correct per group", {
  # factor levels set explicitly → GRP returns female-first
  # female: welfare 1, 3 → mean 2, median 2
  # male:   welfare 2, 4 → mean 3, median 3
  dt <- data.table(
    welfare = as.numeric(1:4),
    weight  = rep(1, 4),
    gender  = factor(c("female", "male", "female", "male"),
                     levels = c("female", "male"))
  )
  res <- compute_welfare(dt, by = "gender", measures = c("mean", "median"))
  expect_equal(res[gender == "female" & measure == "mean",   value], 2,
               tolerance = 1e-9)
  expect_equal(res[gender == "male"   & measure == "mean",   value], 3,
               tolerance = 1e-9)
  expect_equal(res[gender == "female" & measure == "median", value], 2,
               tolerance = 1e-9)
  expect_equal(res[gender == "male"   & measure == "median", value], 3,
               tolerance = 1e-9)
})

test_that("grouped sd^2 == var", {
  dt <- data.table(
    welfare = as.numeric(1:6),
    weight  = rep(1, 6),
    area    = factor(rep(c("urban", "rural"), 3), levels = c("urban", "rural"))
  )
  sd_res  <- compute_welfare(dt, by = "area", measures = "sd")
  var_res <- compute_welfare(dt, by = "area", measures = "var")
  sd_v  <- sd_res[order(area),  value]
  var_v <- var_res[order(area), value]
  expect_equal(sd_v^2, var_v, tolerance = 1e-9)
})

test_that("grouped min/max: correct per group", {
  dt <- data.table(
    welfare = c(1, 3, 5, 2, 4, 6),
    weight  = rep(1, 6),
    area    = factor(rep(c("urban", "rural"), 3), levels = c("urban", "rural"))
  )
  res <- compute_welfare(dt, by = "area", measures = c("min", "max"))
  # urban: welfare 1, 5, 4 → min=1, max=5
  # rural: welfare 3, 2, 6 → min=2, max=6
  expect_equal(res[area == "urban" & measure == "min", value], 1)
  expect_equal(res[area == "urban" & measure == "max", value], 5)
  expect_equal(res[area == "rural" & measure == "min", value], 2)
  expect_equal(res[area == "rural" & measure == "max", value], 6)
})

test_that("grouped nobs: counts rows per group", {
  dt <- data.table(
    welfare = as.numeric(1:6),
    weight  = c(10, 10, 10, 1, 1, 1),   # weights should not affect nobs
    area    = factor(c("urban", "urban", "urban", "rural", "rural", "rural"),
                     levels = c("urban", "rural"))
  )
  res <- compute_welfare(dt, by = "area", measures = "nobs")
  expect_equal(res[area == "urban" & measure == "nobs", value], 3)
  expect_equal(res[area == "rural" & measure == "nobs", value], 3)
})

# ══════════════════════════════════════════════════════════════════════════════
# Multiple dimensions
# ══════════════════════════════════════════════════════════════════════════════

test_that("two-dimension cross-tabulation produces correct row count", {
  dt <- data.table(
    welfare = as.numeric(1:8),
    weight  = rep(1, 8),
    gender  = factor(rep(c("male", "female"), 4),     levels = c("male", "female")),
    area    = factor(rep(c("urban", "rural"), each = 4), levels = c("urban", "rural"))
  )
  res <- compute_welfare(dt, by = c("gender", "area"), measures = c("mean", "median"))
  # 2 genders × 2 areas × 2 measures = 8 rows
  expect_equal(nrow(res), 8L)
})

# ══════════════════════════════════════════════════════════════════════════════
# measures subset
# ══════════════════════════════════════════════════════════════════════════════

test_that("measures subset: only 'mean' and 'p90' returned", {
  res <- compute_welfare(make_five_dt(), measures = c("mean", "p90"))
  expect_setequal(res[["measure"]], c("mean", "p90"))
})

test_that("measures = NULL computes all 11 measures", {
  res <- compute_welfare(make_five_dt())
  expect_setequal(
    res[["measure"]],
    c("mean", "median", "sd", "var", "min", "max", "nobs",
      "p10", "p25", "p75", "p90")
  )
})

test_that("duplicate measure names are silently deduplicated", {
  res_dup    <- compute_welfare(make_five_dt(), measures = c("mean", "mean"))
  res_single <- compute_welfare(make_five_dt(), measures = "mean")
  expect_equal(nrow(res_dup), nrow(res_single))
  expect_equal(sum(res_dup[["measure"]] == "mean"), 1L)
})

# ══════════════════════════════════════════════════════════════════════════════
# Output format
# ══════════════════════════════════════════════════════════════════════════════

test_that("output columns correct (no by)", {
  res <- compute_welfare(make_five_dt(), measures = "mean")
  expect_named(res, c("measure", "value", "population"), ignore.order = TRUE)
})

test_that("output columns correct with by = 'area'", {
  dt <- data.table(
    welfare = as.numeric(1:4),
    weight  = rep(1, 4),
    area    = factor(rep(c("urban", "rural"), 2), levels = c("urban", "rural"))
  )
  res <- compute_welfare(dt, by = "area", measures = "mean")
  expect_true("area"       %in% names(res))
  expect_true("measure"    %in% names(res))
  expect_true("value"      %in% names(res))
  expect_true("population" %in% names(res))
})

test_that("population column reflects weighted group totals", {
  dt <- data.table(
    welfare = as.numeric(1:4),
    weight  = c(1, 2, 3, 4),
    area    = factor(c("urban", "urban", "rural", "rural"), levels = c("urban", "rural"))
  )
  res <- compute_welfare(dt, by = "area", measures = "mean")
  expect_equal(res[area == "urban" & measure == "mean", population], 3)
  expect_equal(res[area == "rural" & measure == "mean", population], 7)
})

test_that("measure column is character (not factor)", {
  res <- compute_welfare(make_five_dt(), measures = c("mean", "sd"))
  expect_type(res[["measure"]], "character")
})

# ══════════════════════════════════════════════════════════════════════════════
# Pre-computed GRP
# ══════════════════════════════════════════════════════════════════════════════

test_that("pre-computed grp produces same result as internally-built grp", {
  dt  <- make_five_dt()
  dt[, gender := factor(rep(c("male", "female"), length.out = 5L),
                        levels = c("male", "female"))]
  grp <- collapse::GRP(dt, by = "gender")

  res_with    <- compute_welfare(dt, by = "gender", measures = c("mean", "median"), grp = grp)
  res_without <- compute_welfare(dt, by = "gender", measures = c("mean", "median"))

  expect_equal(
    res_with[order(gender, measure),    value],
    res_without[order(gender, measure), value],
    tolerance = 1e-9
  )
})
