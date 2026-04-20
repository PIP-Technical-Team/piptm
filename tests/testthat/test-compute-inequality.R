library(testthat)
library(data.table)

# ── Fixture helpers ────────────────────────────────────────────────────────────

# Five-row dataset, welfare = 1:5, equal weights
# Hand-computed Gini for [1,2,3,4,5] equal weights:
#
#   Lorenz curve with (0,0) prepended:
#     cum_w  = c(0, 0.2, 0.4, 0.6, 0.8, 1.0)          (equal weights)
#     cum_wy = c(0, 1,3,6,10,15)/15
#   B = sum_{i=1..5} diff(cum_w)[i] * (cum_wy[i] + cum_wy[i+1]) / 2
#     = 0.2 * sum(c(0+1, 1+3, 3+6, 6+10, 10+15)/15 / 2)
#     = 0.2 * sum(c(1, 4, 9, 16, 25) / 30)
#     = 0.2 * (55/30) = 11/30
#   Gini = 1 - 2*(11/30) = 8/30 = 4/15 ≈ 0.2667
EXPECTED_GINI_5 <- 1 - 2 * (0.2 * sum(c(1, 4, 9, 16, 25) / 30))
# = 1 - 2 * (11/30) = 4/15 ≈ 0.2667

# Hand-computed MLD for [1,2,3,4,5] equal weights:
#   weighted mean = 3
#   MLD = mean(log(3/y)) for y in 1:5
#       = (log(3)+log(3/2)+log(1)+log(3/4)+log(3/5)) / 5
#       = (1.098612 + 0.405465 + 0 - 0.287682 - 0.510826) / 5
#       = 0.705569 / 5 = 0.141114
EXPECTED_MLD_5 <- mean(log(3 / (1:5)))

make_five_dt <- function() {
  data.table(welfare = as.numeric(1:5), weight = rep(1.0, 5L))
}

# ══════════════════════════════════════════════════════════════════════════════
# .gini_sorted() — internal helper
# ══════════════════════════════════════════════════════════════════════════════

test_that(".gini_sorted: perfect equality → 0", {
  expect_equal(piptm:::.gini_sorted(c(5, 5, 5, 5), c(1, 1, 1, 1)), 0,
               tolerance = 1e-9)
})

test_that(".gini_sorted: one person holds all welfare → near 1", {
  # n = 4, maximum Gini ≈ (n-1)/n = 0.75
  g <- piptm:::.gini_sorted(c(0, 0, 0, 4), c(1, 1, 1, 1))
  expect_equal(g, 0.75, tolerance = 1e-9)
})

test_that(".gini_sorted: known 5-point distribution", {
  g <- piptm:::.gini_sorted(1:5, rep(1, 5))
  expect_equal(g, EXPECTED_GINI_5, tolerance = 1e-9)
})

test_that(".gini_sorted: single observation → 0", {
  expect_equal(piptm:::.gini_sorted(10, 1), 0)
})

test_that(".gini_sorted: weighted case differs from unweighted", {
  # Equal welfare but unequal weights — Gini still 0
  expect_equal(piptm:::.gini_sorted(c(3, 3, 3), c(1, 2, 3)), 0,
               tolerance = 1e-9)
  # Unequal welfare, unequal weights
  g_unw <- piptm:::.gini_sorted(1:5, rep(1, 5))
  g_w   <- piptm:::.gini_sorted(1:5, c(5, 4, 3, 2, 1))
  expect_false(isTRUE(all.equal(g_unw, g_w)))
})

test_that(".gini_sorted: all-zero welfare → NA (P0.1 guard)", {
  expect_true(is.na(piptm:::.gini_sorted(c(0, 0, 0), c(1, 1, 1))))
})

# ══════════════════════════════════════════════════════════════════════════════
# compute_inequality() — Gini
# ══════════════════════════════════════════════════════════════════════════════

test_that("gini: perfect equality → 0", {
  dt  <- data.table(welfare = c(5, 5, 5, 5), weight = rep(1, 4))
  res <- compute_inequality(dt, measures = "gini")
  expect_equal(res[measure == "gini", value], 0, tolerance = 1e-9)
})

test_that("gini: known 5-point distribution", {
  res <- compute_inequality(make_five_dt(), measures = "gini")
  expect_equal(res[measure == "gini", value], EXPECTED_GINI_5, tolerance = 1e-9)
})

test_that("gini: zero-welfare rows handled (no NaN)", {
  dt  <- data.table(welfare = c(0, 1, 2, 3), weight = rep(1, 4))
  res <- compute_inequality(dt, measures = "gini")
  expect_true(is.finite(res[measure == "gini", value]))
})

test_that("gini: grouped computation", {
  dt <- data.table(
    welfare = c(1, 2, 3, 4),
    weight  = rep(1, 4),
    gender  = factor(c("male", "male", "female", "female"),
                     levels = c("male", "female"))
  )
  res <- compute_inequality(dt, by = "gender", measures = "gini")
  # Within each group: both groups have 2 identical-gap values
  # male   [1,2]: Gini = .gini_sorted(1:2, c(1,1))
  # female [3,4]: same Gini (ratio is same)
  male_g   <- piptm:::.gini_sorted(c(1, 2), c(1, 1))
  female_g <- piptm:::.gini_sorted(c(3, 4), c(1, 1))
  expect_equal(res[gender == "male"   & measure == "gini", value], male_g,
               tolerance = 1e-9)
  expect_equal(res[gender == "female" & measure == "gini", value], female_g,
               tolerance = 1e-9)
})

# ══════════════════════════════════════════════════════════════════════════════
# compute_inequality() — MLD
# ══════════════════════════════════════════════════════════════════════════════

test_that("mld: perfect equality → 0", {
  dt  <- data.table(welfare = c(4, 4, 4, 4), weight = rep(1, 4))
  res <- compute_inequality(dt, measures = "mld")
  expect_equal(res[measure == "mld", value], 0, tolerance = 1e-9)
})

test_that("mld: known 5-point distribution", {
  res <- compute_inequality(make_five_dt(), measures = "mld")
  expect_equal(res[measure == "mld", value], EXPECTED_MLD_5, tolerance = 1e-9)
})

test_that("mld: zero-welfare rows contribute 0 (no Inf/NaN)", {
  dt  <- data.table(welfare = c(0, 2, 4), weight = rep(1, 3))
  res <- compute_inequality(dt, measures = "mld")
  # wmean = (0+2+4)/3 = 2
  # log_ratio: welfare=0 → 0 (zero-welfare rule); welfare=2 → log(2/2)=0;
  #            welfare=4 → log(2/4) = log(0.5)
  # MLD = mean(c(0, 0, log(0.5))) = log(0.5)/3 ≈ -0.231
  # Negative MLD is expected: the zero-welfare rule replaces log(μ/0)=+Inf
  # with 0, suppressing the poorest individual's contribution and pulling
  # the index below zero.
  expected_mld <- mean(c(0, log(2 / 2), log(2 / 4)))
  expect_equal(res[measure == "mld", value], expected_mld, tolerance = 1e-9)
})

test_that("mld: grouped computation", {
  dt <- data.table(
    welfare = c(1, 3, 2, 6),
    weight  = rep(1, 4),
    area    = factor(c("urban", "urban", "rural", "rural"),
                     levels = c("urban", "rural"))
  )
  res <- compute_inequality(dt, by = "area", measures = "mld")

  # urban: welfare = [1, 3], wmean = 2
  #   MLD = mean(log(2/1), log(2/3)) = mean(0.693, -0.405) = 0.144
  urban_mld <- mean(c(log(2 / 1), log(2 / 3)))
  # rural: welfare = [2, 6], wmean = 4
  #   MLD = mean(log(4/2), log(4/6)) = mean(0.693, -0.405) = 0.144
  rural_mld <- mean(c(log(4 / 2), log(4 / 6)))

  expect_equal(res[area == "urban" & measure == "mld", value], urban_mld,
               tolerance = 1e-9)
  expect_equal(res[area == "rural" & measure == "mld", value], rural_mld,
               tolerance = 1e-9)
})

# ══════════════════════════════════════════════════════════════════════════════
# compute_inequality() — joint / structural tests
# ══════════════════════════════════════════════════════════════════════════════

test_that("measures = NULL computes both gini and mld", {
  res <- compute_inequality(make_five_dt())
  expect_setequal(res[["measure"]], c("gini", "mld"))
})

test_that("measures subset: only 'gini' returns one measure", {
  res <- compute_inequality(make_five_dt(), measures = "gini")
  expect_equal(unique(res[["measure"]]), "gini")
})

test_that("measures subset: only 'mld' returns one measure", {
  res <- compute_inequality(make_five_dt(), measures = "mld")
  expect_equal(unique(res[["measure"]]), "mld")
})

test_that("output columns are correct (no by)", {
  res <- compute_inequality(make_five_dt())
  expect_named(res, c("measure", "value", "population"), ignore.order = TRUE)
})

test_that("output columns correct with by = 'gender'", {
  dt <- data.table(
    welfare = as.numeric(1:6),
    weight  = rep(1, 6),
    gender  = factor(rep(c("male", "female"), 3), levels = c("male", "female"))
  )
  res <- compute_inequality(dt, by = "gender")
  expect_true("gender" %in% names(res))
  expect_true("measure" %in% names(res))
  expect_true("value"   %in% names(res))
  expect_true("population" %in% names(res))
})

test_that("population column reflects weighted group totals", {
  dt <- data.table(
    welfare = as.numeric(1:4),
    weight  = c(1, 2, 3, 4),
    area    = factor(c("urban", "urban", "rural", "rural"),
                     levels = c("urban", "rural"))
  )
  res <- compute_inequality(dt, by = "area")
  expect_equal(res[area == "urban" & measure == "gini", population], 3)
  expect_equal(res[area == "rural" & measure == "gini", population], 7)
})

test_that("two-dimension cross-tabulation produces correct row count", {
  dt <- data.table(
    welfare = as.numeric(1:8),
    weight  = rep(1, 8),
    gender  = factor(rep(c("male", "female"), 4), levels = c("male", "female")),
    area    = factor(rep(c("urban", "rural"), each = 4), levels = c("urban", "rural"))
  )
  res <- compute_inequality(dt, by = c("gender", "area"))
  # 2 genders × 2 areas × 2 measures = 8 rows
  expect_equal(nrow(res), 8L)
})

test_that("pre-computed grp is accepted and produces same result", {
  dt  <- make_five_dt()
  dt[, gender := factor(rep(c("male", "female"), length.out = 5L),
                        levels = c("male", "female"))]
  grp <- collapse::GRP(dt, by = "gender")

  res_with    <- compute_inequality(dt, by = "gender", grp = grp)
  res_without <- compute_inequality(dt, by = "gender")

  expect_equal(res_with[order(gender, measure), value],
               res_without[order(gender, measure), value],
               tolerance = 1e-9)
})

# ═══════════════════════════════════════════════════════════════════════════════
# Additional edge cases (plan spec + review findings)
# ═══════════════════════════════════════════════════════════════════════════════

test_that("gini: all-zero welfare group → NA (P0.1)", {
  dt  <- data.table(welfare = c(0, 0, 0), weight = rep(1, 3))
  res <- compute_inequality(dt, measures = "gini")
  expect_true(is.na(res[measure == "gini", value]))
})

test_that("gini: single-observation group → 0 for that group (P2.5)", {
  # urban has 1 row — .gini_sorted returns 0 for n=1
  dt <- data.table(
    welfare = c(5, 1, 2),
    weight  = rep(1, 3),
    area    = factor(c("urban", "rural", "rural"), levels = c("urban", "rural"))
  )
  res <- compute_inequality(dt, by = "area", measures = "gini")
  expect_equal(res[area == "urban" & measure == "gini", value], 0)
})

test_that("na in by column does not error (P2.6)", {
  dt <- data.table(
    welfare = as.numeric(1:4),
    weight  = rep(1, 4),
    gender  = factor(c("male", NA, "female", "male"), levels = c("male", "female"))
  )
  # collapse::GRP treats NA as a separate group — must not error
  expect_no_error(compute_inequality(dt, by = "gender"))
})

test_that("duplicate measure names are silently deduplicated (P3.3)", {
  res_dup    <- compute_inequality(make_five_dt(), measures = c("gini", "gini"))
  res_single <- compute_inequality(make_five_dt(), measures = "gini")
  expect_equal(nrow(res_dup), nrow(res_single))
  expect_equal(sum(res_dup[["measure"]] == "gini"), 1L)
})
