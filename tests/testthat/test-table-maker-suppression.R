library(data.table)
library(arrow)

# ── Fixture helpers ────────────────────────────────────────────────────────────

reset_piptm_env_sup <- function() {
  env <- piptm:::.piptm_env
  env$manifest_dir    <- NULL
  env$arrow_root      <- NULL
  env$manifests       <- list()
  env$current_release <- NULL
}

#' Fixture with strongly unequal cell weights.
#'
#' gender = 0 (male):  10 rows × weight 0.01  → total weight ≈ 0.10
#' gender = 1 (female): 10 rows × weight 1000 → total weight = 10000
#' gender=0 pop_share ≈ 0.001 %  (well below the default 1 % threshold)
#' gender=1 pop_share ≈ 99.999 %
#'
#' gender is stored as an integer 0/1 so it doubles as a binary analysis_var
#' for shares-family measures.
make_sup_fixture <- function(env = parent.frame()) {
  tmp_arrow    <- withr::local_tempdir(.local_envir = env)
  tmp_manifest <- withr::local_tempdir(.local_envir = env)

  n <- 20L
  dt_raw <- data.table(
    country_code           = "TST",
    surveyid_year          = 2020L,
    welfare_type           = "INC",
    version                = "v01_v01",
    pip_id                 = "TST_2020_ECH_INC_ALL",
    welfare_ppp_2021_01_02 = seq(1, by = 0.5, length.out = n),
    weight                 = rep_len(c(0.01, 1000), n),
    gender                 = rep_len(c(0L, 1L), n)
  )

  dir_path <- file.path(
    tmp_arrow,
    "country_code=TST",
    "surveyid_year=2020",
    "welfare_type=INC",
    "version=v01_v01"
  )
  dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(dt_raw, file.path(dir_path, "data.parquet"))

  entries <- list(list(
    pip_id         = "TST_2020_ECH_INC_ALL",
    survey_id      = "S1",
    country_code   = "TST",
    year           = 2020L,
    welfare_type   = "INC",
    version        = "v01_v01",
    survey_acronym = "ECH",
    module         = "ALL",
    dimensions     = list("gender"),
    welfare_vars   = list("welfare_ppp_2021_01_02"),
    ppp_sort       = 2021L
  ))

  write_fixture_manifest_tm(tmp_manifest, "20260401_TEST", entries)
  list(tmp_arrow = tmp_arrow, tmp_manifest = tmp_manifest)
}

activate_sup_fixture <- function(fx) {
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
}

# ── Tests ──────────────────────────────────────────────────────────────────────

test_that("threshold fires without pop_share in measures (primary bug fix)", {
  fx <- make_sup_fixture()
  activate_sup_fixture(fx)
  withr::defer(reset_piptm_env_sup())

  # pop_share is NOT requested — threshold must still suppress the tiny cell
  expect_warning(
    res <- piptm::table_maker(
      pip_id              = "TST_2020_ECH_INC_ALL",
      analysis_var        = "welfare",
      measures            = "mean",
      by                  = "gender",
      ppp                 = 2021L,
      pop_share_threshold = 0.01
    ),
    "Suppressing"
  )

  # Only the large cell (gender=1) survives
  expect_equal(nrow(res), 1L)
  expect_equal(res$gender, 1L)
  expect_equal(res$measure, "mean")

  # pop_share must NOT appear in output (was not requested by caller)
  expect_false("pop_share" %in% res$measure)
})

test_that("pop_share_threshold = NULL disables suppression entirely", {
  fx <- make_sup_fixture()
  activate_sup_fixture(fx)
  withr::defer(reset_piptm_env_sup())

  expect_no_warning(
    res <- piptm::table_maker(
      pip_id              = "TST_2020_ECH_INC_ALL",
      analysis_var        = "welfare",
      measures            = "mean",
      by                  = "gender",
      ppp                 = 2021L,
      pop_share_threshold = NULL
    )
  )

  # Both gender cells present
  expect_equal(nrow(res), 2L)
})

test_that("invalid threshold is rejected even when pop_share not in measures", {
  fx <- make_sup_fixture()
  activate_sup_fixture(fx)
  withr::defer(reset_piptm_env_sup())

  expect_error(
    piptm::table_maker(
      pip_id              = "TST_2020_ECH_INC_ALL",
      analysis_var        = "welfare",
      measures            = "mean",         # no pop_share
      by                  = "gender",
      ppp                 = 2021L,
      pop_share_threshold = -0.5            # invalid
    ),
    "pop_share_threshold"
  )
})

test_that("pop_share row retained for suppressed cell when explicitly requested", {
  fx <- make_sup_fixture()
  activate_sup_fixture(fx)
  withr::defer(reset_piptm_env_sup())

  expect_warning(
    res <- piptm::table_maker(
      pip_id              = "TST_2020_ECH_INC_ALL",
      analysis_var        = "welfare",
      measures            = c("mean", "pop_share"),
      by                  = "gender",
      ppp                 = 2021L,
      pop_share_threshold = 0.01
    ),
    "Suppressing"
  )

  # gender=0 (suppressed): pop_share retained, mean dropped
  g0 <- res[res$gender == 0L, ]
  expect_true("pop_share" %in% g0$measure)
  expect_false("mean"     %in% g0$measure)

  # gender=1 (not suppressed): both measures present
  expect_equal(nrow(res[res$gender == 1L, ]), 2L)
})

test_that("all share-family measures retained for suppressed cells", {
  # Regression for the share_measures <- 'pop_share' hardcoding bug:
  # target_survey_share (and any future share-family measure) must be
  # retained even when the cell is below the threshold.
  fx <- make_sup_fixture()
  activate_sup_fixture(fx)
  withr::defer(reset_piptm_env_sup())

  # gender (0/1 integer) acts as binary analysis_var for shares computation
  expect_warning(
    res <- piptm::table_maker(
      pip_id              = "TST_2020_ECH_INC_ALL",
      analysis_var        = "gender",
      measures            = c("pop_share", "target_survey_share"),
      by                  = "gender",
      ppp                 = 2021L,
      pop_share_threshold = 0.01
    ),
    "Suppressing"
  )

  # gender=0 (suppressed cell): BOTH share-family rows must be retained
  g0 <- res[res$gender == 0L, ]
  expect_true("pop_share"          %in% g0$measure)
  expect_true("target_survey_share" %in% g0$measure)
})

test_that("threshold has no effect when by = NULL (aggregate mode)", {
  fx <- make_sup_fixture()
  activate_sup_fixture(fx)
  withr::defer(reset_piptm_env_sup())

  # Very aggressive threshold — still no suppression because each "cell" is
  # the entire survey (pop_share = 1.0)
  expect_no_warning(
    res <- piptm::table_maker(
      pip_id              = "TST_2020_ECH_INC_ALL",
      analysis_var        = "welfare",
      measures            = "mean",
      by                  = NULL,
      ppp                 = 2021L,
      pop_share_threshold = 0.99
    )
  )

  expect_equal(nrow(res), 1L)
  expect_equal(res$measure, "mean")
})
