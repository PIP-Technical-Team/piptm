library(data.table)
library(arrow)

# ── Fixture helpers ────────────────────────────────────────────────────────────

# See helper-fixtures.R: reset_piptm_env_tm() is the single shared
# implementation; kept as an alias here so existing call sites are unaffected.
reset_piptm_env_sup <- reset_piptm_env_tm

#' Fixture with strongly unequal cell weights.
#'
#' Thin wrapper around the shared `write_fixture_nr_masking_survey()` helper
#' (see helper-fixtures.R) that also allocates the temp dirs and returns them
#' for `activate_sup_fixture()`.
#'
#' gender is stored as an integer 0/1 so it doubles as a binary analysis_var
#' for shares-family measures.
make_sup_fixture <- function(env = parent.frame()) {
  tmp_arrow    <- withr::local_tempdir(.local_envir = env)
  tmp_manifest <- withr::local_tempdir(.local_envir = env)

  write_fixture_nr_masking_survey(tmp_arrow, tmp_manifest)

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
    "Reporting"
  )

  # Both cells remain; the small cell is masked rather than dropped.
  expect_equal(nrow(res), 2L)
  expect_equal(res$gender, c(0L, 1L))
  expect_equal(res$measure, c("mean", "mean"))
  expect_true(all(c("value", "flag") %in% names(res)))
  expect_true(is.na(res[gender == 0L, value]))
  expect_equal(res[gender == 0L, flag], "NR")
  expect_false(is.na(res[gender == 1L, value]))
  expect_true(is.na(res[gender == 1L, flag]))

  # pop_share must NOT appear in output (was not requested by caller)
  expect_false("pop_share" %in% res$measure)
})

test_that("all groups masked when threshold exceeds every cell's pop_share (all-cells-suppressed edge case)", {
  fx <- make_sup_fixture()
  activate_sup_fixture(fx)
  withr::defer(reset_piptm_env_sup())

  # gender=0 pop_share ~ 9.999e-6; gender=1 pop_share ~ 0.999990. A threshold
  # above both (but still < 1) masks every group in this two-group survey —
  # no row survives unmasked. This guards against any code path (e.g.
  # setcolorder/column-ordering logic) that implicitly assumed at least one
  # surviving unmasked row.
  expect_warning(
    res <- piptm::table_maker(
      pip_id              = "TST_2020_ECH_INC_ALL",
      analysis_var        = "welfare",
      measures            = "mean",
      by                  = "gender",
      ppp                 = 2021L,
      pop_share_threshold = 0.9999999
    ),
    "Reporting"
  )

  # Rows are retained (not dropped) for every group.
  expect_equal(nrow(res), 2L)
  expect_equal(res$gender, c(0L, 1L))
  expect_true(all(c("value", "flag") %in% names(res)))
  expect_true(all(is.na(res$value)))
  expect_true(all(res$flag == "NR"))
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
  expect_true(all(is.na(res$flag)))
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

test_that("all measures are masked for suppressed cell when explicitly requested", {
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
    "Reporting"
  )

  # gender=0 (suppressed): both measures remain and are masked
  g0 <- res[res$gender == 0L, ]
  expect_true("pop_share" %in% g0$measure)
  expect_true("mean" %in% g0$measure)
  expect_true(all(is.na(g0$value)))
  expect_true(all(g0$flag == "NR"))

  # gender=1 (not suppressed): both measures present
  expect_equal(nrow(res[res$gender == 1L, ]), 2L)
})

test_that("share-family measures are masked, not exempted, for suppressed cells", {
  # Regression for the share_measures <- 'pop_share' hardcoding bug:
  # target_survey_share (and any future share-family measure) must be
  # masked like every other measure when the cell is below the threshold —
  # share-family measures are no longer exempted from suppression (see
  # NEWS.md "Breaking changes").
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
    "Reporting"
  )

  # gender=0 (suppressed cell): BOTH share-family rows are masked, not dropped
  g0 <- res[res$gender == 0L, ]
  expect_true("pop_share"          %in% g0$measure)
  expect_true("target_survey_share" %in% g0$measure)
  expect_true(all(is.na(g0$value)))
  expect_true(all(g0$flag == "NR"))
})

test_that("captured suppression warnings surfaced via include_metadata are plain text (no ANSI escapes)", {
  # Regression test: cli_warn() formats its message using ANSI escape
  # sequences (bold/color spans, "i" bullet glyph) when the ambient session
  # is detected as color-capable. table_maker()'s withCallingHandlers()
  # captures conditionMessage(w) verbatim, so those escape codes leak into
  # execution$warnings and are ultimately rendered in the /description
  # endpoint's plain-text warning payload as garbled characters.
  #
  # Expected behavior source (package-convention): warnings surfaced through
  # description metadata are plain human-readable text with no ANSI/terminal
  # escape sequences, since the description endpoint is consumed by non-
  # terminal clients (JSON/text API response, PIP platform UI).
  fx <- make_sup_fixture()
  activate_sup_fixture(fx)
  withr::defer(reset_piptm_env_sup())

  # Force cli to emit ANSI styling regardless of the test runner's terminal,
  # matching what happens when the plumber server process has color support
  # detected (e.g. via NO_COLOR unset / a colorized parent shell).
  withr::local_options(cli.num_colors = 256L)

  out <- piptm::table_maker(
    pip_id              = "TST_2020_ECH_INC_ALL",
    analysis_var        = "welfare",
    measures            = "mean",
    by                  = "gender",
    ppp                 = 2021L,
    pop_share_threshold = 0.01,
    include_metadata    = TRUE
  )

  warnings_out <- out$description_metadata$execution$warnings
  expect_true(length(warnings_out) > 0L)

  # ANSI CSI escape sequences look like ESC [ ... <letter>, e.g. "\033[1m".
  ansi_pattern <- "\u001b\\[[0-9;]*[a-zA-Z]"
  expect_false(
    any(grepl(ansi_pattern, warnings_out)),
    info = paste0("Warning text contains raw ANSI escape codes: ",
                   paste(warnings_out, collapse = " | "))
  )
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
  expect_false("population" %in% names(res))
  expect_true("flag" %in% names(res))
  expect_true(is.na(res$flag))
})

test_that("threshold adds flag and removes population from table_maker output", {
  fx <- make_sup_fixture()
  activate_sup_fixture(fx)
  withr::defer(reset_piptm_env_sup())

  res <- piptm::table_maker(
    pip_id = "TST_2020_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean",
    by = "gender",
    ppp = 2021L,
    pop_share_threshold = NULL
  )

  expect_false("population" %in% names(res))
  expect_type(res$value, "double")
  expect_type(res$flag, "character")
  expect_true(all(is.na(res$flag)))
})

test_that("NA dimension key cell is masked when below threshold", {
  tmp_arrow    <- withr::local_tempdir()
  tmp_manifest <- withr::local_tempdir()

  dt_raw <- data.table(
    country_code           = "TST",
    surveyid_year          = 2020L,
    welfare_type           = "INC",
    version                = "v01_v01",
    pip_id                 = "TST_2020_ECH_INC_ALL",
    welfare_ppp_2021_01_02 = c(1.0, 2.0, 3.0),
    weight                 = c(0.0001, 100, 100),
    gender                 = c(NA_integer_, 0L, 1L)
  )
  dir_path <- file.path(tmp_arrow, "country_code=TST", "surveyid_year=2020",
                        "welfare_type=INC", "version=v01_v01")
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
  piptm::set_manifest_dir(tmp_manifest)
  piptm::set_arrow_root(tmp_arrow)
  withr::defer(reset_piptm_env_sup())

  expect_warning(
    res <- piptm::table_maker(
      pip_id              = "TST_2020_ECH_INC_ALL",
      analysis_var        = "welfare",
      measures            = "mean",
      by                  = "gender",
      ppp                 = 2021L,
      pop_share_threshold = 0.001
    ),
    "Reporting"
  )

  expect_equal(nrow(res), 3L)
  na_row <- res[is.na(gender)]
  expect_equal(nrow(na_row), 1L)
  expect_true(is.na(na_row$value))
  expect_equal(na_row$flag, "NR")
})

test_that("multi-dimension by with independent NA keys masks only the intended cells (no cross-contamination)", {
  tmp_arrow    <- withr::local_tempdir()
  tmp_manifest <- withr::local_tempdir()

  # Three disjoint cells across two dimensions:
  #  - gender=0,    area=NA   (small weight -> masked)
  #  - gender=NA,   area=0    (small weight -> masked, independently of the above)
  #  - gender=1,    area=1    (large weight -> unmasked)
  # Both masked cells carry an NA in a *different* dimension column. A
  # regression in the multi-column NA-join used for masking could either
  # under-mask (miss one of them) or conflate them into a single masked
  # group; this test pins the correct, independent behavior.
  dt_raw <- data.table(
    country_code           = "TST",
    surveyid_year          = 2020L,
    welfare_type           = "INC",
    version                = "v01_v01",
    pip_id                 = "TST_2020_ECH_INC_ALL",
    welfare_ppp_2021_01_02 = c(rep(1.0, 5L), rep(2.0, 5L), rep(3.0, 5L)),
    weight                 = c(rep(0.01, 5L), rep(0.01, 5L), rep(1000, 5L)),
    gender                 = c(rep(0L, 5L), rep(NA_integer_, 5L), rep(1L, 5L)),
    area                   = c(rep(NA_integer_, 5L), rep(0L, 5L), rep(1L, 5L))
  )
  dir_path <- file.path(tmp_arrow, "country_code=TST", "surveyid_year=2020",
                        "welfare_type=INC", "version=v01_v01")
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
    dimensions     = list("gender", "area"),
    welfare_vars   = list("welfare_ppp_2021_01_02"),
    ppp_sort       = 2021L
  ))
  write_fixture_manifest_tm(tmp_manifest, "20260401_TEST", entries)
  piptm::set_manifest_dir(tmp_manifest)
  piptm::set_arrow_root(tmp_arrow)
  withr::defer(reset_piptm_env_sup())

  expect_warning(
    res <- piptm::table_maker(
      pip_id              = "TST_2020_ECH_INC_ALL",
      analysis_var        = "welfare",
      measures            = "mean",
      by                  = c("gender", "area"),
      ppp                 = 2021L,
      pop_share_threshold = 0.001
    ),
    "Reporting"
  )

  expect_equal(nrow(res), 3L)

  cell_a <- res[gender == 0L & is.na(area)]
  cell_b <- res[is.na(gender) & area == 0L]
  cell_c <- res[gender == 1L & area == 1L]

  expect_equal(nrow(cell_a), 1L)
  expect_equal(nrow(cell_b), 1L)
  expect_equal(nrow(cell_c), 1L)

  expect_true(is.na(cell_a$value))
  expect_equal(cell_a$flag, "NR")

  expect_true(is.na(cell_b$value))
  expect_equal(cell_b$flag, "NR")

  expect_false(is.na(cell_c$value))
  expect_true(is.na(cell_c$flag))
})
