# Unit tests for R/errors.R (piptm_abort and piptm_user_error_message)

test_that("piptm_abort() raises piptm_error with intact developer conditionMessage() and embedded user_message", {
  cnd <- tryCatch(
    piptm_abort(
      c(
        "No matching surveys found in manifest for release {.val 20260206}.",
        "i" = "Requested pip_id: {.val COL_2099_FAKE_INC_ALL}"
      ),
      user_message = "The requested country or survey is not available in the selected data release."
    ),
    error = function(e) e
  )

  expect_s3_class(cnd, "piptm_error")
  expect_s3_class(cnd, "rlang_error")
  expect_match(conditionMessage(cnd), "No matching surveys found in manifest")
  expect_match(conditionMessage(cnd), "COL_2099_FAKE_INC_ALL")
  expect_identical(
    piptm_user_error_message(cnd),
    "The requested country or survey is not available in the selected data release."
  )
})

test_that("piptm_abort() preserves custom condition subclasses ahead of piptm_error", {
  cnd <- tryCatch(
    piptm_abort(
      "Developer failure message.",
      user_message = "User-safe failure message.",
      class = "piptm_custom_subclass"
    ),
    error = function(e) e
  )

  expect_s3_class(cnd, "piptm_custom_subclass")
  expect_s3_class(cnd, "piptm_error")
  expect_identical(piptm_user_error_message(cnd), "User-safe failure message.")
})

test_that("piptm_user_error_message() returns fallback for unmapped or non-condition inputs and strips ANSI", {
  stop_cnd <- tryCatch(stop("secret internal path /srv/arrow"), error = function(e) e)
  cli_cnd  <- tryCatch(cli::cli_abort("raw internal {.arg dt} error"), error = function(e) e)
  null_cnd <- tryCatch(piptm_abort("dev msg", user_message = NULL), error = function(e) e)
  ansi_cnd <- tryCatch(
    piptm_abort("dev msg", user_message = "  \033[31mClean user sentence.\033[39m  "),
    error = function(e) e
  )

  expect_identical(piptm_user_error_message(stop_cnd), .PIPTM_GENERIC_USER_ERROR)
  expect_identical(piptm_user_error_message(cli_cnd),  .PIPTM_GENERIC_USER_ERROR)
  expect_identical(piptm_user_error_message(null_cnd), .PIPTM_GENERIC_USER_ERROR)
  expect_identical(piptm_user_error_message(NULL), .PIPTM_GENERIC_USER_ERROR)
  expect_identical(piptm_user_error_message("not_a_condition"), .PIPTM_GENERIC_USER_ERROR)
  expect_identical(
    piptm_user_error_message(stop_cnd, fallback = .PIPTM_DOWNLOAD_PAYLOAD_USER_ERROR),
    .PIPTM_DOWNLOAD_PAYLOAD_USER_ERROR
  )
  expect_identical(piptm_user_error_message(ansi_cnd), "Clean user sentence.")
  expect_identical(.PIPTM_GENERIC_USER_ERROR, "Something went wrong processing this request.")
})

test_that("piptm_abort() validates user_message when provided", {
  expect_error(
    piptm_abort("dev", user_message = ""),
    "user_message"
  )
  expect_error(
    piptm_abort("dev", user_message = "   "),
    "user_message"
  )
  expect_error(
    piptm_abort("dev", user_message = "\033[31m   \033[39m"),
    "user_message"
  )
  expect_error(
    piptm_abort("dev", user_message = NA_character_),
    "user_message"
  )
  expect_error(
    piptm_abort("dev", user_message = c("a", "b")),
    "user_message"
  )
  expect_error(
    piptm_abort("dev", user_message = 123),
    "user_message"
  )
})

# ── Step 3 & 4: Migrated high-traffic sites across R/ modules ────────────────

test_that("pip_lookup() aborts carry user_message while preserving developer conditionMessage()", {
  cnd_len <- tryCatch(
    pip_lookup(c("COL", "BOL"), 2019L, c("INC", "CON")),
    error = function(e) e
  )
  expect_s3_class(cnd_len, "piptm_error")
  expect_match(conditionMessage(cnd_len), "must all have the same length")
  expect_identical(
    piptm_user_error_message(cnd_len),
    "Country, year, and welfare type selections must have the same number of entries."
  )

  cnd_yr <- tryCatch(
    pip_lookup("COL", 2019.5, "INC"),
    error = function(e) e
  )
  expect_s3_class(cnd_yr, "piptm_error")
  expect_match(conditionMessage(cnd_yr), "whole-number years")
  expect_identical(
    piptm_user_error_message(cnd_yr),
    .PIPTM_SURVEY_YEAR_USER_ERROR
  )
})

test_that("table_maker() empty pip_id and filter_base errors carry user_message", {
  cnd_empty <- tryCatch(
    table_maker(pip_id = character(0), analysis_var = "welfare", measures = "mean"),
    error = function(e) e
  )
  expect_s3_class(cnd_empty, "piptm_error")
  expect_match(conditionMessage(cnd_empty), "No surveys to process")
  expect_identical(
    piptm_user_error_message(cnd_empty),
    "No surveys matched your selection. Please select at least one available country and survey year."
  )

  cnd_fb_empty <- tryCatch(
    table_maker(pip_id = "COL_2010_ECH_INC_ALL", analysis_var = "welfare", measures = "mean", filter_base = list()),
    error = function(e) e
  )
  expect_s3_class(cnd_fb_empty, "piptm_error")
  expect_identical(
    piptm_user_error_message(cnd_fb_empty),
    .PIPTM_SAMPLE_FILTER_UNREADABLE_USER_ERROR
  )

  testthat::with_mocked_bindings(
    piptm_filter_categories = function(...) list(list(varname = "area"), list(varname = "gender")),
    .package = "piptm",
    {
      cnd_fb_var <- tryCatch(
        table_maker(pip_id = "COL_2010_ECH_INC_ALL", analysis_var = "welfare", measures = "mean", filter_base = list(not_a_var = 1L)),
        error = function(e) e
      )
      expect_s3_class(cnd_fb_var, "piptm_error")
      expect_identical(
        piptm_user_error_message(cnd_fb_var),
        "One or more selected sample filters are not valid for this data release."
      )

      cnd_fb_val <- tryCatch(
        table_maker(pip_id = "COL_2010_ECH_INC_ALL", analysis_var = "welfare", measures = "mean", filter_base = list(area = "not_an_int")),
        error = function(e) e
      )
      expect_s3_class(cnd_fb_val, "piptm_error")
      expect_identical(
        piptm_user_error_message(cnd_fb_val),
        .PIPTM_SAMPLE_FILTER_VALUES_USER_ERROR
      )

      cnd_fb_inf <- tryCatch(
        table_maker(pip_id = "COL_2010_ECH_INC_ALL", analysis_var = "welfare", measures = "mean", filter_base = list(area = Inf)),
        error = function(e) e
      )
      expect_s3_class(cnd_fb_inf, "piptm_error")
      expect_identical(
        piptm_user_error_message(cnd_fb_inf),
        .PIPTM_SAMPLE_FILTER_VALUES_USER_ERROR
      )
    }
  )
})

test_that("measures.R, manifest.R, and load_survey_microdata() aborts carry expected user_message", {
  cnd_meas <- tryCatch(
    .classify_measures("not_a_measure"),
    error = function(e) e
  )
  expect_s3_class(cnd_meas, "piptm_error")
  expect_identical(
    piptm_user_error_message(cnd_meas),
    "One or more requested statistics are not recognized."
  )

  testthat::with_mocked_bindings(
    piptm_layout_covariates = function(...) {
      list(
        list(varname = "area"),
        list(varname = "gender"),
        list(varname = "educ_level"),
        list(varname = "age_group"),
        list(varname = "pov_status")
      )
    },
    .package = "piptm",
    {
      cnd_by_unknown <- tryCatch(.validate_by("unknown_dim"), error = function(e) e)
      expect_s3_class(cnd_by_unknown, "piptm_error")
      expect_identical(
        piptm_user_error_message(cnd_by_unknown),
        "One or more requested breakdown variables are not available for the selected release."
      )

      cnd_by_many <- tryCatch(
        .validate_by(c("area", "gender", "educ_level", "age_group", "pov_status")),
        error = function(e) e
      )
      expect_s3_class(cnd_by_many, "piptm_error")
      expect_identical(
        piptm_user_error_message(cnd_by_many),
        "At most 4 breakdown variables can be used in a single table."
      )
    }
  )

  cnd_pl <- tryCatch(.validate_poverty_lines(poverty_lines = -1, families = "poverty"), error = function(e) e)
  expect_s3_class(cnd_pl, "piptm_error")
  expect_identical(piptm_user_error_message(cnd_pl), .PIPTM_POVERTY_LINE_USER_ERROR)

  cnd_rel <- tryCatch(piptm_manifest("MISSING_REL_999"), error = function(e) e)
  expect_s3_class(cnd_rel, "piptm_error")
  expect_identical(
    piptm_user_error_message(cnd_rel),
    "The requested data release ('MISSING_REL_999') is not available."
  )

  testthat::with_mocked_bindings(
    piptm_current_release = function() NULL,
    .package = "piptm",
    {
      cnd_no_rel <- tryCatch(load_survey_microdata("COL", 2019L, "INC"), error = function(e) e)
      expect_s3_class(cnd_no_rel, "piptm_error")
      expect_identical(piptm_user_error_message(cnd_no_rel), .PIPTM_NO_ACTIVE_RELEASE_USER_ERROR)
    }
  )
})
