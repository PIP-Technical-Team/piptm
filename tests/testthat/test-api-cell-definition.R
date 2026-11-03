# Plan: .cg-docs/plans/2026-11-02-page2-cell-definition-endpoint.md (Phase 2, Step 5)
# Integration tests for POST /cell-definition using socket-free pr$call()

make_cell_def_req <- function(body = NULL, raw_str = NULL, raw_lines = NULL) {
  body_raw <- if (!is.null(raw_lines)) {
    charToRaw(paste(raw_lines, collapse = "\n"))
  } else if (!is.null(raw_str)) {
    charToRaw(raw_str)
  } else if (!is.null(body)) {
    charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE, null = "null"))
  } else {
    raw(0L)
  }

  req                <- new.env(parent = emptyenv())
  req$REQUEST_METHOD <- "POST"
  req$PATH_INFO      <- "/cell-definition"
  req$QUERY_STRING   <- ""
  req$HTTP_ACCEPT    <- "application/json"
  req$CONTENT_TYPE   <- if (length(body_raw) > 0L) "application/json" else ""
  req$CONTENT_LENGTH <- as.character(length(body_raw))
  req$HTTP_HOST      <- "localhost"
  req$rook.input     <- list(
    read_lines = function() if (!is.null(raw_lines)) raw_lines else rawToChar(body_raw),
    read       = function(l = -1L) body_raw,
    rewind     = function() invisible(NULL)
  )
  req$postBody       <- if (!is.null(raw_lines)) NULL else if (length(body_raw) > 0L) rawToChar(body_raw) else ""
  req
}

parse_cell_def_res <- function(res, simplify = FALSE) {
  body <- res$body
  if (is.raw(body)) body <- rawToChar(body)
  jsonlite::fromJSON(body, simplifyVector = simplify)
}

.get_cell_def_router <- function() {
  skip_if_not_installed("plumber")
  skip_if_not_installed("yaml")
  plumber_path <- tryCatch(
    file.path(rprojroot::find_package_root_file(), "inst", "plumber", "plumber.R"),
    error = function(e) ""
  )
  if (!nzchar(plumber_path) || !file.exists(plumber_path)) {
    plumber_path <- system.file("plumber", "plumber.R", package = "piptm")
  }
  suppressMessages(plumber::plumb(plumber_path))
}

.with_api_fixture_registry <- function(code) {
  ns <- asNamespace("piptm")
  env <- ns$.piptm_env
  old_regs  <- env$registries
  old_cur   <- env$current_release
  old_mans  <- env$manifests
  old_arrow <- env$arrow_root
  on.exit({
    env$registries      <- old_regs
    env$current_release <- old_cur
    env$manifests       <- old_mans
    env$arrow_root      <- old_arrow
  }, add = TRUE)

  fixture_path <- tryCatch(
    file.path(rprojroot::find_package_root_file(), "tests", "testthat", "fixtures", "registry-cell-prose-sample.json"),
    error = function(e) testthat::test_path("fixtures", "registry-cell-prose-sample.json")
  )
  fixture_reg <- jsonlite::read_json(fixture_path, simplifyVector = FALSE)
  fixture_reg[["_comment"]] <- NULL

  env$registries <- list("20260401_TEST" = fixture_reg)
  env$current_release <- "20260401_TEST"
  if (length(env$manifests) == 0L) {
    env$manifests <- list("20260401_TEST" = data.frame(pip_id = "ARG_2014_EPHC_INC_URB"))
  }
  env$arrow_root <- NULL

  force(code)
}

test_that("POST /cell-definition returns 200 for welfare, poverty, and share configurations", {
  pr <- .get_cell_def_router()

  .with_api_fixture_registry({
    # 1. Welfare summary + inequality with nested-object filter_base
    req1 <- make_cell_def_req(list(
      analysis_var = "welfare",
      measures     = c("mean", "median", "gini"),
      row          = "educat4",
      column       = "gender",
      superColumn  = "area",
      filter_base  = list(age_group = "3", educat7 = "4"),
      ppp          = 2021L,
      welfare_type = "INC"
    ))
    res1 <- pr$call(req1)
    expect_equal(res1$status, 200L)
    p1 <- parse_cell_def_res(res1)
    expect_equal(p1$status[[1L]], "success")
    expect_true(all(c("schema_version", "who_is_in_table", "what_a_cell_is", "what_is_shown", "notes") %in% names(p1$data)))
    expect_equal(p1$data$what_a_cell_is$example_cell_label[[1L]], "men with incomplete secondary education living in urban areas")
    expect_length(p1$data$what_is_shown$measures, 3L)

    # 2. Poverty measures with JSON-encoded string filter_base
    req2 <- make_cell_def_req(list(
      analysis_var = "pov_status",
      measures     = c("headcount", "poverty_gap"),
      column       = "gender",
      poverty_line = 2.15,
      filter_base  = '{"area":["0"]}'
    ))
    res2 <- pr$call(req2)
    expect_equal(res2$status, 200L)
    p2 <- parse_cell_def_res(res2)
    expect_equal(p2$status[[1L]], "success")
    expect_true(p2$data$who_is_in_table$filtered[[1L]])

    # 3. Share measures + by = NULL
    req3 <- make_cell_def_req(list(
      analysis_var = "employed",
      measures     = c("pop_share", "target_within_group_share", "target_survey_share")
    ))
    res3 <- pr$call(req3)
    expect_equal(res3$status, 200L)
    p3 <- parse_cell_def_res(res3)
    expect_equal(p3$status[[1L]], "success")
    expect_length(p3$data$what_a_cell_is$dimensions, 0L)
    expect_false(is.null(p3$data$what_is_shown$measures[[1L]]$counts))
    expect_false(is.null(p3$data$what_is_shown$measures[[1L]]$out_of))
    expect_false(is.null(p3$data$what_is_shown$measures[[1L]]$total_note))
  })
})

test_that("POST /cell-definition returns 200 with fallback warning when no release registry is loaded", {
  pr <- .get_cell_def_router()

  ns <- asNamespace("piptm")
  env <- ns$.piptm_env
  old_regs  <- env$registries
  old_cur   <- env$current_release
  old_arrow <- env$arrow_root
  old_env_arrow <- Sys.getenv("PIPTM_ARROW_ROOT", unset = NA_character_)
  on.exit({
    env$registries      <- old_regs
    env$current_release <- old_cur
    env$arrow_root      <- old_arrow
    if (is.na(old_env_arrow)) {
      Sys.unsetenv("PIPTM_ARROW_ROOT")
    } else {
      Sys.setenv(PIPTM_ARROW_ROOT = old_env_arrow)
    }
  }, add = TRUE)

  Sys.unsetenv("PIPTM_ARROW_ROOT")
  env$registries <- list()
  env$current_release <- NULL
  env$arrow_root <- NULL

  req <- make_cell_def_req(raw_lines = c(
    "{",
    '  "analysis_var": "welfare",',
    '  "measures": ["mean"],',
    '  "column": "gender"',
    "}"
  ))
  res <- pr$call(req)
  expect_equal(res$status, 200L)
  p <- parse_cell_def_res(res)
  expect_equal(p$status[[1L]], "success")
  expect_true(length(p$warnings) > 0L)
})

test_that("POST /cell-definition returns 400 for validation errors and 422 for unknown release", {
  pr <- .get_cell_def_router()

  .with_api_fixture_registry({
    # Empty body -> 400
    res_empty <- pr$call(make_cell_def_req(raw_str = ""))
    expect_equal(res_empty$status, 400L)

    # JSON array body -> 400
    res_arr <- pr$call(make_cell_def_req(raw_str = '[{"analysis_var":"welfare","measures":["mean"]}]'))
    expect_equal(res_arr$status, 400L)

    # Vector pop_share_threshold -> 400
    res_vec_thresh <- pr$call(make_cell_def_req(list(
      analysis_var        = "welfare",
      measures            = "mean",
      pop_share_threshold = c(0.001, 0.005)
    )))
    expect_equal(res_vec_thresh$status, 400L)

    # Target share with continuous analysis_var ("age") -> 400
    res_age_share <- pr$call(make_cell_def_req(list(
      analysis_var = "age",
      measures     = "target_survey_share"
    )))
    expect_equal(res_age_share$status, 400L)

    # Missing analysis_var / measures -> 400
    res_missing <- pr$call(make_cell_def_req(list(column = "gender")))
    expect_equal(res_missing$status, 400L)

    # Slot gap (row without column) -> 400
    res_gap <- pr$call(make_cell_def_req(list(
      analysis_var = "welfare",
      measures     = "mean",
      row          = "gender"
    )))
    expect_equal(res_gap$status, 400L)

    # Duplicate variable across slots -> 400
    res_dup <- pr$call(make_cell_def_req(list(
      analysis_var = "welfare",
      measures     = "mean",
      column       = "gender",
      row          = "gender"
    )))
    expect_equal(res_dup$status, 400L)

    # analysis_var also used in a slot -> 400
    res_avar_slot <- pr$call(make_cell_def_req(list(
      analysis_var = "pov_status",
      measures     = "headcount",
      poverty_line = 2.15,
      column       = "pov_status"
    )))
    expect_equal(res_avar_slot$status, 400L)

    # Missing poverty_line when headcount requested -> 400
    res_no_pl <- pr$call(make_cell_def_req(list(
      analysis_var = "pov_status",
      measures     = "headcount"
    )))
    expect_equal(res_no_pl$status, 400L)

    # Target share with analysis_var = "welfare" -> 400
    res_bad_share <- pr$call(make_cell_def_req(list(
      analysis_var = "welfare",
      measures     = "target_survey_share"
    )))
    expect_equal(res_bad_share$status, 400L)

    # Unknown release -> 422
    res_422 <- pr$call(make_cell_def_req(list(
      analysis_var = "welfare",
      measures     = "mean",
      release      = "BOGUS_RELEASE_9999"
    )))
    expect_equal(res_422$status, 422L)
    p_422 <- parse_cell_def_res(res_422)
    expect_equal(p_422$status[[1L]], "error")
  })
})
