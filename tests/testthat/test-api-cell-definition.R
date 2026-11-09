# Plan: .cg-docs/plans/2026-11-04-cell-definition-get-only.md (Phase 2, Step 3)
# Integration tests for GET /cell-definition using socket-free pr$call()

make_cell_def_get_req <- function(params = list()) {
  # Build a query string from a named list, supporting vector values as repeated keys
  # (e.g. measures = c("mean", "gini") -> measures=mean&measures=gini)
  n_keys <- length(params)
  if (n_keys == 0L) {
    qs <- ""
  } else {
    qs_parts <- vector("list", n_keys)
    idx <- 0L
    for (nm in names(params)) {
      vals <- params[[nm]]
      if (is.null(vals)) next
      vals_chr <- as.character(vals)
      vals_chr <- vals_chr[!is.na(vals_chr)]
      if (length(vals_chr) == 0L) next
      enc_vals <- vapply(
        vals_chr,
        function(v) utils::URLencode(v, reserved = TRUE),
        character(1L)
      )
      idx <- idx + 1L
      qs_parts[[idx]] <- paste0(nm, "=", enc_vals)
    }
    if (idx == 0L) {
      qs <- ""
    } else {
      qs <- paste(unlist(qs_parts[seq_len(idx)], use.names = FALSE), collapse = "&")
    }
  }

  req                <- new.env(parent = emptyenv())
  req$REQUEST_METHOD <- "GET"
  req$PATH_INFO      <- "/cell-definition"
  req$QUERY_STRING   <- qs
  req$HTTP_ACCEPT    <- "application/json"
  req$CONTENT_TYPE   <- ""
  req$CONTENT_LENGTH <- "0"
  req$HTTP_HOST      <- "localhost"
  req$rook.input     <- list(
    read_lines = function() "",
    read       = function(l = -1L) raw(0L),
    rewind     = function() invisible(NULL)
  )
  req$postBody       <- ""
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

.CELL_DEF_ROUTER <- NULL

get_cell_def_router <- function() {
  if (is.null(.CELL_DEF_ROUTER)) {
    .CELL_DEF_ROUTER <<- .get_cell_def_router()
  }
  .CELL_DEF_ROUTER
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
  env$manifests[["20260401_TEST"]] <- data.frame(pip_id = "ARG_2014_EPHC_INC_URB")
  env$arrow_root <- NULL

  force(code)
}

test_that("GET /cell-definition returns 200 for welfare, poverty, and share configurations", {
  pr <- get_cell_def_router()

  .with_api_fixture_registry({
    # 1. Welfare summary + inequality with JSON-string filter_base
    req1 <- make_cell_def_get_req(list(
      analysis_var = "welfare",
      measures     = c("mean", "median", "gini"),
      row          = "educat4",
      column       = "gender",
      superColumn  = "area",
      filter_base  = '{"age_group":["3"],"educat7":["4"]}',
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
    req2 <- make_cell_def_get_req(list(
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
    req3 <- make_cell_def_get_req(list(
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

test_that("GET /cell-definition returns 200 with fallback warning when no release registry is loaded", {
  pr <- get_cell_def_router()

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

  req <- make_cell_def_get_req(list(
    analysis_var = "welfare",
    measures     = "mean",
    column       = "gender"
  ))
  res <- pr$call(req)
  expect_equal(res$status, 200L)
  p <- parse_cell_def_res(res)
  expect_equal(p$status[[1L]], "success")
  expect_true(length(p$warnings) > 0L)
})

test_that("GET /cell-definition returns 400 for validation errors and 422 for unknown release", {
  pr <- get_cell_def_router()

  .with_api_fixture_registry({
    # Empty query -> 400
    res_empty <- pr$call(make_cell_def_get_req(list()))
    expect_equal(res_empty$status, 400L)
    expect_equal(parse_cell_def_res(res_empty)$status[[1L]], "error")

    # Vector pop_share_threshold -> 400
    res_vec_thresh <- pr$call(make_cell_def_get_req(list(
      analysis_var        = "welfare",
      measures            = "mean",
      pop_share_threshold = c("0.001", "0.005")
    )))
    expect_equal(res_vec_thresh$status, 400L)
    expect_true(any(grepl("pop_share_threshold", parse_cell_def_res(res_vec_thresh)$errors)))

    # Target share with continuous analysis_var ("age") -> 400
    res_age_share <- pr$call(make_cell_def_get_req(list(
      analysis_var = "age",
      measures     = "target_survey_share"
    )))
    expect_equal(res_age_share$status, 400L)
    expect_true(any(grepl("binary target", parse_cell_def_res(res_age_share)$errors, ignore.case = TRUE)))

    # Missing analysis_var / measures -> 400
    res_missing <- pr$call(make_cell_def_get_req(list(column = "gender")))
    expect_equal(res_missing$status, 400L)
    expect_true(any(grepl("analysis_var", parse_cell_def_res(res_missing)$errors)))

    # Slot gap (row without column) -> 400
    res_gap <- pr$call(make_cell_def_get_req(list(
      analysis_var = "welfare",
      measures     = "mean",
      row          = "gender"
    )))
    expect_equal(res_gap$status, 400L)
    expect_true(any(grepl("Layout slots must be filled in order", parse_cell_def_res(res_gap)$errors)))

    # Duplicate variable across slots -> 400
    res_dup <- pr$call(make_cell_def_get_req(list(
      analysis_var = "welfare",
      measures     = "mean",
      column       = "gender",
      row          = "gender"
    )))
    expect_equal(res_dup$status, 400L)
    expect_true(any(grepl("assigned to more than one slot", parse_cell_def_res(res_dup)$errors)))

    # analysis_var also used in a slot -> 400
    res_avar_slot <- pr$call(make_cell_def_get_req(list(
      analysis_var = "pov_status",
      measures     = "headcount",
      poverty_line = 2.15,
      column       = "pov_status"
    )))
    expect_equal(res_avar_slot$status, 400L)
    expect_true(any(grepl("cannot also be used as a disaggregation", parse_cell_def_res(res_avar_slot)$errors)))

    # Missing poverty_line when headcount requested -> 400
    res_no_pl <- pr$call(make_cell_def_get_req(list(
      analysis_var = "pov_status",
      measures     = "headcount"
    )))
    expect_equal(res_no_pl$status, 400L)
    expect_true(any(grepl("poverty_line", parse_cell_def_res(res_no_pl)$errors)))

    # Target share with analysis_var = "welfare" -> 400
    res_bad_share <- pr$call(make_cell_def_get_req(list(
      analysis_var = "welfare",
      measures     = "target_survey_share"
    )))
    expect_equal(res_bad_share$status, 400L)
    expect_true(any(grepl("binary target", parse_cell_def_res(res_bad_share)$errors, ignore.case = TRUE)))

    # Malformed filter_base JSON string -> 400
    res_bad_fb <- pr$call(make_cell_def_get_req(list(
      analysis_var = "welfare",
      measures     = "mean",
      filter_base  = "{not-valid-json"
    )))
    expect_equal(res_bad_fb$status, 400L)
    expect_true(any(grepl("filter_base", parse_cell_def_res(res_bad_fb)$errors)))

    # Invalid welfare_type -> 400
    res_bad_wt <- pr$call(make_cell_def_get_req(list(
      analysis_var = "welfare",
      measures     = "mean",
      welfare_type = "INVALID"
    )))
    expect_equal(res_bad_wt$status, 400L)
    expect_true(any(grepl("welfare_type", parse_cell_def_res(res_bad_wt)$errors)))

    # Empty filter_base is treated as no filter -> 200
    res_empty_fb <- pr$call(make_cell_def_get_req(list(
      analysis_var = "welfare",
      measures     = "mean",
      column       = "gender",
      filter_base  = ""
    )))
    expect_equal(res_empty_fb$status, 200L)
    p_empty_fb <- parse_cell_def_res(res_empty_fb)
    expect_equal(p_empty_fb$status[[1L]], "success")
    expect_false(p_empty_fb$data$who_is_in_table$filtered[[1L]])

    # Empty pop_share_threshold disables suppression note -> 200
    res_empty_thresh <- pr$call(make_cell_def_get_req(list(
      analysis_var        = "employed",
      measures            = "pop_share",
      column              = "gender",
      pop_share_threshold = ""
    )))
    expect_equal(res_empty_thresh$status, 200L)
    p_empty_thresh <- parse_cell_def_res(res_empty_thresh)
    expect_equal(p_empty_thresh$status[[1L]], "success")
    note_vals <- unlist(p_empty_thresh$data$notes, use.names = FALSE)
    expect_false(any(grepl("shown as \\\"NR\\\"", note_vals, fixed = TRUE)))

    # Unknown release -> 422
    res_422 <- pr$call(make_cell_def_get_req(list(
      analysis_var = "welfare",
      measures     = "mean",
      release      = "BOGUS_RELEASE_9999"
    )))
    expect_equal(res_422$status, 422L)
    p_422 <- parse_cell_def_res(res_422)
    expect_equal(p_422$status[[1L]], "error")
  })
})

test_that("POST /cell-definition is not routed (returns 405)", {
  pr <- get_cell_def_router()
  req <- make_cell_def_get_req(list(analysis_var = "welfare", measures = "mean"))
  req$REQUEST_METHOD <- "POST"
  res <- pr$call(req)
  expect_equal(res$status, 405L)
})

test_that("GET /cell-definition returns what_a_cell_is$items as a JSON array for 0/1/4/2-item cases", {
  pr <- get_cell_def_router()

  .with_api_fixture_registry({
    # (a) 0 slots -> items is an empty JSON array []
    res0 <- pr$call(make_cell_def_get_req(list(
      analysis_var = "welfare",
      measures     = "mean"
    )))
    expect_equal(res0$status, 200L)
    p0 <- parse_cell_def_res(res0)
    expect_type(p0$data$what_a_cell_is$items, "list")
    expect_length(p0$data$what_a_cell_is$items, 0L)
    # Raw JSON shows an array, not null/scalar
    body0 <- res0$body
    if (is.raw(body0)) body0 <- rawToChar(body0)
    expect_match(body0, "\"items\"\\s*:\\s*\\[", perl = TRUE)

    # (b) 1-slot educat4 (5 cats) -> >4 mode, single breakdown line as a 1-element array
    res1 <- pr$call(make_cell_def_get_req(list(
      analysis_var = "welfare",
      measures     = "mean",
      column       = "educat4"
    )))
    expect_equal(res1$status, 200L)
    p1 <- parse_cell_def_res(res1)
    expect_type(p1$data$what_a_cell_is$items, "list")
    expect_length(p1$data$what_a_cell_is$items, 1L)
    body1 <- res1$body
    if (is.raw(body1)) body1 <- rawToChar(body1)
    # Single-element items must remain a JSON array, not an unboxed string
    expect_match(body1, "\"items\"\\s*:\\s*\\[", perl = TRUE)
    expect_false(grepl("\"items\"\\s*:\\s*\"", body1, perl = TRUE))

    # (c) 2x2 area x gender -> 4-element enumeration array
    res4 <- pr$call(make_cell_def_get_req(list(
      analysis_var = "welfare",
      measures     = "mean",
      row          = "area",
      column       = "gender"
    )))
    expect_equal(res4$status, 200L)
    p4 <- parse_cell_def_res(res4)
    expect_type(p4$data$what_a_cell_is$items, "list")
    expect_length(p4$data$what_a_cell_is$items, 4L)

    # (d) educat4 (5) x gender (2) = 10 cells -> 2 per-slot breakdown lines
    res2 <- pr$call(make_cell_def_get_req(list(
      analysis_var = "welfare",
      measures     = "mean",
      row          = "educat4",
      column       = "gender"
    )))
    expect_equal(res2$status, 200L)
    p2 <- parse_cell_def_res(res2)
    expect_type(p2$data$what_a_cell_is$items, "list")
    expect_length(p2$data$what_a_cell_is$items, 2L)
  })
})
