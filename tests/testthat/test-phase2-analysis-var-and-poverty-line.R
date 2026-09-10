helpers_path <- system.file("plumber", "helpers.R", package = "piptm")
if (!nzchar(helpers_path)) {
  helpers_path <- file.path(
    rprojroot::find_package_root_file(), "inst", "plumber", "helpers.R"
  )
}
source(helpers_path)

make_phase2_req <- function(method = "GET", path = "/", query = list()) {
  qs <- if (length(query) > 0L) {
    parts <- unlist(lapply(names(query), function(k) {
      paste0(k, "=", as.character(query[[k]]))
    }))
    paste(parts, collapse = "&")
  } else {
    ""
  }

  req <- new.env(parent = emptyenv())
  req$REQUEST_METHOD <- toupper(method)
  req$PATH_INFO <- path
  req$QUERY_STRING <- qs
  req$HTTP_ACCEPT <- "application/json"
  req$CONTENT_TYPE <- ""
  req$CONTENT_LENGTH <- "0"
  req$HTTP_HOST <- "localhost"
  req$rook.input <- list(
    read_lines = function() "",
    read = function(l = -1L) raw(0L),
    rewind = function() invisible(NULL)
  )
  req
}

parse_phase2_res <- function(res) {
  body <- res$body
  if (is.raw(body)) body <- rawToChar(body)
  jsonlite::fromJSON(body, simplifyVector = TRUE)
}

.phase2_plumber_path <- system.file("plumber", "plumber.R", package = "piptm")
if (!nzchar(.phase2_plumber_path)) {
  .phase2_plumber_path <- file.path(
    rprojroot::find_package_root_file(), "inst", "plumber", "plumber.R"
  )
}

.phase2_router <- if (requireNamespace("plumber", quietly = TRUE)) {
  suppressMessages(plumber::plumb(.phase2_plumber_path))
} else {
  NULL
}

test_that("phase2: validate_table_input() requires analysis_var", {
  result <- validate_table_input(
    pip_id = "COL_2010_ECH_INC_ALL",
    measures = "mean"
  )

  expect_false(result$valid)
  expect_true(any(grepl("analysis_var", result$errors, ignore.case = TRUE)))
})

test_that("phase2: validate_table_input() requires poverty_line for analysis_var='pov_status'", {
  result <- validate_table_input(
    analysis_var = "pov_status",
    pip_id = "COL_2010_ECH_INC_ALL",
    measures = "headcount"
  )

  expect_false(result$valid)
  expect_true(any(grepl("poverty_line", result$errors, ignore.case = TRUE)))
})

test_that("phase2: validate_table_input() requires poverty_line when by includes pov_status", {
  result <- validate_table_input(
    analysis_var = "welfare",
    pip_id = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    by = "pov_status"
  )

  expect_false(result$valid)
  expect_true(any(grepl("poverty_line", result$errors, ignore.case = TRUE)))
})

test_that("phase2: GET /table rejects missing analysis_var with 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.phase2_router), "Router could not be created")

  res <- .phase2_router$call(make_phase2_req("GET", "/table", query = list(
    pip_id = "COL_2010_ECH_INC_ALL",
    measures = "mean"
  )))

  expect_equal(res$status, 400L)
  body <- parse_phase2_res(res)
  expect_equal(body$status, "error")
  expect_true(any(grepl("analysis_var", body$errors, ignore.case = TRUE)))
})

test_that("phase2: GET /table rejects unknown analysis_var with 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.phase2_router), "Router could not be created")

  res <- .phase2_router$call(make_phase2_req("GET", "/table", query = list(
    analysis_var = "unknown_var",
    pip_id = "COL_2010_ECH_INC_ALL",
    measures = "mean"
  )))

  expect_equal(res$status, 400L)
  body <- parse_phase2_res(res)
  expect_equal(body$status, "error")
  expect_true(any(grepl("analysis_var", body$errors, ignore.case = TRUE)))
})

test_that("phase2: GET /table rejects pov_status usage without poverty_line with 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.phase2_router), "Router could not be created")

  res <- .phase2_router$call(make_phase2_req("GET", "/table", query = list(
    analysis_var = "pov_status",
    pip_id = "COL_2010_ECH_INC_ALL",
    measures = "headcount"
  )))

  expect_equal(res$status, 400L)
  body <- parse_phase2_res(res)
  expect_equal(body$status, "error")
  expect_true(any(grepl("poverty_line", body$errors, ignore.case = TRUE)))
})
