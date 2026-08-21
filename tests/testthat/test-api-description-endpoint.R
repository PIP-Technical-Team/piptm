library(data.table)
library(jsonlite)

# ── Request / response helpers (same pattern as test-api-endpoints.R) ────────

make_desc_api_req <- function(method = "GET", path = "/",
                              query = list(), body = NULL) {

  qs <- if (length(query) > 0L) {
    parts <- unlist(lapply(names(query), function(k) {
      paste0(k, "=", as.character(query[[k]]))
    }))
    paste(parts, collapse = "&")
  } else {
    ""
  }

  body_raw <- if (!is.null(body)) {
    charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE))
  } else {
    raw(0L)
  }

  req                <- new.env(parent = emptyenv())
  req$REQUEST_METHOD <- toupper(method)
  req$PATH_INFO      <- path
  req$QUERY_STRING   <- qs
  req$HTTP_ACCEPT    <- "application/json"
  req$CONTENT_TYPE   <- if (!is.null(body)) "application/json" else ""
  req$CONTENT_LENGTH <- as.character(length(body_raw))
  req$HTTP_HOST      <- "localhost"
  req$rook.input     <- list(
    read_lines = function() rawToChar(body_raw),
    read       = function(l = -1L) body_raw,
    rewind     = function() invisible(NULL)
  )
  req
}

parse_desc_api_res <- function(res, simplify = TRUE) {
  body <- res$body
  if (is.raw(body)) body <- rawToChar(body)
  jsonlite::fromJSON(body, simplifyVector = simplify)
}

# ── Router ──────────────────────────────────────────────────────────────────

.desc_plumber_path <- file.path(
  rprojroot::find_package_root_file(), "inst", "plumber", "plumber.R"
)

# ── Tests ─────────────────────────────────────────────────────────────────────

test_that("/description returns 400 for missing analysis_var", {
  router <- plumber::plumb(.desc_plumber_path)
  req <- make_desc_api_req("GET", "/description", query = list(
    pip_id = "COL_2010_ECH_INC_ALL",
    measures = "mean"
  ))
  res <- router$call(req)
  body <- parse_desc_api_res(res)

  expect_equal(res$status, 400L)
  expect_equal(body$status, "error")
  expect_true(any(grepl("analysis_var", body$errors)))
})

test_that("/description returns 400 for missing pip_id", {
  router <- plumber::plumb(.desc_plumber_path)
  req <- make_desc_api_req("GET", "/description", query = list(
    analysis_var = "welfare",
    measures = "mean"
  ))
  res <- router$call(req)
  body <- parse_desc_api_res(res)

  expect_equal(res$status, 400L)
  expect_equal(body$status, "error")
  expect_true(any(grepl("pip_id", body$errors)))
})

test_that("/description returns 400 for missing measures", {
  router <- plumber::plumb(.desc_plumber_path)
  req <- make_desc_api_req("GET", "/description", query = list(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare"
  ))
  res <- router$call(req)
  body <- parse_desc_api_res(res)

  expect_equal(res$status, 400L)
  expect_equal(body$status, "error")
  expect_true(any(grepl("measures", body$errors)))
})

test_that("/description returns 400 for unknown measures", {
  router <- plumber::plumb(.desc_plumber_path)
  req <- make_desc_api_req("GET", "/description", query = list(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "nonexistent_measure"
  ))
  res <- router$call(req)
  body <- parse_desc_api_res(res)

  expect_equal(res$status, 400L)
  expect_true(any(grepl("Unknown measure", body$errors)))
})

test_that("/description returns success envelope with model and markdown", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  router <- plumber::plumb(.desc_plumber_path)
  req <- make_desc_api_req("GET", "/description", query = list(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean"
  ))
  res <- router$call(req)
  body <- parse_desc_api_res(res)

  expect_equal(res$status, 200L)
  expect_equal(body$status, "success")
  expect_true("model" %in% names(body$data))
  expect_true("markdown" %in% names(body$data))
  expect_true(is.character(body$data$markdown))
  expect_true(nchar(body$data$markdown) > 0L)
})

test_that("/description markdown includes key sections", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  router <- plumber::plumb(.desc_plumber_path)
  req <- make_desc_api_req("GET", "/description", query = list(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = c("mean", "gini"),
    by = "gender"
  ))
  res <- router$call(req)
  body <- parse_desc_api_res(res)

  expect_equal(res$status, 200L)
  md <- body$data$markdown
  expect_true(grepl("# Table Description", md))
  expect_true(grepl("## Surveys Analyzed", md))
  expect_true(grepl("## Statistics", md))
  expect_true(grepl("## Cell Definition", md))
})

test_that("/description model has expected structure", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  router <- plumber::plumb(.desc_plumber_path)
  req <- make_desc_api_req("GET", "/description", query = list(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean"
  ))
  res <- router$call(req)
  body <- parse_desc_api_res(res)

  expect_equal(res$status, 200L)
  model <- body$data$model
  expect_true(is.list(model))
  expect_true("metadata" %in% names(model))
  expect_true("surveys" %in% names(model))
  expect_true("statistics" %in% names(model))
  expect_true("cell_definition" %in% names(model))
})

test_that("/description captures warnings from table_maker", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  router <- plumber::plumb(.desc_plumber_path)
  req <- make_desc_api_req("GET", "/description", query = list(
    pip_id = c("COL_2010_ECH_INC_ALL", "ZZZ_9999_X_INC_ALL"),
    analysis_var = "welfare",
    measures = "mean"
  ))
  res <- router$call(req)
  body <- parse_desc_api_res(res)

  expect_equal(res$status, 200L)
  expect_true(length(body$warnings) > 0L)
})
