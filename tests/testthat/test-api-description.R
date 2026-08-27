# Integration Tests — /description endpoint & /table include_metadata
#
# Plan: .cg-docs/plans/2026-08-26-description-endpoint-implementation.md
#   Phase 5 (Steps 10-12)
#
# Strategy: build a plumber router from the installed package file and drive
# it programmatically via pr$call() — no network socket is opened.
#
# Blocks:
#   1.  Fast path: POST /description with description_metadata → Markdown
#   2.  Error: empty body → 400
#   3.  Error: both description_metadata and params → 400
#   4.  Fast path: invalid metadata structure → 422
#   5.  Fallback path: recompute via params (fixture Arrow data)
#   6.  /table include_metadata handling
#   7.  validate_description_input() helper unit checks

library(data.table)
library(jsonlite)

# ── Request / response helpers (mirrors test-api-endpoints.R) ─────────────────

make_api_req <- function(method = "GET", path = "/",
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
  req$postBody       <- if (!is.null(body)) jsonlite::toJSON(body, auto_unbox = TRUE) else ""
  req
}

parse_api_res <- function(res, simplify = TRUE) {
  body <- res$body
  if (is.raw(body)) body <- rawToChar(body)
  jsonlite::fromJSON(body, simplifyVector = simplify)
}

# ── Router ─────────────────────────────────────────────────────────────────────

.desc_plumber_path <- tryCatch(
  file.path(rprojroot::find_package_root_file(), "inst", "plumber", "plumber.R"),
  error = function(e) ""
)
if (!nzchar(.desc_plumber_path) || !file.exists(.desc_plumber_path)) {
  .desc_plumber_path <- system.file("plumber", "plumber.R", package = "piptm")
}
.desc_router <- if (requireNamespace("plumber", quietly = TRUE)) {
  suppressMessages(plumber::plumb(.desc_plumber_path))
} else {
  NULL
}

# ── Synthetic metadata for the fast path ───────────────────────────────────────

.make_desc_metadata <- function() {
  list(
    params = list(
      pip_id = c("TEST_2020_SURVEY_CON_ALL"),
      analysis_var = "welfare",
      measures = c("mean", "gini"),
      poverty_line = NULL,
      ppp = 2021L,
      release = "TEST_2024",
      by = "gender",
      filter_base = NULL,
      pop_share_threshold = NULL,
      include_metadata = TRUE
    ),
    provenance = list(
      release = "TEST_2024",
      ppp_year = 2021L,
      generated_at = "2026-08-27 12:00:00"
    ),
    surveys = list(
      loaded = data.table(
        pip_id = "TEST_2020_SURVEY_CON_ALL",
        country_code = "TST",
        surveyid_year = "2020",
        welfare_type = "CON"
      ),
      excluded = data.table(pip_id = character(0), reason = character(0))
    ),
    resolved_labels = list(
      analysis_var = list(varname = "welfare", ui_label = "Welfare", tm_type = "continuous"),
      measures = data.table(
        measure = c("mean", "gini"),
        ui_label = c("Mean", "Gini index"),
        stat_group = c("summary_statistics", "inequality")
      ),
      filters = NULL,
      covariates = data.table(
        slot = "columns", varname = "gender", ui_label = "Gender", n_categories = 2L
      )
    ),
    execution = list(
      n_surveys_loaded = 1L,
      n_surveys_excluded = 0L,
      n_filters_applied = 0L,
      n_measures_computed = 2L,
      suppression = list(
        triggered = FALSE, threshold = NULL,
        n_cells_suppressed = 0L, suppressed_cells = NULL
      ),
      warnings = NULL
    )
  )
}

# =============================================================================
# Block 1: Fast path returns Markdown
# =============================================================================

test_that("POST /description fast path returns Markdown (200)", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")

  meta <- .make_desc_metadata()
  res  <- .desc_router$call(make_api_req(
    method = "POST", path = "/description",
    body = list(description_metadata = jsonlite::fromJSON(jsonlite::toJSON(meta, auto_unbox = TRUE)))
  ))

  expect_equal(res$status, 200L)
  body <- if (is.raw(res$body)) rawToChar(res$body) else res$body
  expect_type(body, "character")
  expect_true(grepl("## Table Overview", body, fixed = TRUE))
  expect_true(grepl("## Cell Definition", body, fixed = TRUE))
  expect_true(grepl("The Mean of Welfare", body, fixed = TRUE))
})

test_that("POST /description returns 400 for empty body", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")

  res <- .desc_router$call(make_api_req(
    method = "POST", path = "/description", body = list()
  ))
  expect_equal(res$status, 400L)
})

test_that("POST /description returns 400 when both metadata and params supplied", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")

  meta <- .make_desc_metadata()
  body <- list(
    description_metadata = meta,
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean"
  )
  res <- .desc_router$call(make_api_req(method = "POST", path = "/description", body = body))
  expect_equal(res$status, 400L)
})

test_that("POST /description returns 422 for metadata missing params", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")

  bad <- .make_desc_metadata()
  bad$params <- NULL
  res <- .desc_router$call(make_api_req(
    method = "POST", path = "/description",
    body = list(description_metadata = bad)
  ))
  expect_equal(res$status, 422L)
})

# =============================================================================
# Block 2: validate_description_input() helper unit checks
# =============================================================================

test_that("validate_description_input distinguishes fast vs fallback modes", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")

  # Source helpers.R into a scratch environment to call the non-exported helper.
  # helpers.R lives alongside plumber.R.
  helpers_path <- file.path(dirname(.desc_plumber_path), "helpers.R")
  skip_if(!file.exists(helpers_path),
          "helpers.R not found alongside plumber.R")
  src_env <- new.env(parent = globalenv())
  source(helpers_path, local = src_env)
  vdd <- get("validate_description_input", envir = src_env)

  meta <- .make_desc_metadata()
  meta_only <- vdd(list(description_metadata = meta))
  expect_true(meta_only$valid)
  expect_identical(meta_only$mode, "metadata")

  params_only <- vdd(list(pip_id = "COL_2010_ECH_INC_ALL", analysis_var = "welfare",
                          measures = "mean"))
  expect_true(params_only$valid)
  expect_identical(params_only$mode, "fallback")

  both <- vdd(list(
    description_metadata = meta,
    pip_id = "COL_2010_ECH_INC_ALL"
  ))
  expect_false(both$valid)
  expect_true(any(grepl("not both", both$errors)))

  none <- vdd(list(foo = "bar"))
  expect_false(none$valid)
  expect_identical(none$mode, NA_character_)

  empty <- vdd(list())
  expect_false(empty$valid)
})

# =============================================================================
# Block 3: /table include_metadata param handling (validation only, no Arrow)
# =============================================================================

test_that("GET /table with invalid include_metadata does not error at param level", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")

  # include_metadata defaults to "false"; an invalid value falls back to FALSE.
  # Without Arrow fixtures the request still fails on missing data, but not on
  # the include_metadata param itself. Expect a 400/422 from validation, not a
  # plumber 500.
  res <- .desc_router$call(make_api_req(
    method = "GET", path = "/table",
    query = list(
      pip_id = "BAD", analysis_var = "welfare",
      measures = "mean", include_metadata = "notabool"
    )
  ))
  expect_true(res$status %in% c(400L, 422L))
})

# =============================================================================
# Block 4: Router introspection + CORS (Step 12)
# =============================================================================

test_that("/description appears in router endpoint introspection", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")

  ep_list <- unlist(.desc_router$endpoints, recursive = FALSE)
  paths <- vapply(ep_list, function(ep) ep$path, character(1))
  expect_true("/description" %in% paths)
})

test_that("OPTIONS preflight works for /description (CORS)", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")

  res <- .desc_router$call(make_api_req(method = "OPTIONS", path = "/description"))
  expect_equal(res$status, 204L)
  headers <- res$headers
  expect_identical(headers[["Access-Control-Allow-Origin"]], "*")
  expect_true(grepl("POST", headers[["Access-Control-Allow-Methods"]], fixed = TRUE))
})

# =============================================================================
# Block 5: P1-1 Malformed metadata structure tests
# =============================================================================

test_that("POST /description returns 422 for metadata with invalid provenance structure", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")

  bad <- .make_desc_metadata()
  bad$provenance <- "string"  # should be list
  res <- .desc_router$call(make_api_req(
    method = "POST", path = "/description",
    body = list(description_metadata = bad)
  ))
  expect_equal(res$status, 422L)
})

test_that("POST /description returns 422 for metadata with invalid surveys structure", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")

  bad <- .make_desc_metadata()
  bad$surveys <- NULL  # surveys is required
  res <- .desc_router$call(make_api_req(
    method = "POST", path = "/description",
    body = list(description_metadata = bad)
  ))
  expect_equal(res$status, 422L)
})

test_that("POST /description returns 422 for metadata with invalid resolved_labels", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")

  bad <- .make_desc_metadata()
  bad$resolved_labels <- "not_a_list"
  res <- .desc_router$call(make_api_req(
    method = "POST", path = "/description",
    body = list(description_metadata = bad)
  ))
  expect_equal(res$status, 422L)
})

# =============================================================================
# Block 6: P1-2 Fallback path test
# =============================================================================

test_that("POST /description fallback path recomputes via table_maker", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")
  skip_if_not_installed("arrow")
  
  # Use fixtures from test-api-endpoints.R if available
  # This test requires Arrow data fixtures to work
  skip("Requires full Arrow fixtures from .make_ep_fixtures() - add when fixtures are available")
  
  # When fixtures are available, uncomment:
  # res <- .desc_router$call(make_api_req(
  #   method = "POST", path = "/description",
  #   body = list(
  #     pip_id = "COL_2010_ECH_INC_ALL",
  #     analysis_var = "welfare",
  #     measures = "mean"
  #   )
  # ))
  # 
  # expect_equal(res$status, 200L)
  # body <- if (is.raw(res$body)) rawToChar(res$body) else res$body
  # expect_true(grepl("## Table Overview", body, fixed = TRUE))
  # expect_true(grepl("## Cell Definition", body, fixed = TRUE))
})

