# Integration Tests — GET|POST /metadata endpoint
#
# Plan: .cg-docs/plans/2026-11-12-metadata-rework-and-table-maker-decoupling.md
#   Phase 2 (Step 5)
#
# Strategy: build a plumber router from inst/plumber/plumber.R and drive it
# programmatically via pr$call() — no network socket is opened, and no Arrow
# microdata is read by /metadata.

library(data.table)
library(jsonlite)

# ── Request / response helpers ────────────────────────────────────────────────

make_api_req <- function(method = "GET", path = "/",
                         query = list(), body = NULL) {
  qs <- if (length(query) > 0L) {
    parts <- unlist(lapply(names(query), function(k) {
      v <- query[[k]]
      paste0(k, "=", utils::URLencode(as.character(v), reserved = TRUE))
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

# ── Router ────────────────────────────────────────────────────────────────────

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

# ── Manifest + registry fixture (no Arrow microdata needed for /metadata) ─────

activate_metadata_api_fixture <- function(release = "20260401_TEST",
                                          env = parent.frame()) {
  tmp_arrow    <- withr::local_tempdir(.local_envir = env)
  tmp_manifest <- withr::local_tempdir(.local_envir = env)

  entries <- list(
    list(
      pip_id = "COL_2010_ECH_INC_ALL", survey_id = "S1", country_code = "COL",
      country_name = "Colombia", year = 2010L, welfare_type = "INC",
      version = "v01_v01", survey_acronym = "ECH", module = "ALL",
      dimensions = list("gender", "area"),
      welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L
    ),
    list(
      pip_id = "BOL_2000_ECH_INC_ALL", survey_id = "S2", country_code = "BOL",
      country_name = "Bolivia", year = 2000L, welfare_type = "INC",
      version = "v01_v01", survey_acronym = "ECH", module = "ALL",
      dimensions = list(),
      welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L
    )
  )
  write_fixture_manifest_tm(tmp_manifest, release, entries)

  suppressMessages({
    piptm::set_manifest_dir(tmp_manifest)
    piptm::set_arrow_root(tmp_arrow)
  })
  activate_test_registry(release, .local_envir = env)
  withr::defer(reset_piptm_env_tm(), envir = env)
  invisible(release)
}

# =============================================================================
# Block 1: Happy path — GET and POST /metadata (HTML and Markdown + download)
# =============================================================================

test_that("GET /metadata defaults to HTML (200) with attachment header", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")
  activate_metadata_api_fixture()

  res <- .desc_router$call(make_api_req(
    method = "GET", path = "/metadata",
    query = list(
      pip_id = "COL_2010_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = c("mean", "gini"),
      column = "gender"
    )
  ))

  expect_equal(res$status, 200L)
  expect_identical(res$headers[["Content-Type"]], "text/html; charset=utf-8")
  expect_identical(
    res$headers[["Content-Disposition"]],
    'attachment; filename="table-metadata.html"'
  )
  body <- if (is.raw(res$body)) rawToChar(res$body) else res$body
  expect_type(body, "character")
  expect_match(body, "<h2[^>]*>Table Overview</h2>", perl = TRUE)
  expect_match(body, "<h2[^>]*>Data Provenance</h2>", perl = TRUE)
  expect_match(body, "<h2[^>]*>Surveys Selected</h2>", perl = TRUE)
  expect_match(body, "<h2[^>]*>Table Layout Configuration</h2>", perl = TRUE)
  expect_match(body, "<h2[^>]*>Statistics Computed</h2>", perl = TRUE)
  expect_false(grepl("Cell Definition", body, fixed = TRUE))
  expect_false(grepl("Execution Summary", body, fixed = TRUE))
})

test_that("POST /metadata returns Markdown (200) with attachment header when format=markdown", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")
  activate_metadata_api_fixture()

  res <- .desc_router$call(make_api_req(
    method = "POST", path = "/metadata",
    body = list(
      pip_id = "COL_2010_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = c("mean", "gini"),
      column = "gender",
      format = "markdown"
    )
  ))

  expect_equal(res$status, 200L)
  expect_identical(res$headers[["Content-Type"]], "text/plain; charset=UTF-8")
  expect_identical(
    res$headers[["Content-Disposition"]],
    'attachment; filename="table-metadata.md"'
  )
  body <- if (is.raw(res$body)) rawToChar(res$body) else res$body
  expect_match(body, "## Table Overview", fixed = TRUE)
  expect_match(body, "## Data Provenance", fixed = TRUE)
  expect_match(body, "## Surveys Selected", fixed = TRUE)
  expect_match(body, "## Table Layout Configuration", fixed = TRUE)
  expect_match(body, "## Statistics Computed", fixed = TRUE)
  expect_false(grepl("## Cell Definition", body, fixed = TRUE))
  expect_false(grepl("## Execution Summary", body, fixed = TRUE))
})

test_that("GET and POST /metadata produce equivalent documents including filter_base", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")
  activate_metadata_api_fixture()

  res_get <- .desc_router$call(make_api_req(
    method = "GET", path = "/metadata",
    query = list(
      pip_id = "COL_2010_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = "mean",
      column = "gender",
      row = "area",
      filter_base = '{"gender":[1]}',
      format = "markdown"
    )
  ))

  res_post <- .desc_router$call(make_api_req(
    method = "POST", path = "/metadata",
    body = list(
      pip_id = "COL_2010_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = "mean",
      column = "gender",
      row = "area",
      filter_base = list(gender = list(1L)),
      format = "markdown"
    )
  ))

  expect_equal(res_get$status, 200L)
  expect_equal(res_post$status, 200L)

  strip_ts <- function(x) {
    gsub("- Generated at: [^\n]+", "- Generated at: <TS>", x)
  }
  body_get  <- strip_ts(if (is.raw(res_get$body)) rawToChar(res_get$body) else res_get$body)
  body_post <- strip_ts(if (is.raw(res_post$body)) rawToChar(res_post$body) else res_post$body)
  expect_identical(body_get, body_post)
  expect_match(body_get, "## Sample Base Filters", fixed = TRUE)
  expect_match(body_get, "| Rows | area |", fixed = TRUE)
  expect_match(body_get, "| Columns | gender |", fixed = TRUE)
})

# =============================================================================
# Block 2: Validation & error paths (400 and 422)
# =============================================================================

test_that("GET and POST /metadata return 400 when required parameters are missing", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")

  res_empty_post <- .desc_router$call(make_api_req(
    method = "POST", path = "/metadata", body = list()
  ))
  expect_equal(res_empty_post$status, 400L)

  res_missing_pip <- .desc_router$call(make_api_req(
    method = "GET", path = "/metadata",
    query = list(analysis_var = "welfare", measures = "mean")
  ))
  expect_equal(res_missing_pip$status, 400L)
})

test_that("/metadata returns 400 for invalid format", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")

  res <- .desc_router$call(make_api_req(
    method = "POST", path = "/metadata",
    body = list(
      pip_id = "COL_2010_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = "mean",
      format = "pdf"
    )
  ))

  expect_equal(res$status, 400L)
  err <- parse_api_res(res, simplify = FALSE)
  expect_true(grepl("format", paste(unlist(err$errors), collapse = " "), ignore.case = TRUE))
})

test_that("POST /metadata rejects legacy description_metadata bodies with 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")

  res_meta_only <- .desc_router$call(make_api_req(
    method = "POST", path = "/metadata",
    body = list(description_metadata = list(params = list(pip_id = "COL_2010_ECH_INC_ALL")))
  ))
  expect_equal(res_meta_only$status, 400L)

  res_both <- .desc_router$call(make_api_req(
    method = "POST", path = "/metadata",
    body = list(
      description_metadata = list(params = list()),
      pip_id = "COL_2010_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = "mean"
    )
  ))
  expect_equal(res_both$status, 400L)
})

test_that("/metadata rejects gapped or duplicate layout slots with 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")

  res_gap <- .desc_router$call(make_api_req(
    method = "POST", path = "/metadata",
    body = list(
      pip_id = "COL_2010_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = "mean",
      column = "gender",
      superColumn = "area"
    )
  ))
  expect_equal(res_gap$status, 400L)

  res_dup <- .desc_router$call(make_api_req(
    method = "POST", path = "/metadata",
    body = list(
      pip_id = "COL_2010_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = "mean",
      column = "gender",
      row = "gender"
    )
  ))
  expect_equal(res_dup$status, 400L)
})

test_that("/metadata returns 422 on domain errors (all surveys excluded or unknown release)", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")
  activate_metadata_api_fixture()

  # BOL_2000 has no gender dimension -> all surveys excluded -> 422
  res_excl <- .desc_router$call(make_api_req(
    method = "GET", path = "/metadata",
    query = list(
      pip_id = "BOL_2000_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = "mean",
      column = "gender"
    )
  ))
  expect_equal(res_excl$status, 422L)

  # Unknown release -> 422
  res_rel <- .desc_router$call(make_api_req(
    method = "GET", path = "/metadata",
    query = list(
      pip_id = "COL_2010_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = "mean",
      release = "UNKNOWN_RELEASE_9999"
    )
  ))
  expect_equal(res_rel$status, 422L)
})

# =============================================================================
# Block 3: Router introspection, OpenAPI, and CORS
# =============================================================================

test_that("/metadata appears in router endpoint introspection and /description is absent", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")

  ep_list <- unlist(.desc_router$endpoints, recursive = FALSE)
  paths <- vapply(ep_list, function(ep) ep$path, character(1))
  expect_true("/metadata" %in% paths)
  expect_false("/description" %in% paths)
})

test_that("POST /description returns 404 after rename to /metadata", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")

  res <- .desc_router$call(make_api_req(
    method = "POST", path = "/description", body = list()
  ))
  expect_equal(res$status, 404L)
})

test_that("GET and POST /metadata OpenAPI spec does not expose req or description_metadata", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")

  spec <- .desc_router$getApiSpec()

  desc_get  <- spec$paths[["/metadata"]][["get"]]
  desc_post <- spec$paths[["/metadata"]][["post"]]
  expect_false(is.null(desc_get))
  expect_false(is.null(desc_post))

  for (op in list(desc_get, desc_post)) {
    params <- op$parameters %||% list()
    if (length(params) > 0L) {
      param_names <- vapply(params, function(p) p$name %||% "", character(1))
      expect_false("req" %in% param_names)
      expect_false("res" %in% param_names)
      expect_false("description_metadata" %in% param_names)
    }
  }

  rb_props <- names(desc_post$requestBody$content[["application/json"]]$schema$properties)
  expect_false("description_metadata" %in% rb_props)
  expect_true(all(c("pip_id", "analysis_var", "measures", "format") %in% rb_props))
})

test_that("OPTIONS preflight works for /metadata (CORS)", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.desc_router), "Router could not be created")

  res <- .desc_router$call(make_api_req(method = "OPTIONS", path = "/metadata"))
  expect_equal(res$status, 204L)
  headers <- res$headers
  expect_identical(headers[["Access-Control-Allow-Origin"]], "*")
  expect_true(grepl("POST", headers[["Access-Control-Allow-Methods"]], fixed = TRUE))
})
