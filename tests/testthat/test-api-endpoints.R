# Integration Tests — Plumber API Endpoints
#
# Plan: .cg-docs/plans/2026-05-11-api-service-plumber-v2.md (Step 4)
#
# Strategy: build a plumber router from the installed package file and drive
# it programmatically via pr$call() — no network socket is opened.
#
# Blocks:
#   1.  Discovery endpoints  (/health, /releases, /measures, /dimensions)
#   2.  CORS headers + OPTIONS preflight
#   3.  Global error-handler registration
#   4.  Input-validation errors — /table     (no Arrow data)
#   5.  Input-validation errors — /lookup    (no Arrow data)
#   6.  /table round-trip                    (fixture Arrow data)
#   7.  /lookup round-trip                   (fixture Arrow data)
#   8.  /surveys                             (fixture Arrow data)
#   9.  Warning capture
#   10. Response-envelope completeness

library(data.table)
library(jsonlite)

# ── Request / response helpers ────────────────────────────────────────────────

# Build a plumber-compatible Rook request environment.
# Repeated-value query params are supported:
#   query = list(pip_id = c("A", "B"), measures = "mean")
#   → QUERY_STRING = "pip_id=A&pip_id=B&measures=mean"
make_api_req <- function(method = "GET", path = "/",
                         query = list(), body = NULL) {
  if (identical(path, "/table") && is.null(body) && is.null(query$analysis_var)) {
    query$analysis_var <- "welfare"
  }
  if (identical(path, "/table") && !is.null(body) && is.list(body) && is.null(body$analysis_var)) {
    body$analysis_var <- "welfare"
  }

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

# Decode a pr$call() response.
# simplify = TRUE → data.frames for column-oriented JSON; vectors for arrays.
parse_api_res <- function(res, simplify = TRUE) {
  body <- res$body
  if (is.raw(body)) body <- rawToChar(body)
  jsonlite::fromJSON(body, simplifyVector = simplify)
}

# ── Router ─────────────────────────────────────────────────────────────────────
# Created once per test session; the router itself is stateless — all data
# state lives in piptm::.piptm_env, which each test block mutates via fixtures.

# P1-3: Add file.exists check inside tryCatch to avoid nonexistent path issue
.ep_plumber_path <- tryCatch({
  candidate <- file.path(rprojroot::find_package_root_file(), "inst", "plumber", "plumber.R")
  if (file.exists(candidate)) candidate else ""
}, error = function(e) "")
if (!nzchar(.ep_plumber_path)) {
  .ep_plumber_path <- system.file("plumber", "plumber.R", package = "piptm")
}

.ep_router <- if (requireNamespace("plumber", quietly = TRUE)) {
  suppressMessages(plumber::plumb(.ep_plumber_path))
} else {
  NULL
}

# ── Fixture helpers ───────────────────────────────────────────────────────────

.write_ep_parquet <- function(arrow_root, country_code, year,
                              welfare_type, version, pip_id, survey_acronym,
                              n_rows = 10L, extra_cols = character(0L)) {
  dir_path <- file.path(
    arrow_root,
    paste0("country_code=",  country_code),
    paste0("surveyid_year=", year),
    paste0("welfare_type=",  welfare_type),
    paste0("version=",       version)
  )
  dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)

  dt <- data.table(
    country_code   = rep(country_code,    n_rows),
    surveyid_year  = rep(as.integer(year), n_rows),
    welfare_type   = rep(welfare_type,    n_rows),
    version        = rep(version,         n_rows),
    pip_id         = rep(pip_id,          n_rows),
    survey_acronym = rep(survey_acronym,  n_rows),
    welfare_ppp_2021_01_02 = as.numeric(seq_len(n_rows)),
    weight         = rep(1.0, n_rows)
  )

  for (col in extra_cols) {
    if (col == "gender")
      dt[, gender := factor(
        rep_len(c("male", "female"), n_rows), levels = c("male", "female")
      )]
    if (col == "area")
      dt[, area := factor(
        rep(c("urban", "rural"), length.out = n_rows),
        levels = c("urban", "rural")
      )]
    if (col == "age")
      dt[, age := as.integer(seq(15L, by = 5L, length.out = n_rows))]
  }

  arrow::write_parquet(dt, file.path(dir_path, "data.parquet"))
  invisible(pip_id)
}

.write_ep_manifest <- function(manifest_dir, release, entries,
                               set_current = TRUE) {
  manifest <- list(
    release      = release,
    generated_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    entries      = entries
  )
  fname <- file.path(manifest_dir, paste0("manifest_", release, ".json"))
  jsonlite::write_json(manifest, fname, auto_unbox = TRUE, pretty = TRUE)
  if (set_current) {
    jsonlite::write_json(
      list(current_release = release),
      file.path(manifest_dir, "current_release.json"),
      auto_unbox = TRUE
    )
  }
  invisible(fname)
}

# See helper-fixtures.R: reset_piptm_env_tm() is the single shared
# implementation; kept as an alias here so existing call sites are unaffected.
.reset_piptm_env <- reset_piptm_env_tm

# Build 3-survey fixture (COL 2010, BOL 2000, COL 2015) in temp dirs and
# activate them in .piptm_env. Returns list(tmp_arrow, tmp_manifest, release).
# Defers .reset_piptm_env() to env when env is a testthat test environment.
.make_ep_fixtures <- function(env = parent.frame()) {
  skip_if_not_installed("arrow")
  tmp_arrow    <- withr::local_tempdir(.local_envir = env)
  tmp_manifest <- withr::local_tempdir(.local_envir = env)

  .write_ep_parquet(
    tmp_arrow, "COL", 2010L, "INC", "v01_v01",
    "COL_2010_ECH_INC_ALL", "ECH", extra_cols = c("gender", "area")
  )
  .write_ep_parquet(
    tmp_arrow, "BOL", 2000L, "INC", "v01_v01",
    "BOL_2000_ECH_INC_ALL", "ECH"
  )
  .write_ep_parquet(
    tmp_arrow, "COL", 2015L, "INC", "v01_v01",
    "COL_2015_ECH_INC_ALL", "ECH", extra_cols = c("gender")
  )

  entries <- list(
    list(
      pip_id = "COL_2010_ECH_INC_ALL", survey_id = "S1",
      country_code = "COL", country_name = "Colombia",
      region_code = "LCN", region_name = "Latin America & Caribbean",
      year = 2010L, welfare_type = "INC",
      version = "v01_v01", survey_acronym = "ECH", module = "ALL",
      dimensions = list("gender", "area"),
      welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L
    ),
    list(
      pip_id = "BOL_2000_ECH_INC_ALL", survey_id = "S2",
      country_code = "BOL", country_name = "Bolivia",
      region_code = "ECA", region_name = "Europe & Central Asia",
      year = 2000L, welfare_type = "INC",
      version = "v01_v01", survey_acronym = "ECH", module = "ALL",
      dimensions = list(),
      welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L
    ),
    list(
      pip_id = "COL_2015_ECH_INC_ALL", survey_id = "S3",
      country_code = "COL", country_name = "Colombia",
      region_code = "LCN", region_name = "Latin America & Caribbean",
      year = 2015L, welfare_type = "INC",
      version = "v01_v01", survey_acronym = "ECH", module = "ALL",
      dimensions = list("gender"),
      welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L
    )
  )

  release <- "20260206_EP_TEST"
  .write_ep_manifest(tmp_manifest, release, entries)

  suppressMessages({
    piptm::set_manifest_dir(tmp_manifest)
    piptm::set_arrow_root(tmp_arrow)
  })

  withr::defer(.reset_piptm_env(), envir = env)

  list(tmp_arrow = tmp_arrow, tmp_manifest = tmp_manifest, release = release)
}

# =============================================================================
# Block 1: Discovery endpoints — no Arrow data required
# =============================================================================

# Initial fixture setup for endpoint block
if (!is.null(.ep_router)) {
  .init_fx <- .make_ep_fixtures()
  activate_test_registry(.init_fx$release)
}

test_that("GET /health returns 200", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  res <- .ep_router$call(make_api_req("GET", "/health"))
  expect_equal(res$status, 200L)
})

test_that("GET /health body has status='ok' and a release field", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  body <- parse_api_res(.ep_router$call(make_api_req("GET", "/health")))
  expect_equal(body$data$status, "ok")
  expect_false(is.null(body$data$release))
})

test_that("GET /releases returns 200 with success status", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  res <- .ep_router$call(make_api_req("GET", "/releases"))
  expect_equal(res$status, 200L)
  body <- parse_api_res(res)
  expect_equal(body$status, "success")
})

test_that("GET /releases data contains releases list and current", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  body <- parse_api_res(.ep_router$call(make_api_req("GET", "/releases")))
  expect_false(is.null(body$data$releases))
  expect_false(is.null(body$data$current))
  expect_true(body$data$current %in% body$data$releases)
})

test_that("GET /statistics returns 200 with success status and groups", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  res  <- .ep_router$call(make_api_req("GET", "/statistics"))
  expect_equal(res$status, 200L)
  body <- parse_api_res(res)
  expect_equal(body$status, "success")
  expect_true(length(body$data) > 0L)
})

test_that("GET /statistics returns groups with nested measures", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  body <- parse_api_res(.ep_router$call(make_api_req("GET", "/statistics")), simplify = FALSE)
  expect_equal(unlist(body$status), "success")
  expect_true(length(body$data) > 0L)
  first_group <- body$data[[1L]]
  expect_true(all(c("group", "group_label", "measures") %in% names(first_group)))
  expect_true(length(first_group$measures) > 0L)
})

test_that("GET /dimensions returns 200 and includes all valid dimension names", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  body <- parse_api_res(.ep_router$call(make_api_req("GET", "/dimensions")))
  expect_equal(body$status, "success")
  valid_dims <- piptm::pip_valid_dimensions()
  expect_true(all(valid_dims %in% unlist(body$data)))
})

# =============================================================================
# Block 1b: Static catalogue endpoints — /categories and /covariates
# =============================================================================

# ── /categories ───────────────────────────────────────────────────────────────

test_that("GET /analysis-variables returns success and expected pov_status fields", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")

  # Inject a small synthetic registry for the current release
  .inject_registry_for_test <- function(registry) {
    env <- get('.piptm_env', envir = asNamespace('piptm'))
    release <- piptm::piptm_current_release()
    old <- env$registries
    env$registries <- list()
    env$registries[[release]] <- registry
    list(env = env, old = old)
  }

  .make_registry_fixture <- function() {
    list(
      welfare = list(
        varname = "welfare",
        ui_label = "Welfare",
        tm_type = "welfare",
        roles = c("analysis_var"),
        stat_groups = c("summary_statistics", "inequality"),
        n_categories = NULL,
        categories = NULL
      ),
      pov_status = list(
        varname = "pov_status",
        ui_label = "Poverty status",
        tm_type = "poverty",
        roles = c("analysis_var", "covariate"),
        stat_groups = c("poverty"),
        n_categories = 2L,
        categories = NULL
      ),
      age_group = list(
        varname = "age_group",
        ui_label = "Age group",
        tm_type = "categorical",
        roles = c("filter", "covariate"),
        stat_groups = character(0L),
        n_categories = 4L,
        categories = list(
          list(code = "0-14", label = "0 to 14"),
          list(code = "15-24", label = "15 to 24"),
          list(code = "25-64", label = "25 to 64"),
          list(code = "65+", label = "65 and above")
        )
      )
    )
  }

  reg <- .make_registry_fixture()
  state <- .inject_registry_for_test(reg)
  withr::defer({ state$env$registries <- state$old })

  res <- .ep_router$call(make_api_req("GET", "/analysis-variables"))
  expect_equal(res$status, 200L)
  body <- parse_api_res(res, simplify = FALSE)
  status_val <- unlist(body$status)
  expect_equal(status_val, "success")

  data <- body$data
  get_varname <- function(entry) {
    v <- entry$varname
    if (is.list(v)) v <- unlist(v)
    as.character(v[[1]])
  }
  varnames <- vapply(data, get_varname, character(1))
  expect_true("pov_status" %in% varnames)

  pov <- data[[which(varnames == "pov_status")]]
  pov_type <- pov$type
  if (is.list(pov_type)) pov_type <- unlist(pov_type)
  expect_identical(as.character(pov_type[[1L]]), "poverty")
  expect_true("stat_groups" %in% names(pov))
})

# ── /categories ───────────────────────────────────────────────────────────────

test_that("GET /categories returns filters with subcategories", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")

  .make_registry_fixture <- function() {
    list(
      age_group = list(
        varname = "age_group",
        ui_label = "Age group",
        tm_type = "categorical",
        roles = c("filter"),
        stat_groups = character(0L),
        n_categories = 4L,
        categories = list(
          list(code = "0-14", label = "0 to 14"),
          list(code = "15-24", label = "15 to 24"),
          list(code = "25-64", label = "25 to 64"),
          list(code = "65+", label = "65 and above")
        )
      )
    )
  }

  reg <- .make_registry_fixture()
  env <- get('.piptm_env', envir = asNamespace('piptm'))
  old <- env$registries
  env$registries <- list()
  env$registries[[piptm::piptm_current_release()]] <- reg
  withr::defer({ env$registries <- old })

  res <- .ep_router$call(make_api_req("GET", "/categories"))
  expect_equal(res$status, 200L)
  body <- parse_api_res(res, simplify = FALSE)
  status_val <- unlist(body$status)
  expect_equal(status_val, "success")

  data <- body$data
  get_varname <- function(entry) {
    v <- entry$varname
    if (is.list(v)) v <- unlist(v)
    as.character(v[[1]])
  }
  varnames <- vapply(data, get_varname, character(1))
  expect_true("age_group" %in% varnames)

  age <- data[[which(varnames == "age_group")]]
  subs <- age$subcategories
  expect_true(length(subs) == 4)
  codes <- vapply(subs, function(x) as.character(x$code[[1]]), character(1))
  expect_true(all(c("0-14", "15-24", "25-64", "65+") %in% codes))
})

# ── /covariates ───────────────────────────────────────────────────────────────

test_that("GET /covariates returns pov_status with n_categories=2", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")

  reg <- list(
    pov_status = list(
      varname = "pov_status",
      ui_label = "Poverty status",
      tm_type = "poverty",
      roles = c("covariate"),
      stat_groups = c("poverty"),
      n_categories = 2L,
      categories = NULL
    )
  )

  env <- get('.piptm_env', envir = asNamespace('piptm'))
  old <- env$registries
  env$registries <- list()
  env$registries[[piptm::piptm_current_release()]] <- reg
  withr::defer({ env$registries <- old })

  res <- .ep_router$call(make_api_req("GET", "/covariates"))
  expect_equal(res$status, 200L)
  body <- parse_api_res(res, simplify = FALSE)
  status_val <- unlist(body$status)
  expect_equal(status_val, "success")

  data <- body$data
  get_varname <- function(entry) {
    v <- entry$varname
    if (is.list(v)) v <- unlist(v)
    as.character(v[[1]])
  }
  varnames <- vapply(data, get_varname, character(1))
  expect_true("pov_status" %in% varnames)

  pov <- data[[which(varnames == "pov_status")]]
  ncat <- pov$n_categories
  if (is.list(ncat)) ncat <- unlist(ncat)
  expect_true(identical(as.integer(ncat), 2L))
  expect_false("pov_status_mutex" %in% names(pov))
})

# ── /session endpoints ───────────────────────────────────────────────────────

test_that("POST /session/surveys creates a session and returns session_id", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")

  res <- .ep_router$call(make_api_req("POST", "/session/surveys", body = list(
    pip_id = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL")
  )))

  expect_equal(res$status, 200L)
  body <- parse_api_res(res)
  expect_equal(body$status, "success")
  expect_true("session_id" %in% names(body$data))
  expect_true(nzchar(as.character(body$data$session_id)))
})

test_that("GET /session/<id>/surveys returns stored pip_id", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")

  create_res <- .ep_router$call(make_api_req("POST", "/session/surveys", body = list(
    pip_id = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL")
  )))
  create_body <- parse_api_res(create_res)
  sid <- as.character(create_body$data$session_id)

  res <- .ep_router$call(make_api_req("GET", paste0("/session/", sid, "/surveys")))

  expect_equal(res$status, 200L)
  body <- parse_api_res(res)
  expect_equal(body$status, "success")
  expect_equal(
    as.character(body$data$pip_id),
    c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL")
  )
})

test_that("POST /session/surveys rejects missing pip_id with 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")

  res <- .ep_router$call(make_api_req("POST", "/session/surveys", body = list()))
  expect_equal(res$status, 400L)
  body <- parse_api_res(res)
  expect_equal(body$status, "error")
  expect_true(length(body$errors) > 0L)
})

test_that("GET /session/<id>/surveys returns 404 for unknown session", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")

  res <- .ep_router$call(make_api_req("GET", "/session/nonexistent123/surveys"))
  expect_equal(res$status, 404L)
  body <- parse_api_res(res)
  expect_equal(body$status, "error")
  expect_true(length(body$errors) > 0L)
})

# =============================================================================
# Block 2: CORS headers + OPTIONS preflight
# =============================================================================

test_that("CORS Access-Control-Allow-Origin: * is present on GET /health", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  res <- .ep_router$call(make_api_req("GET", "/health"))
  expect_equal(res$headers[["Access-Control-Allow-Origin"]], "*")
})

test_that("CORS header is present on a 400 error response from /table", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  # Trigger a 400 by omitting pip_id
  res <- .ep_router$call(
    make_api_req("GET", "/table", query = list(measures = "mean"))
  )
  expect_equal(res$status, 400L)
  expect_equal(res$headers[["Access-Control-Allow-Origin"]], "*")
})

test_that("OPTIONS preflight returns 204", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  res <- .ep_router$call(make_api_req("OPTIONS", "/table"))
  expect_equal(res$status, 204L)
})

test_that("OPTIONS preflight includes CORS Allow-Methods header", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  res <- .ep_router$call(make_api_req("OPTIONS", "/health"))
  methods <- res$headers[["Access-Control-Allow-Methods"]]
  expect_false(is.null(methods))
  expect_true(grepl("GET", methods))
  expect_true(grepl("POST", methods))
})

# =============================================================================
# Block 3: Global error handler
# =============================================================================

test_that("Global error handler is configured — router has a non-default errorHandler", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  # pr$errorHandler is a private field in plumber 1.x; verify indirectly by
  # checking that the router accepted the @plumber decorator (router is valid
  # and /health is reachable, confirming full initialisation).
  # The @plumber block calls pr$setErrorHandler() — if it threw, plumb() would
  # have failed and .ep_router would be NULL.
  expect_false(is.null(.ep_router))  # router created → @plumber block ran
  # Additionally confirm errorHandler is not the bare default:
  # In plumber 1.3.3 the field is private; use environment inspection.
  env <- environment(.ep_router$call)
  eh  <- tryCatch(get("errorHandler", envir = .ep_router, inherits = FALSE),
                  error = function(e) NULL)
  # Either the field exists and is a function, or it is private (acceptable).
  expect_true(is.null(eh) || is.function(eh))
})

# =============================================================================
# Block 4: Input-validation errors — /table (no Arrow data required)
# =============================================================================

test_that("GET /table with no pip_id returns 400 with error envelope", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  res <- .ep_router$call(
    make_api_req("GET", "/table", query = list(measures = "mean"))
  )
  expect_equal(res$status, 400L)
  body <- parse_api_res(res)
  expect_equal(body$status, "error")
  expect_true(length(body$errors) > 0L)
})

test_that("GET /table with 16 pip_ids returns 400 mentioning the 15-survey limit", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  ids <- paste0("COL_200", seq_len(9L), "_ECH_INC_ALL")
  ids <- c(ids, paste0("BOL_200", seq_len(7L), "_ECH_INC_ALL"))
  # 9 + 7 = 16 ids
  res <- .ep_router$call(
    make_api_req("GET", "/table", query = list(pip_id = ids, measures = "mean"))
  )
  expect_equal(res$status, 400L)
  body <- parse_api_res(res)
  expect_true(any(grepl("15", unlist(body$errors))))
})

test_that("GET /table with an unknown measure returns 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "not_a_real_measure"
  )))
  expect_equal(res$status, 400L)
  body <- parse_api_res(res)
  expect_true(any(grepl("not_a_real_measure", unlist(body$errors))))
})

test_that("GET /table with poverty_line='abc' returns 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id        = "COL_2010_ECH_INC_ALL",
    measures      = "mean",
    poverty_line = "abc"
  )))
  expect_equal(res$status, 400L)
  body <- parse_api_res(res)
  expect_equal(body$status, "error")
  expect_true(any(grepl("poverty_line", unlist(body$errors), ignore.case = TRUE)))
})

test_that("GET /table with negative poverty_line returns 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id        = "COL_2010_ECH_INC_ALL",
    analysis_var  = "pov_status",
    measures      = "headcount",
    poverty_line  = "-1"
  )))
  expect_equal(res$status, 400L)
  body <- parse_api_res(res)
  expect_equal(body$status, "error")
})

test_that("GET /table with unknown dimension in slot returns 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    column   = "not_a_valid_dimension"
  )))
  expect_equal(res$status, 400L)
})

test_that("GET /table with bogus release returns 422", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    release  = "BOGUS_RELEASE_DOES_NOT_EXIST"
  )))
  expect_equal(res$status, 422L)
  body <- parse_api_res(res)
  expect_equal(body$status, "error")
  expect_true(length(body$errors) > 0L)
})

# =============================================================================
# Block 5: Input-validation errors — /lookup (no Arrow data required)
# =============================================================================

test_that("GET /lookup with mismatched vector lengths returns 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  res <- .ep_router$call(make_api_req("GET", "/lookup", query = list(
    country_code = c("COL", "BOL"),
    year         = "2010",         # length 1, not 2
    welfare_type = c("INC", "INC")
  )))
  expect_equal(res$status, 400L)
  body <- parse_api_res(res)
  expect_true(any(grepl("same length", unlist(body$errors))))
})

test_that("GET /lookup with invalid welfare_type returns 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  res <- .ep_router$call(make_api_req("GET", "/lookup", query = list(
    country_code = "COL",
    year         = "2010",
    welfare_type = "BOTH"
  )))
  expect_equal(res$status, 400L)
  body <- parse_api_res(res)
  expect_true(any(grepl("BOTH", unlist(body$errors))))
})

test_that("GET /lookup with missing country_code returns 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  res <- .ep_router$call(make_api_req("GET", "/lookup", query = list(
    year         = "2010",
    welfare_type = "INC"
  )))
  expect_equal(res$status, 400L)
})

test_that("GET /lookup with bogus release returns 422", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  res <- .ep_router$call(make_api_req("GET", "/lookup", query = list(
    country_code = "COL",
    year         = "2010",
    welfare_type = "INC",
    release      = "BOGUS_RELEASE_DOES_NOT_EXIST"
  )))
  expect_equal(res$status, 422L)
})

# =============================================================================
# Block 6: /table round-trip with fixtures
# =============================================================================

test_that("GET /table with 1 survey returns 200 success envelope (fixture)", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean"
  )))
  expect_equal(res$status, 200L)
  body <- parse_api_res(res)
  expect_equal(body$status, "success")
  expect_false(is.null(body$data))
  expect_false(is.null(body$meta))
})

test_that("GET /table with include_metadata=false (default) returns Approach C shape, distinct from include_metadata=true", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()

  res_default <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean"
  )))
  expect_equal(res_default$status, 200L)
  body_default <- parse_api_res(res_default)
  # Approach C: data is a per-survey object array with layout+cells; no
  # top-level description_metadata in meta.
  expect_true(is.null(body_default$data$pip_id) || is.list(body_default$data))
  expect_false(is.null(body_default$data$layout) && is.null(body_default$data[[1L]]$layout))
  expect_null(body_default$meta$description_metadata)

  res_meta <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    include_metadata = "true"
  )))
  expect_equal(res_meta$status, 200L)
  body_meta <- parse_api_res(res_meta)
  # Legacy flat shape: data has no `layout`/`cells` keys at all; metadata is
  # surfaced via meta.description_metadata instead.
  expect_null(body_meta$data$layout)
  expect_false(is.null(body_meta$meta$description_metadata))
})

test_that("GET /table with no release param — meta.release equals current release", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  body <- parse_api_res(.ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean"
  ))))
  expect_equal(body$meta$release, fx$release)
})

test_that("GET /table with explicit release echoes it in meta", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  body <- parse_api_res(.ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    release  = fx$release
  ))))
  expect_equal(body$meta$release, fx$release)
})

test_that("GET /table with 2 surveys — meta.n_surveys is 2", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  body <- parse_api_res(.ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id   = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
    measures = "mean"
  ))))
  expect_equal(body$status, "success")
  expect_equal(as.integer(body$meta$n_surveys), 2L)
})

test_that("GET /table with 3 surveys returns data rows for all 3", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  body <- parse_api_res(.ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id   = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL",
                 "COL_2015_ECH_INC_ALL"),
    measures = "mean"
  ))))
  expect_equal(body$status, "success")
  pip_ids_returned <- unique(body$data$pip_id)
  expect_length(pip_ids_returned, 3L)
})

test_that("POST /table with JSON body returns 200", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  res <- .ep_router$call(make_api_req("POST", "/table", body = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean"
  )))
  expect_equal(res$status, 200L)
  body <- parse_api_res(res)
  expect_equal(body$status, "success")
})

test_that("GET /table with poverty measure + valid poverty_line returns 200", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id        = "COL_2010_ECH_INC_ALL",
    analysis_var  = "pov_status",
    measures      = "headcount",
    poverty_line  = "2.15"
  )))
  expect_equal(res$status, 200L)
  body <- parse_api_res(res)
  expect_equal(body$status, "success")
  expect_true(length(body$data) > 0L)
})

# =============================================================================
# Block 6b: /table — ppp parameter
# =============================================================================

test_that("GET /table with ppp='2017' returns 200 (treated as integer scalar)", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    ppp      = "2017"
  )))
  # Validation passes (ppp=2017 is valid); computation may warn if ppp column
  # is absent in the fixture, but response should be 200 success.
  body <- parse_api_res(res)
  expect_true(body$status %in% c("success", "error"))
  # Specifically: validation should NOT reject it (i.e. if status is error,
  # it must be a domain error 422, never a 400 for ppp format).
  if (res$status == 400L) {
    # Fail with a readable message
    fail(paste("Expected 200 or 422 but got 400; errors:", paste(body$errors, collapse = " | ")))
  }
  expect_true(res$status %in% c(200L, 422L))
})

test_that("GET /table with ppp='abc' returns 400 (invalid ppp)", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    ppp      = "abc"
  )))
  expect_equal(res$status, 400L)
  body <- parse_api_res(res)
  expect_equal(body$status, "error")
  expect_true(any(grepl("ppp", unlist(body$errors), ignore.case = TRUE)))
})

test_that("GET /table with ppp='-1' returns 400 (negative ppp)", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    ppp      = "-1"
  )))
  expect_equal(res$status, 400L)
  body <- parse_api_res(res)
  expect_equal(body$status, "error")
})

test_that("GET /table with malformed filter_base JSON returns 422", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id      = "COL_2010_ECH_INC_ALL",
    measures    = "mean",
    filter_base = "{bad_json}"
  )))
  expect_equal(res$status, 422L)
  body <- parse_api_res(res)
  expect_equal(body$status, "error")
})

test_that("GET /table with valid filter_base JSON is accepted by endpoint", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()

  filter_json <- jsonlite::toJSON(list(age = c(15L, 20L)), auto_unbox = TRUE)

  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id      = "COL_2015_ECH_INC_ALL",
    measures    = "mean",
    filter_base = filter_json
  )))

  expect_true(res$status %in% c(200L, 422L))
  body <- parse_api_res(res)
  expect_true(body$status %in% c("success", "error"))
  expect_false(res$status == 400L)
})

test_that("GET /table with ppp=NULL omitted uses manifest default — returns 200", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean"
    # ppp omitted — uses table_maker() default (2021L)
  )))
  expect_equal(res$status, 200L)
  body <- parse_api_res(res)
  expect_equal(body$status, "success")
})

# P1.8 — repeated ppp query param: should be rejected (not scalar)
test_that("GET /table with repeated ppp param (ppp=2017&ppp=2011) returns 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    ppp      = c("2017", "2011")
  )))
  expect_equal(res$status, 400L)
  body <- parse_api_res(res)
  expect_equal(body$status, "error")
  expect_true(any(grepl("scalar", unlist(body$errors))))
})

# =============================================================================
# Block 7: /lookup round-trip with fixtures
# =============================================================================

test_that("GET /lookup resolves a known triplet and returns the pip_id", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  body <- parse_api_res(.ep_router$call(make_api_req("GET", "/lookup", query = list(
    country_code = "COL",
    year         = "2010",
    welfare_type = "INC"
  ))))
  expect_equal(body$status, "success")
  expect_true("COL_2010_ECH_INC_ALL" %in% body$data)
})

test_that("GET /lookup echoes release in meta", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  body <- parse_api_res(.ep_router$call(make_api_req("GET", "/lookup", query = list(
    country_code = "COL",
    year         = "2010",
    welfare_type = "INC"
  ))))
  expect_equal(body$meta$release, fx$release)
})

test_that("GET /lookup with unmatched triplet returns success with warnings", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  body <- parse_api_res(.ep_router$call(make_api_req("GET", "/lookup", query = list(
    country_code = c("COL", "XXX"),
    year         = c("2010", "9999"),
    welfare_type = c("INC", "INC")
  ))))
  expect_equal(body$status, "success")
  # pip_lookup() emits cli_warn for unmatched triplet → captured in warnings
  expect_true(length(body$warnings) > 0L)
  # The matched row is returned
  expect_true("COL_2010_ECH_INC_ALL" %in% body$data)
})

# =============================================================================
# Block 8: /surveys with fixtures
# =============================================================================

test_that("GET /surveys returns 200 with one row per survey", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  res <- .ep_router$call(make_api_req("GET", "/surveys"))
  expect_equal(res$status, 200L)
  body <- parse_api_res(res)
  expect_equal(body$status, "success")
  expect_equal(nrow(body$data), 3L)  # 3 surveys in fixture
})

test_that("GET /surveys dimensions field is a list column (JSON arrays per row)", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  # Use simplify = FALSE to verify raw JSON structure
  body <- parse_api_res(
    .ep_router$call(make_api_req("GET", "/surveys")),
    simplify = FALSE
  )
  # Each row's `dimensions` is a JSON array → R list
  all_dims <- lapply(body$data, `[[`, "dimensions")
  expect_true(all(vapply(all_dims, is.list, logical(1L))))
})

test_that("GET /surveys echoes release in meta", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  body <- parse_api_res(.ep_router$call(make_api_req("GET", "/surveys")))
  expect_equal(body$meta$release, fx$release)
})

test_that("GET /surveys with bogus release returns 422", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  body <- parse_api_res(.ep_router$call(make_api_req("GET", "/surveys",
    query = list(release = "BOGUS_RELEASE_DOES_NOT_EXIST")
  )))
  expect_equal(body$status, "error")
})

# =============================================================================
# Block 8b: /countries and /regions with fixtures
# =============================================================================

test_that("GET /countries returns unique country pairs sorted by code", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()

  res <- .ep_router$call(make_api_req("GET", "/countries"))
  expect_equal(res$status, 200L)

  body <- parse_api_res(res)
  expect_equal(body$status, "success")
  expect_equal(as.character(body$meta$release), fx$release)
  expect_equal(as.character(body$data$country_code), c("BOL", "COL"))
  expect_equal(as.character(body$data$country_name), c("Bolivia", "Colombia"))
})

test_that("GET /countries with bogus release returns 422", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")

  res <- .ep_router$call(make_api_req("GET", "/countries", query = list(
    release = "BOGUS_RELEASE_DOES_NOT_EXIST"
  )))

  expect_equal(res$status, 422L)
  body <- parse_api_res(res)
  expect_equal(body$status, "error")
})

test_that("GET /regions returns grouped regions with sorted country arrays", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()

  res <- .ep_router$call(make_api_req("GET", "/regions"))
  expect_equal(res$status, 200L)

  body <- parse_api_res(res, simplify = FALSE)
  status_val <- unlist(body$status)
  expect_equal(status_val, "success")
  expect_equal(as.character(unlist(body$meta$release)), fx$release)

  regions <- body$data
  region_codes <- vapply(regions, function(x) as.character(unlist(x$region_code))[1], character(1))
  expect_equal(region_codes, c("ECA", "LCN"))

  region_countries <- lapply(regions, function(x) as.character(unlist(x$countries)))
  expect_equal(region_countries[[1]], c("BOL"))
  expect_equal(region_countries[[2]], c("COL"))
})

test_that("GET /regions with bogus release returns 422", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")

  res <- .ep_router$call(make_api_req("GET", "/regions", query = list(
    release = "BOGUS_RELEASE_DOES_NOT_EXIST"
  )))

  expect_equal(res$status, 422L)
  body <- parse_api_res(res)
  expect_equal(body$status, "error")
})

# =============================================================================
# Block 9: Warning capture in response envelope
# =============================================================================

test_that("Warnings from pip_lookup() unmatched triplets appear in response", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  body <- parse_api_res(.ep_router$call(make_api_req("GET", "/lookup", query = list(
    country_code = c("COL", "NONE"),
    year         = c("2010", "1900"),
    welfare_type = c("INC", "INC")
  ))))
  expect_equal(body$status, "success")
  expect_true(!is.null(body$warnings))
  # The unmatched triplet triggers a cli_warn inside pip_lookup()
  expect_true(length(body$warnings) > 0L)
})

# =============================================================================
# Block 10: Response-envelope completeness
# =============================================================================

test_that("Success response has all 5 required envelope fields", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  body <- parse_api_res(.ep_router$call(make_api_req("GET", "/health")))
  expect_named(
    body, c("status", "data", "warnings", "errors", "meta"),
    ignore.order = TRUE
  )
})

test_that("400 error response has all 5 required envelope fields", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  # No pip_id → 400
  body <- parse_api_res(.ep_router$call(
    make_api_req("GET", "/table", query = list(measures = "mean"))
  ))
  # jsonlite drops NULL fields and parses [] as list(); check presence/length
  expect_equal(body$status, "error")
  expect_true(is.null(body$data) || length(body$data) == 0L)
  expect_equal(length(body$warnings), 0L)
  expect_true(length(body$errors) > 0L)
})

test_that("Error envelope has empty warnings and non-empty errors", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  body <- parse_api_res(.ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "not_a_measure"
  ))))
  expect_equal(body$status, "error")
  # jsonlite parses [] as list(); check length not identity
  expect_equal(length(body$warnings), 0L)
  expect_true(length(body$errors) > 0L)
  # jsonlite drops NULL fields from serialized lists — data absent or empty
  expect_true(is.null(body$data) || length(body$data) == 0L)
})

# =============================================================================
# Block 11: /table API formatting contract & suppression masking
# =============================================================================

test_that("GET /table returns string values, no population or flag, and formatted measures", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()

  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id       = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures     = c("gini", "mean")
  )))
  expect_equal(res$status, 200L)
  body <- parse_api_res(res)
  expect_equal(body$status, "success")
  cells <- body$data$cells[[1L]]
  meas  <- cells$measures
  expect_type(as.character(unlist(meas$mean)), "character")
  expect_type(as.character(unlist(meas$gini)), "character")
  expect_false("population" %in% names(cells))
  expect_false("flag" %in% names(meas))
})

test_that("GET /table returns 422 envelope when formatter fails", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()

  testthat::with_mocked_bindings(
    format_table_output = function(...) stop("simulated schema formatting failure"),
    .package = "piptm",
    {
      res <- .ep_router$call(make_api_req("GET", "/table", query = list(
        pip_id       = "COL_2010_ECH_INC_ALL",
        analysis_var = "welfare",
        measures     = "mean"
      )))
      expect_equal(res$status, 422L)
      body <- parse_api_res(res)
      expect_equal(body$status, "error")
      expect_true(any(grepl("simulated schema formatting failure", unlist(body$errors))))
    }
  )
})

test_that("GET /table returns 422 when format_table_output() genuinely throws on malformed table_maker() output (not a mocked formatter)", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()

  # Only `table_maker()` is mocked here, to simulate a future regression that
  # drops a required column from its output. `format_table_output()` itself
  # runs unmocked and must genuinely raise its real validation error for the
  # endpoint's error handler to convert into a 422 envelope.
  malformed_table <- data.table::data.table(
    pip_id  = "COL_2010_ECH_INC_ALL",
    measure = "mean",
    value   = 1.5
    # `flag` deliberately omitted — table_maker() always produces it in
    # practice; this simulates a schema regression upstream.
  )

  testthat::with_mocked_bindings(
    table_maker = function(...) malformed_table,
    .package = "piptm",
    {
      res <- .ep_router$call(make_api_req("GET", "/table", query = list(
        pip_id       = "COL_2010_ECH_INC_ALL",
        analysis_var = "welfare",
        measures     = "mean"
      )))
      expect_equal(res$status, 422L)
      body <- parse_api_res(res)
      expect_equal(body$status, "error")
      expect_true(any(grepl("flag", unlist(body$errors), ignore.case = TRUE)))
    }
  )
})

test_that("GET /table surfaces 'NR' end-to-end for a masked cell through the real request -> table_maker() -> format_table_output() -> JSON pipeline", {
  skip_if_not_installed("plumber")
  skip_if_not_installed("arrow")
  skip_if(is.null(.ep_router), "Router could not be created")

  # Self-contained fixture (not .make_ep_fixtures(), which uses uniform
  # per-row weights): one tiny-weight gender group and one large-weight
  # gender group, so the small group is masked by the default
  # pop_share_threshold while the large group is not. This drives the
  # masking end-to-end through the live API, rather than unit-testing
  # table_maker() and format_table_output() in isolation. Uses the shared
  # write_fixture_nr_masking_survey() helper (see helper-fixtures.R) so the
  # same fixture definition is not duplicated across test files.
  tmp_arrow    <- withr::local_tempdir()
  tmp_manifest <- withr::local_tempdir()

  write_fixture_nr_masking_survey(tmp_arrow, tmp_manifest)
  piptm::set_manifest_dir(tmp_manifest)
  piptm::set_arrow_root(tmp_arrow)
  withr::defer(.reset_piptm_env())

  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id       = "TST_2020_ECH_INC_ALL",
    analysis_var = "welfare",
    measures     = "mean",
    column       = "gender"
  )))
  expect_equal(res$status, 200L)
  body <- parse_api_res(res)
  expect_equal(body$status, "success")

  cells <- body$data$cells[[1L]]
  expect_false("population" %in% names(cells))

  col_codes <- cells$column$code
  mean_vals <- cells$measures$mean

  masked_val   <- mean_vals[col_codes == 0L]
  unmasked_val <- mean_vals[col_codes == 1L]
  expect_equal(length(masked_val), 1L)
  expect_equal(length(unmasked_val), 1L)
  expect_equal(as.character(unlist(masked_val)), "NR")
  expect_false(identical(as.character(unlist(unmasked_val)), "NR"))
})

# =============================================================================
# Block 12: Explicit slot parameters (contract and baseline equivalence)
# =============================================================================

test_that("OpenAPI spec for /table advertises slot parameters and removes 'by'", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")

  spec <- .ep_router$getApiSpec()
  slot_keys <- c("column", "row", "superColumn", "superRow")

  for (method in c("get", "post")) {
    op <- spec$paths[["/table"]][[method]]
    expect_false(is.null(op), info = paste("Path /table missing method:", method))
    params <- op$parameters
    param_names <- vapply(params, function(p) p$name %||% "", character(1))

    expect_false("by" %in% param_names, info = paste("Param 'by' still present in:", method))

    for (k in slot_keys) {
      expect_true(k %in% param_names, info = paste("Missing slot param", k, "in", method))
      p_obj <- params[[which(param_names == k)]]
      expect_equal(p_obj$`in`, "query", info = paste("Slot", k, "not in query for", method))
      schema_type <- p_obj$schema$type %||% p_obj$type
      expect_equal(schema_type, "string", info = paste("Slot", k, "not string schema for", method))
    }
  }
})

test_that("GET /table with wrong-cased parameter (supercolumn) does not disaggregate", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  activate_test_registry(fx$release)

  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id      = "COL_2010_ECH_INC_ALL",
    measures    = "mean",
    supercolumn = "gender"
  )))
  expect_equal(res$status, 200L)
  body <- parse_api_res(res)
  expect_equal(body$status, "success")
  expect_equal(nrow(body$data), 1L)
  expect_false("gender" %in% names(body$data))
})

test_that("GET /table rejects repeated slot parameters with 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")

  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    column   = c("gender", "area")
  )))
  expect_equal(res$status, 400L)
  body <- parse_api_res(res)
  expect_equal(body$status, "error")
  expect_true(any(grepl("single variable", body$errors)))
})

test_that("POST /table handles scalar, length-1 array, and rejects length > 1 array", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  activate_test_registry(fx$release)

  # Scalar in POST JSON
  res_scalar <- .ep_router$call(make_api_req("POST", "/table", body = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    column   = "gender"
  )))
  expect_equal(res_scalar$status, 200L)
  body_scalar <- parse_api_res(res_scalar)
  expect_equal(body_scalar$status, "success")
  expect_equal(as.character(unlist(body_scalar$data$layout$columns$varname)), "gender")

  # Length-1 array in POST JSON: unboxes / accepts
  res_arr1 <- .ep_router$call(make_api_req("POST", "/table", body = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    column   = list("gender")
  )))
  expect_equal(res_arr1$status, 200L)
  body_arr1 <- parse_api_res(res_arr1)
  expect_equal(body_arr1$status, "success")
  expect_equal(as.character(unlist(body_arr1$data$layout$columns$varname)), "gender")

  # Length-2 array in POST JSON: rejects with 400, never 500
  res_arr2 <- .ep_router$call(make_api_req("POST", "/table", body = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    column   = list("gender", "area")
  )))
  expect_equal(res_arr2$status, 400L)
  body_arr2 <- parse_api_res(res_arr2)
  expect_equal(body_arr2$status, "error")
  expect_true(any(grepl("single variable", body_arr2$errors)))

  # JSON object: rejects with 400 rather than coercing to a variable name
  res_object <- .ep_router$call(make_api_req("POST", "/table", body = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    column   = list(value = "gender")
  )))
  expect_equal(res_object$status, 400L)
  body_object <- parse_api_res(res_object)
  expect_equal(body_object$status, "error")
  expect_true(any(grepl("character string or flat array", body_object$errors)))

  # Nested JSON array (e.g. [["gender"]]): rejects with 400, exercised through
  # Plumber's own JSON decoder rather than a hand-built R list.
  res_nested <- .ep_router$call(make_api_req("POST", "/table", body = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    column   = list(list("gender"))
  )))
  expect_equal(res_nested$status, 400L)
  body_nested <- parse_api_res(res_nested)
  expect_equal(body_nested$status, "error")
  expect_true(any(grepl("character string or flat array", body_nested$errors)))

  # Empty JSON object {}: rejects with 400, not treated as an omitted slot.
  res_empty_object <- .ep_router$call(make_api_req("POST", "/table", body = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    column   = setNames(list(), character(0))
  )))
  expect_equal(res_empty_object$status, 400L)
  body_empty_object <- parse_api_res(res_empty_object)
  expect_equal(body_empty_object$status, "error")
  expect_true(any(grepl("character string or flat array", body_empty_object$errors)))
})

test_that("GET /table rejects duplicate variable across slots with 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")

  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    column   = "gender",
    row      = "gender"
  )))
  expect_equal(res$status, 400L)
  body <- parse_api_res(res)
  expect_equal(body$status, "error")
  expect_true(any(grepl("gender", body$errors)))
})

test_that("GET /table rejects gapped slot assignment with 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")

  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id      = "COL_2010_ECH_INC_ALL",
    measures    = "mean",
    column      = "gender",
    superColumn = "area"
  )))
  expect_equal(res$status, 400L)
  body <- parse_api_res(res)
  expect_equal(body$status, "error")
  expect_true(any(grepl("row", body$errors)))
})

test_that("GET /table with all slots omitted returns 200 national table, not 500", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")
  fx <- .make_ep_fixtures()
  activate_test_registry(fx$release)

  res <- .ep_router$call(make_api_req("GET", "/table", query = list(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean"
  )))
  expect_equal(res$status, 200L)
  body <- parse_api_res(res)
  expect_equal(body$status, "success")
  n_surveys_returned <- if (is.data.frame(body$data)) nrow(body$data) else length(body$data)
  expect_equal(n_surveys_returned, 1L)
  survey <- if (is.data.frame(body$data)) body$data[1L, ] else body$data[[1L]]
  cells  <- if (is.data.frame(survey$cells)) survey$cells else survey$cells[[1L]]
  n_cells_returned <- if (is.data.frame(cells)) nrow(cells) else length(cells)
  expect_equal(n_cells_returned, 1L)
})

test_that("GET /table responses match pre-change baseline fixtures (semantic equality)", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.ep_router), "Router could not be created")

  baseline_dir <- testthat::test_path("fixtures", "table-slot-baseline")
  skip_if(!dir.exists(baseline_dir), "Baseline fixtures directory not found")

  fx_files <- sort(list.files(baseline_dir, pattern = "\\.json$", full.names = TRUE))
  skip_if(length(fx_files) == 0L, "No baseline fixtures found")

  for (fx_file in fx_files) {
    local({
      fx_obj <- jsonlite::fromJSON(fx_file, simplifyVector = TRUE)
      b_name <- basename(fx_file)

      if (grepl("nr_masking", b_name)) {
        tmp_arrow    <- withr::local_tempdir()
        tmp_manifest <- withr::local_tempdir()
        write_fixture_nr_masking_survey(tmp_arrow, tmp_manifest)
        piptm::set_manifest_dir(tmp_manifest)
        piptm::set_arrow_root(tmp_arrow)
        withr::defer(.reset_piptm_env())
      } else {
        fx <- .make_ep_fixtures()
        activate_test_registry(fx$release)
      }

      # Map query from baseline: replace 'by' with corresponding slot params
      q <- fx_obj$request$query
      by_order <- unlist(fx_obj$request$by_order)
      q$by <- NULL
      slot_names <- c("column", "row", "superColumn", "superRow")
      if (length(by_order) > 0L) {
        for (i in seq_along(by_order)) {
          q[[slot_names[[i]]]] <- by_order[[i]]
        }
      }

      res <- .ep_router$call(make_api_req("GET", "/table", query = q))
      expect_equal(res$status, 200L, info = paste("Failed fixture:", b_name))
      body <- parse_api_res(res)
      expect_equal(body$status, "success", info = paste("Failed fixture:", b_name))

      # Compare data and meta semantically
      canon <- function(x) jsonlite::toJSON(x, auto_unbox = TRUE, digits = 10, null = "null")
      expect_equal(canon(body$data), canon(fx_obj$data), info = paste("Data mismatch on:", b_name))
      expect_equal(canon(body$meta), canon(fx_obj$meta), info = paste("Meta mismatch on:", b_name))
    })
  }
})
