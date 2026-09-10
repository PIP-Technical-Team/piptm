# Plumber API Router — Table Maker
#
# Plan: .cg-docs/plans/2026-05-11-api-service-plumber-v2.md (Steps 2–3)
#
# Architecture
# ------------
# Filters  (this file, top section)
#   2a.  source helpers
#   2b.  CORS filter
#   2c.  Global error handler   — always 500, never stack traces
#   2d.  Request logger
#
# Endpoints (this file, bottom section — see Step 3)
#   GET|POST /table
#   GET /lookup
#   GET /surveys
#   GET /countries
#   GET /regions
#   GET /surveys-ui
#   GET /releases
#   GET /analysis-variables
#   GET /dimensions
#   GET /categories
#   GET /covariates
#   POST /session/surveys
#   GET /session/:id/surveys
#   GET /statistics
#   GET /health

# ── 2a. Source helpers ────────────────────────────────────────────────────────
# Use system.file() so the path is correct whether the package is installed or
# loaded via devtools::load_all().  Do NOT replace with a relative path —
# source("helpers.R") breaks if plumb() is called from any directory other
# than inst/plumber/.  Sourced once at router startup.

# Fail fast: check plumber version before sourcing helpers, so a version
# mismatch produces an actionable error rather than one buried after source().
if (utils::packageVersion("plumber") < "1.1.0") {
  stop(
    "plumber >= 1.1.0 is required (for @plumber decorator / setErrorHandler). ",
    "Found: ", utils::packageVersion("plumber")
  )
}

.helpers_path <- system.file("plumber", "helpers.R", package = "piptm")
if (!nzchar(.helpers_path)) {
  stop(
    "Could not locate inst/plumber/helpers.R. ",
    "Ensure piptm is installed or loaded via devtools::load_all() ",
    "before starting the API."
  )
}
source(.helpers_path)

# ── 2b. CORS filter ───────────────────────────────────────────────────────────

#* @filter cors
function(req, res) {
  res$setHeader("Access-Control-Allow-Origin",  "*")
  res$setHeader(
    "Access-Control-Allow-Methods",
    "GET, POST, OPTIONS"
  )
  res$setHeader(
    "Access-Control-Allow-Headers",
    "Content-Type"
  )

  # Handle OPTIONS preflight — return 204 No Content.
  # Must be 204 (not 200) with an empty body; Chrome >= 124 rejects 200/{}
  # preflight responses for cross-origin requests.
  if (identical(req$REQUEST_METHOD, "OPTIONS")) {
    res$status <- 204L
    return("")
  }

  plumber::forward()
}

# ── 2c. Global error handler ──────────────────────────────────────────────────
# Safety net: only fires for errors that escape all handler-level
# capture_with_warnings() blocks — i.e., genuine bugs.
# ALWAYS returns 500. Never echoes stack traces or internal details.

#* @plumber
function(pr) {
  pr$setErrorHandler(function(req, res, err) {
    res$status <- 500L
    list(
      status   = "error",
      data     = NULL,
      warnings = character(),
      errors   = "Internal server error.",
      meta     = list()
    )
  })
}

# ── 2d. Request logger ────────────────────────────────────────────────────────

#* @filter logger
function(req) {
  start <- proc.time()[["elapsed"]]
  plumber::forward()
  # Note: if the global errorHandler fires for an uncaught exception, the
  # lines below may not execute for that request.  500-level errors are
  # therefore not guaranteed to appear in the log.
  elapsed <- round(proc.time()[["elapsed"]] - start, 3L)
  message(
    format(Sys.time(), "%Y-%m-%dT%H:%M:%S"),
    " ", req$REQUEST_METHOD,
    " ", req$PATH_INFO,
    " [", elapsed, "s]"
  )
}

# =============================================================================
# Endpoints (Step 3)
# =============================================================================

# ── GET|POST /table ───────────────────────────────────────────────────────────

#* Compute poverty, inequality, and welfare measures for one or more surveys.
#*
#* @param analysis_var:character Analysis variable name (required).
#* @param pip_id:[character] Survey identifiers (required, repeatable; max 15).
#* @param measures:[character] Measure names (required, repeatable).
#* @param poverty_line:numeric Poverty line value (required when
#*   `analysis_var = "pov_status"`).
#* @param by:[character] Disaggregation dimensions (optional, repeatable).
#* @param ppp:integer PPP reference year (optional; default `2021`). Must be a
#*   positive integer; invalid values return HTTP 400.
#* @param filter_base:character JSON-encoded sample-base filter object
#*   (optional), for example `{"gender":[0],"age_group":[1,2]}`.
#* @param pop_share_threshold:numeric Optional cell-suppression threshold
#*   (default `0.01`). Use null/empty to disable suppression.
#* @param release:character Release ID (optional; defaults to current release).
#* @serializer json list(na = "null")
#* @get /table
#* @post /table
function(analysis_var = NULL, pip_id = NULL, measures = NULL, poverty_line = NULL, by = NULL,
         ppp = 2021L, filter_base = NULL, pop_share_threshold = 0.01,
         release = NULL, res) {

  check <- validate_table_input(
    analysis_var = analysis_var,
    pip_id = pip_id,
    measures = measures,
    poverty_line = poverty_line,
    by = by,
    ppp = ppp,
    pop_share_threshold = pop_share_threshold
  )
  if (!check$valid) return(api_error(check$errors, 400L, res))
  poverty_line        <- check$poverty_line
  ppp                 <- check$ppp
  pop_share_threshold <- check$pop_share_threshold

  out <- capture_with_warnings({
    parsed_filter_base <- if (is.null(filter_base)) {
      NULL
    } else {
      jsonlite::fromJSON(filter_base)
    }

    rel  <- resolve_release(release)
    data <- piptm::table_maker(
      pip_id        = pip_id,
      analysis_var  = analysis_var,
      measures      = measures,
      poverty_line  = poverty_line,
      by            = by,
      ppp           = ppp,
      filter_base   = parsed_filter_base,
      release       = rel,
      pop_share_threshold = pop_share_threshold
    )
    list(data = data, rel = rel)
  })
  if (!is.null(out$error)) return(api_error(out$error, 422L, res))

  api_response(
    out$result$data,
    warnings = out$warnings,
    meta = list(
      release   = out$result$rel,
      n_surveys = tryCatch(
        data.table::uniqueN(out$result$data, by = "pip_id"),
        error = function(e) NA_integer_
      )
    )
  )
}

# ── GET /lookup ───────────────────────────────────────────────────────────────

#* Resolve `country_code`/`year`/`welfare_type` combinations to `pip_id` values.
#*
#* @param country_code:[character] ISO3 country codes (repeatable).
#* @param year:[integer] Survey years (repeatable).
#* @param welfare_type:[character] Welfare type (`INC` or `CON`, repeatable).
#* @param release:character Release ID (optional; defaults to current release).
#* @serializer json list(na = "null")
#* @get /lookup
function(country_code = NULL, year = NULL, welfare_type = NULL, release = NULL, res) {
  check <- validate_lookup_input(country_code, year, welfare_type)
  if (!check$valid) return(api_error(check$errors, 400L, res))
  year <- check$year

  out <- capture_with_warnings({
    rel  <- resolve_release(release)
    data <- piptm::pip_lookup(country_code, year, welfare_type, rel)
    list(data = data, rel = rel)
  })
  if (!is.null(out$error)) return(api_error(out$error, 422L, res))

  api_response(
    out$result$data,
    warnings = out$warnings,
    meta = list(release = out$result$rel)
  )
}

# ── GET /surveys ──────────────────────────────────────────────────────────────

#* Return the full survey manifest for a release.
#*
#* The `dimensions` field is serialized as a JSON array in each row.
#*
#* @param release:character Release ID (optional; defaults to current release).
#* @serializer json list(na = "null")
#* @get /surveys
function(release = NULL, res) {
  out <- capture_with_warnings({
    rel  <- resolve_release(release)
    data <- piptm::piptm_manifest(rel)
    list(data = data, rel = rel)
  })
  if (!is.null(out$error)) return(api_error(out$error, 422L, res))

  api_response(
    out$result$data,
    warnings = out$warnings,
    meta = list(release = out$result$rel)
  )
}

# ── GET /countries ────────────────────────────────────────────────────────────

#* Return unique country metadata from the release manifest.
#*
#* @param release:character Release ID (optional; defaults to current release).
#* @serializer json list(na = "null")
#* @get /countries
function(release = NULL, res) {
  out <- capture_with_warnings({
    rel <- resolve_release(release)
    manifest <- piptm::piptm_manifest(rel)

    data <- unique(
      manifest[!is.na(country_code), .(country_code, country_name)]
    )
    data.table::setorder(data, country_code)

    list(data = data, rel = rel)
  })
  if (!is.null(out$error)) return(api_error(out$error, 422L, res))

  api_response(
    out$result$data,
    warnings = out$warnings,
    meta = list(release = out$result$rel)
  )
}

# ── GET /regions ──────────────────────────────────────────────────────────────

#* Return region metadata and member country codes from the release manifest.
#*
#* @param release:character Release ID (optional; defaults to current release).
#* @serializer json list(na = "null")
#* @get /regions
function(release = NULL, res) {
  out <- capture_with_warnings({
    rel <- resolve_release(release)
    manifest <- piptm::piptm_manifest(rel)

    data <- manifest[!is.na(region_code), .(
      countries = list(sort(unique(country_code[!is.na(country_code)])))
    ), by = .(region_code, region_name)]

    data.table::setorder(data, region_code)

    list(data = data, rel = rel)
  })
  if (!is.null(out$error)) return(api_error(out$error, 422L, res))

  api_response(
    out$result$data,
    warnings = out$warnings,
    meta = list(release = out$result$rel)
  )
}

# ── GET /surveys-ui ───────────────────────────────────────────────────────────

#* Return a UI-ready survey catalog (`pip_id`, labels, and dimensions).
#*
#* @param release:character Release ID (optional; defaults to current release).
#* @serializer json list(na = "null")
#* @get /surveys-ui
function(release = NULL, res) {
  out <- capture_with_warnings({
    rel  <- resolve_release(release)
    data <- piptm::piptm_surveys_ui(rel)
    list(data = data, rel = rel)
  })
  if (!is.null(out$error)) return(api_error(out$error, 422L, res))

  api_response(
    out$result$data,
    warnings = out$warnings,
    meta = list(release = out$result$rel)
  )
}

# ── GET /releases ─────────────────────────────────────────────────────────────

#* List loaded releases and identify the current release.
#*
#* @serializer json list(na = "null")
#* @get /releases
function() {
  api_response(list(
    releases = names(piptm::piptm_manifests()),
    current  = piptm::piptm_current_release()
  ))
}

# ── GET /analysis-variables ───────────────────────────────────────────────────

#* Return analysis variables for Decision 2.
#*
#* Includes `varname`, `label`, `type`, `poverty_line_slider`, and
#* `stat_groups` for registry entries with role `analysis_var`.
#*
#* @param release:character Release ID (optional; defaults to current).
#* @serializer json list(na = "null")
#* @get /analysis-variables
function(release = NULL, res) {
  out <- capture_with_warnings({
    rel  <- resolve_release(release)
    data <- piptm::piptm_analysis_variables(rel)
    list(data = data, rel = rel)
  })
  if (!is.null(out$error)) return(api_error(out$error, 422L, res))
  api_response(out$result$data, warnings = out$warnings,
               meta = list(release = out$result$rel))
}

# ── GET /dimensions ───────────────────────────────────────────────────────────

#* List all valid disaggregation dimensions.
#*
#* @serializer json list(na = "null")
#* @get /dimensions
function() {
  api_response(piptm::pip_valid_dimensions())
}

# ── GET /categories ──────────────────────────────────────────────────────────

#* Return categorical variables for the sample-base filter panel.
#*
#* Behavior:
#* - If `pip_id` is omitted, returns the full category catalog.
#* - If `pip_id` is provided, returns only variables present in all matched
#*   surveys (intersection over manifest `dimensions`).
#*
#* @param pip_id:[character] Optional survey identifiers (repeatable).
#* @param release:character Release ID (optional; defaults to current).
#* @serializer json list(na = "null")
#* @get /categories
function(pip_id = NULL, release = NULL, res) {
  out <- capture_with_warnings({
    rel  <- resolve_release(release)
    data <- piptm::piptm_filter_categories(rel)

    if (is.null(pip_id) || length(pip_id) == 0L) {
      result <- list(
        data = data,
        rel = rel,
        filtered = FALSE,
        n_surveys = NA_integer_
      )
    } else {
      mf <- piptm::piptm_manifest(rel)
      requested_ids <- unique(as.character(unlist(pip_id, use.names = FALSE)))
      matched <- mf[pip_id %chin% requested_ids]

      if (nrow(matched) == 0L) {
        result <- list(
          data = list(),
          rel = rel,
          filtered = TRUE,
          n_surveys = 0L
        )
      } else {
        dims_list <- lapply(
          matched$dimensions,
          function(x) as.character(unlist(x, use.names = FALSE))
        )
        common_dims <- if (length(dims_list) == 0L) {
          character(0L)
        } else {
          Reduce(intersect, dims_list)
        }

        filtered_data <- if (length(common_dims) == 0L) {
          list()
        } else {
          Filter(function(x) isTRUE(x$varname %in% common_dims), data)
        }

        result <- list(
          data = filtered_data,
          rel = rel,
          filtered = TRUE,
          n_surveys = nrow(matched)
        )
      }
    }

    result
  })
  if (!is.null(out$error)) return(api_error(out$error, 422L, res))
  api_response(out$result$data, warnings = out$warnings,
               meta = list(
                 release = out$result$rel,
                 filtered = out$result$filtered,
                 n_surveys = out$result$n_surveys
               ))
}

# ── GET /covariates ───────────────────────────────────────────────────────────

#* Return layout covariates for the Decision 3 slicing panel.
#*
#* Behavior:
#* - If `pip_id` is omitted, returns the full covariate catalog.
#* - If `pip_id` is provided, returns only covariates present in all matched
#*   surveys (intersection over manifest `dimensions`).
#*
#* @param pip_id:[character] Optional survey identifiers (repeatable).
#* @param release:character Release ID (optional; defaults to current).
#* @serializer json list(na = "null")
#* @get /covariates
function(pip_id = NULL, release = NULL, res) {
  out <- capture_with_warnings({
    rel  <- resolve_release(release)
    data <- piptm::piptm_layout_covariates(rel)

    if (is.null(pip_id) || length(pip_id) == 0L) {
      result <- list(
        data = data,
        rel = rel,
        filtered = FALSE,
        n_surveys = NA_integer_
      )
    } else {
      mf <- piptm::piptm_manifest(rel)
      requested_ids <- unique(as.character(unlist(pip_id, use.names = FALSE)))
      matched <- mf[pip_id %chin% requested_ids]

      if (nrow(matched) == 0L) {
        result <- list(
          data = list(),
          rel = rel,
          filtered = TRUE,
          n_surveys = 0L
        )
      } else {
        dims_list <- lapply(
          matched$dimensions,
          function(x) as.character(unlist(x, use.names = FALSE))
        )
        common_dims <- if (length(dims_list) == 0L) {
          character(0L)
        } else {
          Reduce(intersect, dims_list)
        }

        filtered_data <- if (length(common_dims) == 0L) {
          list()
        } else {
          Filter(function(x) isTRUE(x$varname %in% common_dims), data)
        }

        result <- list(
          data = filtered_data,
          rel = rel,
          filtered = TRUE,
          n_surveys = nrow(matched)
        )
      }
    }

    result
  })
  if (!is.null(out$error)) return(api_error(out$error, 422L, res))
  api_response(out$result$data, warnings = out$warnings,
               meta = list(
                 release = out$result$rel,
                 filtered = out$result$filtered,
                 n_surveys = out$result$n_surveys
               ))
}

# ── POST /session/surveys ────────────────────────────────────────────────────

#* Store selected survey identifiers and return a session ID.
#*
#* Sessions are in-memory, process-local, and expire after one hour.
#*
#* @param pip_id:[character] Survey identifiers to store (required, repeatable).
#* @serializer json list(na = "null")
#* @post /session/surveys
function(pip_id = NULL, res) {
  check <- validate_session_pip_id(pip_id)
  if (!check$valid) return(api_error(check$errors, 400L, res))

  out <- capture_with_warnings({
    sid <- create_session(check$pip_id)
    list(session_id = sid)
  })

  if (!is.null(out$error)) return(api_error(out$error, 422L, res))

  api_response(out$result, warnings = out$warnings)
}

# ── GET /session/:id/surveys ─────────────────────────────────────────────────

#* Retrieve stored survey identifiers for a given session ID.
#*
#* @param id:character Session identifier.
#* @serializer json list(na = "null")
#* @get /session/<id>/surveys
function(id, res) {
  out <- capture_with_warnings({
    pip_id <- get_session_surveys(id)
    if (is.null(pip_id)) {
      cli::cli_abort("Session not found or expired.")
    }
    list(pip_id = pip_id)
  })

  if (!is.null(out$error)) return(api_error(out$error, 404L, res))

  api_response(out$result, warnings = out$warnings)
}


# ── GET /health ───────────────────────────────────────────────────────────────

#* Server health check; returns `status = "ok"` and current release.
#*
#* @serializer json list(na = "null")
#* @get /health
function() {
  api_response(list(
    status  = "ok",
    release = piptm::piptm_current_release()
  ))
}

# ── GET /statistics ───────────────────────────────────────────────────────────

#* Return statistical measure groups and their measures.
#*
#* Measure definitions are loaded from the release registry built from
#* `inst/extdata/tm_measure_spec.yaml`.
#*
#* @param release:character Release ID (optional; defaults to current release).
#* @serializer json list(na = "null")
#* @get /statistics
function(release = NULL, res) {
  out <- capture_with_warnings({
    rel  <- resolve_release(release)
    data <- piptm::piptm_stat_groups(rel)
    list(data = data, rel = rel)
  })
  if (!is.null(out$error)) return(api_error(out$error, 422L, res))
  api_response(out$result$data, warnings = out$warnings,
               meta = list(release = out$result$rel))
}
