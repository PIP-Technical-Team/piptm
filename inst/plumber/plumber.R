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
#   GET /releases
#   GET /measures
#   GET /dimensions
#   GET /categories
#   GET /covariates
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

#* Compute poverty, inequality, and welfare measures for one or more surveys
#*
#* @param analysis_var:character Analysis variable name (required)
#* @param pip_id:character Survey identifiers (max 15, repeatable)
#* @param measures:character Measure names (repeatable)
#* @param poverty_line:numeric Single poverty line value (required when analysis_var is "pov_status")
#* @param by:character Disaggregation dimensions (optional, repeatable)
#* @param ppp:integer PPP reference year (e.g. 2021; optional). Must be a
#*   positive whole number. When omitted, defaults to 2021. Non-integer or
#*   non-positive values return HTTP 400.
#* @param filter_base:character JSON-encoded sample-base filter object
#*   (optional), e.g. {"gender":[0],"age_group":[1,2]}
#* @param pop_share_threshold:numeric Population share threshold for cell
#*   suppression (optional; default 0.01). Set to empty/null to disable.
#* @param release:character Release ID (optional; defaults to current release)
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

#* Resolve country/year/welfare_type triplets to pip_ids
#*
#* @param country_code:character ISO3 country codes (repeatable)
#* @param year:integer Survey years (repeatable)
#* @param welfare_type:character Welfare type: INC or CON (repeatable)
#* @param release:character Release ID (optional; defaults to current release)
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

#* Return the full survey manifest for a release
#*
#* The `dimensions` field in each row is serialised as a JSON array.
#*
#* @param release:character Release ID (optional; defaults to current release)
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

#* Return unique country metadata from the release manifest
#*
#* @param release:character Release ID (optional; defaults to current release)
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

#* Return region metadata with member country codes from the release manifest
#*
#* @param release:character Release ID (optional; defaults to current release)
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

#* Return UI-friendly survey catalogue (pip_id, country label, dimensions, etc.)
#*
#* @param release:character Release ID (optional; defaults to current release)
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

#* List all loaded release IDs and identify the current release
#*
#* @serializer json list(na = "null")
#* @get /releases
function() {
  api_response(list(
    releases = names(piptm::piptm_manifests()),
    current  = piptm::piptm_current_release()
  ))
}

# ── GET /measures ─────────────────────────────────────────────────────────────

# TODO read from measure registry 


# ── GET /analysis-variables ─────────────────────────────────────────────────────────────

#* Return the catalogue of analysis variables for Decision 2
#*
#* Filters the variable registry to entries with role "analysis_var" and
#* returns varname, label, type, poverty_line_slider, and stat_groups for
#* each. The UI uses this to populate the analysis variable dropdown and
#* to know which statistics are valid for each variable.
#*
#* @param release:character Release ID (optional; defaults to current)
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

#* List all valid disaggregation dimension names
#*
#* @serializer json list(na = "null")
#* @get /dimensions
function() {
  api_response(piptm::pip_valid_dimensions())
}

# ── GET /categories ──────────────────────────────────────────────────────────

#* Return the full catalogue of categorical variables for the sample-base
#* filter panel
#*
#* Static endpoint — always returns the complete universe of filterable
#* categorical variables regardless of which surveys are selected.  The UI
#* renders each variable as a filter with toggleable subcategory chips (all
#* selected by default); multiple active filters are combined as intersections
#* at query time.
#*
#* @param release:character Release ID (optional; defaults to current)
#* @serializer json list(na = "null")
#* @get /categories
function(release = NULL, res) {
  out <- capture_with_warnings({
    rel  <- resolve_release(release)
    data <- piptm::piptm_filter_categories(rel)
    list(data = data, rel = rel)
  })
  if (!is.null(out$error)) return(api_error(out$error, 422L, res))
  api_response(out$result$data, warnings = out$warnings,
               meta = list(release = out$result$rel))
}

# ── GET /covariates ───────────────────────────────────────────────────────────

#* Return the full catalogue of layout covariates for the Decision 3 table
#* slicing panel
#*
#* Static endpoint — always returns the complete universe of covariates
#* available for the four layout slots (Columns, Rows, Super Columns,
#* Super Rows) regardless of which surveys are selected.  Each entry carries
#* the covariate varname, its UI label, and the number of categories it
#* produces in the table layout.  The UI displays the category count next to
#* the covariate name when it is assigned to a slot.
#*
#* The catalogue is derived from pip_tablemaker_categories() with the
#* addition of pov_status (Poverty status), which is specific to Decision 3.
#* Poverty status is subject to a mutual exclusivity rule with the Decision 2
#* poverty analysis variable — the UI enforces this using the varname field.
#*
#* @param release:character Release ID (optional; defaults to current)
#* @serializer json list(na = "null")
#* @get /covariates
function(release = NULL, res) {
  out <- capture_with_warnings({
    rel  <- resolve_release(release)
    data <- piptm::piptm_layout_covariates(rel)
    list(data = data, rel = rel)
  })
  if (!is.null(out$error)) return(api_error(out$error, 422L, res))
  api_response(out$result$data, warnings = out$warnings,
               meta = list(release = out$result$rel))
}


# ── GET /health ───────────────────────────────────────────────────────────────

#* Server health check — returns status ok and the current release
#*
#* @serializer json list(na = "null")
#* @get /health
function() {
  api_response(list(
    status  = "ok",
    release = piptm::piptm_current_release()
  ))
}


#' GET /statistics
#'
#' Returns the full list of statistical measure groups and their measures
#'
#' The measure specification is embedded in the release registry at build time
#' (via [piptm::build_variable_registry()]) from the
#' `inst/extdata/tm_measure_spec.yaml` file. Adding or modifying measures
#' requires updating that file and rebuilding the registry.
#'
#' @section Response shape:
#' ```json
#' {
#'   "status": ["success"],
#'   "data": [
#'     {
#'       "group": ["summary_statistics"],
#'       "group_label": ["Summary Statistics"],
#'       "measures": [
#'         { "measure": ["mean"], "label": ["Mean"] },
#'         ...
#'       ]
#'     },
#'     ...
#'   ],
#'   "warnings": [],
#'   "errors": [],
#'   "meta": { "release": ["20260401_TEST"] }
#' }
#' ```
#'
#' @param release Optional. Character scalar release identifier
#'   (e.g. `"20260401_TEST"`). When `NULL` (default), the current active
#'   release is resolved automatically via `piptm::piptm_current_release()`.
#' @param res Plumber response object — injected automatically by the router.
#'
#' @return A JSON response via [api_response()], or an error response via
#'   [api_error()] with HTTP 422 when the release cannot be resolved or the
#'   measure spec is absent from the registry.
#'
#' @seealso [piptm::piptm_stat_groups()], [piptm::build_variable_registry()]
#'
#' @examples
#' \dontrun{
#' # Plumber route registration
#' pr$handle("GET", "/statistics", function(release = NULL, res) { ... })
#' }

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
