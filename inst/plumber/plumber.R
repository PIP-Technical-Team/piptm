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
#* @param pip_id:character Survey identifiers (max 15, repeatable)
#* @param measures:character Measure names (repeatable)
#* @param poverty_lines:numeric Poverty line values (optional, repeatable)
#* @param by:character Disaggregation dimensions (optional, repeatable)
#* @param ppp:integer PPP reference year (e.g. 2021; optional). Must be a
#*   positive whole number. When omitted, defaults to 2021. Non-integer or
#*   non-positive values return HTTP 400.
#* @param pop_share_threshold:numeric Population share threshold for cell
#*   suppression (optional; default 0.01). Set to empty/null to disable.
#* @param release:character Release ID (optional; defaults to current release)
#* @serializer json list(na = "null")
#* @get /table
#* @post /table
function(pip_id = NULL, measures = NULL, poverty_lines = NULL, by = NULL,
         ppp = 2021L, pop_share_threshold = 0.01, release = NULL, res) {

  check <- validate_table_input(pip_id, measures, poverty_lines, by, ppp,
                                pop_share_threshold)
  if (!check$valid) return(api_error(check$errors, 400L, res))
  poverty_lines       <- check$poverty_lines
  ppp                 <- check$ppp
  pop_share_threshold <- check$pop_share_threshold

  out <- capture_with_warnings({
    rel  <- resolve_release(release)
    data <- piptm::table_maker(
      pip_id        = pip_id,
      measures      = measures,
      poverty_lines = poverty_lines,
      by            = by,
      ppp           = ppp,
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


# ── GET /analysis variables ─────────────────────────────────────────────────────────────

# TODO

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
#* @serializer json list(na = "null")
#* @get /categories
function() {
  api_response(piptm::pip_tablemaker_categories())
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
#* @serializer json list(na = "null")
#* @get /covariates
function() {
  api_response(piptm::pip_tablemaker_covariates())
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
