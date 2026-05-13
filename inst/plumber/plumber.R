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
#   GET /health

# ── 2a. Source helpers ────────────────────────────────────────────────────────
# Use system.file() so the path is correct whether the package is installed or
# loaded via devtools::load_all().  Do NOT replace with a relative path —
# source("helpers.R") breaks if plumb() is called from any directory other
# than inst/plumber/.  Sourced once at router startup.

.helpers_path <- system.file("plumber", "helpers.R", package = "piptm")
if (!nzchar(.helpers_path)) {
  stop(
    "Could not locate inst/plumber/helpers.R. ",
    "Ensure piptm is installed or loaded via devtools::load_all() ",
    "before starting the API."
  )
}
source(.helpers_path)

# Require plumber >= 1.1.0 for @plumber decorator and pr$setErrorHandler().
if (utils::packageVersion("plumber") < "1.1.0") {
  stop(
    "plumber >= 1.1.0 is required (for @plumber decorator / setErrorHandler). ",
    "Found: ", utils::packageVersion("plumber")
  )
}

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
#* @param release:character Release ID (optional; defaults to current release)
#* @serializer json list(na = "null")
#* @get /table
#* @post /table
function(pip_id, measures, poverty_lines = NULL, by = NULL,
         release = NULL, res) {

  check <- validate_table_input(pip_id, measures, poverty_lines, by)
  if (!check$valid) return(api_error(check$errors, 400L, res))
  poverty_lines <- check$poverty_lines

  out <- capture_with_warnings({
    rel  <- resolve_release(release)
    data <- piptm::table_maker(
      pip_id        = pip_id,
      measures      = measures,
      poverty_lines = poverty_lines,
      by            = by,
      release       = rel
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
function(country_code, year, welfare_type, release = NULL, res) {
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

#* List all available measure names and their families
#*
#* @serializer json list(na = "null")
#* @get /measures
function() {
  m <- piptm::pip_measures()
  api_response(data.table::data.table(
    measure = names(m),
    family  = unname(m)
  ))
}

# ── GET /dimensions ───────────────────────────────────────────────────────────

#* List all valid disaggregation dimension names
#*
#* @serializer json list(na = "null")
#* @get /dimensions
function() {
  api_response(piptm:::.VALID_DIMENSIONS)
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
