# Plumber API Router — Table Maker
#
# Plan: .cg-docs/plans/2026-05-11-api-service-plumber-v2.md (Steps 2–3)
#
# Architecture
# ------------
# Filters  (this file, top section)
#   2a.  source helpers
#   2b.  CORS filter
#   2c.  Global error handler + OpenAPI spec enrichment
#   2d.  Request logger
#
# Endpoints (this file, bottom section — see Step 3)
#   GET|POST /table
#   POST /table/download
#   POST /metadata
#   GET /cell-definition
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
#   GET /statistics
#   GET /health

#* @apiTitle PIP Table Maker API
#* @apiVersion 0.1.0
#* @apiDescription REST API for computing cross-tabulated poverty, inequality, welfare, and population/target share indicators from harmonized World Bank Poverty and Inequality Platform (PIP) household survey microdata.

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

# ── 2c. Global error handler & OpenAPI specification enrichment ──────────────
# Safety net: error handler only fires for errors that escape all handler-level
# capture_with_warnings() blocks — i.e., genuine bugs.
# ALWAYS returns 500. Never echoes stack traces or internal details.

enrich_openapi_spec <- function(spec) {
  pkg_ver <- tryCatch(
    as.character(utils::packageVersion("piptm")),
    error = function(e) "0.1.0"
  )

  spec$info <- list(
    title   = "PIP Table Maker API",
    version = pkg_ver,
    description = paste(
      "REST API for computing cross-tabulated poverty, inequality, summary-statistic,",
      "and population/target share indicators from harmonized World Bank Poverty and",
      "Inequality Platform (PIP) household survey microdata.\n\n",
      "### 3-Step Table Maker Workflow\n",
      "1. **Step 1 — Survey Selection**: Use `GET /countries`, `GET /regions`, and",
      "`GET /surveys-ui` to select between **1 and 15 surveys** (`pip_id`).\n",
      "2. **Step 2 — Table Configuration**:\n",
      "   - **Decision 1 (Sample Base Filters)**: Call `GET /categories?pip_id=...` to list",
      "categorical variables common to all selected surveys and build an optional `filter_base` JSON filter.\n",
      "   - **Decision 2 (Analysis Variable & Statistics)**: Call `GET /analysis-variables` and",
      "`GET /statistics` to choose one `analysis_var` and one or more compatible `measures`",
      "(plus `poverty_line` when analysis variable is poverty status).\n",
      "   - **Decision 3 (Layout Slots)**: Call `GET /covariates?pip_id=...` and assign up to",
      "4 disaggregation covariates in strict order: `column` -> `row` -> `superColumn` -> `superRow`.",
      "**Note**: When `\"pov_status\"` is selected as a layout covariate in any slot, the UI must activate the poverty line slider (`poverty_line`).\n",
      "   - **Live Cell Preview**: Call `GET /cell-definition` to render a three-section plain-English",
      "explanation of what each cell in the configured table represents.\n",
      "3. **Step 3 — Table Computation & Export**:\n",
      "   - **Compute Table**: Call `GET /table` or `POST /table` to compute cell estimates.\n",
      "   - **Table Metadata & Info**: Call `GET /metadata` or `POST /metadata` to download an HTML or Markdown",
      "metadata and provenance document for the configured table.\n",
      "   - **Download File**: Call `POST /table/download` to flatten the `/table` response `data`",
      "array into a downloadable `.csv` or `.rds` file.\n\n",
      "### Response Envelope & HTTP Status Codes\n",
      "All JSON endpoints return a uniform envelope:\n",
      "```json\n",
      "{\n",
      "  \"status\": \"success\",\n",
      "  \"data\": {},\n",
      "  \"warnings\": [],\n",
      "  \"errors\": [],\n",
      "  \"meta\": { \"release\": \"20260401_TEST\" }\n",
      "}\n",
      "```\n",
      "- **Exceptions**: `GET|POST /metadata` returns raw HTML (`text/html; charset=utf-8`) or",
      "Markdown (`text/plain`) on `200`; `POST /table/download` returns binary file bytes",
      "(`text/csv` or `application/octet-stream`) on `200`. On error, both return the standard",
      "JSON error envelope.\n",
      "- **HTTP Status Codes**: `200` (Success), `400` (Invalid parameter format, missing required",
      "parameter, or layout-slot rule violation), `413` (Payload > 25 MB on `/table/download`),",
      "`422` (Domain/computation failure such as unknown `release` or all surveys excluded by",
      "filters/covariates), `500` (Unexpected internal server error)."
    )
  )

  spec$tags <- list(
    list(name = "Step 1 — Survey Selection"),
    list(name = "Step 2 — Table Configuration"),
    list(name = "Step 3 — Table Computation & Export"),
    list(name = "System & Releases"),
    list(name = "Optional / Developer Endpoints (Not Needed for UI)")
  )

  valid_analysis_vars <- tryCatch(
    unique(c("welfare", "pov_status", piptm::pip_optional_dims())),
    error = function(e) c("welfare", "pov_status", "age", "educy", "hsize", "electricity", "imp_wat_rec", "imp_san_rec")
  )
  valid_measures <- tryCatch(
    names(piptm:::.MEASURE_REGISTRY),
    error = function(e) c("mean", "median", "gini", "headcount", "poverty_gap", "severity", "watts", "pop_share")
  )
  valid_dims <- tryCatch(
    piptm::pip_valid_dimensions(),
    error = function(e) c("gender", "area", "age_group", "educat4", "educat5", "educat7")
  )
  valid_slot_vars <- unique(c("pov_status", valid_dims))

  required_by_path <- list(
    "/table"           = c("analysis_var", "pip_id", "measures"),
    "/metadata"        = c("analysis_var", "pip_id", "measures"),
    "/cell-definition" = c("analysis_var", "measures"),
    "/lookup"          = c("country_code", "year", "welfare_type")
  )

  tag_by_path <- list(
    "/surveys-ui"         = "Step 1 — Survey Selection",
    "/countries"          = "Step 1 — Survey Selection",
    "/regions"            = "Step 1 — Survey Selection",
    "/categories"         = "Step 2 — Table Configuration",
    "/analysis-variables" = "Step 2 — Table Configuration",
    "/statistics"         = "Step 2 — Table Configuration",
    "/covariates"         = "Step 2 — Table Configuration",
    "/cell-definition"    = "Step 2 — Table Configuration",
    "/table"              = "Step 3 — Table Computation & Export",
    "/metadata"           = "Step 3 — Table Computation & Export",
    "/table/download"     = "Step 3 — Table Computation & Export",
    "/health"             = "System & Releases",
    "/releases"           = "System & Releases",
    "/lookup"             = "Optional / Developer Endpoints (Not Needed for UI)",
    "/surveys"            = "Optional / Developer Endpoints (Not Needed for UI)",
    "/dimensions"         = "Optional / Developer Endpoints (Not Needed for UI)"
  )

  enrich_param <- function(p, path) {
    nm <- p$name %||% ""
    req_names <- required_by_path[[path]] %||% character()
    p$required <- nm %in% req_names

    if (is.null(p$schema)) {
      p$schema <- list(type = "string")
    }

    if (identical(nm, "analysis_var")) {
      p$schema$type <- "string"
    } else if (identical(nm, "pip_id")) {
      p$schema$type <- "array"
      p$schema$items <- list(
        type    = "string",
        pattern = .PIP_ID_PATTERN
      )
      if (path %in% c("/table", "/metadata")) {
        p$schema$minItems <- 1L
        p$schema$maxItems <- .MAX_SURVEYS_PER_REQUEST
      }
    } else if (identical(nm, "measures")) {
      p$schema$type <- "array"
      p$schema$minItems <- 1L
      p$schema$items <- list(
        type = "string"
      )
    } else if (identical(nm, "poverty_line")) {
      p$schema$type <- "number"
      p$schema$format <- "double"
      p$schema$minimum <- 0
      p$schema$exclusiveMinimum <- TRUE
    } else if (nm %in% c("column", "row", "superColumn", "superRow")) {
      p$schema$type <- "string"
    } else if (identical(nm, "ppp")) {
      p$schema$type <- "integer"
      p$schema$default <- 2021L
      p$schema$minimum <- 1L
    } else if (identical(nm, "filter_base")) {
      p$schema$type <- "string"
    } else if (identical(nm, "pop_share_threshold")) {
      p$schema$type <- "number"
      p$schema$format <- "double"
      p$schema$default <- 0.001
      p$schema$minimum <- 0
      p$schema$maximum <- 1
      p$schema$exclusiveMinimum <- TRUE
      p$schema$exclusiveMaximum <- TRUE
    } else if (identical(nm, "release")) {
      p$schema$type <- "string"
    } else if (identical(nm, "format")) {
      p$schema$type <- "string"
      p$schema$default <- "html"
    } else if (identical(nm, "welfare_type")) {
      if (identical(path, "/lookup")) {
        p$schema$type <- "array"
        p$schema$minItems <- 1L
        p$schema$items <- list(
          type = "string"
        )
      } else {
        p$schema$type <- "string"
      }
    } else if (identical(nm, "country_code")) {
      p$schema$type <- "array"
      p$schema$minItems <- 1L
      p$schema$items <- list(
        type = "string"
      )
    } else if (identical(nm, "year")) {
      p$schema$type <- "array"
      p$schema$minItems <- 1L
      p$schema$items <- list(
        type = "integer"
      )
    }

    p
  }

  json_error_response <- function(desc) {
    list(
      description = desc,
      content = list(
        "application/json" = list(
          schema = list(
            type = "object",
            properties = list(
              status   = list(type = "string", enum = list("error"), example = "error"),
              data     = list(type = "object", nullable = TRUE),
              warnings = list(type = "array", items = list(type = "string")),
              errors   = list(type = "array", items = list(type = "string")),
              meta     = list(type = "object")
            )
          )
        )
      )
    )
  }

  summary_by_path <- list(
    "/table"              = "Compute cross-tabulated poverty, inequality, summary-statistic, and share measures for up to 15 surveys.",
    "/table/download"     = "Flatten a computed `/table` response payload into a downloadable CSV or RDS file.",
    "/metadata"           = "Render a downloadable structured table metadata and provenance document in HTML or Markdown.",
    "/lookup"             = "[Optional — Not needed in UI] Resolve `(country_code, year, welfare_type)` survey triples to canonical `pip_id` identifiers.",
    "/surveys"            = "[Optional — Not needed in UI] Return the full survey manifest table for a release, including internal file metadata.",
    "/countries"          = "Return all unique countries (`country_code` and `country_name`) available in the release manifest.",
    "/regions"            = "Return World Bank regional groupings (`region_code`, `region_name`) and their member `countries` arrays.",
    "/surveys-ui"         = "Return the lightweight, UI-ready survey catalog for populating the Step 1 survey selection grid.",
    "/releases"           = "List all loaded PIP data releases and identify the active default release.",
    "/analysis-variables" = "Return all valid `analysis_var` options for Decision 2, including compatible statistic groups and slider rules.",
    "/dimensions"         = "[Optional / WIP — Not needed in UI] Return the flat list of all valid disaggregation dimension identifiers.",
    "/categories"         = "Return categorical variables and category codes for the Decision 1 sample-base filter panel.",
    "/covariates"         = "Return disaggregation covariates and category labels for the Decision 3 table layout slots.",
    "/health"             = "Verify API server readiness and return `\"ok\"` status with the current default release ID.",
    "/statistics"         = "Return all statistical measure groups and their individual measures for Decision 2.",
    "/cell-definition"    = "Return a live three-section plain-English explanation of what each cell in the configured table represents."
  )

  description_by_path <- list(
    "/table" = paste(
      "**UI Workflow Mapping**: Step 2 (\"Generate table\") and Step 3 (Results view).\n\n",
      "Computes requested `measures` for `analysis_var` across one or more harmonized household surveys (`pip_id`),",
      "optionally restricted to a sub-population (`filter_base`) and disaggregated across up to four table layout slots (`column`, `row`, `superColumn`, `superRow`).\n\n",
      "### How to Use Parameters\n",
      "- **Required parameters**: Always supply `analysis_var`, at least one `pip_id` (1 to 15 surveys), and at least one `measures` value.\n",
      "- **Conditional `poverty_line`**: Required whenever `analysis_var = \"pov_status\"` or `\"pov_status\"` is placed in any layout slot (e.g., `poverty_line=2.15` for the $2.15/day extreme poverty line).\n",
      "- **Layout slots (`column`, `row`, `superColumn`, `superRow`)**: Optional disaggregation covariates from `GET /covariates`. Must be filled in strict order (`column` -> `row` -> `superColumn` -> `superRow`) with no gaps and no duplicate variables; `analysis_var` cannot also appear in a layout slot. Omit all four slots to compute a single national/aggregate cell per survey.\n",
      "- **Sample-base filter (`filter_base`)**: Optional JSON-encoded object mapping categorical variable names from `GET /categories` to arrays of retained category codes, e.g. `{\"gender\":[1],\"area\":[1]}`.\n",
      "- **Cell suppression (`pop_share_threshold`)**: Cells representing a population share below this threshold (default `0.001` = 0.1%) are masked as `\"NR\"` (Not Representative). Pass an empty value to disable suppression.\n\n",
      "### Example Request (Query String)\n",
      "`/table?analysis_var=welfare&pip_id=COL_2008_GEIH_INC_ALL&measures=mean&measures=gini&column=gender&row=area&ppp=2021`"
    ),
    "/table/download" = paste(
      "**UI Workflow Mapping**: Step 3 (\"Export / Download table\" button).\n\n",
      "Stateless converter that takes the `data` array returned by `GET /table` or `POST /table`",
      "(the Approach C `{layout, cells}` structure) and flattens it into a rectangular table",
      "(one row per cell, with measure values as columns) without re-querying Arrow microdata.\n\n",
      "### Request Body Fields (JSON Object)\n",
      "- `format` *(string, optional, default `\"csv\"`)*: Output file format — either `\"csv\"` (`text/csv; charset=utf-8` attachment `table.csv`) or `\"rds\"` (`application/octet-stream` attachment `table.rds`).\n",
      "- `data` *(array of objects, required)*: The non-empty `data` array copied directly from a `/table` response envelope (each element containing `pip_id`, `country_code`, `surveyid_year`, `welfare_type`, `analysis_var`, `measures`, `layout`, and `cells`). Do not wrap the outer `/table` envelope fields (`status`, `warnings`, `errors`, `meta`).\n",
      "- Maximum request body size is **25 MB** (larger payloads return HTTP `413`)."
    ),
    "/metadata" = paste(
      "**UI Workflow Mapping**: Step 3 (\"Download metadata\" button).\n\n",
      "Renders a structured 6-section table metadata, configuration summary, and data provenance document directly from the in-memory release manifest and variable registry (zero Arrow microdata I/O).\n\n",
      "### How to Use Parameters\n",
      "- Accepts the exact same table configuration parameters as `GET|POST /table` (`analysis_var`, `pip_id`, `measures`, and optional `poverty_line`, `column`, `row`, `superColumn`, `superRow`, `filter_base`, `ppp`, `pop_share_threshold`, `release`), plus `format` (`\"html\"` or `\"markdown\"`, default `\"html\"`).\n",
      "- On `GET /metadata`, pass `filter_base` as a JSON-encoded string (e.g., `{\"gender\":[1]}`); on `POST /metadata`, `filter_base` may be either a JSON string or a native nested JSON object.\n\n",
      "### Output Format & Download Headers\n",
      "- `format = \"html\"` (default): Returns inline-styled HTML with `Content-Type: text/html; charset=utf-8` and `Content-Disposition: attachment; filename=\"table-metadata.html\"`.\n",
      "- `format = \"markdown\"`: Returns plain Markdown text with `Content-Type: text/plain` and `Content-Disposition: attachment; filename=\"table-metadata.md\"`.\n",
      "- On HTTP `400` or `422` errors, returns a JSON-encoded standard error envelope string."
    ),
    "/lookup" = paste(
      "**UI Workflow Mapping**: Optional programmatic helper (not needed by the UI; the UI uses `GET /surveys-ui`).\n\n",
      "Translates parallel arrays of 3-letter ISO country codes (`country_code`), 4-digit survey years (`year`), and welfare types (`welfare_type`: `\"INC\"` or `\"CON\"`) into the canonical `pip_id` strings accepted by `/table`, `/categories`, and `/covariates`.\n\n",
      "### Usage Rules\n",
      "- `country_code`, `year`, and `welfare_type` are **all required** and must be supplied with the **exact same number of elements** (paired positionally: the *i*-th country, *i*-th year, and *i*-th welfare type form one lookup key).\n",
      "- Example single lookup: `/lookup?country_code=COL&year=2008&welfare_type=INC`"
    ),
    "/surveys" = paste(
      "**UI Workflow Mapping**: Optional developer/manifest inspection endpoint (not needed by the UI; the UI uses `GET /surveys-ui`).\n\n",
      "Returns every survey registered in the release manifest, including `pip_id`, `country_code`, `country_name`, `region_code`, `region_name`, `surveyid_year`, `survey_acronym`, `welfare_type`, `ppp_default`, and the per-survey `dimensions` array. For the Step 1 survey selector grid, use `GET /surveys-ui`, which returns only `pip_id`, `country_code`, `country_label`, `year`, `welfare_type`, `welfare_label`, and `version`."
    ),
    "/countries" = paste(
      "**UI Workflow Mapping**: Step 1 (Country search box and filter list).\n\n",
      "Returns a sorted array of `{country_code, country_name}` records derived from the active release manifest (e.g., `{\"country_code\": \"COL\", \"country_name\": \"Colombia\"}`). Use this endpoint to populate the Step 1 country selector before filtering surveys in `GET /surveys-ui`."
    ),
    "/regions" = paste(
      "**UI Workflow Mapping**: Step 1 (Region filter chips).\n\n",
      "Returns each region present in the release manifest along with a sorted array of member ISO3 `countries` codes (e.g., `{\"region_code\": \"LAC\", \"region_name\": \"Latin America & Caribbean\", \"countries\": [\"ARG\", \"BOL\", \"COL\", ...]}`). Used by the UI to filter the Step 1 country/survey grid by World Bank region."
    ),
    "/surveys-ui" = paste(
      "**UI Workflow Mapping**: Step 1 (Main country / year / welfare-type survey selection grid).\n\n",
      "Returns one record per available survey containing `pip_id`, `country_code`, `country_label`, `year`, `welfare_type` (`INC` or `CON`), `welfare_label`, and `version`. Pass up to 15 selected `pip_id` values from this catalog to `GET /categories`, `GET /covariates`, and `GET|POST /table`."
    ),
    "/releases" = paste(
      "**UI Workflow Mapping**: Cross-cutting release selector / header badge.\n\n",
      "Returns `{ \"releases\": [\"...\"], \"current\": \"...\" }` in `data`, showing every release manifest loaded in memory and which release ID is used when the optional `release` parameter is omitted on other endpoints. Takes no parameters."
    ),
    "/analysis-variables" = paste(
      "**UI Workflow Mapping**: Step 2, Decision 2 (\"Variable of interest\" dropdown).\n\n",
      "Returns every variable with role `analysis_var` in the release registry (`welfare`, `pov_status`, continuous variables such as `age`, `educy`, `hsize`, and binary access indicators such as `imp_wat_rec`, `imp_san_rec`, `electricity`). Each item includes:\n",
      "- `varname`: Canonical variable identifier to pass as `analysis_var` in `/table` and `/cell-definition`.\n",
      "- `label`: Human-readable UI label.\n",
      "- `type`: Variable type (`continuous`, `binary`, etc.).\n",
      "- `poverty_line_slider`: Boolean indicating whether the UI should display the poverty line input (`true` for `pov_status`).\n",
      "- `stat_groups`: Array of compatible statistic group IDs from `GET /statistics` (used to enable/disable measure checkboxes)."
    ),
    "/dimensions" = paste(
      "> **Note (Optional / Work in Progress)**: Not needed for the UI (use `GET /covariates` and `GET /categories` instead).\n\n",
      "**UI Workflow Mapping**: Low-level dimension schema reference (for UI layout slot dropdowns with labels and survey-intersection filtering, use `GET /covariates` instead).\n\n",
      "Returns the character array of all recognized disaggregation dimension names (e.g., `\"gender\"`, `\"area\"`, `\"age_group\"`, `\"educat4\"`, `\"educat5\"`, `\"educat7\"`, etc.). Takes no parameters."
    ),
    "/categories" = paste(
      "**UI Workflow Mapping**: Step 2, Decision 1 (\"Filter sample base\" panel).\n\n",
      "Returns filterable categorical variables along with their category codes and labels (used to construct the `filter_base` JSON parameter on `/table`, `/cell-definition`, and `/metadata`).\n\n",
      "### Survey-Intersection Filtering (`pip_id`)\n",
      "- **When `pip_id` is omitted**: Returns the full catalog of filterable variables for the release (`meta.filtered = false`, `meta.n_surveys = null`).\n",
      "- **When `pip_id` is provided** (repeatable, e.g., `?pip_id=COL_2008_GEIH_INC_ALL`): Returns **only** variables present in *every* matched survey (the intersection of each survey's manifest `dimensions`), with `meta.filtered = true` and `meta.n_surveys` set to the number of matched surveys."
    ),
    "/covariates" = paste(
      "**UI Workflow Mapping**: Step 2, Decision 3 (\"Slice table by covariates\" — `column`, `row`, `superColumn`, `superRow` selectors).\n\n",
      "Returns the authoritative list of covariates that can be assigned to the four layout slots (`column`, `row`, `superColumn`, `superRow`) in `/table`, `/cell-definition`, and `/metadata`.\n\n",
      "### Poverty Line Slider Activation (`pov_status`)\n",
      "Each covariate entry includes `poverty_line_slider` (`true` for `pov_status`, `false` for all other covariates). **When the user selects `\"pov_status\"` in any layout slot (`column`, `row`, `superColumn`, or `superRow`), the UI must activate the poverty line slider (`poverty_line`)**, just as when `analysis_var = \"pov_status\"` in Decision 2.\n\n",
      "### Survey-Intersection Filtering (`pip_id`)\n",
      "- **When `pip_id` is omitted**: Returns the full covariate catalog for the release (`meta.filtered = false`, `meta.n_surveys = null`).\n",
      "- **When `pip_id` is provided** (repeatable, e.g., `?pip_id=COL_2008_GEIH_INC_ALL`): Returns **only** covariates available across *all* matched surveys (intersection of manifest `dimensions`), preventing users from selecting a layout covariate that would exclude surveys during computation."
    ),
    "/health" = paste(
      "**UI Workflow Mapping**: Infrastructure liveness probe and startup connectivity check.\n\n",
      "Lightweight health endpoint that requires no parameters and performs no disk or Arrow I/O. Returns `data: { \"status\": \"ok\", \"release\": \"<current_release>\" }`."
    ),
    "/statistics" = paste(
      "**UI Workflow Mapping**: Step 2, Decision 2 (\"Select statistics\" grouped dropdown / checkboxes).\n\n",
      "Returns the four measure families defined in `inst/extdata/tm_measure_spec.yaml`:\n",
      "- `summary_statistics` (*Summary Statistics*): `mean`, `median`, `sd`, `var`, `min`, `max`, `nobs`, `p10`, `p25`, `p75`, `p90`, `sum`.\n",
      "- `poverty` (*Poverty*): `headcount`, `poverty_gap`, `severity`, `watts`, `pop_poverty`.\n",
      "- `inequality` (*Inequality*): `gini`, `mld`.\n",
      "- `shares` (*Shares*): `pop_share`, `target_within_group_share`, `target_survey_share`.\n\n",
      "Cross-reference with `stat_groups` from `GET /analysis-variables` to determine which measure groups are valid for the active `analysis_var`."
    ),
    "/cell-definition" = paste(
      "**UI Workflow Mapping**: Step 2 (\"What each cell in your table means\" live preview card).\n\n",
      "Pure function of the Step 2 table configuration parameters and the release variable registry. Call this endpoint whenever the user changes filters, variables, measures, or layout slots on Page 2 to update the live explanation card.\n\n",
      "### Response Structure (`data`)\n",
      "- `who_is_in_table`: Explains the sample base after applying `filter_base` (or the full survey population when unfiltered).\n",
      "- `what_a_cell_is`: Uniform `{ title, structure_text, items, example_text, dimensions, example_cell_label }` object. When the table has **4 or fewer cells**, `items` enumerates every combined cell in plain English; when it has **more than 4 cells**, `items` lists one category-breakdown line per active layout slot (max 4 lines) plus `example_text` for the first cell.\n",
      "- `what_is_shown`: Plain-English interpretation of each requested measure in `measures` (including numerator `counts` and denominator `out_of` for share measures).\n",
      "- `notes`: Methodological notes (PPP year, poverty line, and cell-suppression threshold).\n\n",
      "### Example Request\n",
      "`/cell-definition?analysis_var=welfare&measures=mean&measures=gini&column=gender&row=area&ppp=2021&welfare_type=INC`"
    )
  )

  for (path in names(spec$paths)) {
    for (method in names(spec$paths[[path]])) {
      op <- spec$paths[[path]][[method]]
      if (!is.null(summary_by_path[[path]])) {
        op$summary <- summary_by_path[[path]]
      }
      if (!is.null(description_by_path[[path]])) {
        op$description <- description_by_path[[path]]
      }
      if (!is.null(tag_by_path[[path]])) {
        op$tags <- list(tag_by_path[[path]])
      }
      if (!is.null(op$parameters) && length(op$parameters) > 0L) {
        op$parameters <- Filter(
          function(p) !(p$name %||% "") %in% c("req", "res"),
          op$parameters
        )
        op$parameters <- lapply(op$parameters, enrich_param, path = path)
      }

      if (path %in% c("/table", "/metadata", "/cell-definition", "/lookup")) {
        op$responses[["400"]] <- json_error_response(
          "Bad Request — Missing required parameter, invalid parameter value/type, or layout-slot ordering violation."
        )
      }
      if (path %in% c("/table", "/metadata", "/cell-definition", "/lookup", "/surveys", "/countries",
                      "/regions", "/surveys-ui", "/analysis-variables", "/categories",
                      "/covariates", "/statistics")) {
        op$responses[["422"]] <- json_error_response(
          "Unprocessable Entity — Unknown release ID, unreadable filter JSON, or domain/computation error (e.g., no selected surveys contain the requested covariates)."
        )
      }
      op$responses[["500"]] <- json_error_response(
        "Internal Server Error — Unexpected server failure."
      )

      spec$paths[[path]][[method]] <- op
    }
  }

  # ── GET|POST /metadata requestBody & responses ──────────────────────────────
  if (!is.null(spec$paths[["/metadata"]][["get"]])) {
    spec$paths[["/metadata"]][["get"]]$responses[["200"]] <- list(
      description = "Rendered table metadata and provenance document (HTML when `format = \"html\"`, Markdown when `format = \"markdown\"`).",
      content = list(
        "text/html"  = list(schema = list(type = "string")),
        "text/plain" = list(schema = list(type = "string"))
      )
    )
  }
  if (!is.null(spec$paths[["/metadata"]][["post"]])) {
    metadata_op <- spec$paths[["/metadata"]][["post"]]
    metadata_op$parameters <- list()
    metadata_op$requestBody <- list(
      required    = TRUE,
      description = paste(
        "JSON object specifying the table configuration parameters (`pip_id`, `analysis_var`, `measures`,",
        "and optional `poverty_line`, `column`, `row`, `superColumn`, `superRow`, `filter_base`, `ppp`,",
        "`pop_share_threshold`, `release`, and `format`)."
      ),
      content = list(
        "application/json" = list(
          schema = list(
            type        = "object",
            required    = list("analysis_var", "pip_id", "measures"),
            description = "Table configuration parameters and output `format` for metadata generation.",
            properties  = list(
              format = list(
                type        = "string",
                enum        = list("html", "markdown"),
                default     = "html",
                description = "Output format: `\"html\"` (default, inline-styled HTML attachment `table-metadata.html`) or `\"markdown\"` (plain Markdown attachment `table-metadata.md`)."
              ),
              analysis_var = list(
                type        = "string",
                enum        = as.list(valid_analysis_vars),
                description = "[Required] Target analysis variable from `GET /analysis-variables` (e.g., `\"welfare\"`, `\"pov_status\"`)."
              ),
              pip_id = list(
                type        = "array",
                minItems    = 1L,
                maxItems    = .MAX_SURVEYS_PER_REQUEST,
                items       = list(type = "string", pattern = .PIP_ID_PATTERN),
                description = "[Required] Array of 1 to 15 survey IDs (e.g., `[\"COL_2008_GEIH_INC_ALL\"]`)."
              ),
              measures = list(
                type        = "array",
                minItems    = 1L,
                items       = list(type = "string", enum = as.list(valid_measures)),
                description = "[Required] Array of measure names from `GET /statistics` (e.g., `[\"mean\", \"gini\"]`)."
              ),
              poverty_line = list(
                type        = "number",
                description = "[Conditional] Daily poverty line in PPP dollars (e.g., `2.15`). Required when `analysis_var = \"pov_status\"` or `\"pov_status\"` is in a layout slot."
              ),
              column = list(
                type        = "string",
                enum        = as.list(valid_slot_vars),
                description = "[Optional] 1st layout slot (Columns) covariate from `GET /covariates`."
              ),
              row = list(
                type        = "string",
                enum        = as.list(valid_slot_vars),
                description = "[Optional] 2nd layout slot (Rows) covariate from `GET /covariates` (requires `column`)."
              ),
              superColumn = list(
                type        = "string",
                enum        = as.list(valid_slot_vars),
                description = "[Optional] 3rd layout slot (Super Columns) covariate from `GET /covariates` (requires `column` and `row`)."
              ),
              superRow = list(
                type        = "string",
                enum        = as.list(valid_slot_vars),
                description = "[Optional] 4th layout slot (Super Rows) covariate from `GET /covariates` (requires `column`, `row`, and `superColumn`)."
              ),
              filter_base = list(
                type        = "object",
                description = "[Optional] Sample-base filter object mapping variable names from `GET /categories` to arrays of category codes (e.g., `{\"gender\": [1]}`)."
              ),
              ppp = list(
                type        = "integer",
                default     = 2021L,
                description = "[Optional] PPP reference year (positive integer, default `2021`)."
              ),
              pop_share_threshold = list(
                type        = "number",
                default     = 0.001,
                description = "[Optional] Cell-suppression threshold in `(0, 1)` (default `0.001`)."
              ),
              release = list(
                type        = "string",
                example     = "20260401_TEST",
                description = "[Optional] Release ID from `GET /releases` (e.g., `\"20260401_TEST\"`; defaults to current release)."
              )
            )
          ),
          examples = list(
            metadata_html = list(
              summary     = "HTML metadata document",
              description = "Builds metadata from the release manifest and registry and returns inline-styled HTML.",
              value       = list(
                pip_id       = list("COL_2008_GEIH_INC_ALL"),
                analysis_var = "welfare",
                measures     = list("mean", "gini"),
                column       = "gender",
                ppp          = 2021L,
                format       = "html"
              )
            ),
            metadata_markdown = list(
              summary     = "Markdown metadata document",
              description = "Builds metadata from the release manifest and registry and returns plain Markdown text.",
              value       = list(
                pip_id       = list("COL_2008_GEIH_INC_ALL"),
                analysis_var = "welfare",
                measures     = list("mean", "gini"),
                ppp          = 2021L,
                format       = "markdown"
              )
            )
          )
        )
      )
    )
    metadata_op$responses[["200"]] <- list(
      description = "Rendered table metadata and provenance document (HTML when `format = \"html\"`, Markdown when `format = \"markdown\"`).",
      content = list(
        "text/html"  = list(schema = list(type = "string")),
        "text/plain" = list(schema = list(type = "string"))
      )
    )
    metadata_op$responses[["400"]] <- json_error_response(
      "Bad Request — Missing required parameter, invalid `format`, or invalid table parameters."
    )
    metadata_op$responses[["422"]] <- json_error_response(
      "Unprocessable Entity — Unknown release ID, unreadable filter JSON, or domain validation error (e.g., all surveys excluded by requested filters/covariates)."
    )
    spec$paths[["/metadata"]][["post"]] <- metadata_op
  }

  # ── POST /table/download requestBody & responses ───────────────────────────
  if (!is.null(spec$paths[["/table/download"]][["post"]])) {
    dl_op <- spec$paths[["/table/download"]][["post"]]
    dl_op$parameters <- list()
    sample_download_data <- list(
      list(
        pip_id        = "COL_2008_GEIH_INC_ALL",
        country_code  = "COL",
        surveyid_year = 2008L,
        welfare_type  = "INC",
        poverty_line  = NULL,
        analysis_var  = "welfare",
        measures      = list("mean", "gini"),
        layout        = list(
          columns = list(
            varname    = "gender",
            label      = "Gender",
            categories = list(
              list(code = 1L, label = "Male"),
              list(code = 2L, label = "Female")
            )
          ),
          rows         = NULL,
          superColumns = NULL,
          superRows    = NULL
        ),
        cells = list(
          list(
            superRow    = NULL,
            row         = NULL,
            superColumn = NULL,
            column      = list(code = 1L, label = "Male"),
            measures    = list(mean = "8.45", gini = "0.312")
          ),
          list(
            superRow    = NULL,
            row         = NULL,
            superColumn = NULL,
            column      = list(code = 2L, label = "Female"),
            measures    = list(mean = "8.12", gini = "0.298")
          )
        )
      )
    )

    dl_op$requestBody <- list(
      required    = TRUE,
      description = paste(
        "JSON object with optional `format` (`\"csv\"` or `\"rds\"`, default `\"csv\"`) and required `data`",
        "(the non-empty `data` array of Approach C survey objects returned by `GET|POST /table`).",
        "Maximum request size is 25 MB."
      ),
      content = list(
        "application/json" = list(
          schema = list(
            type       = "object",
            required   = list("data"),
            properties = list(
              format = list(
                type        = "string",
                enum        = list("csv", "rds"),
                default     = "csv",
                example     = "csv",
                description = "Download file format: `\"csv\"` (UTF-8 CSV attachment `table.csv`) or `\"rds\"` (R serialized data.frame attachment `table.rds`)."
              ),
              data = list(
                type        = "array",
                minItems    = 1L,
                description = "Non-empty array of survey table objects copied directly from the `data` field of a `/table` response.",
                items       = list(
                  type       = "object",
                  required   = list("pip_id", "country_code", "surveyid_year", "welfare_type", "analysis_var", "measures", "layout", "cells"),
                  properties = list(
                    pip_id        = list(type = "string", example = "COL_2008_GEIH_INC_ALL"),
                    country_code  = list(type = "string", example = "COL"),
                    surveyid_year = list(type = "integer", example = 2008L),
                    welfare_type  = list(type = "string", enum = list("INC", "CON"), example = "INC"),
                    poverty_line  = list(type = "number", nullable = TRUE, example = NULL),
                    analysis_var  = list(type = "string", example = "welfare"),
                    measures      = list(type = "array", items = list(type = "string"), example = list("mean", "gini")),
                    layout        = list(type = "object", description = "Active layout slot definitions (`columns`, `rows`, `superColumns`, `superRows`)."),
                    cells         = list(type = "array", items = list(type = "object"), description = "Computed cells with slot coordinates and `measures` key-value map.")
                  )
                )
              )
            )
          ),
          examples = list(
            csv_download = list(
              summary     = "Download as CSV (default)",
              description = "Flattens the `/table` response `data` array into `table.csv`.",
              value       = list(
                format = "csv",
                data   = sample_download_data
              )
            ),
            rds_download = list(
              summary     = "Download as R data.frame (.rds)",
              description = "Flattens the `/table` response `data` array into a binary `table.rds` file.",
              value       = list(
                format = "rds",
                data   = sample_download_data
              )
            )
          )
        )
      )
    )
    dl_op$responses[["200"]] <- list(
      description = "File download attachment (`table.csv` or `table.rds`).",
      content = list(
        "text/csv"                 = list(schema = list(type = "string", format = "binary")),
        "application/octet-stream" = list(schema = list(type = "string", format = "binary"))
      )
    )
    dl_op$responses[["400"]] <- json_error_response(
      "Bad Request — Empty/malformed JSON body, unsupported `format`, or invalid/empty `data` array."
    )
    dl_op$responses[["413"]] <- json_error_response(
      "Payload Too Large — Request body exceeds the 25 MB limit."
    )
    spec$paths[["/table/download"]][["post"]] <- dl_op
  }

  spec
}

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

  pr$setApiSpec(enrich_openapi_spec)
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

#* Compute cross-tabulated poverty, inequality, summary-statistic, and share measures for up to 15 surveys.
#* @tag "Step 3 — Table Computation & Export"
#* @param analysis_var:character [Required] Target variable to analyze from `GET /analysis-variables` (e.g., `"welfare"` for monetary welfare, `"pov_status"` for poverty status, `"age"`/`"educy"`/`"hsize"` for continuous variables, or a binary indicator such as `"electricity"`).
#* @param pip_id:[character] [Required] Survey identifier(s) to compute (repeatable, 1 to 15 values). Format: `ISO3_YYYY_<SURVEY>_<WELFARE>_<DOMAIN>` from `GET /surveys-ui` (e.g., `"COL_2008_GEIH_INC_ALL"`).
#* @param measures:[character] [Required] Statistical measure ID(s) to compute (repeatable, 1 or more values) from `GET /statistics` (e.g., `"mean"`, `"median"`, `"gini"`, `"headcount"`, `"poverty_gap"`, `"pop_share"`). Must be compatible with the selected `analysis_var`.
#* @param poverty_line:numeric [Conditional] Daily poverty line in constant international PPP dollars (positive number, e.g., `2.15`, `3.65`, or `6.85`). Required when `analysis_var = "pov_status"` or when `"pov_status"` is assigned to any layout slot.
#* @param column:character [Optional] Disaggregation covariate assigned to the 1st layout slot (Columns), chosen from `GET /covariates` (e.g., `"gender"` or `"area"`). Leave empty for a national-level table without disaggregation.
#* @param row:character [Optional] Disaggregation covariate assigned to the 2nd layout slot (Rows), chosen from `GET /covariates` (e.g., `"area"` or `"educat4"`). Requires `column` to be filled first.
#* @param superColumn:character [Optional] Disaggregation covariate assigned to the 3rd layout slot (Super Columns), chosen from `GET /covariates`. Requires both `column` and `row` to be filled first.
#* @param superRow:character [Optional] Disaggregation covariate assigned to the 4th layout slot (Super Rows), chosen from `GET /covariates`. Requires `column`, `row`, and `superColumn` to be filled first.
#* @param ppp:integer [Optional] Purchasing Power Parity (PPP) reference year used for monetary welfare conversion (positive integer, default `2021`; e.g., `2021` or `2017`).
#* @param filter_base:character [Optional] JSON-encoded sample-base filter object mapping variable names from `GET /categories` to arrays of kept category codes (e.g., `'{"gender":[1],"age_group":[2,3]}'`). Omit to include the full survey sample.
#* @param pop_share_threshold:numeric [Optional] Minimum cell population-share threshold strictly between `0` and `1` (default `0.001`, i.e., 0.1% of the sample base) below which cell estimates are masked as `"NR"`. Send empty/null to disable suppression.
#* @param release:character [Optional] PIP release identifier from `GET /releases` (e.g., `"20260401_TEST"`). Omit to use the active default release.
#* @serializer json list(na = "null")
#* @get /table
#* @post /table
function(analysis_var = NULL, pip_id = NULL, measures = NULL, poverty_line = NULL,
         column = NULL, row = NULL, superColumn = NULL, superRow = NULL,
         ppp = 2021L, filter_base = NULL, pop_share_threshold = 0.001,
         release = NULL, res) {

  check <- validate_table_input(
    analysis_var = analysis_var,
    pip_id = pip_id,
    measures = measures,
    poverty_line = poverty_line,
    column = column,
    row = row,
    superColumn = superColumn,
    superRow = superRow,
    ppp = ppp,
    pop_share_threshold = pop_share_threshold
  )
  if (!check$valid) return(api_error(check$errors, 400L, res))
  poverty_line        <- check$poverty_line
  ppp                 <- check$ppp
  pop_share_threshold <- check$pop_share_threshold
  by                  <- check$by

  out <- capture_with_warnings({
    parsed_filter_base <- parse_filter_base_input(filter_base)

    rel  <- resolve_release(release)

    result_data <- piptm::table_maker(
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

    result_data <- piptm::format_table_output(result_data)

    n_surveys <- data.table::uniqueN(result_data, by = "pip_id")

    result_data <- piptm::serialize_table_response(
      dt           = result_data,
      slots        = check$slots,
      analysis_var = analysis_var,
      poverty_line = poverty_line,
      measures     = measures,
      release      = rel
    )

    list(
      data = result_data,
      rel = rel,
      n_surveys = n_surveys
    )
  })
  if (!is.null(out$error)) return(api_error(out$error, 422L, res))

  meta <- list(
    release   = out$result$rel,
    n_surveys = out$result$n_surveys
  )

  api_response(
    out$result$data,
    warnings = out$warnings,
    meta = meta
  )
}

# -- POST /table/download ------------------------------------------------------

#* Flatten a computed `/table` response payload into a downloadable CSV or RDS file.
#* @tag "Step 3 — Table Computation & Export"
#* @param body:object [Required] JSON object containing `format` (`"csv"` or `"rds"`, default `"csv"`) and `data` (the non-empty `data` array of survey objects returned by `/table`).
#* @parser text
#* @serializer null
#* @post /table/download
function(req, body = NULL, res) {
  err <- function(msg, code) {
    res$setHeader("Content-Type", "application/json")
    charToRaw(as.character(error_json(msg, code, res)))
  }

  max_bytes <- 25 * 1024 * 1024
  clen <- suppressWarnings(as.numeric(req$CONTENT_LENGTH))
  if (length(clen) == 1L && !is.na(clen) && clen > max_bytes) {
    return(err("Request body too large (max 25 MB).", 413L))
  }

  raw_body <- tryCatch(
    if (!is.null(body) && nzchar(body)) {
      body
    } else if (!is.null(req$postBody)) {
      req$postBody
    } else {
      req$rook.input$read_lines()
    },
    error = function(e) NULL
  )
  # Some request sources (e.g. rook.input$read_lines()) may return a
  # multi-element character vector; collapse to a single string before any
  # `&&`/length-1 check, otherwise R >= 4.2 throws an unhandled
  # "'length = N' in coercion to 'logical(1)'" error (HTTP 500) instead of
  # the intended 400/413 JSON envelope.
  if (is.character(raw_body) && length(raw_body) > 1L) {
    raw_body <- paste(raw_body, collapse = "\n")
  }
  if (!is.null(raw_body) && length(raw_body) == 1L && nzchar(raw_body) &&
      nchar(raw_body, type = "bytes") > max_bytes) {
    return(err("Request body too large (max 25 MB).", 413L))
  }

  body <- if (is.null(raw_body) || length(raw_body) == 0L || !nzchar(raw_body)) {
    NULL
  } else {
    tryCatch(jsonlite::fromJSON(raw_body, simplifyVector = FALSE), error = function(e) NULL)
  }
  if (is.null(body) || !is.list(body) || length(body) == 0L || is.null(names(body))) {
    return(err("Request body must be a non-empty JSON object.", 400L))
  }

  fmt <- tolower(trimws(as.character(unlist(body$format %||% "csv"))[1L]))
  if (is.na(fmt) || !fmt %in% c("csv", "rds")) {
    return(err("`format` must be one of: csv, rds.", 400L))
  }

  flat_res <- tryCatch(
    list(tbl = piptm::flatten_table_response(body$data), error = NULL),
    error = function(e) {
      list(
        tbl   = NULL,
        error = piptm:::piptm_user_error_message(
          e,
          fallback = piptm:::.PIPTM_DOWNLOAD_PAYLOAD_USER_ERROR
        )
      )
    }
  )
  if (!is.null(flat_res$error) || is.null(flat_res$tbl) || nrow(flat_res$tbl) == 0L) {
    msg <- flat_res$error %||% piptm:::.PIPTM_DOWNLOAD_PAYLOAD_USER_ERROR
    return(err(msg, 400L))
  }
  tbl <- flat_res$tbl

  tmp <- tempfile(fileext = paste0(".", fmt))
  on.exit(unlink(tmp), add = TRUE)
  if (identical(fmt, "csv")) {
    data.table::fwrite(tbl, tmp)
    ctype <- "text/csv; charset=utf-8"
  } else {
    saveRDS(as.data.frame(tbl), tmp)
    ctype <- "application/octet-stream"
  }

  res$setHeader("Content-Type", ctype)
  res$setHeader("Content-Disposition", sprintf('attachment; filename="table.%s"', fmt))
  readBin(tmp, "raw", n = file.size(tmp))
}

# ── GET|POST /metadata ────────────────────────────────────────────────────────

#* Render a downloadable structured table metadata and provenance document in HTML or Markdown.
#* @tag "Step 3 — Table Computation & Export"
#* @param analysis_var:character [Required] Target variable to analyze from `GET /analysis-variables` (e.g., `"welfare"` for monetary welfare, `"pov_status"` for poverty status, `"age"`/`"educy"`/`"hsize"` for continuous variables, or a binary indicator such as `"electricity"`).
#* @param pip_id:[character] [Required] Survey identifier(s) to document (repeatable, 1 to 15 values). Format: `ISO3_YYYY_<SURVEY>_<WELFARE>_<DOMAIN>` from `GET /surveys-ui` (e.g., `"COL_2008_GEIH_INC_ALL"`).
#* @param measures:[character] [Required] Statistical measure ID(s) to document (repeatable, 1 or more values) from `GET /statistics` (e.g., `"mean"`, `"median"`, `"gini"`, `"headcount"`, `"poverty_gap"`, `"pop_share"`). Must be compatible with the selected `analysis_var`.
#* @param poverty_line:numeric [Conditional] Daily poverty line in constant international PPP dollars (positive number, e.g., `2.15`, `3.65`, or `6.85`). Required when `analysis_var = "pov_status"` or when `"pov_status"` is assigned to any layout slot.
#* @param column:character [Optional] Disaggregation covariate assigned to the 1st layout slot (Columns), chosen from `GET /covariates` (e.g., `"gender"` or `"area"`). Leave empty for a national-level table without disaggregation.
#* @param row:character [Optional] Disaggregation covariate assigned to the 2nd layout slot (Rows), chosen from `GET /covariates` (e.g., `"area"` or `"educat4"`). Requires `column` to be filled first.
#* @param superColumn:character [Optional] Disaggregation covariate assigned to the 3rd layout slot (Super Columns), chosen from `GET /covariates`. Requires both `column` and `row` to be filled first.
#* @param superRow:character [Optional] Disaggregation covariate assigned to the 4th layout slot (Super Rows), chosen from `GET /covariates`. Requires `column`, `row`, and `superColumn` to be filled first.
#* @param ppp:integer [Optional] Purchasing Power Parity (PPP) reference year used for monetary welfare conversion (positive integer, default `2021`; e.g., `2021` or `2017`).
#* @param filter_base:character [Optional] JSON-encoded sample-base filter object mapping variable names from `GET /categories` to arrays of kept category codes (e.g., `'{"gender":[1],"age_group":[2,3]}'`). Omit to include the full survey sample.
#* @param pop_share_threshold:numeric [Optional] Minimum cell population-share threshold strictly between `0` and `1` (default `0.001`, i.e., 0.1% of the sample base) below which cell estimates are masked as `"NR"`. Send empty/null to disable suppression.
#* @param release:character [Optional] PIP release identifier from `GET /releases` (e.g., `"20260401_TEST"`). Omit to use the active default release.
#* @param format:character [Optional] Output document format: `"html"` (default, inline-styled HTML download `table-metadata.html`) or `"markdown"` (plain Markdown download `table-metadata.md`).
#* @serializer text
#* @get /metadata
#* @post /metadata
function(analysis_var = NULL, pip_id = NULL, measures = NULL, poverty_line = NULL,
         column = NULL, row = NULL, superColumn = NULL, superRow = NULL,
         ppp = 2021L, filter_base = NULL, pop_share_threshold = 0.001,
         release = NULL, format = "html", req, res) {

  # Reject legacy `description_metadata` request bodies explicitly (HTTP 400).
  if (!is.null(req$argsBody) && "description_metadata" %in% names(req$argsBody)) {
    return(error_json(
      "`description_metadata` is no longer supported; pass table parameters directly.",
      400L, res
    ))
  }

  if (is.null(format) || !is.character(format) || length(format) != 1L || is.na(format)) {
    return(error_json("`format` must be a single string (`html` or `markdown`).", 400L, res))
  }
  fmt <- tolower(trimws(format))
  if (!(fmt %in% c("html", "markdown"))) {
    return(error_json("`format` must be one of: markdown, html.", 400L, res))
  }

  check <- validate_table_input(
    analysis_var        = analysis_var,
    pip_id              = pip_id,
    measures            = measures,
    poverty_line        = poverty_line,
    column              = column,
    row                 = row,
    superColumn         = superColumn,
    superRow            = superRow,
    ppp                 = ppp,
    pop_share_threshold = pop_share_threshold
  )
  if (!check$valid) return(error_json(check$errors, 400L, res))

  out <- capture_with_warnings({
    parsed_filter_base <- parse_filter_base_input(filter_base)
    rel                <- resolve_release(release)

    model <- piptm::build_description_model(
      pip_id              = pip_id,
      analysis_var        = analysis_var,
      measures            = measures,
      poverty_line        = check$poverty_line,
      column              = check$slots$column,
      row                 = check$slots$row,
      superColumn         = check$slots$superColumn,
      superRow            = check$slots$superRow,
      filter_base         = parsed_filter_base,
      ppp                 = check$ppp %||% 2021L,
      pop_share_threshold = check$pop_share_threshold,
      release             = rel
    )

    rendered <- if (identical(fmt, "html")) {
      piptm::render_description_html(model)
    } else {
      piptm::render_description_markdown(model)
    }
    list(content = rendered)
  })
  if (!is.null(out$error)) return(error_json(out$error, 422L, res))

  if (identical(fmt, "html")) {
    res$setHeader("Content-Type", "text/html; charset=utf-8")
    res$setHeader("Content-Disposition", 'attachment; filename="table-metadata.html"')
  } else {
    res$setHeader("Content-Disposition", 'attachment; filename="table-metadata.md"')
  }

  out$result$content
}

# ── GET /lookup ───────────────────────────────────────────────────────────────

#* [Optional — Not needed in UI] Resolve `(country_code, year, welfare_type)` survey triples to canonical `pip_id` identifiers.
#* @tag "Optional / Developer Endpoints (Not Needed for UI)"
#* @param country_code:[character] [Required] 3-letter ISO3 country code(s) (repeatable, paired by position with `year` and `welfare_type`; e.g., `"COL"`, `"ARM"`).
#* @param year:[integer] [Required] 4-digit survey year(s) (repeatable, same length as `country_code`; e.g., `2010`, `2012`).
#* @param welfare_type:[character] [Required] Survey welfare aggregate type(s): `"INC"` (income) or `"CON"` (consumption) (repeatable, same length as `country_code`).
#* @param release:character [Optional] PIP release identifier from `GET /releases` (e.g., `"20260401_TEST"`). Omit to use the active default release.
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

#* [Optional — Not needed in UI] Return the full survey manifest table for a release, including internal file metadata.
#* @tag "Optional / Developer Endpoints (Not Needed for UI)"
#* @param release:character [Optional] PIP release identifier from `GET /releases` (e.g., `"20260401_TEST"`). Omit to use the active default release.
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

#* Return all unique countries (`country_code` and `country_name`) available in the release manifest.
#* @tag "Step 1 — Survey Selection"
#* @param release:character [Optional] PIP release identifier from `GET /releases` (e.g., `"20260401_TEST"`). Omit to use the active default release.
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

#* Return World Bank regional groupings (`region_code`, `region_name`) and their member `countries` arrays.
#* @tag "Step 1 — Survey Selection"
#* @param release:character [Optional] PIP release identifier from `GET /releases` (e.g., `"20260401_TEST"`). Omit to use the active default release.
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

#* Return the lightweight, UI-ready survey catalog for populating the Step 1 survey selection grid.
#* @tag "Step 1 — Survey Selection"
#* @param release:character [Optional] PIP release identifier from `GET /releases` (e.g., `"20260401_TEST"`). Omit to use the active default release.
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

#* List all loaded PIP data releases and identify the active default release.
#* @tag "System & Releases"
#* @serializer json list(na = "null")
#* @get /releases
function() {
  api_response(list(
    releases = names(piptm::piptm_manifests()),
    current  = piptm::piptm_current_release()
  ))
}

# ── GET /analysis-variables ───────────────────────────────────────────────────

#* Return all valid `analysis_var` options for Decision 2, including compatible statistic groups and slider rules.
#* @tag "Step 2 — Table Configuration"
#* @param release:character [Optional] PIP release identifier from `GET /releases` (e.g., `"20260401_TEST"`). Omit to use the active default release.
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

#* [Optional / WIP — Not needed in UI] Return the flat list of all valid disaggregation dimension identifiers.
#* @tag "Optional / Developer Endpoints (Not Needed for UI)"
#* @serializer json list(na = "null")
#* @get /dimensions
function() {
  api_response(piptm::pip_valid_dimensions())
}

# ── GET /categories ──────────────────────────────────────────────────────────

#* Return categorical variables and category codes for the Decision 1 sample-base filter panel.
#* @tag "Step 2 — Table Configuration"
#* @param pip_id:[character] [Optional] Survey identifier(s) selected in Step 1 (repeatable; e.g., `"COL_2008_GEIH_INC_ALL"`). When supplied, restricts the response to filter variables available across all specified surveys.
#* @param release:character [Optional] PIP release identifier from `GET /releases` (e.g., `"20260401_TEST"`). Omit to use the active default release.
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
      matched <- mf[data.table::`%chin%`(pip_id, requested_ids)]

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

#* Return disaggregation covariates and category labels for the Decision 3 table layout slots.
#* @tag "Step 2 — Table Configuration"
#* @param pip_id:[character] [Optional] Survey identifier(s) selected in Step 1 (repeatable; e.g., `"COL_2008_GEIH_INC_ALL"`). When supplied, restricts the response to layout covariates common to all specified surveys.
#* @param release:character [Optional] PIP release identifier from `GET /releases` (e.g., `"20260401_TEST"`). Omit to use the active default release.
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
      matched <- mf[data.table::`%chin%`(pip_id, requested_ids)]

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

# ── GET /health ───────────────────────────────────────────────────────────────

#* Verify API server readiness and return `"ok"` status with the current default release ID.
#* @tag "System & Releases"
#* @serializer json list(na = "null")
#* @get /health
function() {
  api_response(list(
    status  = "ok",
    release = piptm::piptm_current_release()
  ))
}

# ── GET /statistics ───────────────────────────────────────────────────────────

#* Return all statistical measure groups and their individual measures for Decision 2.
#* @tag "Step 2 — Table Configuration"
#* @param release:character [Optional] PIP release identifier from `GET /releases` (e.g., `"20260401_TEST"`). Omit to use the active default release.
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

# ── GET /cell-definition ──────────────────────────────────────────────────────

#* Return a live three-section plain-English explanation of what each cell in the configured table represents.
#* @tag "Step 2 — Table Configuration"
#* @param analysis_var:character [Required] Target variable being analyzed from `GET /analysis-variables` (e.g., `"welfare"`, `"pov_status"`, `"age"`, `"electricity"`).
#* @param measures:[character] [Required] Statistical measure ID(s) selected in Step 2 (repeatable, 1 or more values from `GET /statistics`; e.g., `"mean"`, `"gini"`, `"pop_share"`). Note: `target_within_group_share` and `target_survey_share` require a binary target `analysis_var`.
#* @param poverty_line:numeric [Conditional] Daily poverty line in constant international PPP dollars (positive number, e.g., `2.15`). Required when `analysis_var = "pov_status"`, `"pov_status"` is in any layout slot, or any poverty measure (`headcount`, `poverty_gap`, `severity`, `watts`, `pop_poverty`) is requested.
#* @param column:character [Optional] Disaggregation covariate in the 1st layout slot (Columns) from `GET /covariates` (e.g., `"gender"`).
#* @param row:character [Optional] Disaggregation covariate in the 2nd layout slot (Rows) from `GET /covariates` (e.g., `"area"`). Requires `column` to be filled first.
#* @param superColumn:character [Optional] Disaggregation covariate in the 3rd layout slot (Super Columns) from `GET /covariates`. Requires `column` and `row` to be filled first.
#* @param superRow:character [Optional] Disaggregation covariate in the 4th layout slot (Super Rows) from `GET /covariates`. Requires `column`, `row`, and `superColumn` to be filled first.
#* @param filter_base:character [Optional] JSON-encoded sample-base filter object mapping variable names from `GET /categories` to arrays of category codes (e.g., `'{"gender":[1],"age_group":[2,3]}'`).
#* @param ppp:integer [Optional] PPP reference year used in monetary welfare wording (positive integer, default `2021`; e.g., `2021` or `2017`).
#* @param pop_share_threshold:numeric [Optional] Cell-suppression population-share threshold strictly between `0` and `1` (default `0.001` = 0.1%). Send empty/null to omit the suppression note.
#* @param welfare_type:character [Optional] Welfare aggregate wording hint: `"INC"` (renders `"daily income"`), `"CON"` (renders `"daily consumption"`), or omit to render generic `"daily welfare"`.
#* @param release:character [Optional] PIP release identifier from `GET /releases` (e.g., `"20260401_TEST"`). Omit to use the active default release.
#* @serializer json list(na = "null")
#* @get /cell-definition
function(analysis_var = NULL, measures = NULL, poverty_line = NULL,
         column = NULL, row = NULL, superColumn = NULL, superRow = NULL,
         filter_base = NULL, ppp = 2021L, pop_share_threshold = 0.001,
         welfare_type = NULL, release = NULL, res) {

  check <- validate_cell_definition_query(
    analysis_var        = analysis_var,
    measures            = measures,
    poverty_line        = poverty_line,
    column              = column,
    row                 = row,
    superColumn         = superColumn,
    superRow            = superRow,
    filter_base         = filter_base,
    ppp                 = ppp,
    pop_share_threshold = pop_share_threshold,
    welfare_type        = welfare_type,
    release             = release
  )
  if (!check$valid) {
    return(api_error(check$errors, 400L, res))
  }

  out <- capture_with_warnings({
    rel_in <- check$release
    rel <- if (!is.null(rel_in) && nzchar(rel_in)) {
      resolve_release(rel_in)
    } else {
      tryCatch(piptm::piptm_current_release(), error = function(e) NULL)
    }

    exp_res <- piptm::build_cell_explanation(
      analysis_var        = check$analysis_var,
      measures            = check$measures,
      poverty_line        = check$poverty_line,
      column              = check$slots$column,
      row                 = check$slots$row,
      superColumn         = check$slots$superColumn,
      superRow            = check$slots$superRow,
      filter_base         = check$filter_base,
      ppp                 = check$ppp,
      pop_share_threshold = check$pop_share_threshold,
      welfare_type        = check$welfare_type,
      release             = rel
    )

    list(
      data     = exp_res[c("schema_version", "who_is_in_table", "what_a_cell_is", "what_is_shown", "notes")],
      warnings = as.character(unlist(exp_res$warnings, use.names = FALSE)),
      rel      = rel
    )
  })

  if (!is.null(out$error)) {
    return(api_error(out$error, 422L, res))
  }

  all_warnings <- unique(c(out$warnings, out$result$warnings))
  meta_out <- if (!is.null(out$result$rel)) list(release = out$result$rel) else list()
  api_response(out$result$data, warnings = all_warnings, meta = meta_out)
}
