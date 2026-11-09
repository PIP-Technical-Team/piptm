# Table Maker API Contract

**Version:** 2026-08-31  
**Owner:** Table Maker API Team  
**Audience:** PIP Platform UI/IT team  
**Plan Reference:** `.cg-docs/plans/2026-08-31-api-contract-openapi.md`

This document specifies every public endpoint exposed by the `piptm` Plumber service. It maps the API surface to the three-step Table Maker UI, lists request and response schemas, records validation rules, and captures operational constraints (CORS, error handling).

---

## Response Envelope

All endpoints (unless otherwise stated) return JSON conforming to the standard envelope below.

```yaml
components:
  schemas:
    ResponseEnvelope:
      type: object
      required: [status, data, warnings, errors, meta]
      properties:
        status:
          type: string
          enum: [success, error]
        data:
          type: object
          nullable: true
          description: Endpoint-specific payload. Null when status=error.
        warnings:
          type: array
          items:
            type: string
        errors:
          type: array
          items:
            type: string
        meta:
          type: object
          description: Metadata such as release ID, counts, description metadata, etc.
```

- Success responses: `status="success"`, `errors=[]`; `data` carries payload; `warnings` may contain informational messages.
- Error responses: `status="error"`, `data=null`; `errors` lists plain-English, user-safe sentences that UI clients can display directly in a banner or toast without client-side parsing or sanitization.

## Error Codes

| HTTP | Meaning | Common Causes |
|------|---------|---------------|
| 200  | Success | Request completed; warnings possible |
| 400  | Validation failure | Missing/invalid parameters, mutually exclusive fields, malformed JSON bodies, invalid download payload |
| 404  | Not found | Unknown route or missing resource |
| 413  | Payload too large | `POST /table/download` body exceeds 25 MB |
| 422  | Domain/processing failure | Survey/country unavailable, PPP year unavailable, unsupported breakdown covariates, empty sample-filter results, unknown release |
| 500  | Internal error | Unexpected exception (stack traces never exposed) |

### User-Safe `errors[]` Guarantee

Every string returned in `errors[]` across HTTP 422, 413, and 500 responses (as well as HTTP 400 payload rejections on `POST /table/download`) is guaranteed to be a plain-English sentence safe for direct display to end users (no internal R variable names such as `pip_id`, `manifest`, `triplet`, `Parquet`, `Arrow`, or `Approach C`, and no `cli`/glue markup). HTTP 400 parameter validation messages from `validate_table_input()`, `validate_lookup_input()`, and `validate_description_input()` report invalid request parameters directly.

Representative domain and payload error messages:

| Scenario | HTTP | `errors[]` Message |
|----------|------|--------------------|
| Requested country/survey not in release | 422 | `"The requested country or survey is not available in the selected data release."` |
| Empty survey selection after resolution | 422 | `"No surveys matched your selection. Please select at least one available country and survey year."` |
| Requested PPP year unavailable across selected surveys | 422 | `"None of the selected surveys have data available for the <ppp> PPP standard. Please switch to another PPP year."` |
| Selected surveys lack requested breakdown covariate(s) | 422 | `"None of the selected surveys have data for the selected covariate: Area. Remove this covariate or choose a different survey."` (or, for 2+ covariates: `"None of the selected surveys have data for the selected covariates: Area, Gender. Remove one or more of these covariates or choose a different survey."`) |
| Selected surveys lack sample-filter covariate(s) | 422 | `"None of the selected surveys have data for the selected covariate: Age. Remove this covariate or choose a different survey."` (or, for 2+ covariates: `"None of the selected surveys have data for the selected covariates: Age, Gender. Remove one or more of these covariates or choose a different survey."`) |
| Sample filter matches 0 survey observations | 422 | `"No survey observations matched the selected sample filters. Try broadening your filter selection."` |
| Malformed/unreadable sample filter JSON (`filter_base`) | 422 | `"The selected sample filter could not be read. Please reset your filters and try again."` |
| Unknown data release | 422 | `"The requested data release ('<release>') is not available."` |
| Incomplete/invalid `description_metadata` on `POST /description` | 422 | `"The table description could not be generated because the provided table metadata is incomplete or invalid."` |
| Incomplete/invalid `data` payload on `POST /table/download` | 400 | `"The table data could not be exported because the download payload is incomplete or invalid."` |
| Any unmapped internal/schema error | 422 / 500 | `"Something went wrong processing this request."` *(raw developer diagnostic is logged server-side to `stderr` with the `[piptm][unmapped_error]` prefix)* |

## UI Step Mapping

| UI Step | Description | Endpoints |
|---------|-------------|-----------|
| Step 1 — Survey selection | Discover releases, manifest rows, and categories | `/releases`, `/surveys`, `/surveys-ui`, `/countries`, `/regions`, `/categories`, `/lookup` |
| Step 2 — Configure measures & cuts | List analysis variables, statistics, layout covariates, valid dimensions | `/analysis-variables`, `/statistics`, `/covariates`, `/dimensions` |
| Step 3 — Results | Run computations, render descriptions, and download table outputs | `/table` (GET+POST), `/description`, `/table/download` |
| Utility | Cross-cutting health | `/health` |

---

## Endpoint Reference

Each endpoint entry lists purpose, parameters, and response shape. For brevity, response schemas reference the `ResponseEnvelope` above unless noted.

### `/health` — GET (Utility)
- **Purpose:** Health probe reporting API availability and the current release.
- **Parameters:** None.
- **Response:** `data = { "status": "ok", "release": "2024Q4" }`

### `/releases` — GET (Step 1)
- **Purpose:** List loaded releases and identify the current release.
- **Parameters:** None.
- **Response:** `data = { "releases": [ ... ], "current": "<release-id>" }`

### `/surveys` — GET (Step 1)
- **Purpose:** Return full manifest rows for a release.
- **Query parameters:**
  - `release` *(optional, string)* — Defaults to the current release.
- **Response:** `data = [ { pip_id, country_code, welfare_type, dimensions, ... }, ... ]`; `meta.release` echoes the resolved release.

### `/surveys-ui` — GET (Step 1)
- **Purpose:** Provide a UI-ready survey catalogue with friendly labels and available dimensions for Step 1 grids.
- **Query parameters:** `release` *(optional)* — Defaults to current release.
- **Response:** `data = [ { "pip_id": "COL_2019_GEIH_INC_ALL", "country_code": "COL", "country_label": "Colombia", "year": 2019, "welfare_type": "INC", "welfare_label": "Income", "version": "V2024_03", "dimensions": ["gender","area",...] }, ... ]`; `meta.release` echoes the resolved release.

### `/countries` — GET (Step 1)
- **Purpose:** List unique countries present in the manifest.
- **Query parameters:** `release` *(optional)*.
- **Response:** Sorted array of `{ country_code, country_name }`.

### `/regions` — GET (Step 1)
- **Purpose:** Provide region metadata with member countries.
- **Query parameters:** `release` *(optional)*.
- **Response:** Array of `{ region_code, region_name, countries: [ISO3,…] }`.

### `/categories` — GET (Steps 1–2)
- **Purpose:** List categorical variables available for the sample-base filter panel.
- **Query parameters:**
  - `pip_id` *(optional, repeatable)* — Filters to dimensions present in **all** listed surveys (intersection, not union).
  - `release` *(optional)* — Defaults to current release.
- **Response:** `data = [ { varname, label, categories: [...] }, ... ]`; `meta.filtered=true` when `pip_id` provided; `meta.n_surveys` shows matched survey count.

### `/covariates` — GET (Steps 2–3)
- **Purpose:** List covariates eligible for the four layout slots (columns, rows, super columns, super rows).
- **Query parameters:** Same as `/categories`; intersection logic applies.
- **Response:** `data = [ { "varname": "gender", "label": "Gender", "n_categories": 2 }, ... ]`; when filtered via `pip_id`, `meta.filtered=true` and `meta.n_surveys` indicates the intersection size.

### `/analysis-variables` — GET (Step 2)
- **Purpose:** List analysis variables (Decision 2) with type, label, stat groups, and whether a poverty line slider is required.
- **Query parameters:** `release` *(optional)*.
- **Response:** `data = [ { "varname": "welfare", "label": "Welfare", "type": "continuous", "poverty_line_slider": false, "stat_groups": ["summary","inequality"], "n_categories": null }, ... ]`; `meta.release` echoes the resolved release.

### `/statistics` — GET (Step 2)
- **Purpose:** Return measure groups with member statistics (summary stats, inequality, poverty, welfare, shares).
- **Query parameters:** `release` *(optional)*.
- **Response:** `data = [ { "group": "summary", "group_label": "Summary Statistics", "measures": [ { "measure": "mean", "label": "Mean" }, { "measure": "median", "label": "Median" }, ... ] }, ... ]`; `meta.release` echoes the resolved release.

### `/dimensions` — GET (Step 2)
- **Purpose:** Return the canonical list of valid disaggregation dimensions.
- **Parameters:** None.
- **Response:** `data = [ "gender", "area", "education", ... ]`.

### `/lookup` — GET (Step 1)
- **Purpose:** Resolve `(country_code, year, welfare_type)` triplets to canonical `pip_id`s.
- **Query parameters (parallel arrays):**
  - `country_code` *(required, repeatable ISO3 strings)*
  - `year` *(required, repeatable, integers)*
  - `welfare_type` *(required, repeatable, values `INC` or `CON`)*
  - `release` *(optional)* — Defaults to current release
- **Validation:** `country_code`, `year`, and `welfare_type` **must** have identical lengths (zip semantics). Length mismatch returns HTTP 400.
- **Response:** Array of `{ country_code, year, welfare_type, pip_id }` with `meta.release`.

### `/table` — GET & POST (Step 3)
- **Purpose:** Execute the Table Maker computation engine for up to 15 surveys.
- **Core parameters (query for GET; form or JSON body for POST):**
  - `pip_id` *(required, array ≤15)* — Canonical survey IDs.
  - `analysis_var` *(required, string)* — `welfare`, `pov_status`, or entries from `/analysis-variables`.
  - `measures` *(required, array)* — Names from `/statistics`.
  - `poverty_line` *(required when `analysis_var` == `pov_status` or any slot includes `pov_status`)* — Positive numeric.
  - `column` *(optional, string)* — Variable placed in the Columns slot. Exactly one variable name returned by `/dimensions`.
  - `row` *(optional, string)* — Variable placed in the Rows slot. Exactly one variable name returned by `/dimensions`.
  - `superColumn` *(optional, string)* — Variable placed in the Super Columns slot. Exactly one variable name returned by `/dimensions`.
  - `superRow` *(optional, string)* — Variable placed in the Super Rows slot. Exactly one variable name returned by `/dimensions`.
  - `ppp` *(optional, integer)* — PPP reference year. Default `2021`.
  - `filter_base` *(optional, JSON object)* — Keys are categorical variables; values are arrays of allowed codes (e.g., `{ "gender": [0], "age_group": [1,2] }`).
  - `pop_share_threshold` *(optional, numeric in (0,1))* — Default `0.001` (0.1%); `null` disables masking.
  - `release` *(optional)* — Defaults to current release.
  - `include_metadata` *(optional, string)* — Case-insensitive; any spelling of `"true"` (`"true"`, `"True"`, `"TRUE"`) returns description metadata alongside table output. Any other value (or omission) disables metadata.
  _HTTP transport note_: Query parameters arrive as strings; the types above describe the server-side coercion performed before validation.
- **Responses:**
  - `200 OK` (default) — `data` is an array of per-survey objects formatted according to Approach C (see structure below); `meta = { "release": <release>, "n_surveys": <count> }`.
  - `200 OK` (`include_metadata="true"`) — `data = [<TableRow>,…]`; `meta.description_metadata` populated.
  - `400` — Validation failure (e.g., too many surveys, missing poverty line, invalid PPP).
  - `422` — Domain failure (manifest mismatch, computation error).
- **Notes:**
  - **Approach C Response Format (`include_metadata=false`)**:
    - This Approach C shape applies **only** when `include_metadata` is falsy (the default). When `include_metadata="true"`, `data` remains flat `<TableRow>` rows and `description_metadata` is attached under `meta.description_metadata`.
    - `data` contains one object per survey in `pip_id`.
    - Each survey object has header fields (`pip_id`, `country_code`, `surveyid_year`, `welfare_type`, `poverty_line`, `analysis_var`, `measures[]`).
    - `layout`: fixed keys `columns`, `rows`, `superColumns`, `superRows` (`null` for unused slots). Each active slot contains `varname`, `label`, and ordered `categories` (`{ "code": <val>, "label": <str> }`). Missing categories use `code: null`.
    - `cells`: dense array containing all observed (computed or masked) category combinations. Each cell carries all four slot keys (`superRow`, `row`, `superColumn`, `column`, each `{ "code": <val>, "label": <str> }` or `null`) and a `measures` object keyed by measure name with formatted display strings (e.g. `"12.50"`, `"NR"`, or `null`). Cells omit `population`.
    - Ordering: Category ordering in `layout` and cell ordering in `cells[]` are deterministic and registry-sourced (`superRows` → `rows` → `superColumns` → `columns`).
  - **Layout slot rules**:
    - Query parameters are case-sensitive camelCase (`column`, `row`, `superColumn`, `superRow`).
    - Slots must be filled in order (`column` → `row` → `superColumn` → `superRow`). A gapped assignment (e.g. `column=gender&superRow=area` without `row`) returns HTTP 400. Even though `column=gender&superRow=area` groups the same underlying observations as `column=gender&row=area`, the rejection is a deliberate layout rule that prevents degenerate table headers (super-axis bands with no child axis).
    - Each slot accepts a single variable name; repeating a slot parameter returns HTTP 400.
    - Assigning the same variable to multiple slots returns HTTP 400.
    - The four slots structurally cap `/table` disaggregation to four dimensions. Direct callers of R package function `table_maker(by = ...)` are capped at `length(by) <= 4` via defensive assertion.
  - `filter_base` JSON must be parsable; invalid JSON returns HTTP 422.
  - `pip_id` and `measures` arrays must be non-empty; size >15 returns HTTP 400.
  - All requests are sample-weighted; the poverty line is always PPP USD per day.

#### Example (`POST /table`)

```json
{
  "pip_id": ["COL_2019_GEIH_INC_ALL"],
  "analysis_var": "welfare",
  "measures": ["mean", "gini"],
  "column": "gender",
  "ppp": 2021,
  "filter_base": {"area":["URB"]},
  "include_metadata": "false"
}
```

---

### `/table/download` — POST (Step 3)

- **Purpose:** Convert an already-computed `/table` result into downloadable CSV or RDS bytes without recomputation.
- **Body:**
  ```json
  {
    "format": "csv",
    "data": [
      {
        "pip_id": "COL_2019_GEIH_INC_ALL",
        "country_code": "COL",
        "surveyid_year": 2019,
        "welfare_type": "INC",
        "poverty_line": null,
        "analysis_var": "welfare",
        "measures": ["mean", "gini"],
        "layout": {
          "columns": {
            "varname": "gender",
            "label": "Gender",
            "categories": [
              { "code": 0, "label": "Female" },
              { "code": 1, "label": "Male" }
            ]
          },
          "rows": null,
          "superColumns": null,
          "superRows": null
        },
        "cells": [
          {
            "superRow": null,
            "row": null,
            "superColumn": null,
            "column": { "code": 0, "label": "Female" },
            "measures": { "mean": "12.50", "gini": "45.67" }
          }
        ]
      }
    ]
  }
  ```
  - `format` *(optional, string)* — `csv` (default) or `rds`.
  - `data` *(required, non-empty array of Approach C survey objects)* — copied directly from `/table` response `data` (`include_metadata = false`).
- **Responses:**
  - `200 OK` (`format=csv`) — raw CSV bytes; `Content-Type: text/csv; charset=utf-8`; `Content-Disposition: attachment; filename="table.csv"`.
  - `200 OK` (`format=rds`) — raw RDS bytes; `Content-Type: application/octet-stream`; `Content-Disposition: attachment; filename="table.rds"`.
  - `400` — malformed/empty JSON body, unsupported `format`, legacy flat-row payload, or invalid/empty Approach C `data` payload.
  - `413` — request payload exceeds 25 MB.
- **Notes:**
  - This endpoint is stateless and does not call `table_maker()`.
  - Error responses use the standard JSON envelope with `Content-Type: application/json`.
  - **Exported table structure (Shape A — one row per cell, measures as columns)**:
    - `flatten_table_response()` converts the Approach C `data[]` payload into a rectangular table with one row per element of `cells[]` (stacked across surveys in `data[]` order):
      1. **6 leading survey header columns** (always present): `pip_id` (character), `country_code` (character), `surveyid_year` (integer), `welfare_type` (character), `analysis_var` (character), `poverty_line` (numeric; `NA` / empty in CSV when `null`).
      2. **Active dimension columns**: one character column per active (non-`null`) slot in `layout`, ordered `superRows` → `rows` → `superColumns` → `columns`, named after `layout[[slot]]$varname` (e.g. `educat4`, `area`, `gender`) and populated with each cell's category `label` (e.g. `"None"`, `"Rural"`, `"Female"`, `"Unknown"`). Unused slots are omitted; a national table (no active slots) has zero dimension columns.
      3. **Measure columns**: one character column per measure in `measures[]` order, preserving formatted display strings verbatim (`"12.50"`, `"NR"`, or `NA`/empty when `null`). Measure columns are never coerced to numeric.

---

### `/description` — POST (Step 3)

- **Purpose:** Render a natural-language table description either from cached metadata or by recomputing.
- **Body modes (mutually exclusive):**
  1. **Metadata fast path** — `{ "description_metadata": <object produced by /table include_metadata=true> }`
  2. **Fallback recomputation** — Same parameter set as `/table` (pip_id, measures, etc.).
- **Validation:** Supplying both `description_metadata` **and** table parameters returns HTTP 400. When recomputing, the same validators as `/table` apply.
- **Response:** Plain-text Markdown (`Content-Type: text/plain`) describing the requested table. Errors use JSON envelopes with HTTP 400/422.
- **Errors:** Even though the serializer is `text/plain`, error bodies are JSON objects (`{ "status": "error", ... }`). Consumers should parse the body as JSON when `status="error"`.

---

## Shared Schemas & Common Structures

### Table Output Rows

`/table` produces long-format records formatted for API presentation. Numeric values are formatted as strings:
- Proportions and percentage measures (`gini`, `headcount`, `poverty_gap`, `severity`, `pop_share`, `target_within_group_share`, `target_survey_share`) are scaled by 100 with 2 decimal places (e.g. `"6.70"`).
- Counts (`pop_poverty`, `nobs`) are formatted as whole numbers.
- Other continuous measures (`mean`, `median`, `sd`, `var`, `min`, `max`, `p10`, `p25`, `p75`, `p90`, `sum`, `mld`, `watts`) are formatted with 2 decimal places.
- Below-threshold cells, and cells whose population share could not be computed (`NA`, e.g. a zero-denominator group), are masked with `value = "NR"`.
- Genuine missing or non-finite values (`NA`, `NaN`, `Inf`, `-Inf`) are serialized as `null`.
- The `population` column is removed from `/table` output.

```yaml
TableRow:
  type: object
  required: [pip_id, analysis_var, measure, value]
  properties:
    pip_id: { type: string }
    analysis_var: { type: string }
    measure: { type: string }
    value:
      type: string
      nullable: true
      description: Formatted number string, "NR" for masked cells, or null for genuine missing.
    "<dimension>":
      type: string
      description: Top-level column per filled disaggregation slot variable (e.g. gender, area).
    poverty_line: { type: number, nullable: true }
    warnings: { type: array, items: { type: string } }
```

### Description Metadata

When `include_metadata=true`, `/table` returns an additional object consumed by `/description`:

```yaml
DescriptionMetadata:
  type: object
  required: [params, layout, measures]
  properties:
    params:
      type: object
      description: The final resolved request parameters.
    layout:
      type: object
      description: Slot assignments (columns, rows, super columns, super rows).
    measures:
      type: array
      items: { type: string }
```

## Validation Rules (Cross-Cutting)

1. **Survey Cap:** `/table` enforces a hard limit of 15 `pip_id` entries per request.
2. **Identifiers:** `pip_id` must match `^[A-Z]{3}_[0-9]{4}_[A-Z0-9_-]{1,40}$`.
3. **PPP Year:** `ppp` must be a single positive integer. Strings must contain digits only (`"2017"` valid, `"2e3"` rejected).
4. **Poverty Line:** Required whenever poverty status is analyzed or disaggregated; must be positive finite numeric.
5. **filter_base:** JSON object where keys are categorical variables and values are arrays of allowed category codes. Invalid JSON returns HTTP 422.
6. **Lookup Triplets:** `country_code`, `year`, `welfare_type` operate as zipped triplets; unequal lengths are rejected.
7. **Optional Defaults:** `ppp=2021`, `pop_share_threshold=0.001`, `include_metadata="false"`, `release=<current>`.
8. **Intersection Logic:** `/categories` and `/covariates` return only dimensions present in **all** supplied surveys when `pip_id` filters are used.
9. **Download Payload Guard:** `/table/download` rejects payloads larger than 25 MB with HTTP 413.

## CORS

The API enables permissive CORS for browser clients:

```
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, OPTIONS
Access-Control-Allow-Headers: Content-Type
```

`OPTIONS` preflight requests return `204 No Content` with empty body as required by modern browsers.

## Examples & Quick Reference

| Endpoint | Methods | Typical Use |
|----------|---------|-------------|
| `/table` | GET, POST | Execute tables for selected surveys |
| `/table/download` | POST | Convert `/table` rows into downloadable CSV or RDS bytes |
| `/description` | POST | Render natural-language table description |
| `/cell-definition` | GET | Return Page 2 three-section cell explanation JSON |
| `/analysis-variables` | GET | Populate Decision 2 variable picker |
| `/statistics` | GET | Populate statistic picker chips |
| `/categories` | GET | Power Step 1 Available Variables filter chips |
| `/covariates` | GET | Populate layout slot drawers |
| `/lookup` | GET | Resolve free-form triplets to `pip_id`s |

## Change Control

- All changes to this contract must be traced back to a `/cg-plan` entry.
- For new endpoints, include: purpose, UI mapping, parameter table, schema snippet, validation rules, and sample payloads.
- Update this document whenever Plumber handlers gain or drop parameters, or when validation/business rules change.
