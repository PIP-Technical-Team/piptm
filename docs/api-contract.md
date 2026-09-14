# Table Maker API Contract

**Version:** 2026-08-31  
**Owner:** Table Maker API Team  
**Audience:** PIP Platform UI/IT team  
**Plan Reference:** `.cg-docs/plans/2026-08-31-api-contract-openapi.md`

This document specifies every public endpoint exposed by the `piptm` Plumber service. It maps the API surface to the three-step Table Maker UI, lists request and response schemas, records validation rules, and captures operational constraints (sessions, CORS, error handling).

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
- Error responses: `status="error"`, `data=null`; `errors` lists messages suitable for UI display.

## Error Codes

| HTTP | Meaning | Common Causes |
|------|---------|---------------|
| 200  | Success | Request completed; warnings possible |
| 400  | Validation failure | Missing/invalid parameters, mutually exclusive fields, malformed JSON bodies |
| 404  | Not found | `/session/<id>/surveys` when the session expired or never existed |
| 422  | Domain/processing failure | Manifest lookup errors, computation failures, release mismatch |
| 500  | Internal error | Unexpected exception (stack traces never exposed) |

## UI Step Mapping

| UI Step | Description | Endpoints |
|---------|-------------|-----------|
| Step 1 — Survey selection | Discover releases, manifest rows, categories, and persist selections | `/releases`, `/surveys`, `/surveys-ui`, `/countries`, `/regions`, `/categories`, `/lookup`, `/session/surveys`, `/session/<id>/surveys` |
| Step 2 — Configure measures & cuts | List analysis variables, statistics, layout covariates, valid dimensions | `/analysis-variables`, `/statistics`, `/covariates`, `/dimensions` |
| Step 3 — Results | Run computations, render descriptions, fetch metadata | `/table` (GET+POST), `/description` |
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

### `/session/surveys` — POST (Step 1)
- **Purpose:** Persist selected survey identifiers in an in-memory session (used to resume selections later).
- **Body:**
  ```json
  { "pip_id": ["COL_2019_GEIH_INC_ALL", "BOL_2018_ECH_CON_ALL"] }
  ```
  - Accepts one or more values matching `^[A-Z]{3}_[0-9]{4}_[A-Z0-9_-]{1,40}$`. (The `/table` endpoint enforces the 15-survey execution cap.)
- **Response:** `data = { "session_id": "a1b2c3d4e5f6" }`.
- **Session rules:** See [Session Management Details](#session-management-details) below.

### `/session/<id>/surveys` — GET (Step 1)
- **Purpose:** Fetch stored survey IDs by session ID.
- **Path parameter:** `id` *(required, string)*.
- **Responses:**
  - `200 OK` — `data = { "pip_id": [ ... ] }`
  - `404 Not Found` — Session missing or expired.

### `/table` — GET & POST (Step 3)
- **Purpose:** Execute the Table Maker computation engine for up to 15 surveys.
- **Core parameters (query for GET; form or JSON body for POST):**
  - `pip_id` *(required, array ≤15)* — Canonical survey IDs.
  - `analysis_var` *(required, string)* — `welfare`, `pov_status`, or entries from `/analysis-variables`.
  - `measures` *(required, array)* — Names from `/statistics`.
  - `poverty_line` *(required when `analysis_var` == `pov_status` or `by` includes `pov_status`)* — Positive numeric.
  - `by` *(optional, array)* — Dimensions (≤4 entries) returned by `/dimensions`; requests with more than four entries are rejected server-side.
  - `ppp` *(optional, integer)* — PPP reference year. Default `2021`.
  - `filter_base` *(optional, JSON object)* — Keys are categorical variables; values are arrays of allowed codes (e.g., `{ "gender": [0], "age_group": [1,2] }`).
  - `pop_share_threshold` *(optional, numeric in (0,1))* — Default `0.01`; `null` disables suppression.
  - `release` *(optional)* — Defaults to current release.
  - `include_metadata` *(optional, string)* — Case-insensitive; any spelling of `"true"` (`"true"`, `"True"`, `"TRUE"`) returns description metadata alongside table output. Any other value (or omission) disables metadata.
  _HTTP transport note_: Query parameters arrive as strings; the types above describe the server-side coercion performed before validation.
- **Responses:**
  - `200 OK` (default) — `data` is a long-format array of TableRow objects (see [Shared Schemas](#shared-schemas--common-structures)); `meta = { "release": <release>, "n_surveys": <count>, "description_metadata": null }`.
  - `200 OK` (`include_metadata="true"`) — `data = { "data": [<TableRow>,…], "description_metadata": <DescriptionMetadata> }`; `meta.description_metadata` populated.
  - `400` — Validation failure (e.g., too many surveys, missing poverty line, invalid PPP).
  - `422` — Domain failure (manifest mismatch, computation error).
- **Notes:**
  - `filter_base` JSON must be parsable; invalid JSON returns HTTP 400.
  - `pip_id` and `measures` arrays must be non-empty; size >15 returns HTTP 400.
  - All requests are sample-weighted; the poverty line is always PPP USD per day.

#### Example (`POST /table`)

```json
{
  "pip_id": ["COL_2019_GEIH_INC_ALL"],
  "analysis_var": "welfare",
  "measures": ["mean", "gini"],
  "by": ["gender"],
  "ppp": 2021,
  "filter_base": {"area":["URB"]},
  "include_metadata": "false"
}
```

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

`/table` produces long-format records similar to:

```yaml
TableRow:
  type: object
  required: [pip_id, analysis_var, measure, value]
  properties:
    pip_id: { type: string }
    analysis_var: { type: string }
    measure: { type: string }
    value: { type: number }
    by:
      type: object
      description: Key/value map of selected dimensions.
    poverty_line: { type: number, nullable: true }
    population: { type: number, nullable: true }
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

1. **Survey Cap:** `/table` enforces a hard limit of 15 `pip_id` entries per request. Other endpoints (for example `/session/surveys`) store whatever length is provided but downstream `/table` calls must respect the 15-survey cap.
2. **Identifiers:** `pip_id` must match `^[A-Z]{3}_[0-9]{4}_[A-Z0-9_-]{1,40}$`.
3. **PPP Year:** `ppp` must be a single positive integer. Strings must contain digits only (`"2017"` valid, `"2e3"` rejected).
4. **Poverty Line:** Required whenever poverty status is analyzed or disaggregated; must be positive finite numeric.
5. **filter_base:** JSON object where keys are categorical variables and values are arrays of allowed category codes. Invalid JSON returns HTTP 400.
6. **Sessions:** Stored process-locally; expire after 3600 seconds; retrieval after expiry returns 404.
7. **Lookup Triplets:** `country_code`, `year`, `welfare_type` operate as zipped triplets; unequal lengths are rejected.
8. **Optional Defaults:** `ppp=2021`, `pop_share_threshold=0.01`, `include_metadata="false"`, `release=<current>`.
9. **Intersection Logic:** `/categories` and `/covariates` return only dimensions present in **all** supplied surveys when `pip_id` filters are used.

## Session Management Details

- Session IDs are random 12-character alphanumeric strings.
- Collision handling retries up to 20 times before aborting with an error.
- Session storage is in-memory (`new.env()`); load-balanced deployments do **not** share sessions across instances.
- Expired sessions are purged on retrieval attempts.

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
| `/description` | POST | Render natural-language table description |
| `/analysis-variables` | GET | Populate Decision 2 variable picker |
| `/statistics` | GET | Populate statistic picker chips |
| `/categories` | GET | Power Step 1 Available Variables filter chips |
| `/covariates` | GET | Populate layout slot drawers |
| `/lookup` | GET | Resolve free-form triplets to `pip_id`s |
| `/session/surveys` & `/session/<id>/surveys` | POST / GET | Save/restore survey selections |

## Change Control

- All changes to this contract must be traced back to a `/cg-plan` entry.
- For new endpoints, include: purpose, UI mapping, parameter table, schema snippet, validation rules, and sample payloads.
- Update this document whenever Plumber handlers gain or drop parameters, or when validation/business rules change.
