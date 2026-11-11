# Table Maker API — Swagger UI Guide

This guide documents every endpoint exposed by the Plumber API in
`inst/plumber/plumber.R`, how to exercise each one from Swagger UI, and how
each endpoint maps to the three-step Table Maker UI wizard described in
`docs/ui-description.md`.

## Start the API

In R:

```r
library(piptm)
pr <- plumber::plumb(system.file("plumber", "plumber.R", package = "piptm"))
pr$run(port = 8080)
```

## Open Swagger UI

Navigate to: http://localhost:8080/__docs__/

Swagger UI groups the 15 endpoints into five sections matching the Table Maker flow:

1. **Step 1 — Survey Selection**: `GET /surveys-ui`, `GET /countries`, `GET /regions`
2. **Step 2 — Table Configuration**: `GET /categories`, `GET /analysis-variables`, `GET /statistics`, `GET /covariates`, `GET /cell-definition`
3. **Step 3 — Table Computation & Export**: `GET /table`, `POST /table`, `POST /metadata` *(WIP — table metadata & info)*, `POST /table/download`
4. **System & Releases**: `GET /health`, `GET /releases`
5. **Optional / Developer Endpoints (Not Needed for UI)**: `GET /lookup`, `GET /surveys`, `GET /dimensions` *(WIP)*

Required query parameters are badged `* required`, enum-constrained parameters provide dropdown selectors, and `POST /metadata` and `POST /table/download` include ready-to-run JSON request-body examples in the Swagger **Examples** selector.

## Response envelope

Every JSON endpoint (all endpoints except `POST /metadata` and
`POST /table/download`) returns the
same envelope shape:

```json
{
  "status": "success",
  "data": { "...": "..." },
  "warnings": [],
  "errors": [],
  "meta": { "release": "..." }
}
```

- `status` is `"success"` or `"error"`.
- On error, `data` is `null`, `errors` holds one or more human-readable
  messages, and the HTTP status code is `400` (validation failure) or `422`
  (domain/runtime failure). Uncaught bugs fall through to the global error
  handler and always return `500` with a generic message.
- `meta` commonly includes `release`; some endpoints add `filtered` and
  `n_surveys` (see `/categories` and `/covariates`).

`POST /metadata` is one exception: its serializer is `text`, so a
successful response body is raw HTML or Markdown (not the JSON envelope), and
only its error responses are JSON-encoded strings using the same envelope
shape.

`POST /table/download` is the other exception: successful responses are file
bytes (`text/csv` or `application/octet-stream`) with attachment headers;
error responses remain JSON envelopes.

## Endpoint reference

### `GET|POST /table` — compute a table (Step 3 "Generate table")

Computes poverty, inequality, and summary-statistic measures for one or more
surveys. This is the endpoint behind the Step 2 "Generate table" button and
Step 3 results.

Query parameters (works identically as a POST form body):

| Parameter | Type | Required | Notes |
|---|---|---|---|
| `analysis_var` | string | yes | e.g. `welfare`, `pov_status`, or any optional dimension from `GET /analysis-variables` |
| `pip_id` | string[] | yes | repeatable; 1–15 survey identifiers |
| `measures` | string[] | yes | repeatable; must be in `GET /statistics` |
| `poverty_line` | number | conditional | required when `analysis_var = "pov_status"` or any slot contains `pov_status` |
| `column` | string | no | Decision-3 Columns slot (single variable from `GET /dimensions`) |
| `row` | string | no | Decision-3 Rows slot (single variable from `GET /dimensions`) |
| `superColumn` | string | no | Decision-3 Super Columns slot (single variable from `GET /dimensions`) |
| `superRow` | string | no | Decision-3 Super Rows slot (single variable from `GET /dimensions`) |
| `ppp` | integer | no | PPP reference year; default `2021` |
| `filter_base` | string | no | JSON-encoded sample-base filter object, e.g. `{"gender":[0],"age_group":[1,2]}` (Decision 1) |
| `pop_share_threshold` | number | no | cell-masking threshold (`NR`) in `(0, 1)`; default `0.001` (0.1%); send empty/omit to disable |
| `release` | string | no | defaults to the current release |
| `include_metadata` | string | no | `"true"` to also return `description_metadata` in `meta` (used to power `POST /metadata` fast path); default `"false"` |

`pip_id` format: each value must match `^[A-Z]{3}_[0-9]{4}_[A-Z0-9_-]{1,40}$`
(3-letter ISO3 country code, 4-digit year, then survey-info segment), e.g.
`ARM_2012_ILCS_CON_ALL`. This pattern is enforced server-side; it is reused
below for `/metadata` and `/lookup` examples.

Example (Swagger "Try it out", query parameters):

```
analysis_var=welfare
pip_id=ARM_2012_ILCS_CON_ALL
measures=mean
measures=gini
ppp=2021
```

Errors: `400` for invalid parameters (e.g. too many `pip_id`, malformed
`pip_id` format, unknown `measures`, malformed `ppp`); `422` when validation
passes but computation fails (e.g. unknown release, malformed `filter_base`
JSON, domain error from `table_maker()`).

### `POST /metadata` — render table metadata

Request body must be JSON. Two mutually exclusive modes; supplying both
returns `400`.

**Fast path** — render only, no recomputation. Wrap the metadata returned by
`/table?include_metadata=true` (or previously cached) under
`description_metadata`:

```json
{
  "description_metadata": {
    "params": {
      "pip_id": ["ARM_2012_ILCS_CON_ALL"],
      "analysis_var": "welfare",
      "measures": ["mean", "gini"],
      "ppp": 2021
    },
    "provenance": { "release": "20260401_TEST", "ppp_year": 2021 },
    "surveys": { "loaded": ["..."], "summary": ["..."] },
    "resolved_labels": {}
  }
}
```

Note: the fast path does not re-validate `params.pip_id` against the
`pip_id` pattern above — it trusts `description_metadata` as already-validated
metadata (typically echoed back from a prior `/table?include_metadata=true`
call) and only checks that `description_metadata` and `params` are present.

**Fallback path** — recomputes via `table_maker(include_metadata = TRUE)`.
Supply table parameters directly at the top level of the body (same field
names as `/table`, but as native JSON types rather than query-string values —
see note below on `filter_base`). Unlike `/table`, this path has no query-arg
defaults: omitting `ppp` or `pop_share_threshold` here does not fall back to
`2021`/`0.01` — it passes `NULL` through to `table_maker()`, which falls back
to the manifest's PPP default and disables cell suppression entirely. Send
explicit values if you want `/table`-equivalent behavior. The `pip_id` values
here ARE validated against the pattern above (unlike the fast path).

```json
{
  "pip_id": ["ARM_2012_ILCS_CON_ALL"],
  "analysis_var": "welfare",
  "measures": ["mean", "gini"],
  "ppp": 2021
}
```

Optional `format` field (either mode): `"html"` (default —
`Content-Type: text/html; charset=utf-8`, inline-styled) or `"markdown"`
(`Content-Type: text/plain`). Any other value returns `400`.

Click **Execute** → the response body is the rendered description directly
(HTML or Markdown text), not a JSON envelope. Validation and runtime failures
return a JSON-encoded error string with status `400` or `422`.

### `POST /table/download` — convert an existing table result to CSV/RDS

Converts already-computed `/table` rows into downloadable file bytes.
This endpoint is stateless and does not recompute via `table_maker()`.

Request body (JSON):

```json
{
  "format": "csv",
  "data": [
    {
      "pip_id": "BIH_2011_HBS_CON_ALL",
      "country_code": "BIH",
      "surveyid_year": 2011,
      "welfare_type": "CON",
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

- `format` is optional and defaults to `csv`; allowed values: `csv`, `rds`.
- `data` is required and must be a non-empty array of Approach C survey objects
  copied from `/table` response `data` when `include_metadata=false`.

Expected responses:

- `200` + `Content-Type: text/csv; charset=utf-8` + `Content-Disposition: attachment; filename="table.csv"`
- `200` + `Content-Type: application/octet-stream` + `Content-Disposition: attachment; filename="table.rds"`
- `400` for malformed body / invalid `format` / empty `data` / legacy flat-row payload
- `413` for payloads larger than 25 MB

### `GET /lookup` — resolve country/year/welfare_type to pip_id

Resolves `country_code` / `year` / `welfare_type` triples (equal-length,
repeatable vectors) to `pip_id` values.

| Parameter | Type | Required |
|---|---|---|
| `country_code` | string[] | yes |
| `year` | integer[] | yes |
| `welfare_type` | string[] (`INC` or `CON`) | yes |
| `release` | string | no |

All three repeatable parameters must have the same length (paired by
position).

### `GET /surveys` — full survey manifest

Returns the complete manifest for a release, including a `dimensions` array
per row. No filters/params besides `release`.

### `GET /surveys-ui` — Step 1 survey grid data

Returns a UI-ready survey catalog (`pip_id`, `country_code`, `country_label`,
`year`, `welfare_type`, `welfare_label`, and `version`) — this is the data
source for the Step 1 country/year/welfare-type grid described in
`docs/ui-description.md`. It does not include per-survey dimensions; use
`GET /categories` and `GET /covariates` for variable availability. Client-side
code builds the grid rows, tooltips, and "N / 15 selected" counter from this
payload; it is not paginated or pre-filtered by the API.

### `GET /countries` — Step 1 country list / search

Unique `country_code` / `country_name` pairs derived from the manifest, sorted
by `country_code`. Backs the Step 1 country search box.

### `GET /regions` — Step 1 region filter chip

Region metadata with member `country_code` arrays. Backs the Step 1 region
filter chip.

### `GET /releases` — list loaded releases

Returns `{ releases: [...], current: "..." }`. No request parameters. Note:
unlike other endpoints this returns the payload directly as `data` with no
`release` param of its own — there's nothing to resolve.

### `GET /analysis-variables` — Decision 2 variable dropdown

Returns analysis variables for Decision 2: each entry has `varname`, `label`,
`type` (continuous/binary/etc.), `stat_groups`, and `poverty_line_slider`.
Client code uses these fields to gate which statistics are enabled/disabled per
`docs/ui-description.md`'s "Statistic gating by variable type" rules.

### `GET /dimensions` — valid disaggregation dimension names

Returns the flat list of dimension names that pass `/table` slot validation
(`column`, `row`, `superColumn`, `superRow`). This is not guaranteed to be
identical to the set `GET /covariates` returns; `table_maker()` runs its own
internal check against the covariate registry, so a slot value accepted here can
still fail with `422` if it is not also a valid covariate. For Decision 3 layout
slots, use `GET /covariates` as the authoritative source. No request parameters.

### `GET /categories` — Decision 1 sample-base filter panel

Categorical variables (with subcategories) for the Decision 1 filter panel.

| Parameter | Type | Required |
|---|---|---|
| `pip_id` | string[] | no — omit for the full catalog |
| `release` | string | no |

When `pip_id` is supplied, the response is restricted to variables present in
*every* matched survey (intersection of manifest `dimensions`); `meta.filtered`
is `true` and `meta.n_surveys` reports how many supplied `pip_id` values
matched the manifest.

### `GET /covariates` — Decision 3 layout-slot covariates

Same shape and filtering behavior as `/categories`, but returns the covariate
list for the four Decision 3 layout slots (Columns, Rows, Super Columns,
Super Rows) instead of sample-base filter variables.

| Parameter | Type | Required |
|---|---|---|
| `pip_id` | string[] | no — omit for the full catalog |
| `release` | string | no |

### `GET /statistics` — Decision 2 statistic groups

Returns the statistic groups and their measures (Shares and Counts, Summary
Statistics, Inequality, Poverty), loaded from the release's measure registry
(`inst/extdata/tm_measure_spec.yaml`). This is the source for the Decision 2
"grouped statistic dropdown".

### `GET /cell-definition` — Page 2 cell explanation (Step 2)

Returns the three-section natural-language cell explanation (`who_is_in_table`,
`what_a_cell_is`, `what_is_shown`, `notes`) for the current Page 2 configuration
without calling `table_maker()` or accessing Arrow microdata.

The `what_a_cell_is` section carries a uniform `{ title, structure_text, items,
example_text }` shape (`dimensions` and `example_cell_label` remain available for
structural reference). `items` is always a JSON array of plain-English strings
and never exceeds 4 entries:

- **4 or fewer total cells** → `items` enumerates every combined cell
  (e.g., `"Men living in urban areas"`), in registry category order.
- **More than 4 total cells** → `items` lists one line per active layout slot
  (e.g., `"Column (gender): men or women."`), at most four lines.
- **No layout slot selected** → `items` is `[]` and `example_text` is `null`.

When a layout-slot variable is also constrained by `filter_base`, only the
retained categories appear in `items` and in the total cell count. When the
variable registry is unavailable, `items` is `[]` and the cell-count suffix is
omitted.

Query parameters (Swagger "Try it out"):

| Parameter | Type | Required | Notes |
|---|---|---|---|
| `analysis_var` | string | yes | e.g. `welfare`, `pov_status`, or any dimension from `GET /analysis-variables` |
| `measures` | string[] | yes | repeatable; e.g. `measures=mean&measures=gini` |
| `poverty_line` | number | conditional | required when `analysis_var = "pov_status"`, `"pov_status"` appears in any slot, or poverty measures requested |
| `column`, `row`, `superColumn`, `superRow` | string | no | layout slots (filled in order without gaps, from `GET /covariates`) |
| `ppp` | integer | no | PPP reference year; default `2021` |
| `filter_base` | string | no | JSON-encoded sample-base filter object, e.g. `{"age_group":["3"],"educat7":["4"]}` |
| `pop_share_threshold` | number | no | cell-masking threshold in `(0, 1)`; default `0.001` (0.1%); send empty to disable |
| `welfare_type` | string | no | `"INC"` or `"CON"`; defaults to `"daily welfare"` when omitted |
| `release` | string | no | defaults to the current release |

Example (Swagger "Try it out", query parameters):

```
analysis_var=welfare
measures=mean
measures=median
measures=gini
column=gender
row=educat4
superColumn=area
filter_base={"age_group":["3"],"educat7":["4"]}
ppp=2021
welfare_type=INC
```

Equivalent percent-encoded request URL:

```text
/cell-definition?analysis_var=welfare&measures=mean&measures=median&measures=gini&column=gender&row=educat4&superColumn=area&filter_base=%7B%22age_group%22%3A%5B%223%22%5D%2C%22educat7%22%3A%5B%224%22%5D%7D&ppp=2021&welfare_type=INC
```

In `what_is_shown.measures[]`, `text` and `total_note` provide the displayed
prose, while `counts` and `out_of` (populated for share measures) are optional
supporting fields.

### `GET /health` — health check

Returns `{ "status": "ok", "release": "..." }`. As with `/releases`, `release`
lives directly in `data`, not `meta` (this endpoint passes no `meta` argument).
No request parameters. Use this to verify the API is reachable before testing
other endpoints.

## Mapping endpoints to the UI wizard

| UI step | Endpoints used |
|---|---|
| Step 1 — survey grid | `GET /surveys-ui`, `GET /countries`, `GET /regions`, `GET /surveys` |
| Step 2, Decision 1 — sample base | `GET /categories` |
| Step 2, Decision 2 — statistics | `GET /analysis-variables`, `GET /statistics` |
| Step 2, Decision 3 — layout slots | `GET /covariates` |
| Step 2 — live cell explanation | `GET /cell-definition` |
| Step 2 footer / Step 3 results | `GET|POST /table`, `POST /metadata`, `POST /table/download` |
| Cross-cutting | `GET /releases`, `GET /health`, `GET /dimensions` |

## Key points

1. All JSON endpoints return the envelope described above; check `status`
   before reading `data`.
2. `POST /metadata` and `POST /table/download` do not use the JSON envelope
   on success — they return rendered text and file bytes respectively.
3. `filter_base` encoding differs by endpoint: on query-string endpoints
   (`/table` and `GET /cell-definition`) it is a **JSON-encoded string** that
   the server parses with `jsonlite::fromJSON()`. On `/metadata`'s fallback
   path (a JSON body) it must be a **native nested JSON object**, not a string
   — it is used as-is with no parsing step. On `GET /cell-definition`, malformed
   JSON returns `400` (consistent with validation).
4. `pip_id` is capped at 15 values per `/table` request (Step 1's "at most 15
   surveys" rule is enforced server-side, not just in the UI).
5. `/categories` and `/covariates` support the same optional `pip_id`
   intersection-filtering behavior — pass the Step 1 selection to narrow
   Decision 1 / Decision 3 options to variables common to all selected
   surveys.
6. For `/table/download`, submit only `{format, data}` where `data` is the
   array from `/table`; do not include envelope fields (`status`, `warnings`,
   `errors`, `meta`).
