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

## Response envelope

Every JSON endpoint (all endpoints except `POST /description`) returns the
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

`POST /description` is the one exception: its serializer is `text`, so a
successful response body is raw HTML or Markdown (not the JSON envelope), and
only its error responses are JSON-encoded strings using the same envelope
shape.

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
| `poverty_line` | number | conditional | required when `analysis_var = "pov_status"` or `"pov_status"` appears in `by` |
| `by` | string[] | no | repeatable; disaggregation dimensions from `GET /dimensions` (supports up to the 4 Decision‑3 layout slots) |
| `ppp` | integer | no | PPP reference year; default `2021` |
| `filter_base` | string | no | JSON-encoded sample-base filter object, e.g. `{"gender":[0],"age_group":[1,2]}` (Decision 1) |
| `pop_share_threshold` | number | no | cell-suppression threshold in `(0, 1)`; default `0.01`; send empty/omit to disable |
| `release` | string | no | defaults to the current release |
| `include_metadata` | string | no | `"true"` to also return `description_metadata` in `meta` (used to power `POST /description` fast path); default `"false"` |

`pip_id` format: each value must match `^[A-Z]{3}_[0-9]{4}_[A-Z0-9_-]{1,40}$`
(3-letter ISO3 country code, 4-digit year, then survey-info segment), e.g.
`ARM_2012_ILCS_CON_ALL`. This pattern is enforced server-side; it is reused
below for `/description`, `/lookup`, and `/session/surveys` examples.

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

### `POST /description` — render a natural-language table description

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
    "provenance": { "release": "TEST_2024", "ppp_year": 2021 },
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

Returns a UI-ready survey catalog (`pip_id`, labels, and dimensions) — this is
the data source for the Step 1 country/year/welfare-type grid described in
`docs/ui-description.md`. Client-side code builds the grid rows, tooltips, and
"N / 15 selected" counter from this payload; it is not paginated or
pre-filtered by the API.

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
`type` (continuous/binary/etc.), and `stat_groups`. Client code uses `type`
and `stat_groups` to gate which statistics are enabled/disabled per
`docs/ui-description.md`'s "Statistic gating by variable type" rules. Note:
this endpoint does not return a `poverty_line_slider` field — the poverty-line
slider is a client-side UI rule triggered by `analysis_var`/`by` containing
`pov_status` (see `/table`'s `poverty_line` requirement), not a server-supplied
flag.

### `GET /dimensions` — valid disaggregation dimension names

Returns the flat list of dimension names that pass `/table`'s outer `by`
validation (`400`-level check). This is not guaranteed to be identical to the
set `GET /covariates` returns; `table_maker()` runs its own internal check
against the covariate registry, so a `by` value accepted here can still fail
with `422` if it is not also a valid covariate. For Decision 3 layout slots,
use `GET /covariates` as the authoritative source. No request parameters.

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

### `POST /session/surveys` — store a Step 1 survey selection

Stores a validated set of `pip_id` values server-side (in-memory,
process-local, expires after 1 hour) and returns `{ "session_id": "..." }`.
Useful for handing off a Step 1 selection across page loads/tabs without
re-transmitting the full list.

| Parameter | Type | Required |
|---|---|---|
| `pip_id` | string[] | yes — repeatable; must match the canonical `pip_id` pattern |

### `GET /session/:id/surveys` — retrieve a stored survey selection

Path parameter `id` is the session ID returned by `POST /session/surveys`.
Returns `{ "pip_id": [...] }`. Returns `404` if the session does not exist or
has expired.

### `GET /statistics` — Decision 2 statistic groups

Returns the statistic groups and their measures (Shares and Counts, Summary
Statistics, Inequality, Poverty), loaded from the release's measure registry
(`inst/extdata/tm_measure_spec.yaml`). This is the source for the Decision 2
"grouped statistic dropdown".

### `GET /health` — health check

Returns `{ "status": "ok", "release": "..." }`. As with `/releases`, `release`
lives directly in `data`, not `meta` (this endpoint passes no `meta` argument).
No request parameters. Use this to verify the API is reachable before testing
other endpoints.

## Mapping endpoints to the UI wizard

| UI step | Endpoints used |
|---|---|
| Step 1 — survey grid | `GET /surveys-ui`, `GET /countries`, `GET /regions`, `GET /surveys`, `POST /session/surveys`, `GET /session/:id/surveys` |
| Step 2, Decision 1 — sample base | `GET /categories` |
| Step 2, Decision 2 — statistics | `GET /analysis-variables`, `GET /statistics` |
| Step 2, Decision 3 — layout slots | `GET /covariates` |
| Step 2 footer / Step 3 results | `GET|POST /table`, `POST /description` |
| Cross-cutting | `GET /releases`, `GET /health`, `GET /dimensions` |

## Key points

1. All JSON endpoints return the envelope described above; check `status`
   before reading `data`.
2. `POST /description` is the only endpoint that does not use the envelope on
   success — its body is the rendered HTML/Markdown itself.
3. `filter_base` encoding differs by endpoint: on `/table` (a query-string
   endpoint) it is a **JSON-encoded string** that the server parses with
   `jsonlite::fromJSON()` — a malformed string returns `422`, not `400`. On
   `/description`'s fallback path (a JSON body) it must be a **native nested
   JSON object**, not a string — it is used as-is with no parsing step.
4. `pip_id` is capped at 15 values per `/table` request (Step 1's "at most 15
   surveys" rule is enforced server-side, not just in the UI).
5. `/categories` and `/covariates` support the same optional `pip_id`
   intersection-filtering behavior — pass the Step 1 selection to narrow
   Decision 1 / Decision 3 options to variables common to all selected
   surveys.
