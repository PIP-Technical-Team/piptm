# piptm

Computation engine and API backend for the PIP Table Maker.

`piptm` loads harmonized survey microdata from Arrow/Parquet partitions,
resolves release-specific manifests, and computes poverty, inequality,
welfare, and share statistics through `table_maker()`.

## Current scope

- R API for table computation: `table_maker()`
- Survey ID resolver: `pip_lookup()`
- Plumber HTTP API for UI/integration workflows
- Release-aware metadata endpoints for UI selectors and constraints

## Setup

`piptm` reads data locations from environment variables:

```text
PIPTM_ARROW_ROOT=Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/arrow
PIPTM_MANIFEST_DIR=Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/manifests
PIPTM_REGISTRY_DIR=Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/registry
```

Set them in `~/.Renviron` (for example with `usethis::edit_r_environ()`),
then restart R.

If you do not set environment variables, configure paths explicitly:

```r
piptm::set_arrow_root("path/to/arrow")
piptm::set_manifest_dir("path/to/manifests")
```

For the variable registry, set `PIPTM_REGISTRY_DIR` before loading `{piptm}`
(or reload the package after setting it). There is currently no exported
`set_registry_dir()` helper.

```r
Sys.setenv(PIPTM_REGISTRY_DIR = "path/to/registry")
devtools::load_all()
```

## R usage

### `table_maker()`

Core inputs:

- `pip_id`: one or more canonical survey IDs
- `analysis_var`: analysis variable (for example `welfare`, `pov_status`, or allowed optional dimensions)
- `measures`: one or more registered measures
- `by`: optional disaggregation dimensions
- `poverty_line`: required when `analysis_var = "pov_status"` or `by` includes `pov_status`
- `ppp`: PPP year (defaults to `2021L`)
- `pop_share_threshold`: suppression threshold in `(0, 1)` (default `0.01`; set `NULL` to disable)
- `release`: optional release ID (defaults to current release)

Example:

```r
res <- piptm::table_maker(
  pip_id = c("COL_2019_GEIH_INC_ALL"),
  analysis_var = "welfare",
  measures = c("mean", "gini"),
  by = c("gender", "area"),
  ppp = 2021L,
  release = NULL,
  pop_share_threshold = 0.01
)
res
```

### `pip_lookup()`

Resolve `(country_code, year, welfare_type)` triplets to canonical `pip_id`:

```r
ids <- piptm::pip_lookup(
  country_code = c("COL", "BOL"),
  year = c(2019L, 2018L),
  welfare_type = c("INC", "CON"),
  release = NULL
)
ids
```

## Running the API

Install plumber if needed:

```r
install.packages("plumber")
```

Run from R:

```r
piptm::run_api()
```

Run from command line:

```powershell
Rscript inst/plumber/run.R
```

Override host/port:

```powershell
$env:PIPTM_API_PORT = "9000"
$env:PIPTM_API_HOST = "127.0.0.1"
Rscript inst/plumber/run.R
```

## API endpoints (current)

| Endpoint | Method(s) | Purpose |
|---|---|---|
| `/health` | GET | Health check + current release |
| `/releases` | GET | Available releases + current |
| `/surveys` | GET | Full manifest rows for a release |
| `/surveys-ui` | GET | UI-friendly survey catalogue |
| `/countries` | GET | Country list from manifest |
| `/regions` | GET | Region list with member countries |
| `/dimensions` | GET | Valid disaggregation dimensions |
| `/analysis-variables` | GET | Analysis variable catalogue |
| `/statistics` | GET | Measure-group catalogue |
| `/categories` | GET | Filter categories for sample-base UI |
| `/covariates` | GET | Layout covariates for table slicing |
| `/lookup` | GET | Resolve triplets to `pip_id` |
| `/table` | GET, POST | Compute table output |

All endpoints return a common envelope:

```json
{
  "status": "success",
  "data": {},
  "warnings": [],
  "errors": [],
  "meta": {}
}
```

Error status conventions:

- `400`: request validation failures
- `422`: domain/processing failures (including unknown release)
- `500`: uncaught internal errors

## Insomnia collection

Request templates for these endpoints live in:

- `insomnia-collection.json`

Use this file to import and test endpoints without manual request creation.

## Limitations (current)

- API server is single-process/single-worker.
- No auth or rate limiting.
- Performance depends on network access to Arrow and manifest stores.
