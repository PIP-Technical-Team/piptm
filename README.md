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
| `/session/surveys` | POST | Create a session and store a survey selection |
| `/session/<id>/surveys` | GET | Retrieve the survey selection for a session |

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

## Deploying the API to Azure

This section documents how to deploy the Table Maker API service to the Azure
cloud environment. Deployment is managed through Azure DevOps pipelines.

> **Out of scope**: This section covers only the API deployment itself. Preparing
> the upstream data inputs (Arrow/Parquet partitions and release manifests) is a
> separate process documented elsewhere.

### Prerequisites

- Access to the PIP project in Azure DevOps (no special permissions required
  beyond standard team membership).
- The data inputs are already in place in the PIP repository **before** the
  first deployment (or before adding a new data release).

### Related repositories

| Repository | Purpose |
|---|---|
| [PIP-TABLEMAKER-API][api-repo] | API source code — `main.R`, `Dockerfile`, package source |
| [PIP-TABLEMAKER-DATA][data-repo] | Arrow/Parquet data partitions, release manifests, and registry files |
| [Azure Releases][releases-page] | Release pipeline definitions and run history (all environments) |

[api-repo]: https://dev.azure.com/ITSOC-DEVSECOPS-ORG2/ITSES-POVERTYSCOREAPI/_git/PIP-TABLEMAKER-API
[data-repo]: https://dev.azure.com/ITSOC-DEVSECOPS-ORG2/ITSES-POVERTYSCOREAPI/_git/PIP-TABLEMAKER-DATA
[releases-page]: https://dev.azure.com/ITSOC-DEVSECOPS-ORG2/ITSES-POVERTYSCOREAPI/_release?definitionId=31&view=mine&_a=releases

### Azure DevOps pipelines

Three pipelines handle the full deployment. Each pipeline exists in three
variants — one per environment:

| Pipeline | DEV | QA | PROD |
|---|---|---|---|
| **1. Copy data** | [DEV][copy-dev] | [QA][copy-qa] | [PROD][copy-prod] |
| **2. Build** | [DEV][build-dev] | [QA][build-qa] | [PROD][build-prod] |
| **3. Release** | [DEV][release-dev] | [QA][release-qa] | [PROD][release-prod] |

[copy-dev]: https://dev.azure.com/ITSOC-DEVSECOPS-ORG2/ITSES-POVERTYSCOREAPI/_build?definitionId=2046&_a=summary
[copy-qa]: https://dev.azure.com/ITSOC-DEVSECOPS-ORG2/ITSES-POVERTYSCOREAPI/_build?definitionId=2054&_a=summary
[copy-prod]: #
[build-dev]: https://dev.azure.com/ITSOC-DEVSECOPS-ORG2/ITSES-POVERTYSCOREAPI/_build?definitionId=2056
[build-qa]: #
[build-prod]: #
[release-dev]: https://dev.azure.com/ITSOC-DEVSECOPS-ORG2/ITSES-POVERTYSCOREAPI/_release?definitionId=31&view=mine&_a=releases
[release-qa]: #
[release-prod]: #

> Replace the `#` placeholders above with the actual Azure DevOps pipeline URLs
> once they are stable.

**What each pipeline does:**

1. **Copy data to cloud storage** — Uploads Arrow/Parquet data partitions,
   release manifests and registry to Cloud Storage. 
2. **Build** — Builds the Docker image from the current package source and
   pushes it to the container registry.
3. **Release** — Builds the final Docker image, deploys the container to Azure
   Container Apps, mounts the cloud storage volume, and starts the API service.
   This pipeline is **automatically triggered** when the Build pipeline
   completes successfully — you do not need to run it manually.

### Execution order

```
[Copy data]  ──►  [Build]  ──►  [Release]  (auto-triggered)
   │
   └── Run only when:
       • First-time deployment (cloud storage not yet mounted)
       • New data release (updated Arrow/Parquet partitions or manifests or registry)
       • Otherwise: skip — go straight to Build
```

**Standard deployment (code change only):**
1. Run the **Build** pipeline for the target environment.
2. Wait for it to succeed — the **Release** pipeline fires automatically.

**First-time deployment or new data release:**
1. Run the **Copy data** pipeline for the target environment.
2. Once it succeeds, run the **Build** pipeline.
3. The **Release** pipeline fires automatically.

Always follow the environment promotion order: **DEV → QA → PROD**.

### Verifying deployment

After the Release pipeline completes, confirm the API is healthy:

```bash
# Replace <host> with the deployed service URL for the target environment
curl https://<host>/health
```

Expected response:

```json
{
  "status": "success",
  "data": { "release": "<release-id>", "status": "healthy" },
  "warnings": [],
  "errors": [],
  "meta": {}
}
```

You can also use the Insomnia collection (`insomnia-collection.json`) to run a
fuller set of endpoint checks against the deployed environment. Update the base
URL in Insomnia to point to the target environment before running.
