# piptm
Computational backend for the Poverty and Inequality Platform (PIP) Table Maker tool

## Setup

`piptm` reads the shared Arrow repository path and manifest directory from
two environment variables. Set them once in your `~/.Renviron` (run
`usethis::edit_r_environ()` to open the file):

```
PIPTM_ARROW_ROOT=Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/arrow
PIPTM_MANIFEST_DIR=Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/manifests
```

Restart R after saving. The package will load the manifest and configure
the Arrow root automatically on `library(piptm)`.

If the variables are not set (e.g. a machine without the network drive
mounted), the package starts in dev/testing mode — configure paths manually:

```r
piptm::set_arrow_root("path/to/arrow")
piptm::set_manifest_dir("path/to/manifests")
```

## Running the API

`{piptm}` ships a Plumber-based HTTP API that exposes `table_maker()` and
supporting discovery functions. You need `plumber` installed:

```r
install.packages("plumber")
```

### Start with one function call

```r
library(piptm)
piptm::run_api()           # listens on 0.0.0.0:8080
piptm::run_api(port = 9000, host = "127.0.0.1")  # custom port / localhost only
```

### Start from the command line

```bash
Rscript inst/plumber/run.R
```

Override port and host via environment variables:

```bash
PIPTM_API_PORT=9000 PIPTM_API_HOST=127.0.0.1 Rscript inst/plumber/run.R
```

### Endpoints

| Endpoint | Methods | Description |
|---|---|---|
| `GET /health` | GET | Server health check |
| `GET /releases` | GET | List all release IDs and the current release |
| `GET /measures` | GET | Measure names and computation families |
| `GET /dimensions` | GET | Valid disaggregation dimension names |
| `GET /surveys` | GET | Full manifest rows for a release |
| `GET /lookup` | GET | Resolve country/year/welfare triplets to pip_ids |
| `GET\|POST /table` | GET, POST | Compute poverty / inequality / welfare measures |

All responses share a structured envelope:

```json
{
  "status":   "success",
  "data":     ...,
  "warnings": [],
  "errors":   [],
  "meta":     { "release": "20260401_TEST", "n_surveys": 3 }
}
```

### Known limitations (v1)

- Single-threaded: ≤3 s target for 15 surveys; larger batches may be slower.
- No authentication, rate limiting, or multi-worker support.
