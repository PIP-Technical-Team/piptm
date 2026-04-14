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
