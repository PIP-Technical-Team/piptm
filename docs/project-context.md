# Project Context

## Overview
This repository contains the **{piptm} package**, which implements the **computation engine** for the new Table Maker feature of the PIP platform.

The goal of the **{piptm}** package is to compute poverty, inequality, and welfare measures from survey datasets produced by the {pipdata} package. These datasets are stored in Parquet format and partitioned by `country_code`, `year`, and `welfare_type`. The package computes measures across user-specified breakdown dimensions and includes orchestration utilities to execute computations at scale across multiple surveys.

The currently supported breakdown dimensions are:

- **gender**
- **education**
- **area** (urban/rural)
- **age**

Up to 4 dimensions can be specified per request. The output is a cross-tabulation of all requested dimension combinations.

The package operates as part of the broader Table Maker project, whose pipeline includes the following stages:

* **clean survey microdata** produced by {pipdata} -implemented in {pipdata}

* **create and store Arrow partitions** of the cleaned survey datasets -implemented in {pipdata}

* **compute poverty, inequality and welfare measures** -implemented in {piptm}

* **orchestrate the computations at scale** across multiple surveys -implemented in {piptm}

* **serve the computed measures** through an API and integrate them into the PIP platform -for future implementation

The core pipeline can be summarized as:

Raw survey microdata  
→ {pipdata} harmonization  
→ Clean survey datasets (.qs2)  
→ Arrow/Parquet datasets  
→ {piptm} computation engine  
→ Structured measure outputs

---

## Scope of This Package

The `{piptm}` package is responsible for:

* Loading microdata from a **partitioned Arrow dataset repository**

* Validating requested surveys against a **release manifest**

* Computing poverty, inequality, and welfare measures

* Returning structured output tables suitable for downstream use

Key responsibilities include:

* Implementing core **measure functions** (poverty, inequality, welfare statistics)

* Implementing a **measure orchestrator** that handles single-measure requests across multiple surveys

* Managing the **release manifest system**

* Loading Arrow microdata partitions on demand

* Providing a top-level API function (`table_maker()`)

---

## Out of Scope

The following components are **not part of this repository**:

- Raw microdata ingestion
- Survey harmonization
- Arrow dataset generation
- API layer and platform integration
- UI development 

These responsibilities belong to other components of the broader system.

---

## Package Architecture

The {piptm} package is designed to support **multiple PIP releases simultaneously** for reproducibility. The architecture follows a **Manifest-First with Lazy Validation** approach.

### Initialization (.onLoad)

During package load:

1. Read manifest directory path from `PIPTM_MANIFEST_DIR` environment variable (or override via `piptm::set_manifest_dir()`)
2. Scan the manifest directory for all `manifest_*.json` files
3. Load all manifests into memory as a named list in `.piptm_env$manifests` (keyed by release ID)
4. Load `current_release.json` to set the default release
5. Store Arrow repository root path in `.piptm_env$arrow_root`

**No microdata is loaded at startup** — all data loading is on-demand per API call.

### Multi-Release Design

- **~10 manifests** expected to coexist (~1-2 MB each)
- **All manifests loaded eagerly** at startup (trivial memory cost, fast lookup)
- **Release as explicit API parameter**: `table_maker(release = "20260206", ...)`
- **Default to current release** when `release` is not specified
- **Only PROD releases** are exposed (no TEST/DEV manifests)

### Configuration

- **Environment variable**: `PIPTM_MANIFEST_DIR` — set in `.Renviron` for production
- **Function override**: `piptm::set_manifest_dir(path)` — for dev/testing

---

## Master Arrow Repository

The Master Arrow Repository is a persistent, partitioned storage system that holds all survey microdata in Parquet format. It serves as the single source of truth for {piptm} computations.

### Repository Structure

The repository uses a **3-level partition scheme**:

```
arrow/
├── country=<country_code>/
│   └── year=<year>/
│       └── welfare=<welfare_type>/
│           ├── <survey_id>-0.parquet
│           └── <survey_id>-0.parquet  ← multiple versions may coexist
```

**Example:**
```
Y:/PIP_ingestion_pipeline_V2/pip_repository/arrow/
├── country=COL/
│   ├── year=2010/
│   │   └── welfare=INC/
│   │       ├── COL_2010_ECH_V01_M_V01_A_INC-0.parquet
│   │       └── COL_2010_ECH_V01_M_V02_A_INC-0.parquet  ← newer version
│   └── year=2022/
│       └── welfare=INC/
│           └── COL_2022_GEIH_V01_M_V02_A_INC-0.parquet
```

### Partition Generation Requirements

**Input Data Requirements:**

Each input `.qs2` file must contain these columns before partitioning:
- `country_code` (character) — ISO3 code (e.g., "COL")
- `year` (integer) — Survey year
- `welfare_type` (character) — "INC" or "CON"
- `welfare` (numeric) — PPP-adjusted welfare value
- `weight` (numeric) — Survey sampling weight (always positive)
- Breakdown dimensions: `gender`, `area`, `education`, `age` (categorical)
- Metadata: `survey_id`, `survey_acronym`, etc.

**Input Filename Convention:**

```
<country>_<year>_<acronym>_<vermast>_<veralt>_<welfare>.qs2

Examples:
COL_2010_ECH_V01_M_V01_A_INC.qs2
COL_2010_ECH_V01_M_V02_A_INC.qs2
COL_2022_GEIH_V01_M_V02_A_INC.qs2
```

**Partition Generation Process:**

```r
library(arrow)

# Write dataset with partitioning
arrow::write_dataset(
  dataset = dt,  # data.table with required columns
  path = arrow_root,
  format = "parquet",
  partitioning = c("country_code", "year", "welfare_type"),
  basename_template = "COL_2010_ECH_V01_M_V02_A_INC-{i}.parquet"
)
```

### Multi-Version Handling

- **Shared Repository**: One Arrow repository for all releases (not per-release copies)
- **Version-Encoded Filenames**: Files within a partition leaf are distinguished by their survey identity in the filename
- **Version Coexistence**: Multiple versions of the same survey (same country-year-welfare) accumulate in the same partition directory
- **Manifest Resolution**: The release manifest determines which file to read for each release

**Example Multi-Version Scenario:**

```
arrow/country=COL/year=2010/welfare=INC/
├── COL_2010_ECH_V01_M_V01_A_INC-0.parquet  ← Release 2025-09-01
└── COL_2010_ECH_V01_M_V02_A_INC-0.parquet  ← Release 2026-02-06
```

---

## Release Manifest

The release manifest is a **JSON file** that serves as a reproducibility contract between {pipdata} (data producer) and {piptm} (data consumer). Each PIP release has its own manifest.

### Purpose

1. **Reproducibility**: Locks which survey versions belong to a release
2. **Validation**: Defines available surveys for computation
3. **Metadata**: Provides breakdown dimension availability per survey
4. **Version Resolution**: Maps partition keys to exact Parquet files

### Manifest Structure

**Location:**
```
Y:/PIP_ingestion_pipeline_V2/manifests/
├── manifest_20250901.json
├── manifest_20260206.json
├── manifest_20260315.json
└── current_release.json  ← pointer to latest release
```

**JSON Schema:**

```json
{
  "release_id": "20260206",
  "arrow_root": "Y:/PIP_ingestion_pipeline_V2/pip_repository/arrow/",
  "surveys": [
    {
      "country_code": "COL",
      "year": 2010,
      "welfare_type": "INC",
      "survey_id": "COL_2010_ECH_V01_M_V02_A_PIP",
      "survey_acronym": "ECH",
      "vermast": "V01",
      "veralt": "V02",
      "file_path": "country=COL/year=2010/welfare=INC/COL_2010_ECH_V01_M_V02_A_INC-0.parquet",
      "available_dimensions": ["gender", "area", "education"],
      "module": "ALL"
    }
  ]
}
```

### Required Fields Per Survey Entry

| Field | Type | Description |
|---|---|---|
| `country_code` | string | ISO3 country code |
| `year` | integer | Survey year |
| `welfare_type` | string | "INC" or "CON" |
| `survey_id` | string | Full survey identifier |
| `survey_acronym` | string | Short survey name |
| `vermast` | string | Master version (e.g., "V01") |
| `veralt` | string | Alternative version (e.g., "V02") |
| `file_path` | string | Relative path from `arrow_root` to Parquet file |
| `available_dimensions` | array | Breakdown dimensions present in this survey |
| `module` | string | Processing module |

### Manifest Generation Process

Manifest generation is performed by {pipdata} (or a shared utility) **after Arrow partition generation** for each release.

**Workflow:**

1. Load release inventory from `pipload::load_pip_release_inventory(release_id)`
2. Scan Arrow repository for all `.parquet` files
3. For each survey in the inventory:
   - Match survey metadata to Parquet files in the Arrow repository
   - Select the correct file based on `vermast`/`veralt`
   - Read Parquet schema to discover available breakdown dimensions
   - Build manifest entry with `file_path`, `available_dimensions`, etc.
4. Write manifest JSON to `manifests/manifest_<release_id>.json`
5. Update `current_release.json` if this is the latest PROD release

**Invocation:**

```r
generate_release_manifest(
  release_id = "20260206",
  arrow_root = "Y:/PIP_ingestion_pipeline_V2/pip_repository/arrow/",
  output_path = "Y:/PIP_ingestion_pipeline_V2/manifests/manifest_20260206.json"
)
``` 


### Multi-Release Support in {piptm}

**Architecture:**

```
{piptm} Package Initialization (.onLoad)
         ↓
    ┌────────────────────────────┐
    │ Get manifest directory     │
    │ (from PIPTM_MANIFEST_DIR   │
    │  env var or override)      │
    └────────────────────────────┘
         ↓
    ┌────────────────────────────┐
    │ Scan for manifest_*.json   │
    │ Load all manifests         │
    └────────────────────────────┘
         ↓
    ┌────────────────────────────┐
    │ Store in .piptm_env        │
    │ as named list:             │
    │ manifests[["20260206"]]    │
    └────────────────────────────┘
         ↓
    ┌────────────────────────────┐
    │ Load current_release.json  │
    │ Or other release           |
    └────────────────────────────┘
```

**In-Memory Structure:**

```
.piptm_env$manifests
├── "20250901" → manifest data.table
├── "20260206" → manifest data.table
└── "20260315" → manifest data.table

.piptm_env$current_release
└── "20260206"
```

### Data Flow with Manifest

```
table_maker(release = "20260206", country = "COL", year = 2010, ...)
         ↓
    ┌────────────────────────────┐
    │ Look up manifest           │
    │ manifests[["20260206"]]    │
    └────────────────────────────┘
         ↓
    ┌────────────────────────────┐
    │ Filter for requested       │
    │ surveys (country, year,    │
    │ welfare_type)              │
    └────────────────────────────┘
         ↓
    ┌────────────────────────────┐
    │ Get file_path from         │
    │ manifest entry             │
    └────────────────────────────┘
         ↓
    ┌────────────────────────────┐
    │ Lazy validation:           │
    │ - File exists?             │
    │ - Expected columns?        │
    └────────────────────────────┘
         ↓
    ┌────────────────────────────┐
    │ Load data:                 │
    │ arrow::read_parquet()      │
    │ or open_dataset()          │
    └────────────────────────────┘
         ↓
    ┌────────────────────────────┐
    │ Convert to data.table      │
    │ Pass to compute_measures() │
    └────────────────────────────┘
```

---

## Computation Engine

The package implements a computation engine that operates on microdata loaded from Arrow partitions.

### Core Measure Functions

**Poverty Measures (FGT):**
- `compute_fgt()` — Computes all three Foster-Greer-Thorbecke measures simultaneously:
  - Headcount (α=0): share of population below poverty line
  - Poverty gap (α=1): average normalized shortfall
  - Poverty severity (α=2): average squared normalized shortfall

**Inequality Measures:**
- `compute_gini()` — Gini coefficient

**Welfare Statistics:**
- `compute_mean_welfare()` — Weighted mean
- `compute_median_welfare()` — Weighted median
- `compute_population()` — Total weighted population

Each function:

* Receives pre-processed microdata (PPP-adjusted `welfare`, positive `weight`, breakdown dimensions)
* Computes the requested statistic, broken down by up to 4 dimensions (gender, education, area, age)
* Returns a cross-tabulated result plus associated metadata

**Note:** The user selects **one measure at a time** per API call.

### FGT Computation Design

The FGT poverty measures use a **unified data.table grouped aggregation** approach:

```r
compute_fgt(dt, poverty_lines, by = NULL)
```

**Implementation strategy:**
1. Cross-join microdata with poverty lines (memory: N_rows × N_lines)
2. Compute poverty gap vectorized: `gap = max((z - y) / z, 0)`
3. Single grouped aggregation computes all 3 measures + population in one pass
4. Uses data.table GForce optimization for `sum()` operations

### Measure Orchestrator

A central function compute_measures() coordinates the computation of a single measure across one or more surveys.

Responsibilities:

* Handle single-measure requests across multiple surveys

* Apply breakdown dimensions (up to 4: gender, education, area, age)

* Aggregate results into a cross-tabulated output

* Return a standardized output table

## Data Loading

Microdata loading occurs through a dedicated function:

```r
load_survey_microdata(release, country, year, welfare_type)
```

**Process:**

1. Look up the manifest for the specified release: `manifests[[release]]`
2. Filter manifest for requested survey (country, year, welfare_type)
3. Get exact `file_path` from manifest entry
4. **Lazy validation**:
   - Verify the Parquet file exists
   - Verify expected columns are present (welfare, weight, requested breakdown dimensions)
5. Load data:
   - Single survey: `arrow::read_parquet(file_path) |> as.data.table()`
   - Multiple surveys (10–15 max): `arrow::open_dataset(file_paths) |> collect() |> as.data.table()`
6. Return data.table to the computation engine

**Error handling:**
- If requested survey is not in the manifest → return NULL with warning
- If file does not exist → error (manifest drift)
- If expected columns missing → error (data quality issue)

## API Function

The main exposed function of the package is:

```r
table_maker(
  release = NULL,        # defaults to current release
  country,               # character vector (1–15 countries)
  year,                  # integer vector (1–15 years)
  welfare_type,          # "INC" or "CON"
  measure,               # one measure: "headcount", "poverty_gap", "severity", "gini", "mean", etc.
  poverty_lines = NULL,  # numeric vector (for poverty measures)
  by = NULL              # character vector of breakdown dimensions (max 4)
)
```

**Responsibilities:**

1. Validate survey requests against the release manifest
2. Load required microdata (10–15 surveys max per call)
3. Pre-process data (PPP adjustment, weight validation)
4. Execute measure computation via `compute_measures()` orchestrator
5. Return standardized cross-tabulated output table

**The function supports:**

* **Multi-release queries**: explicit `release` parameter for reproducibility
* **Multiple surveys** (up to 10–15 per call)
* **One measure at a time**: `measure` is a single value, not a vector
* **Multiple breakdown dimensions** (up to 4: gender, education, area, age)
* **Multiple poverty lines** (for poverty measures only)
* **Cross-tabulated output**: all combinations of requested breakdown dimensions

# Current Development Status

The project is currently in the architecture and planning stage, focusing on the data ingestion and computation layer.

A preliminary phased implementation roadmap has been proposed and is pending team alignment. See `docs/roadmap.md`.