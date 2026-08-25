# API Reference

## Functions

<!-- cg:auto:functions -->

### Core API

| Function | Description |
|----------|-------------|
| `table_maker()` | Top-level API: orchestrate multi-survey measure computation with breakdown dimensions |
| `compute_measures()` | Orchestrator for batch execution across surveys and breakdown dimensions |
| `load_survey_microdata()` | Manifest lookup + lazy file validation + Arrow loading |

### Measure Functions

| Function | Family | Description |
|----------|--------|-------------|
| `compute_fgt()` | Poverty | Compute FGT class poverty measures (headcount, poverty gap, severity, Watts index) |
| `compute_gini()` | Inequality | Compute Gini coefficient (requires pre-sorted welfare vector) |
| `compute_mean_welfare()` | Welfare | Compute mean welfare statistics |

### Utility Functions

| Function | Description |
|----------|-------------|
| `pip_allowed_cols()` | List all allowed column names in the Arrow schema |
| `.build_parquet_paths()` | Build Parquet file paths from survey identifiers (internal) |

<!-- cg:auto:end -->

## Parameters

<!-- cg:auto:parameters -->

### `table_maker()`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `measures` | `character` | — | One or more measure names to compute |
| `by` | `character` | `NULL` | Breakdown dimensions (e.g. `c("gender", "area")`) |
| `povertyline` | `numeric` | `1.90` | Poverty line for FGT measures |
| `with_meta` | `logical` | `FALSE` | Return metadata alongside data (specification, execution, provenance, warnings) |
| ... | | | |

### `compute_fgt()`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `welfare` | `numeric` | — | Welfare vector (sorted within survey) |
| `weight` | `numeric` | — | Survey sampling weights |
| `povertyline` | `numeric` | `1.90` | Poverty line |
| `alpha` | `numeric` | `0` | FGT alpha parameter (0=headcount, 1=gap, 2=severity) |

### `compute_gini()`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `welfare` | `numeric` | — | Pre-sorted welfare vector |
| `weight` | `numeric` | — | Survey sampling weights |

<!-- cg:auto:end -->

## Return Values

<!-- cg:auto:return-values -->

All measure functions return a `data.table` with columns:

| Column | Type | Description |
|--------|------|-------------|
| `pip_id` | `character` | Survey identifier |
| `measure` | `character` | Measure name (e.g. `headcount`, `gini`) |
| `value` | `numeric` | Computed measure value |
| `breakdown_*` | `character/factor` | Breakdown dimension columns (if `by` specified) |
| `nobs` | `integer` | Number of observations used |

### `table_maker()` Return Value

**Default mode** (`with_meta = FALSE`): Returns a keyed `data.table` with metadata joined from the release manifest.

**Metadata mode** (`with_meta = TRUE`): Returns a named list with 5 components:

| Component | Type | Description |
|-----------|------|-------------|
| `data` | `data.table` | Computed statistics (same structure as default mode) |
| `specification` | `list` | Requested parameters as provided by user (preserves NULL values) |
| `execution` | `list` | Execution results: resolved parameters, loaded/excluded surveys, PPP column used |
| `provenance` | `list` | Package version and timestamp |
| `warnings` | `list` | Captured warnings during computation |

**Execution metadata fields:**
- `requested_pip_id`: Original survey IDs requested by user
- `loaded_surveys`: `data.table` of surveys that contributed data (columns: `pip_id`, `country_code`, `surveyid_year`, `welfare_type`)
- `excluded_surveys`: `data.table` of excluded surveys with `stage` (manifest | filter_pre | dimension_pre) and `reason`
- `resolved_release`: Release ID used after resolution (even if user passed NULL)
- `resolved_ppp`: PPP year used after resolution
- `ppp_column_used`: Physical welfare column name (e.g. `"welfare_ppp_2021"`)

<!-- cg:auto:end -->

---

← [Home](README.md)
