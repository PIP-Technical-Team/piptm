---
date: 2026-04-03
title: "Manifest Generation and Version Partition Filtering"
status: decided
chosen-approach: "Release-Inventory-Driven Manifest with Partition Filters"
supersedes-partially: "2026-03-17-multi-release-manifest-architecture.md"
tags: [architecture, manifest, partitions, version, pipdata, piptm, phase-0]
---

# Manifest Generation and Version Partition Filtering

## Context

The March 17 brainstorm established the multi-release manifest architecture. Since then, the Arrow dataset partition layout has evolved: `version` is now a **partition variable** (subfolder) rather than being encoded in filenames. This changes how manifests are generated and how `{piptm}` loads data.

This brainstorm refines manifest generation logic in `{pipdata}` and consumption logic in `{piptm}` to align with the current partition structure.

### What is superseded from the March 17 brainstorm

The following points from `2026-03-17-multi-release-manifest-architecture.md` are **superseded** by this decision:

1. **Version-encoded filenames** — The partition layout `country/year/welfare/version` replaces the filename-based approach (`XXX_YYYY_SURVEY_V01_ZZZ.parquet` inside `country/year/welfare/`).
2. **Explicit file paths in the manifest** — The manifest no longer stores `file_path` per entry. Instead it stores four partition filter keys (`country_code`, `year`, `welfare_type`, `version`), and `{piptm}` uses Arrow's native partition pushdown to select data.
3. **Loading pattern** — `arrow::read_parquet(manifest_entry$file_path)` is replaced by `arrow::open_dataset(arrow_root) |> filter(...)`.

Everything else from the March 17 brainstorm remains valid: multi-release registry, eager loading at startup, external manifest directory, JSON format, `release` as explicit API parameter, `current_release.json` pointer file.

## Requirements

### Partition Layout (canonical, going forward)

```
arrow/
└── country=COL/year=2010/welfare=INC/version=v01_v05/
    └── COL_2010_GEIH_INC_ALL-0.parquet
```

- `version` is derived from `vermast` + `veralt` (e.g., `v01` + `v05` → `v01_v05`)
- For a given `(country, year, welfare)`, multiple version subfolders may exist
- The manifest determines which version applies to each release

### Manifest Generation (`{pipdata}`)

- **One-time step per release**, triggered after the release inventory is frozen
- Uses the release inventory (`pipload::load_pip_release_inventory()`) as the authoritative source
- Constructs `version` from `vermast` and `veralt`
- Inspects each partition to determine available dimensions from the fixed set: `age`, `gender`, `educ4`, `educ5`, `educ7`, `area`
- Writes a JSON manifest to the external manifest directory

### Manifest Consumption (`{piptm}`)

- Opens the shared Arrow dataset once
- Filters using the four partition keys from the manifest entry: `country_code`, `year`, `welfare_type`, `version`
- Arrow partition pushdown handles version selection natively — no file path management needed

### Manifest Schema Per Entry

```json
{
  "release": "20260206",
  "generated_at": "2026-04-03T17:00:00Z",
  "entries": [
    {
      "pip_id": "COL_2010_GEIH_INC_ALL",
      "survey_id": "COL_2010_GEIH_v01_M_v05_A_GMD_ALL",
      "country_code": "COL",
      "year": 2010,
      "welfare_type": "INC",
      "version": "v01_v05",
      "survey_acronym": "GEIH",
      "module": "ALL",
      "dimensions": ["area", "gender", "age"]
    }
  ]
}
```

**Field descriptions**:

| Field | Purpose |
|-------|---------|
| `pip_id` | Unique survey identifier |
| `survey_id` | DLW-side identifier |
| `country_code` | Partition filter key |
| `year` | Partition filter key |
| `welfare_type` | Partition filter key (`INC` or `CON`) |
| `version` | Partition filter key (e.g., `v01_v05`) |
| `survey_acronym` | Survey name (e.g., `GEIH`, `SUSENAS`) |
| `module` | Module type (currently always `ALL`) |
| `dimensions` | Array of available dimension columns from: `age`, `gender`, `educ4`, `educ5`, `educ7`, `area` |

### Dimension Introspection

- Known dimension universe: `age`, `gender`, `educ4`, `educ5`, `educ7`, `area`
- At manifest-generation time, `{pipdata}` opens each partition (filtered to the correct version) and checks which of these columns are present and non-trivially populated
- This avoids runtime inspection in `{piptm}`

## Approaches Considered

### Approach 1: Release-Inventory-Driven Manifest with Partition Filters ✅

**Summary**: Manifest generation in `{pipdata}` reads the release inventory, constructs version from `vermast`/`veralt`, inspects each partition for available dimensions, and writes a JSON manifest. `{piptm}` opens a single shared Arrow dataset and filters using the four partition keys.

**Pros**:
- Arrow partition pushdown on `version` is native and efficient — no file path management
- Clean separation: manifest stores logical keys, not physical paths
- Dimension introspection at generation time means `{piptm}` knows what's queryable without opening data
- Single `open_dataset()` call works for both single and multi-survey loads
- Fully aligned with the current `generate_arrow_dataset()` partition layout

**Cons**:
- Dimension introspection adds time to manifest generation (must open each partition once)
- If the dimension column set changes, manifests need regeneration

**Effort**: Medium

### Approach 2: Release-Inventory-Driven Manifest Without Dimension Introspection

**Summary**: Same as Approach 1 but skip dimension inspection. Record only partition keys and identifiers. `{piptm}` discovers dimensions at load time.

**Pros**:
- Manifest generation is instant (no I/O beyond reading the inventory)
- Simpler generation code

**Cons**:
- `{piptm}` must inspect dimensions at runtime, adding latency to every request
- Cannot validate or pre-plan computation without loading data first

**Effort**: Small

### Approach 3: Hybrid — Manifest with Dimension Lookup Table

**Summary**: Manifest stores only partition keys. A separate static lookup table maps survey types to available dimensions.

**Pros**:
- Fast manifest generation
- Dimensions available without opening data

**Cons**:
- Adds a second file to maintain and keep in sync
- Dimensions vary per survey, not per survey type — lookup becomes complex
- Indirection adds complexity without clear benefit

**Effort**: Small–Medium

## Decision

**Approach 1: Release-Inventory-Driven Manifest with Partition Filters** was chosen.

Rationale:
- The `version` partition variable is already in place — filtering on it via Arrow is the natural, efficient approach
- Storing logical partition keys rather than physical file paths makes the manifest portable and decoupled from filesystem structure
- Dimension introspection at generation time is a one-time cost per release (~seconds per partition) and eliminates repeated runtime inspection
- The fixed dimension universe (`age`, `gender`, `educ4`, `educ5`, `educ7`, `area`) makes introspection straightforward and bounded

## Updated Architecture Summary

```
PIP Infrastructure Filesystem
├── arrow/                              ← Shared Arrow repository (all versions)
│   └── country=XXX/year=YYYY/welfare=ZZZ/version=vMM_vAA/
│       └── XXX_YYYY_SURVEY_ZZZ_ALL-0.parquet
│
└── manifests/                          ← External manifest directory
    ├── current_release.json            ← Pointer to current PROD release
    ├── manifest_20250901.json
    ├── manifest_20260206.json
    └── manifest_20260315.json

{piptm} Package (in memory)
├── .piptm_env$manifest_dir             ← Path to manifests/ (from env var)
├── .piptm_env$manifests                ← Named list: release_id → data.table
├── .piptm_env$current_release          ← Default release ID
└── .piptm_env$arrow_root               ← Path to shared Arrow repository root
```

### Data flow per API call:

1. `table_maker(release = "20260206", country = "COL", year = 2010, ...)` is called
2. Look up `manifests[["20260206"]]` — O(1) list access
3. Filter manifest for requested survey — get `country_code`, `year`, `welfare_type`, `version`
4. Open shared Arrow dataset with partition filters:
   ```r
   arrow::open_dataset(arrow_root) |>
     dplyr::filter(
       country_code == "COL",
       year == 2010,
       welfare_type == "INC",
       version == "v01_v05"
     ) |>
     dplyr::collect() |>
     data.table::as.data.table()
   ```
5. Compute measures, return results

## Next Steps

1. **Update `generate_manifest()` in `{pipdata}`** — implement release-inventory-driven generation with version construction and dimension introspection
2. **Define dimension introspection logic** — function that checks which of the 6 known dimension columns are present/populated in a partition
3. **Update manifest loading in `{piptm}`** — parse new schema (partition keys instead of file paths)
4. **Update `load_survey_microdata()` in `{piptm}`** — use `open_dataset() |> filter()` pattern instead of `read_parquet(file_path)`
5. **Hand off to `/cg-plan`** for implementation planning of both `{pipdata}` and `{piptm}` changes