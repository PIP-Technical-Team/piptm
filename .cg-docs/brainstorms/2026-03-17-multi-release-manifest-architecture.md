---
date: 2026-03-17
title: "Multi-Release Manifest Architecture"
status: decided
chosen-approach: "Multi-Release Manifest Registry"
depends-on: "2026-03-16-data-pipeline-architecture.md"
tags: [architecture, manifest, multi-release, reproducibility, phase-0, phase-1]
---

# Multi-Release Manifest Architecture

## Context

The original data pipeline architecture brainstorm (2026-03-16) established a Manifest-First with Lazy Validation approach where a single release manifest is loaded at startup. This follow-up brainstorm addresses the need to support **multiple PIP releases simultaneously**, since users may want to query results from different releases for reproducibility.

This brainstorm refines the original architecture to treat `release` as an explicit API parameter.

## Requirements Established Through Q&A

### Multi-Release Support
- The PIP release is an **explicit parameter** in the API (`table_maker(release = "20260206", ...)`)
- Default: current/latest PROD release when `release` is not specified
- Only PROD releases are exposed (no TEST/DEV manifests)
- ~10 releases expected to coexist at any given time

### Manifest Storage
- Manifests are stored in an **external directory on PIP infrastructure**, not inside the {piptm} package
- This decouples data releases from code releases — a new PIP data release does not require a new {piptm} version
- Format: **JSON** — human-readable, language-agnostic, inspectable, works natively with future API layers (Python/Node.js)
- Each manifest is ~1–2 MB (~2,500 survey entries × ~10 fields)

### Configuration
- Manifest directory path configured via **environment variable** (`PIPTM_MANIFEST_DIR`), set in `.Renviron` for production
- **Function override** available (`piptm::set_manifest_dir()`) for dev/testing flexibility

### Shared Arrow Repository
- **One shared Arrow repository** for all releases (not per-release copies)
- Different releases may reference different survey versions for the same `country_code / year / welfare_type` partition
- **Version-encoded filenames** distinguish files within a partition leaf:
  ```
  arrow/country=COL/year=2010/welfare=INC/
    COL_2010_ECH_V01_M_V01_A_INC.parquet
    COL_2010_ECH_V01_M_V02_A_INC.parquet
  ```
- Most partitions contain only one file (single version); multi-version is the exception
- The manifest stores the **exact file path** per survey entry, resolving which file to read
- Old versions are retained; the manifest is the filter, not file deletion

### Loading Pattern
```r
# Single survey — manifest gives the file path
dt <- arrow::read_parquet(manifest_entry$file_path) |> as.data.table()

# Multiple surveys (10–15 max) — open_dataset on explicit file list
paths <- manifest[requested_surveys, file_path]
ds <- arrow::open_dataset(paths) |> dplyr::collect() |> as.data.table()
```

## Approaches Considered

### Approach A: Multi-Release Manifest Registry ✅

At startup, {piptm} scans the external manifest directory, loads all PROD release manifests into a named list (keyed by release ID), and stores them in a package environment. Each API call includes a `release` parameter that selects the appropriate manifest.

**How it works**:
- Manifest directory contains files like `manifest_20250901.json`, `manifest_20260206.json`
- `.onLoad()` scans the directory, reads all JSON manifests, stores them in `.piptm_env$manifests` — a named list keyed by release ID
- The "current" release is determined by convention (latest by date, or flagged in a `current_release.json` pointer file)
- `table_maker(release = "20260206", ...)` looks up `manifests[["20260206"]]`, then proceeds with lazy validation as before

**Pros**:
- All ~10 manifests loaded once at startup (trivial memory cost)
- Release selection is a simple list lookup — zero overhead per call
- Clean separation: data releases are independent of package releases
- Compatible with all prior decisions (lazy validation, version-encoded filenames, JSON format)

**Cons**:
- Startup reads ~10 JSON files from a network share (a few seconds at most)
- Needs a convention for identifying the "current" release

**Effort**: Small–Medium

### Approach B: Lazy Manifest Loading per Release

At startup, only discover which manifests exist (filenames only). Parse a manifest into memory only when its release is first requested.

**Pros**:
- Faster startup (no JSON parsing at load)
- Only loads what's needed

**Cons**:
- First call for each release has a latency spike
- More complex caching logic
- Marginal benefit given only ~10 manifests

**Effort**: Small–Medium

### Approach C: Single-Release with Explicit Switching

Only one manifest loaded at a time. User calls `piptm::set_release()` to swap the active manifest.

**Pros**:
- Simplest implementation

**Cons**:
- Stateful — global mutable state
- Cannot compare across releases without switching
- `release` is not an explicit API parameter, reducing call-level reproducibility

**Effort**: Small

## Decision

**Approach A: Multi-Release Manifest Registry** was chosen.

Rationale:
- With ~10 manifests at ~1–2 MB each, eager loading is trivially cheap and avoids caching complexity (Approach B)
- Keeping `release` as an explicit API parameter on every call is the strongest design for reproducibility (vs. Approach C's global state)
- Named-list lookup is O(1) and adds zero overhead to API calls
- Clean alignment with the existing Manifest-First with Lazy Validation architecture

## Updated Architecture Summary

Incorporating both this brainstorm and the original (2026-03-16):

```
PIP Infrastructure Filesystem
├── arrow/                              ← Shared Arrow repository (all versions)
│   └── country=XXX/year=YYYY/welfare=ZZZ/
│       ├── XXX_YYYY_SURVEY_V01_ZZZ.parquet
│       └── XXX_YYYY_SURVEY_V02_ZZZ.parquet
│
└── manifests/                          ← External manifest directory
    ├── current_release.json            ← Pointer to current PROD release
    ├── manifest_20250901.json          ← Release manifest (PROD only)
    ├── manifest_20260206.json
    └── manifest_20260315.json

{piptm} Package (in memory)
├── .piptm_env$manifest_dir             ← Path to manifests/ (from env var or override)
├── .piptm_env$manifests                ← Named list: release_id → data.table of survey entries
├── .piptm_env$current_release          ← Default release ID
└── .piptm_env$arrow_root               ← Path to shared Arrow repository root
```

### Data flow per API call:
1. `table_maker(release = "20260206", country = "COL", year = 2010, ...)` is called
2. Look up `manifests[["20260206"]]` — O(1) list access
3. Filter manifest for requested surveys — get file paths
4. **Lazy validation**: verify each file exists and expected columns are present
5. Load data: `arrow::read_parquet(file_path)` or `open_dataset(paths)` for multiple
6. Convert to `data.table`, compute measures, return results

## Next Steps

1. **Define manifest JSON schema** — exact fields including `file_path` per survey entry
2. **Define `current_release.json` schema** — pointer file structure
3. **Design manifest generation function** — reads release inventory, resolves versions, writes JSON to manifest directory
4. **Update `.onLoad()` design** — scan manifest dir, load all, set current release
5. **Update `load_survey_microdata()` design** — accept `release` parameter, use file paths from manifest
6. **Update `table_maker()` signature** — add `release` parameter with default to current
