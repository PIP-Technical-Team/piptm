---
date: 2026-03-16
title: "Data Pipeline Architecture — Ingestion, Manifest, and Computation Design"
status: decided
chosen-approach: "Manifest-First with Lazy Validation"
tags: [architecture, data-pipeline, arrow, manifest, phase-0, phase-1]
---

# Data Pipeline Architecture — Ingestion, Manifest, and Computation Design

## Context

The {piptm} package is the computation engine for the PIP Table Maker. Before implementing any code, we needed to clarify the data flow from {pipdata}'s clean survey datasets through Arrow partitions to {piptm}'s measure computations. Key decisions were needed around partition layout, manifest design, validation strategy, and how breakdown dimensions are handled.

## Requirements

### Data Source
- ~2,500 surveys stored as partitioned Arrow/Parquet datasets
- Partition scheme: `country_code / year / welfare_type` (3 levels, settled)
- Each partition leaf may contain one or more Parquet files
- Typical partition: thousands of rows × 10–20 columns
- {pipdata} is solely responsible for generating the Parquet partitions

### Partition Scheme Nuances
- A single country-year may have one survey covering both income and consumption (split into two welfare_type partitions), or two separate surveys (one per welfare_type)
- The user doesn't care which survey the data comes from — only the welfare type matters
- Survey identity is metadata, not a partition key
- `domain` / `reporting_level` is not a partition dimension currently (no multi-domain surveys exist), but may become a breakdown dimension in the future

### Release Manifest
- Auto-generated per release from `pipload::load_pip_release_inventory()`
- Must contain:
  - Partition key: `country_code`, `year`, `welfare_type`
  - Survey identity for provenance: `survey_id` (resolves which survey version to use when multiple exist)
  - Available breakdown dimensions per survey (sourced from release inventory)
- Single JSON artifact per release for portability and reproducibility

### Computation
- Up to 10–15 surveys per `table_maker()` call (not the full 2.5K)
- Multiple poverty lines per call (e.g., $2.15, $3.65, $6.85)
- Up to 4 categorical breakdown dimensions per call
- Output is a cross-tabulation of all dimension combinations (not marginal tables)
- The 4 dimensions have positional semantics (columns, rows, super-columns, super-rows) but this is a presentation concern for the API layer — {piptm} returns flat cross-tabulated results

### Integrity
- {piptm} validates manifest ↔ partition consistency
- Validation should be cheap and scoped to the surveys actually requested (lazy, not eager)

### Boundary
- {pipdata} owns: `.qs2` → Parquet conversion, Arrow repository population
- {piptm} owns: manifest loading, partition reading, measure computation
- {piptm} does NOT write to the Arrow repository

## Approaches Considered

### Approach 1: Manifest-First with Lazy Validation ✅

Generate a self-contained manifest JSON at release creation time. {piptm} loads it on package init, validates partition existence lazily (only when a survey is requested), and uses `arrow::open_dataset()` with partition filtering.

**How it works**:
- At release time, a function reads the release inventory, resolves survey versions, discovers available breakdown columns per survey, and writes a `manifest.json`
- {piptm} loads the manifest in `.onLoad()` into a package environment
- When `table_maker()` is called, it filters the manifest for the requested surveys, then calls `arrow::open_dataset() |> dplyr::filter()` to load partitions
- Integrity check: verify the partition exists and the expected breakdown columns are present — done at load time per survey, not upfront for all 2.5K

**Pros**:
- Fast startup (no scanning 2.5K partitions)
- Integrity checks happen only for the 10–15 surveys actually requested
- Manifest is a single portable artifact for reproducibility
- `open_dataset()` with predicate pushdown is efficient for this data scale
- Clean separation from {pipdata} and {pipload}

**Cons**:
- Manifest can drift from actual data if the repository is modified after manifest generation
- Two sources of truth (manifest + inventory) that must stay in sync

**Effort**: Medium

### Approach 2: Inventory-as-Manifest with Schema Discovery

Use the release inventory from `pipload::load_pip_release_inventory()` directly as the manifest. Discover available breakdown columns by reading Parquet schemas at load time.

**Pros**:
- Single source of truth (no manifest drift)
- No additional artifact to generate or manage

**Cons**:
- Runtime dependency on {pipload} — tighter coupling
- Schema inspection per survey adds latency
- Breakdown availability not known until load time
- Inventory may lack column-level metadata

**Effort**: Small

### Approach 3: Manifest-First with Eager Validation

Same as Approach 1, but validate all manifest entries against the Arrow repository at package load time.

**Pros**:
- Full integrity assurance at startup

**Cons**:
- Startup cost: scanning 2.5K partition directories on a network share
- Overkill given only 10–15 surveys are used per call
- Network dependency at load time could cause initialization failures

**Effort**: Medium

## Decision

**Approach 1: Manifest-First with Lazy Validation** was chosen.

Rationale:
- The data scale (~2.5K surveys, thousands of rows each) does not warrant eager validation of the full repository
- Per-request lazy validation is efficient and sufficient given the 10–15 survey limit per call
- A standalone manifest JSON provides a clean reproducibility artifact per release
- Decoupling {piptm} from {pipload} at runtime keeps the package self-contained
- `arrow::open_dataset()` with predicate pushdown on partition columns is the natural fit for the 3-level partition scheme

## Next Steps

1. **Define manifest JSON schema** — exact fields, structure, and validation rules
2. **Design manifest generation function** — to be called at release time, reading from the release inventory and writing `manifest.json`
3. **Implement `load_survey_microdata()`** — lazy validation + `arrow::open_dataset()` + filter + `data.table` conversion
4. **Implement core measure functions** — starting with poverty headcount, gini, mean welfare
5. **Implement `compute_measures()` orchestrator** — multi-measure, multi-dimension cross-tabulation
6. **Implement `table_maker()` top-level API** — tying together manifest validation, data loading, and computation
