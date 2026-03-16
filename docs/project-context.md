# Project Context

## Overview
This repository contains the **{piptm} package**, which implements the **computation engine** for the new Table Maker feature of the PIP platform.

The goal of the **{piptm}** package is to compute poverty, inequality, and welfare measures from survey datasets produced by the {pipdata} package. These datasets are stored in Parquet format and partitioned by `country_code`, `year`, and `welfare_type`. The package provides functions to compute these measures across user-specified dimensions, such as age, gender, or education, and includes orchestration utilities to execute computations at scale across multiple surveys and measures.

The package operates as part of the broader Table Maker project, whose pipeline includes the following stages:

* **clean survey microdata** produced by {pipdata} -implemented in {pipdata}

* **create and store Arrow partitions** of the cleaned survey datasets -implemented in {pipdata}

* **compute poverty, inequality and welfare measures** -implemented in {piptm}

* **orchestrate the computations at scale** across multiple surveys and measures -implemented in {piptm}

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

* Implementing a **measure orchestrator** that handles multi-measure requests

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
## Master Arrow Repository

## Release Manifest

## Package Architecture
The {piptm} package initializes by loading the release manifest.

During package load:

1. `.onLoad()` reads the release manifest JSON

2. The manifest is stored in a package environment (`.piptm_manifest`)

3. The Arrow repository path is set (`PIPTM_ARROW_PATH`)

No microdata is loaded at startup.

Microdata is loaded only when requested via API calls.

## Computation Engine

The package implements a computation engine that operates on microdata loaded from Arrow partitions.

Core measure functions include:

`compute_poverty_headcount()`

`compute_poverty_gap()`

`compute_poverty_severity()`

`compute_mean_welfare()`

`compute_median_welfare()`

`compute_percentile()`

`compute_gini()`

`population()`

Each function:

* Receives microdata

* Computes the requested statistic, broken down by maximum 4 dimensions

* Returns a value plus associated metadata

### Measure Orchestrator

A central function compute_measures() coordinates the computation of multiple measures.

Responsibilities:

* Handle requests for multiple measures

* Apply breakdown dimensions

* Aggregate results

* Return a standardized output table

## Data Loading

Microdata loading occurs through a dedicated function:

``` {r}
load_survey_microdata(country, year, welfare_type)
```

Process:

* Validate survey against the release manifest

* Load the corresponding Arrow partition

* Convert to data.table in memory

* Return dataset to the computation engine

If a requested survey is not in the manifest, the system returns NULL with a warning.

## For the API
The main exposed function of the package is:

`table_maker()`

Responsibilities:

* Validate survey requests

* Load required microdata

* Execute measure computations

* Return final result tables

The function supports:

* Multiple surveys

* Multiple measures

* Multiple breakdown dimensions

* Multiple poverty lines

# Current Development Status

The project is currently in the architecture and planning stage, focusing on the data ingestion and computation layer.

A preliminary phased implementation roadmap has been proposed and is pending team alignment. See `docs/roadmap.md`.