# Table Maker Development Roadmap

Date: March 15, 2026  
Version: 1.0 (Draft)  
Status: Awaiting team approval

---

# Overview

This document describes the high-level implementation roadmap for the Table Maker system of the PIP platform.

The roadmap focuses on the core data pipeline:

Raw survey microdata → Arrow datasets → {piptm} computation engine → structured measure outputs.

API integration and platform services are not included in this roadmap and will be planned separately.

---

# System Architecture Summary

RAW MICRODATA  
↓  
{pipdata} harmonization  
↓  
CLEAN SURVEY DATA (.qs2)  
↓  
Arrow dataset generation  
↓  
MASTER ARROW REPOSITORY  
↓  
Release Manifest  
↓  
{piptm} computation engine  
↓  
Structured output tables

---

# Implementation Phases

The development process is divided into three major phases.

---

# Phase 0 — Arrow Dataset Generation

Owner: Data Team  
Estimated timeline: 2–3 weeks

## Objective

Establish a persistent, partitioned Arrow dataset repository that will serve as the data foundation for the system.

## Key Tasks

### Establish Master Arrow Repository

Example location:

Y:\PIP_ingestion_pipeline_V2\pip_repository\pip_data\tb_data\arrow\parquet\

Requirements:

- Store all survey partitions
- Retain historical partitions permanently
- Append new partitions without deleting old data

### Partition Structure

Datasets are partitioned by:

- country_code
- year
- welfare_type

Example:

country=COL/year=2010/welfare=INC

### Arrow Generation Pipeline

The {pipdata} package will implement functions to:

- Convert cleaned survey datasets into Arrow partitions
- Append partitions to the master repository
- Maintain schema consistency

### Release Manifest Creation

After dataset generation for a release, a release manifest will be created.

Location within {piptm}:

inst/release_manifest_YYYYMMDD.json

The manifest defines:

- which partitions belong to the release
- release metadata
- optional exclusions

### Phase Gate

Phase 0 is complete when:

- Arrow datasets exist for all surveys
- Schema validation passes
- A release manifest is generated and validated

---

# Phase 1 — Computation Engine

Owner: Technical Team  
Estimated timeline: 4–6 weeks

## Objective

Implement the core computation engine within the {piptm} package.

## Key Tasks

### Initialize {piptm} Package

Create standard R package structure:

R/  
tests/  
inst/  
docs/

Implement `.onLoad()` to:

- load release manifest
- validate manifest schema
- store manifest in `.piptm_manifest`
- set `PIPTM_ARROW_PATH`

### Implement Measure Functions

Core measures include:

- poverty headcount
- poverty gap
- poverty severity
- mean welfare
- median welfare
- percentiles
- Gini coefficient
- population totals

Each function must:

- accept microdata input
- compute a statistic
- return value and metadata

### Implement Measure Orchestrator

Create a `compute_measures()` function responsible for:

- coordinating measure execution
- applying breakdown dimensions
- returning a standardized result table

### Phase Gate

Phase 1 is complete when:

- all measures are implemented
- accuracy benchmarks are passed

Dependencies: Phase 0 must be complete and Arrow datasets available.

---

# Phase 2 — Data Integration & Release Management

Owner: Data/API Technical Team  
Estimated timeline: 2–3 weeks

## Objective

Connect the computation engine to Arrow datasets and implement release-aware data loading.

## Key Tasks

### Implement Microdata Loader

Function:

load_survey_microdata(country, year, welfare_type)

Responsibilities:

- validate requests against the release manifest
- load Arrow partitions
- convert to data.table
- return dataset to the computation engine

### Implement Manifest System

Provide functions to inspect the loaded manifest:

- validate_survey_request()
- get_available_surveys()
- get_survey_metadata()
- get_release_info()

### Implement Top-Level API

Create the primary function:

table_maker()

Responsibilities:

- validate requested surveys
- load necessary microdata
- run measure computations
- return final result tables

### Phase Gate

Phase 2 is complete when:

- microdata loading works with Arrow
- manifest validation works correctly
- integration tests pass

Dependencies: Phase 1 complete; Arrow datasets and manifest available.

---

# Phase 3 — Validation & Documentation

Owner: Technical Teams

## Objectives

Ensure correctness, performance, and usability of the system.

## Key Tasks

- unit test suite
- performance benchmarking
- developer documentation
- user documentation

Target:

≥ 90% test coverage

---

# Estimated Timeline

Sequential execution:

Phase 0 — 2–3 weeks  
Phase 1 — 4–6 weeks  
Phase 2 — 2–3 weeks  
Phase 3 — validation and documentation

Total estimated development time: 10–15 weeks

---

# Key Components Summary

Component | Responsibility
--- | ---
{pipdata} | Generate Arrow datasets from harmonized survey data
Arrow repository | Persistent microdata storage
Release manifest | Define which survey partitions belong to a release
{piptm} | Computation engine for poverty and welfare measures
table_maker() | Primary API entry point for computing measures