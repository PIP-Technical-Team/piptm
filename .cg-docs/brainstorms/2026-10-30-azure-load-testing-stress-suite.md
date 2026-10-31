---
date: 2026-10-30
title: "Azure Load Testing Stress Test Suite for Table Maker API"
status: decided
scope: "Standard"
artifact-schema-version: 1
chosen-approach: "Approach 1 - Self-Bootstrapping Python Locust Suite + Azure Load Testing YAML Bundle (Curated Scenario Catalog + Known-Good Survey Pool)"
tags: [load-testing, stress-test, azure-load-testing, locust, api, plumber, performance, slo]
---
<!-- Valid status values: decided, in-progress, abandoned -->

# Azure Load Testing Stress Test Suite for Table Maker API

## Context

In preparation for the launch of the Table Maker project, the team needs a comprehensive stress-testing suite that can be run against the deployed Plumber HTTP API (e.g., QA at `https://tablemakerapiqa.worldbank.org/`) using **Azure Load Testing** and handed off as a turnkey bundle to World Bank ITS and Azure DevOps engineers.

Key architectural & operational observations discovered during the brainstorm:
- **Single-threaded R Plumber process per container**: Each container replica runs a single R process (`collapse` configured to `min(4, physical_cores)` threads). Concurrent requests hitting the same replica queue sequentially. A primary risk under concurrent load is **head-of-line blocking**, where heavy 15-survey `/table` computations delay lightweight UI metadata calls (`/surveys-ui`, `/covariates`) queued behind them.
- **Private Network Resolution**: `tablemakerapiqa.worldbank.org` resolves to `wbg-pdmz-itsoc-tf-qa-appgw.worldbank.org` (`10.168.201.20`), an internal World Bank private VNet IP behind Azure Application Gateway. Azure Load Testing therefore requires **VNet injection (Private endpoint mode)** into the target subnet, or local execution from a machine on the WB network.
- **Azure Load Testing Engine Support**: Azure Load Testing natively supports **Python (Locust)** and **Apache JMeter** (it does not run R natively). Using Python (Locust) enables clean in-memory JSON payload chaining (`/table` response fed directly into `POST /description` and `POST /table/download`) and allows local pre-flight testing before handing the bundle to ITS.

## Requirements

1. **Primary Goal & SLO Monitoring**:
   - Evaluate API performance when multiple concurrent requests arrive simultaneously (both gradual ramp-up to find the saturation point and sudden spike/burst traffic).
   - Establish and verify Service Level Objectives (SLOs), such as maintaining `p95 <= 3000ms` latency and `< 1%` error rate across configurable virtual user tiers (e.g., 10 baseline, 30-50 heavy, 100+ stress/breaking point).
2. **Execution & Handoff Model**:
   - Start with manual / on-demand execution (locally via CLI/web UI and in Azure Load Testing Portal/CLI), structured so ITS / Azure DevOps can easily automate it in CI/CD pipelines later.
   - Deliverable must be a self-contained, turnkey folder (e.g., `load-testing/`) that can be handed directly to ITS with clear configuration and VNet instructions.
3. **Full Realistic User Journey (Excluding Session Endpoints)**:
   - **Health & Discovery**: `GET /health`, `GET /releases`, `GET /surveys-ui`, `GET /surveys`, `GET /countries`, `GET /regions`, `GET /analysis-variables`, `GET /statistics`, `GET /dimensions`, `GET /categories`, `GET /covariates`, `GET /lookup`.
   - **Core Table Computation (`GET /table` & `POST /table`)**: Using the current explicit layout-slot contract (`column`, `row`, `superColumn`, `superRow`), `filter_base`, `ppp`, `poverty_line`, `pop_share_threshold`, and `include_metadata=true` across three workload tiers:
     - *Tier 1 (Lightweight)*: 1 survey, 1 layout slot, basic welfare/poverty measure.
     - *Tier 2 (Typical)*: 3-5 surveys, 2 layout slots, `filter_base` applied, multiple measures.
     - *Tier 3 (Heavy / Max Cap)*: 15 surveys (server-side hard cap), 3-4 layout slots (`column`, `row`, `superColumn`, `superRow`), multiple measure families + shares, `include_metadata=true`.
   - **Chained Follow-up Endpoints**:
     - `POST /description`: Consumes the live `description_metadata` returned by `/table?include_metadata=true` (fast path) as well as parameter-based requests.
     - `POST /table/download`: Consumes the live Approach C `data` array returned by `/table` to generate both `csv` and `rds` binary downloads.
   - **Explicit Exclusion**: Do NOT include `POST /session/surveys` or `GET /session/<id>/surveys`, as session storage endpoints are being removed from the API.
4. **Scenario Design & Validation**:
   - Use a **curated scenario catalog + known-good survey pool** (with optional live bootstrap from `/surveys-ui`) so virtual users always send valid combinations of surveys, variables, and slots (testing real Arrow I/O and `collapse` compute rather than 400/422 validation errors) while varying surveys to defeat OS file-cache bias.
   - Validate response envelopes (`status == "success"`, non-empty `data`, valid binary/markdown payloads for download/description) so silent logical errors are caught alongside HTTP 5xx/timeouts.
5. **Scope Boundaries (Out of Scope for v1)**:
   - Modifying `{piptm}` R source code or Plumber endpoint implementations (any performance bottlenecks discovered will be addressed in follow-up tasks).
   - Modifying `.Rbuildignore` is deferred to the `/cg-work` implementation step so `R CMD check` ignores the new `load-testing/` directory.

## Approaches Considered

### Approach 1: Self-Bootstrapping Python Locust Suite + Azure Load Testing YAML Bundle (Chosen)
- **Summary**: A turnkey `load-testing/` bundle built on Python **Locust (`locustfile.py`)**, a curated `scenarios.json` catalog + known-good survey pool, an **`azure-load-test.yaml`** configuration file, and an **ITS handoff guide (`README.md`)**.
- **Pros**:
  - First-class native support in Azure Load Testing (`kind: Locust`).
  - Clean, readable Python code that chains `/table` -> `/description` and `/table` -> `/table/download` directly in memory without brittle regex/XML extractors.
  - Parameterized via environment variables (`USERS`, `SPAWN_RATE`, `RUN_TIME`, `LOAD_SHAPE`, `TARGET_HOST`) so user counts and workload mixes can be adjusted anytime without code changes.
  - Can be tested locally on the WB network before handing off to ITS.
- **Cons**:
  - Requires Python + `locust` package for local runs (standard in Azure Load Testing cloud engines).
- **Effort**: Medium (1-2 days).

### Approach 2: Apache JMeter (`.jmx`) Test Plan + CSV Data Configs
- **Summary**: Traditional XML-based JMeter test plan paired with CSV parameter files.
- **Pros**: Familiar to legacy JMeter-only QA teams.
- **Cons**: Machine-generated XML is hard to review/maintain in Git; chaining nested Approach C JSON responses from `/table` into `/table/download` and `/description` requires fragile JSR223 Groovy scripts inside XML.
- **Effort**: Medium-Large (2-3 days).

### Approach 3: Dual-Format Bundle (Locust Primary + JMeter `.jmx` Fallback)
- **Summary**: Shared `scenarios.json` driving both a full Locust suite and a simplified JMeter plan.
- **Pros**: Covers both engines if ITS has strict JMeter-only pipeline templates.
- **Cons**: Double maintenance overhead whenever API contracts evolve.
- **Effort**: Medium-Large (2-3 days).

## Decision

**Chosen Approach**: **Approach 1 — Self-Bootstrapping Python Locust Suite + Azure Load Testing YAML Bundle**, driven by a **curated scenario catalog and known-good survey pool**.

**Why**:
- Captures the full user journey (including head-of-line blocking between heavy 15-survey `/table` calls and fast UI discovery calls, plus chained `/description` and `/table/download` POST bodies) in clean, maintainable Python code.
- Provides a complete, zero-R-dependency handoff package for World Bank ITS / Azure DevOps with explicit configuration for private VNet injection (`10.168.201.20` / `tablemakerapiqa.worldbank.org`) and configurable SLO thresholds.
- Using a curated scenario catalog + known-good survey pool guarantees every request stresses real Arrow I/O and R computation rather than failing early on invalid parameter combinations.

## Next Steps

1. Run `/cg-plan` to design the implementation plan for the `load-testing/` bundle:
   - Inspect `docs/api-contract.md`, `insomnia-collection.json`, and `inst/plumber/plumber.R` to design the exact **Scenario Matrix (`scenarios.json`)** covering Discovery, Lookup, Tier 1/2/3 `/table` queries (explicit slots `column`, `row`, `superColumn`, `superRow`), `/description`, and `/table/download` (excluding `/session/*`).
   - Design `locustfile.py` with weighted user personas, response envelope assertions,chained `/table` -> `/description` + `/table/download` flows, and configurable load shapes (ramp-up vs. simultaneous burst/spike).
   - Create `azure-load-test.yaml` with parameterized concurrency settings and SLO pass/fail criteria (`p95 <= 3000ms`, error rate `< 1%`).
   - Write the ITS & Developer Handoff Guide (`load-testing/README.md`) covering local execution, Azure Portal / CLI setup, VNet subnet configuration for `tablemakerapiqa.worldbank.org`, and interpreting results (queueing latency vs. container replica scaling).
   - Include `.Rbuildignore` update in the implementation plan so `load-testing/` does not affect `devtools::check()`.
