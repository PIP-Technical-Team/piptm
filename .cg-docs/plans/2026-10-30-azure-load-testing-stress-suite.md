---
date: 2026-10-30
title: "Azure Load Testing Stress Test Suite for Table Maker API"
status: active
scope: "Standard"
brainstorm: ".cg-docs/brainstorms/2026-10-30-azure-load-testing-stress-suite.md"
language: "Python"
estimated-effort: "medium"
deviation-policy: "ask"
artifact-schema-version: 1
phases: 2
tags: [load-testing, stress-test, azure-load-testing, locust, api, performance, slo]
---

# Plan: Azure Load Testing Stress Test Suite for Table Maker API

## Objective

Build a self-contained, maintainable `load-testing/` bundle that World Bank ITS can run manually in Azure Load Testing against the privately reachable Table Maker API. The suite will model realistic concurrent user journeys, separate light/typical/heavy computation tiers, exercise ramp and spike traffic, enforce configurable client-side SLOs, and preserve a clear path to later Azure DevOps automation.

## Context

The Table Maker API is an R Plumber service deployed behind the private QA hostname `https://tablemakerapiqa.worldbank.org/`. DNS currently resolves through a World Bank application gateway to a private `10.x.x.x` address, so authoritative Azure runs require Azure Load Testing VNet injection or an equivalent network path managed by ITS.

The primary launch risk is concurrency around a single-threaded R request process per container replica. Heavy `/table` requests can queue lighter discovery requests behind them, so measuring only isolated table calls would miss head-of-line blocking. Existing benchmarks also show that Arrow scan count, network I/O, and warm-cache effects materially affect results; scenario names and survey rotation must make those effects diagnosable.

The API has two distinct `/table` response contracts that cannot be combined into one downstream chain:

- `include_metadata=false` returns nested Approach C survey objects and is the valid input to `POST /table/download`.
- `include_metadata=true` returns flat table rows and exposes `description_metadata` at `meta.description_metadata`; that metadata is the valid fast-path input to `POST /description`.

The implementation therefore uses separate valid chains for download and description traffic. It excludes `/session/*`, because those endpoints are scheduled for removal.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | Deliver a self-contained Python Locust bundle under `load-testing/` that runs locally and is uploadable to Azure Load Testing. | Approved brainstorm |
| R2 | Model discovery traffic and Tier 1 (1 survey), Tier 2 (3-5 surveys), and Tier 3 (15 surveys) `/table` workloads using valid explicit layout slots. | Approved brainstorm; `docs/api-contract.md` |
| R3 | Use a curated, versioned scenario catalog and known-good survey pools, supplemented by live discovery only when compatibility can be proven. | Approved brainstorm |
| R4 | Exercise both GET and POST `/table`, metadata-enabled `/table` -> `/description`, Approach C `/table` -> `/table/download`, and `/description` fallback recomputation. | Approved brainstorm; `inst/plumber/plumber.R` |
| R5 | Validate semantic success, response envelopes, content types, non-empty payloads, survey counts, and downstream chain inputs instead of treating every HTTP 200 as success. | Approved brainstorm; API testing learnings |
| R6 | Support configurable baseline/heavy/stress concurrency, duration, spawn rate, deterministic seed, target host, workload mix, and both ramp and spike profiles without Python edits. | Approved brainstorm |
| R7 | Supply Azure Load Testing `v0.1` Locust configuration with configurable `p95 <= 3000 ms` and error rate `< 1%` defaults, safe auto-stop, and placeholders for private subnet/app-component settings. | Microsoft Azure Load Testing YAML reference |
| R8 | Provide offline Python tests and linting that do not need QA, plus a low-volume live smoke procedure that requires an explicitly supplied reachable target. | Completion contract |
| R9 | Provide an ITS handoff guide for local use, Azure Portal/CLI configuration, VNet injection, interpreting results, changing load/SLO values, and later CI/CD adoption. | Approved brainstorm |
| R10 | Keep load-testing assets out of the R package build and do not modify R or Plumber behavior. | Approved brainstorm; `.Rbuildignore` |
| R11 | Exclude all `/session/*` requests and avoid committing credentials, concrete private subnet IDs, or other secrets. | User decision; security boundary |
| R12 | Preserve reproducibility and diagnostic value through deterministic scenario IDs, named Locust requests, optional seeded selection, and explicit reporting of bootstrap/fallback decisions. | Charter; performance learnings |

## Phase 1: Scenario Engine and Locust Journeys

### 1. Establish the load-testing package and configuration contract
- **Requirements**: R1, R6, R8, R10, R11, R12
- **Files**: `load-testing/pyproject.toml`, `load-testing/requirements.txt`, `load-testing/loadtest/config.py`, `load-testing/loadtest/__init__.py`, `load-testing/tests/test_config.py`
- **Details**:
  - Create an isolated Python 3.11+ project using Locust as the runtime dependency and `pytest`/`ruff` as development dependencies. Keep the Azure runtime dependency list minimal and pinned or bounded in `requirements.txt`; use `pyproject.toml` for local tooling configuration.
  - Define one typed configuration boundary that reads environment variables once and validates them before users spawn. Include `TARGET_HOST`, `LOAD_PROFILE` (`ramp` or `spike`), `USERS`, `SPAWN_RATE`, `RUN_TIME`, `RAMP_STEPS`, `SPIKE_USERS`, `SPIKE_HOLD_SECONDS`, `RANDOM_SEED`, `SCENARIO_CATALOG`, `BOOTSTRAP_MODE`, `REQUEST_TIMEOUT`, and workload weights.
  - Keep SLO values (`SLO_P95_MS`, `SLO_ERROR_PERCENT`) available to documentation/config generation, while Azure YAML remains the authoritative Azure failure gate.
  - Reject unsafe or nonsensical values (non-HTTPS remote host unless explicitly local, negative users, zero duration, invalid profile, weights that do not sum to a positive value). Normalize host URLs without embedding QA credentials.
  - Do not use `print()` or stdlib `logging`; use Locust event/reporting facilities for runtime diagnostics. Add `loguru` only if diagnostics cannot be expressed cleanly through Locust.
- **Test Scenarios**: default configuration; baseline/heavy/stress overrides; ramp and spike profiles; deterministic seed; invalid numeric/profile/host values; local HTTP opt-in.
- **Tests**: `uv run --project load-testing pytest load-testing/tests/test_config.py`; `uv run --project load-testing ruff check load-testing`
- **Acceptance criteria**: Configuration is validated before test execution, every tunable load parameter is externally configurable, and offline tests demonstrate deterministic parsing and rejection behavior.

### 2. Define and validate the curated scenario catalog
- **Requirements**: R2, R3, R4, R5, R11, R12
- **Files**: `load-testing/scenarios.json`, `load-testing/loadtest/scenarios.py`, `load-testing/tests/fixtures/*.json`, `load-testing/tests/test_scenarios.py`
- **Details**:
  - Define a catalog schema with stable scenario IDs, tier, weight, supported transport (`GET`/`POST`), analysis variable, measures, ordered slot map (`column`, `row`, `superColumn`, `superRow`), optional poverty line/filter/PPP, requested survey count, and required covariates.
  - Seed the catalog from current API-contract and Insomnia examples, but translate stale `by` examples to the current explicit slot contract. Include at minimum:
    - Tier 1 national and one-slot welfare/poverty queries.
    - Tier 2 3-5-survey two-slot queries, including a valid `filter_base` and binary/share analysis.
    - Tier 3 15-survey three/four-slot heavy queries with multiple measure families, including an order-dependent measure such as Gini or median so the suite covers full-microdata computation.
    - Separate scenarios for Approach C download, metadata fast-path description, and fallback recomputation.
  - Store known-good survey pools and required common dimensions by target release or use explicit placeholders documented for ITS replacement. Never silently mix surveys lacking the dimensions a scenario requires.
  - Validate identifier format, tier survey limits, non-empty measures, poverty-line rules, contiguous/unique layout slots, valid downstream mode, and absence of `/session` paths. Treat invalid catalog content as startup failure, not runtime noise.
  - Keep intentional validation/error scenarios out of the primary SLO workload. If retained for diagnostics, place them in an opt-in non-SLO profile with separate request names.
- **Test Scenarios**: all three valid tiers; GET query encoding; POST JSON shape; poverty line required; gapped/duplicate slots; >15 surveys; missing survey pool; forbidden session path; deterministic weighted selection.
- **Tests**: `uv run --project load-testing pytest load-testing/tests/test_scenarios.py`
- **Acceptance criteria**: Every primary catalog entry is statically valid, maps to a named workload tier, and either has a compatible curated survey pool or fails clearly before load generation.

### 3. Implement API bootstrap, semantic assertions, and chain helpers
- **Requirements**: R3, R4, R5, R8, R11, R12
- **Files**: `load-testing/loadtest/api.py`, `load-testing/loadtest/bootstrap.py`, `load-testing/tests/test_api.py`, `load-testing/tests/test_bootstrap.py`, `load-testing/tests/fixtures/*.json`
- **Details**:
  - Implement reusable request wrappers that assign low-cardinality Locust names such as `discovery:/surveys-ui`, `table:tier1:get`, `table:tier3:post`, `description:metadata`, `description:recompute`, `download:csv`, and `download:rds`. Never put survey IDs or query strings in metric names.
  - For JSON-envelope endpoints, require expected status, `status == "success"`, empty semantic errors, and endpoint-specific non-empty data/meta checks. Accept JSON `[]`/`null` representation differences only when semantically equivalent.
  - Validate `/description` by status, content type (`text/html` or `text/plain`), and non-empty recognizable content. Validate downloads by status, expected `Content-Type`/`Content-Disposition`, non-empty bytes, and a conservative maximum retained response size.
  - Implement two independent chain helpers:
    - Metadata chain: `/table?include_metadata=true` -> extract `meta.description_metadata` -> `POST /description`.
    - Download chain: `/table?include_metadata=false` -> extract Approach C `data` -> `POST /table/download` for CSV or RDS.
  - Do not retain large responses on the virtual user longer than the chain needs. Do not log survey microdata or full response bodies.
  - Add startup discovery through `/health`, `/releases`, `/surveys-ui`, `/categories`, `/covariates`, `/analysis-variables`, and `/statistics`. In live-bootstrap mode, intersect requested dimensions across candidate surveys and prove a generated scenario is compatible; otherwise use the curated pool. Make fallback/abort behavior explicit through `BOOTSTRAP_MODE` (`curated`, `live`, `live-with-curated-fallback`).
  - A network/bootstrap failure must not cause all virtual users to generate invalid requests. Abort startup or use the explicitly configured fallback.
- **Test Scenarios**: successful envelope; HTTP 200 semantic error; missing/empty data; null/empty warnings; metadata extraction; wrong response shape for download; CSV/RDS headers; malformed discovery response; live bootstrap success; curated fallback; no compatible surveys.
- **Tests**: `uv run --project load-testing pytest load-testing/tests/test_api.py load-testing/tests/test_bootstrap.py`
- **Acceptance criteria**: Unit fixtures prove each endpoint and both chains accept current contracts, reject mismatched shapes, and report failures to Locust without crashing the entire runner.

### 4. Build realistic virtual-user journeys and load shapes
- **Requirements**: R2, R4, R5, R6, R8, R12
- **Files**: `load-testing/locustfile.py`, `load-testing/loadtest/users.py`, `load-testing/loadtest/shapes.py`, `load-testing/tests/test_users.py`, `load-testing/tests/test_shapes.py`
- **Details**:
  - Implement a realistic user journey that performs discovery at session start, waits using configurable think-time, and selects weighted Tier 1/2/3 table scenarios plus description/download follow-ups. Keep metadata traffic active while heavy table traffic runs so head-of-line blocking appears in endpoint-level metrics.
  - Use `catch_response=True` and semantic assertions so malformed HTTP 200 responses count as Locust failures.
  - Ensure chained follow-ups are conditional: a failed parent request records its own failure and skips the child rather than generating secondary invalid requests.
  - Implement deterministic ramp and spike `LoadTestShape` classes using validated configuration. Ramp should support baseline -> heavy -> stress steps (defaults 10 -> 50 -> 100); spike should establish a baseline, jump to configured spike users, hold, and recover. Permit ITS to override every value through environment variables or Locust configuration.
  - Ensure user counts represent concurrent Locust users, not a guaranteed simultaneous number of `/table` calls; document that task weights and think time control endpoint arrival rates.
  - Keep the main module importable offline. Avoid network calls at module import; perform bootstrap through Locust lifecycle hooks.
- **Test Scenarios**: request-name stability; parent-chain failure; tier weighting; discovery interleaving; ramp boundaries; spike transition/hold/recovery; runtime expiry; deterministic selection; import/list without network.
- **Tests**: `uv run --project load-testing pytest load-testing/tests/test_users.py load-testing/tests/test_shapes.py`; `uv run --project load-testing locust -f load-testing/locustfile.py --list`
- **Acceptance criteria**: Both profiles produce expected user/spawn-rate schedules, all primary endpoint groups appear under stable metric names, and the module lists/imports without reaching QA.

## Phase 2: Azure Packaging, Verification, and ITS Handoff

### 5. Package Azure Load Testing configuration and SLO gates
- **Requirements**: R1, R6, R7, R9, R11
- **Files**: `load-testing/azure-load-test.yaml`, `load-testing/locust.conf`, `load-testing/tests/test_azure_config.py`
- **Details**:
  - Create an Azure Load Testing YAML using the current documented schema: `version: v0.1`, a valid `testId`, `testType: Locust`, `testPlan: locustfile.py`, `engineInstances`, `configurationFiles`, `properties.userPropertyFile`, `env`, `failureCriteria`, and `autoStop`.
  - Use supported client criteria syntax with defaults equivalent to `p95(response_time_ms) > 3000` and `percentage(error) > 1`. Add per-request criteria only where a distinct endpoint SLO is justified; do not hide table failures through an aggregate dominated by metadata calls.
  - Include `scenarios.json` and any Python package files required by Azure as configuration/upload artifacts, respecting Azure's same-folder filename rules. If Azure cannot preserve package directories through the chosen upload path, package helpers as a ZIP artifact or keep the runtime script self-contained while retaining tested modules locally; verify this before finalizing.
  - Keep `subnetId`, `publicIPDisabled`, `appComponents`, resource IDs, authentication, and secret references as documented ITS-supplied values. Do not commit a real subnet/resource ID or credentials.
  - Configure auto-stop high enough to protect the service from sustained catastrophic errors without prematurely hiding the saturation point (document the chosen threshold and window).
- **Test Scenarios**: required YAML keys; Locust test type; filename existence; valid test ID; environment names align with Python config; supported failure-criteria grammar; no secret-looking values; no unresolved local filesystem paths.
- **Tests**: `uv run --project load-testing pytest load-testing/tests/test_azure_config.py`
- **Acceptance criteria**: The Azure configuration references a complete upload set, uses only documented `v0.1` keys/criteria, exposes safe placeholders for ITS, and passes offline structural tests.

### 6. Complete verification, package isolation, and ITS documentation
- **Requirements**: R8, R9, R10, R11, R12
- **Files**: `load-testing/README.md`, `load-testing/.env.example`, `.Rbuildignore`, all Phase 1/2 files
- **Details**:
  - Write a concise handoff guide that distinguishes:
    - Offline setup/tests (`uv sync`, pytest, Ruff, Locust `--list`).
    - Local low-volume execution from a machine that can resolve/reach the private hostname.
    - Azure Portal upload and Private traffic/VNet injection, including separate nondelegated subnet, same subscription/region requirement, available IP capacity, Network Contributor permissions, DNS resolution, and optional `publicIPDisabled` implications.
    - Azure CLI/Azure DevOps adoption using the committed YAML without implementing the pipeline now.
    - How to change users, spawn rates, duration, ramp/spike values, scenario weights, target host, deterministic seed, and SLO criteria.
    - How to interpret p50/p95/p99, error rate, requests/sec, discovery-vs-table latency, cold/warm cache effects, engine saturation, queueing/head-of-line blocking, and container replica scaling.
    - A results-recording template with environment, release, timestamp, engine count, load profile, scenario mix, Container App replica limits, and observed threshold/breaking point.
  - Explain that 10/50/100 users are starting probes, not a promised production capacity. Recommend a sequence: 1-user preflight, 10-user baseline, stepped 50/100 run, then targeted spike, stopping or adjusting if the agreed error/latency guard is reached.
  - Add `^load-testing$` to `.Rbuildignore` and verify the load suite is absent from the R source tarball. Do not change `DESCRIPTION` or add Python as an R package dependency.
  - Run all offline tests and lint. Run a 1-user/low-rate, short smoke test only when an explicitly supplied target is reachable; do not run high load against QA without ITS authorization. Capture Locust CSV or HTML output as execution evidence, but do not commit environment-specific result files by default.
  - If QA cannot be reached from the implementation environment, stop final completion unless the user approves an exception for V5; do not substitute mocked success for the live smoke requirement.
- **Test Scenarios**: clean setup from documented commands; missing target; unreachable private DNS; 1-user smoke; R build source contents; no session paths; no secrets; all internal links/files present.
- **Tests**: `uv sync --project load-testing`; `uv run --project load-testing pytest load-testing/tests`; `uv run --project load-testing ruff check load-testing`; `uv run --project load-testing locust -f load-testing/locustfile.py --list`; documented short headless smoke command; `R CMD build .` and archive-content check.
- **Acceptance criteria**: A new ITS engineer can validate and configure the bundle from the README, offline gates pass, a reachable-target smoke run succeeds or has an explicit approved exception, and the R source package excludes `load-testing/`.

## Testing Strategy

- **Offline unit tests**: Use `pytest` fixtures for current API envelopes, Approach C payloads, metadata-enabled flat responses, descriptions, CSV/RDS headers, and malformed responses. No unit test contacts QA.
- **Static quality**: Use Ruff for syntax, imports, style, and common defects. Keep public helper signatures typed and test configuration/catalog parsing exhaustively.
- **Contract tests**: Treat `docs/api-contract.md`, `inst/plumber/plumber.R`, and current R integration fixtures as authoritative. Assert the two `/table` response modes separately.
- **Load-shape tests**: Call shape tick logic against controlled clocks/configuration to verify exact ramp and spike transitions without sleeping.
- **Smoke test**: Run a short one-user headless Locust execution against a reachable target. It proves networking, current discovery, at least one table scenario, and semantic response handling, but it does not establish performance capacity.
- **Azure acceptance**: ITS performs the authoritative VNet-injected baseline, ramp, and spike runs. Preserve request names/tier labels so Azure charts can distinguish metadata queueing from compute latency.
- **SLO interpretation**: Azure failure criteria gate the test. Locust semantic failures contribute to error percentage; p95 is evaluated globally and, where configured, by critical request name. Report p99 and resource metrics diagnostically even when they are not hard gates.

## Documentation Checklist

- [ ] State prerequisites: Python/uv for local use, private-network reachability, Azure Load Testing permissions, and ITS-owned Azure identifiers.
- [ ] Document every environment variable with default, allowed values, and example.
- [ ] Explain curated vs. live bootstrap behavior and deterministic seed usage.
- [ ] Explain Tier 1/2/3 scenarios and the distinction between virtual users and concurrent `/table` requests.
- [ ] Explain the separate description and download chains.
- [ ] Document local web UI, headless smoke, Azure Portal, and future Azure DevOps routes.
- [ ] Document VNet/subnet/DNS requirements and placeholders.
- [ ] Document SLO defaults, auto-stop, and how to adjust them safely.
- [ ] Include troubleshooting for DNS timeout, 400/422 scenario incompatibility, 413 download payload, 5xx errors, and load-generator saturation.
- [ ] Include a reproducible results-recording template and explicit prohibition on unapproved production stress runs.

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Curated survey IDs become stale after a release change | High error rate measures invalid identifiers rather than API capacity | Version pools by release; support live discovery; fail startup when compatibility cannot be proven; document refresh workflow. |
| Warm OS/Arrow cache makes repeated surveys look unrealistically fast | Misleading launch capacity | Rotate compatible survey pools with a deterministic seed; label tiers; record cold/warm run order and release. |
| Single aggregate p95 is dominated by fast metadata calls | Heavy `/table` regression is hidden | Use stable request names and add critical per-request Azure criteria after confirming naming in a smoke run. |
| Locust generator or Azure engine saturates before the API | False API bottleneck | Monitor engine CPU, scale `engineInstances`, compare requests/sec per engine, and document generator saturation checks. |
| The private QA endpoint is unreachable from local or Azure engines | No authoritative live evidence | Require WB-network local execution or VNet injection; document subnet/DNS prerequisites; halt or obtain explicit V5 exception. |
| `/table` response modes are accidentally mixed | `/description` or `/table/download` produces invalid traffic | Separate chain helpers and response fixtures; assert metadata vs. Approach C shapes before child calls. |
| Download responses create high load-generator memory use | Locust engine becomes bottleneck | Bound scenario weights/payload sizes, discard bytes immediately, and use download traffic sparingly in the realistic mix. |
| High stress damages QA availability or interferes with other users | Operational incident | Start with 1/10 users, use auto-stop, require ITS-approved window, never target production without explicit authorization. |
| Azure schema/upload behavior differs from local assumptions | Handoff bundle cannot start | Test YAML/file references offline and perform an ITS/Azure dry run; use ZIP artifacts or a self-contained runtime script if directory uploads are flattened. |
| API authentication or gateway policy changes | Requests fail before reaching Plumber | Keep optional headers/secrets parameterized; route secrets through Azure Key Vault; treat material auth design as a deviation requiring approval. |

## Out of Scope

- Changing R computation, Plumber endpoints, API contracts, caching, worker count, Container App scaling, or Azure gateway configuration.
- Provisioning Azure Load Testing resources, VNets/subnets, managed identities, Key Vault entries, or Azure DevOps pipelines.
- Removing `/session/*` endpoints from the API; the suite only excludes them.
- Browser/UI rendering or end-to-end frontend performance testing.
- Running high-concurrency tests against QA or any production endpoint without ITS authorization.
- Fixing bottlenecks found by the suite; findings become separate roadmap work.

## Completion Contract

### Outcome

A self-contained `load-testing/` bundle can run locally and in Azure Load Testing with configurable ramp and spike profiles, execute curated realistic Table Maker API journeys without `/session/*`, validate semantic responses, and hand ITS the files and private-network instructions needed to operate it.

### Verification Surface

| ID | Evidence Required | Command/Artifact | Required |
|----|-------------------|------------------|----------|
| V1 | Python scenario/config validation tests pass, including catalog schema, environment parsing, response assertions, and both chained endpoint flows | `uv run --project load-testing pytest load-testing/tests` | yes |
| V2 | Python lint passes for the Locust suite and tests | `uv run --project load-testing ruff check load-testing` | yes |
| V3 | Locust imports and enumerates the configured ramp and spike profiles without contacting QA | `uv run --project load-testing locust -f load-testing/locustfile.py --list` plus focused pytest coverage | yes |
| V4 | Azure configuration is valid against the documented `v0.1` Locust schema and references every required uploaded file | `load-testing/azure-load-test.yaml` review plus YAML/config tests | yes |
| V5 | A local smoke run completes against an explicitly supplied reachable target and records successful discovery plus at least one `/table` journey | Documented smoke command and Locust CSV/HTML output | yes |
| V6 | R package build isolation remains intact | `.Rbuildignore` contains `^load-testing$`; `R CMD build .` or equivalent archive-content check confirms exclusion | yes |
| V7 | ITS handoff documentation covers private VNet injection, configurable load/SLO values, local/manual Azure execution, result interpretation, and later CI/CD adoption | `load-testing/README.md` checklist test/review | yes |

### Constraints

| ID | Constraint | Check |
|----|------------|-------|
| C1 | Do not modify R package or Plumber behavior | Git diff limited to `load-testing/` and `.Rbuildignore` |
| C2 | Never include `/session/*` traffic | Static test rejects session paths in the catalog and Locust task definitions |
| C3 | `/description` receives `meta.description_metadata` from a metadata-enabled `/table` call; `/table/download` receives Approach C `data` from a metadata-disabled call | Chained-flow unit tests with representative response fixtures |
| C4 | Scenario selection must avoid invalid survey/dimension combinations and preserve reproducibility | Curated catalog validation, common-covariate discovery, deterministic seed option, explicit fallback/abort behavior |
| C5 | SLO thresholds and user counts remain configurable, not hardcoded into Python logic | Environment/config tests for users, spawn rate, duration, profile, host, seed, p95, and error-rate settings |
| C6 | QA/private infrastructure identifiers and credentials are not committed as secrets | Placeholder subnet/resource IDs; optional secrets documented through Azure Key Vault/environment configuration |
| C7 | No performance claim is inferred from mocked/offline tests | Documentation distinguishes offline validation, local smoke testing, and authoritative Azure load results |

### Boundaries

- Allowed: `load-testing/locustfile.py`, curated scenario/config JSON, Azure YAML, Python dependency metadata, pytest tests/fixtures, handoff documentation, and `.Rbuildignore`.
- Allowed: local validation and a low-volume smoke run against a user/ITS-provided reachable endpoint.
- Out of scope: API optimization, Plumber/container scaling changes, Azure resource provisioning, credentials, frontend/browser testing, production load execution, and session endpoint removal.

### Iteration Policy

1. Implement offline schema/config/unit validation first.
2. Add dynamic API bootstrap and curated workload selection with deterministic failure when no valid scenario can be assembled.
3. Implement separately named request metrics for discovery, light/typical/heavy table workloads, description fast path, description recomputation, CSV download, and RDS download.
4. Add ramp and spike shapes plus Azure `v0.1` YAML failure criteria (`p95(response_time_ms) > 3000`, `percentage(error) > 1` by default).
5. Run only a low-volume reachable-target smoke test during implementation; ITS owns high-concurrency Azure execution.
6. Under stored policy `ask`, pause before changing API contracts, adding authentication handling not evidenced by the target, or expanding into Azure infrastructure code.

### Blocked-Stop Conditions

- No reachable API target is available for required smoke verification and no explicit exception is approved.
- The live API contract differs materially from `docs/api-contract.md` or prevents valid scenario construction.
- Azure Load Testing requires an undocumented corporate template or authentication mechanism that changes the deliverable design.
- Required validation fails after reasonable fixes.
- Work would require modifying API/package behavior or provisioning private Azure infrastructure.
- A required deviation is found and approval is unavailable.
