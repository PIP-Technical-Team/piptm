# Table Maker API — Azure Load Testing & Locust Suite

Self-contained Python Locust stress-testing bundle for the `{piptm}` Table Maker Plumber API, designed for both local developer smoke runs and authoritative execution in **Azure Load Testing** (handed off to World Bank ITS).

> **Important**: Offline `pytest` runs validate request/response contracts, UI variable/measure rules, and configuration safety only — they **do not** prove live API capacity. Authoritative capacity and SLO measurements require running against the deployed QA environment from inside the World Bank network or via Azure Load Testing VNet injection.

---

## 1. What This Suite Tests (13-Endpoint UI Flow)

Virtual users model the real 3-step Table Maker UI wizard across **13 active endpoints** (and never call removed `/description`, `/session/*`, or developer-only `/surveys`, `/lookup`, `/dimensions`, nor send `include_metadata`):

| Stage | Endpoints Exercised | Latency Class |
|---|---|---|
| **System** | `GET /health`, `GET /releases` | **Class A** (`p95 <= 1000 ms`) |
| **Page 1 — Survey Selection** | `GET /regions`, `GET /countries`, `GET /surveys-ui` | **Class A** (`p95 <= 1000 ms`) |
| **Page 2 — Table Configuration** | `GET /categories`, `GET /analysis-variables`, `GET /statistics`, `GET /covariates`, `GET /cell-definition` | **Class A** (`p95 <= 1000 ms`) |
| **Page 3 — Compute & Export** | `GET\|POST /table` (Tiers 1–2), `POST /table/download` (`csv`, `rds`) | **Class B** (`p95 <= 3000 ms`) |
| **Page 3 — Heavy Compute** | `GET\|POST /table` (Tier 3: 10–15 surveys or 5-survey 4-slot filtered quantiles/inequality) | **Class C** (`p95 <= 5000 ms`) |
| **Page 3 — Metadata Document** | `GET\|POST /metadata` (`format=html\|markdown`, zero Arrow I/O) | **Class A** (`p95 <= 1000 ms`) |

### UI Parameter Rules Enforced in `scenarios.json`
- **Single `analysis_var` per request** with `measures` restricted strictly to that variable's allowed `stat_groups` from `inst/extdata/tm_variable_spec.yaml` and `tm_measure_spec.yaml`:
  - `welfare` -> `summary_statistics` and/or `inequality`
  - `pov_status` -> `poverty` (requires positive `poverty_line`)
  - Continuous (`age`, `educy`, `hsize`) -> `summary_statistics` only
  - Binary (`electricity`, `female`, `imp_wat_rec`, etc.) -> `shares` only
- **Parent-child layout slots**: `column` and `row` are independent (`row`-only is valid); `superColumn` requires `column`; `superRow` requires `row`; no duplicate variables across slots; `analysis_var` cannot also appear in a slot.
- **Partial survey exclusion & expected-422**:
  - In multi-survey requests, if some selected surveys lack a requested covariate/filter, `/table` returns `200 OK` with the surviving surveys (`1 <= returned <= requested`) and a warning — validated as a pass.
  - Scenario `tier1-expected-422-missing-dim` intentionally requests a rarely available dimension (`industrycat10_2_year`) on a single survey to verify `/table` returns a clean `HTTP 422` user-facing error envelope (recorded as an expected pass when `422` is returned).

---

## 2. Scaling Model: Test Engines vs. Virtual Users vs. API Replicas

- **Azure Test Engines (`engineInstances: 1` in `azure-load-test.yaml`)**: This is the number of client-side Azure load-generator VMs running Locust. Because virtual users pause `1-3s` between actions, **1 test engine easily drives 30–100+ virtual users** and is never the bottleneck.
- **Virtual Users (`RAMP_STEPS=2,5,15,30`, `SPIKE_USERS=40`)**: Anchored as an order-of-magnitude starting estimate from ~2M requests/month across the broader PIP platform (~0.8 RPS 24/7 average):
  - `2` users (~1 RPS): normal weekday peak
  - `5` users (~2–3 RPS): busy period
  - `15` users (~7 RPS): launch / workshop burst
  - `30` users (~15 RPS): extreme stress test (>15x platform average)
  - *Note*: These numbers are a starting calibration, not measured Table Maker demand; revisit once post-launch telemetry exists.
- **API Container Replicas (ITS-owned)**: Each R Plumber process is single-threaded per replica (using 4 internal `collapse` C threads). Concurrency scaling on the server side (min/max replicas, CPU/memory per replica, HTTP queue length) is configured by ITS on the Azure Container App.

---

## 3. Traffic Modes (`TRAFFIC_MODE`)

- **`TRAFFIC_MODE=mixed` (default)**: Virtual users run weighted Page 1/2 discovery (`DISCOVERY_WEIGHT=10`), Tier 1 (`TIER1_WEIGHT=60`), Tier 2 (`TIER2_WEIGHT=30`), and Tier 3 (`TIER3_WEIGHT=10`) journeys, calling `GET /cell-definition` before `/table` and chaining to `/metadata` or `/table/download`.
- **`TRAFFIC_MODE=stampede`**: Every virtual user repeatedly fires the **same** heavy `/table` scenario concurrently (selected via `STAMPEDE_SCENARIO_ID=<id>` or the first scenario in `STAMPEDE_TIER=3`) to measure head-of-line blocking when `N` users click "Generate Table" simultaneously.

---

## 4. Latency Classes & Pass/Fail SLOs

Every Locust request name carries its latency class tag so Azure Load Testing charts and CSV exports can be filtered by class instead of relying only on a single blended `p95`:

| Class | Request Name Pattern | Target `p95` | Env Var |
|---|---|---|---|
| **Class A** (UI / Preview / Metadata) | `discovery:class-a:*`, `preview:class-a:/cell-definition`, `metadata:class-a:*` | `<= 1000 ms` | `SLO_CLASS_A_P95_MS=1000` |
| **Class B** (Tier 1–2 `/table` & `/table/download`) | `table:tier1:class-b:*`, `table:tier2:class-b:*`, `download:class-b:*` | `<= 3000 ms` | `SLO_CLASS_B_P95_MS=3000` |
| **Class C** (Tier 3 heavy `/table`) | `table:tier3:class-c:*` | `<= 5000 ms` | `SLO_CLASS_C_P95_MS=5000` |
| **Global Error Rate** | All requests | `< 1%` | `SLO_ERROR_PERCENT=1` |

In `azure-load-test.yaml`, Azure Load Testing `v0.1` evaluates aggregate `clientMetrics` (`p95(response_time_ms) > 3000`, `percentage(error) > 1`) plus `autoStop` (`80%` error rate over `120s`). Use the Azure portal request-name filter (`class-a`, `class-b`, `class-c`) or the exported `stats.csv` to verify each class against its `1000 ms` / `3000 ms` / `5000 ms` budget.

---

## 5. Configuration Reference

| Variable | Default | Description |
|---|---|---|
| `TARGET_HOST` | `https://tablemakerapiqa.worldbank.org` | Base URL of the Table Maker API |
| `LOAD_PROFILE` | `ramp` | `ramp` or `spike` |
| `TRAFFIC_MODE` | `mixed` | `mixed` (realistic UI flow) or `stampede` (pinned concurrent `/table` query) |
| `STAMPEDE_TIER` | `3` | Target tier (`1`, `2`, or `3`) when `TRAFFIC_MODE=stampede` and no scenario ID is set |
| `STAMPEDE_SCENARIO_ID` | *(empty)* | Optional scenario ID from `scenarios.json` to pin in `stampede` mode |
| `USERS` | `30` | Baseline or target virtual users |
| `RAMP_STEPS` | `2,5,15,30` | Stepped user counts across equal slices of `RUN_TIME` |
| `SPIKE_USERS` | `40` | Peak user count during `spike` profile |
| `SPIKE_HOLD_SECONDS` | `120` | Duration to hold peak spike load |
| `SPAWN_RATE` | `5` | Users spawned per second |
| `RUN_TIME` | `15m` | Total run duration (`60s`, `15m`, `1h`) |
| `RANDOM_SEED` | `20261030` | Deterministic seed for scenario/survey selection |
| `SCENARIO_CATALOG` | `scenarios.json` | Path to the scenario catalog JSON file (relative to `load-testing/` or absolute) |
| `BOOTSTRAP_MODE` | `curated` | `curated`, `live`, or `live-with-curated-fallback` |
| `REQUEST_TIMEOUT` | `120` | Per-request HTTP timeout in seconds |
| `THINK_TIME_MIN` / `MAX` | `1` / `3` | Pause window (seconds) between virtual user tasks |
| `TIER1_WEIGHT` / `TIER2_WEIGHT` / `TIER3_WEIGHT` | `60` / `30` / `10` | Relative weights for Tier 1, Tier 2, and Tier 3 `/table` tasks |
| `DISCOVERY_WEIGHT` | `10` | Relative weight for Page 1/2 discovery sweep |
| `SLO_CLASS_A_P95_MS` | `1000` | Class A p95 target (ms) |
| `SLO_CLASS_B_P95_MS` | `3000` | Class B p95 target (ms) |
| `SLO_CLASS_C_P95_MS` | `5000` | Class C p95 target (ms) |
| `SLO_P95_MS` | `3000` | Aggregate fallback p95 threshold (ms) |
| `SLO_ERROR_PERCENT` | `1` | Maximum allowed error percentage |
| `ALLOW_INSECURE_LOCAL` | `false` | Set `true` only when testing `http://127.0.0.1` or `http://localhost` |

---

## 6. Running Locally & Verifying Offline

```powershell
# Run the offline unit test suite (54+ tests)
& "load-testing\.venv\Scripts\pytest.exe" load-testing/tests -v

# Run ruff linter
& "load-testing\.venv\Scripts\ruff.exe" check load-testing

# 1-user smoke run against a local Plumber instance on http://127.0.0.1:8000
$env:TARGET_HOST = "http://127.0.0.1:8000"
$env:ALLOW_INSECURE_LOCAL = "true"
$env:RAMP_STEPS = "1"
$env:RUN_TIME = "30s"
& "load-testing\.venv\Scripts\locust.exe" -f load-testing/locustfile.py --headless
```

## 7. Azure Load Testing Handoff (ITS)

1. Upload all files in `load-testing/` (`locustfile.py`, `loadtest/*.py`, `scenarios.json`, `requirements.txt`, `locust.conf`, `azure-load-test.yaml`) — note that `locustfile.py` includes a flat-import fallback if Azure flattens uploaded files into a single directory.
2. Inject the test into a private VNet subnet (`subnetId`) that can resolve and route to `https://tablemakerapiqa.worldbank.org`.
3. Optionally attach the QA Container App under `appComponents` to correlate client latency with server CPU, memory, and replica count.
4. Stop immediately if unexpected `5xx` responses or sustained error rates occur (`autoStop` halts the run automatically at `>= 80%` errors over `120s`).
