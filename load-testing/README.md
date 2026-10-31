# Table Maker API Load Testing

This isolated Python 3.11+ bundle drives realistic Table Maker API discovery,
table, description, and download journeys with Locust. It never calls
`/session/*`, contains no credentials, and is excluded from the R source package.

Offline tests prove request construction and response semantics. They do not
establish API capacity. Authoritative load runs require an approved ITS window
and a network path to the private target.

## Workloads

- Tier 1: one-survey national and one-slot requests.
- Tier 2: three-to-five-survey, two-slot, filtered/share requests.
- Tier 3: 15-survey, three/four-slot requests with median or Gini computation.
- Metadata chain: `/table?include_metadata=true` then `meta.description_metadata`
  to `/description`.
- Download chain: `/table?include_metadata=false` Approach C `data` to
  `/table/download` as CSV or RDS.
- Fallback chain: table parameters sent directly to `/description` for
  recomputation.

The committed survey pools are starting candidates and must be validated for the
target release. `BOOTSTRAP_MODE=live` replaces each pool with surveys proven by
`/surveys-ui` to contain the scenario's common dimensions. The
`live-with-curated-fallback` mode reports live incompatibility by falling back to
the curated pools; use it only after those pools have been validated for the
release. The seed makes selection repeatable, not request completion order under
concurrency.

Locust user counts are concurrent virtual users, not guaranteed simultaneous
`/table` calls. Task weights and think time determine endpoint arrival rates.

## Local Setup

Install [uv](https://docs.astral.sh/uv/), then run from the repository root:

```powershell
uv sync --project load-testing
uv run --project load-testing pytest load-testing/tests
uv run --project load-testing ruff check load-testing
uv run --project load-testing locust -f load-testing/locustfile.py --list
```

For the web UI on a machine that can reach the target:

```powershell
$env:TARGET_HOST = "https://tablemakerapiqa.worldbank.org"
$env:BOOTSTRAP_MODE = "live"
uv run --project load-testing locust -f load-testing/locustfile.py --web-host 127.0.0.1
```

The committed `locust.conf` defaults to headless operation. Add
`--headless=false` for the web UI if the installed Locust version does not let
the command-line UI option override the file.

## Smoke Test

Run only after ITS supplies or approves a reachable non-production target. This
one-user command validates networking and contracts; it is not a benchmark:

```powershell
$env:TARGET_HOST = "https://<ITS-supplied-reachable-host>"
$env:BOOTSTRAP_MODE = "live"
$env:LOAD_PROFILE = "ramp"
$env:RAMP_STEPS = "1"
$env:RUN_TIME = "60s"
$env:SPAWN_RATE = "1"
uv run --project load-testing locust -f load-testing/locustfile.py --headless --csv load-testing/results/smoke --html load-testing/results/smoke.html
```

Do not commit `results/`. A successful smoke must show discovery and at least one
`table:tier1:*` request with no semantic failures.

For a local HTTP Plumber process only, set `TARGET_HOST=http://127.0.0.1:8000`
and `ALLOW_INSECURE_LOCAL=true`. Remote HTTP targets are rejected.

## Configuration

| Variable | Default | Meaning |
|---|---:|---|
| `TARGET_HOST` | QA HTTPS host | Absolute target URL; no credentials/query |
| `LOAD_PROFILE` | `ramp` | `ramp` or `spike` |
| `USERS` | `100` | Hard cap for ramp steps and spike baseline bound |
| `SPAWN_RATE` | `5` | Users started per second |
| `RUN_TIME` | `15m` | Positive duration using seconds/minutes/hours |
| `RAMP_STEPS` | `10,50,100` | Equal-duration ramp user levels |
| `SPIKE_USERS` | `100` | Spike user level |
| `SPIKE_HOLD_SECONDS` | `120` | Spike hold duration |
| `RANDOM_SEED` | `20261030` | Repeatable scenario/survey selection seed |
| `SCENARIO_CATALOG` | `scenarios.json` | Catalog path or uploaded basename |
| `BOOTSTRAP_MODE` | `curated` | `curated`, `live`, or `live-with-curated-fallback` |
| `REQUEST_TIMEOUT` | `120` | Per-request seconds |
| `THINK_TIME_MIN/MAX` | `1` / `3` | Wait range between user tasks |
| `TIER1/2/3_WEIGHT` | `60/30/10` | Relative table workload weights |
| `DISCOVERY_WEIGHT` | `10` | Relative discovery weight |
| `SLO_P95_MS` | `3000` | Documented/local SLO value; Azure YAML gates runs |
| `SLO_ERROR_PERCENT` | `1` | Documented/local SLO value; Azure YAML gates runs |
| `ALLOW_INSECURE_LOCAL` | `false` | Permit HTTP only for loopback hosts |

Edit `failureCriteria.clientMetrics` in `azure-load-test.yaml` to change Azure
gates. The defaults fail above 3000 ms aggregate p95 or above 1% errors. Semantic
HTTP 200 failures count as errors. Stable request names allow request-specific
criteria after an Azure smoke confirms how names appear. Auto-stop protects QA
after 80% errors sustained for 120 seconds; this deliberately permits brief
saturation signals without sustaining catastrophic traffic.

## Azure Load Testing

1. Create or select an Azure Load Testing resource in the same subscription and
   region as the VNet.
2. Provide a separate, nondelegated IPv4 subnet with enough free addresses for
   the configured engines. Grant the load-test operator Network Contributor.
3. Confirm private DNS resolves the target from that subnet.
4. If setting `publicIPDisabled: true`, provide NAT Gateway, Firewall, or another
   outbound path. This key is valid only with `subnetId`.
5. Replace the commented `subnetId` and optional `appComponents` examples with
   ITS-owned resource IDs. Never commit credentials or concrete private IDs.
6. Upload `azure-load-test.yaml`. Azure flattens configuration artifacts by
   basename; the runtime modules deliberately support flat imports. The YAML
   lists every helper, `scenarios.json`, `requirements.txt`, and `locust.conf`.
7. Run a 1-user preflight, then 10-user baseline, stepped 50/100 ramp, and a
   targeted spike. Stop or adjust when the agreed guard is reached.

The Azure Portal can create/update a test from the YAML. For future automation,
use the same file with Azure CLI or the Azure Load Testing Azure DevOps task;
resource provisioning and pipeline definitions are intentionally out of scope.
Secrets required by a future gateway must come from Azure Key Vault or secure
pipeline variables, not this repository.

## Interpreting Results

- Compare p50, p95, and p99; a widening tail often indicates queueing.
- Compare `discovery:*` latency with `table:tier*:*`. Discovery degradation while
  Tier 3 runs indicates head-of-line blocking in the R request process.
- Track requests/second and error rate alongside Container App replicas, CPU,
  memory, and Azure load-engine CPU. A saturated generator is not an API limit.
- Record cold versus warm run order. Repeated surveys can benefit from OS and
  Arrow caches and overstate cold performance.
- Treat 10/50/100 users as probes, never promised production capacity.

## Troubleshooting

- DNS timeout: verify VNet injection, private DNS linkage, subnet route/NSG, and
  the target's private resolution from the load engine.
- HTTP 400/422: refresh survey IDs and common dimensions with
  `BOOTSTRAP_MODE=live`; check ordered slots and poverty-line requirements.
- HTTP 413: reduce download scenario weight or payload survey/dimension size.
- HTTP 5xx or high auto-stop errors: stop the run, inspect application/gateway
  logs, and restart at the last safe user level.
- Low throughput with high engine CPU: increase `engineInstances` before
  attributing the bottleneck to the API.
- No Tier 3 live scenario: the target release lacks 15 surveys sharing all
  required dimensions; update the catalog rather than sending invalid traffic.

## Results Record

```text
Timestamp (UTC):
Operator / approved window:
Target environment and release:
Azure Load Testing resource / engine count:
VNet/subnet mode (no secret resource IDs):
Load profile, users, spawn rate, duration, seed:
Scenario catalog revision and bootstrap outcome:
Tier/discovery weights:
Container App min/max/current replicas:
Cold or warm cache run:
p50 / p95 / p99 by request name:
Requests/sec and error percentage:
First SLO breach / observed breaking point:
Engine CPU and application resource observations:
Notes and follow-up issue:
```

Never run stress traffic against production or shared QA without explicit ITS
authorization.
