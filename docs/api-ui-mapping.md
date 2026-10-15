# Table Maker API — UI mapping

## `GET /health`

- **What it does:** Confirms the API is running. Returns current release.
- **UI:** Startup check. Not a wizard step.
- **Depends on:** Nothing.

## `GET /releases`

- **What it does:** Lists available releases and flags the current one.
- **UI:** Startup. Use `current` as `release` on later calls.
- **Depends on:** Nothing.

## `GET /countries`

- **What it does:** Lists countries in the release (`country_code`, `country_name`).
- **UI:** Step 1 — country search box.
- **Depends on:** Optional `release` from `GET /releases`.

## `GET /regions`

- **What it does:** Lists regions and their member countries.
- **UI:** Step 1 — Region filter chip.
- **Depends on:** Optional `release` from `GET /releases`.

## `GET /surveys-ui`

- **What it does:** Returns every survey (`pip_id`, country, year, welfare type, available variables).
- **UI:** Step 1 — survey grid. User selects `pip_id` values here (max 15).
- **Depends on:** Optional `release` from `GET /releases`.

## `POST /session/surveys`

- **What it does:** Saves the selected `pip_id` list. Returns a `session_id`.
- **UI:** Step 1 — persist selection so a refresh on Step 2 does not lose it.
- **Depends on:** `pip_id` from `GET /surveys-ui`.

## `GET /session/<id>/surveys`

- **What it does:** Returns the saved `pip_id` list for a session.
- **UI:** Step 1 — restore selection after reload or crash.
- **Depends on:** `session_id` from `POST /session/surveys`.

## `GET /categories`

- **What it does:** Returns filter variables and their subcategories.
- **UI:** Step 2, Decision 1 — sample-base filters.
- **Depends on:** Selected `pip_id` from Step 1 (pass them so only variables present in all selected surveys are returned).

## `GET /analysis-variables`

- **What it does:** Returns analysis variables (`varname`, `label`, `type`, `stat_groups`).
- **UI:** Step 2, Decision 2 — analysis-variable dropdown.
- **Depends on:** Optional `release` from `GET /releases`.
- **How it gates statistics:** Each variable has `stat_groups`. After the user picks a variable, only those groups are valid. Match them to `group` on `GET /statistics`. Do not offer measures from other groups. From the registry: `welfare` has `summary_statistics` and `inequality`; `pov_status` has `poverty`.

## `GET /statistics`

- **What it does:** Returns all statistic groups and their measure names (`group`, `group_label`, `measures`).
- **UI:** Step 2, Decision 2 — grouped statistic dropdown (max 4).
- **Depends on:** The chosen analysis variable from `GET /analysis-variables`. Optional `release`.
- **How to use it:** This endpoint returns every group. Filter it in the UI: keep only groups whose `group` is in the chosen variable’s `stat_groups`. From the measure spec: `pov_status` → `poverty` (`headcount`, `poverty_gap`, `severity`, `watts`, `pop_poverty`). `welfare` → `summary_statistics` (`mean`, `median`, `sd`, `var`, `min`, `max`, `nobs`, `p10`, `p25`, `p75`, `p90`, `sum`) and `inequality` (`gini`, `mld`). Send the selected `measure` keys to `/table`.

## `GET /covariates`

- **What it does:** Returns variables that can slice the table (`varname`, `label`, `n_categories`).
- **UI:** Step 2, Decision 3 — four layout slots (Columns, Rows, Super Columns, Super Rows). These become `by` on `/table`.
- **Depends on:** Selected `pip_id` from Step 1 (pass them so only covariates present in all selected surveys are returned).

## `GET /table` or `POST /table`

- **What it does:** Computes the table.
- **UI:** Step 2 footer — "Generate table." Step 3 — results.
- **Depends on:**
  - `pip_id` from Step 1 (required, max 15)
  - `analysis_var` from `GET /analysis-variables` (required)
  - `measures` from `GET /statistics` (required)
  - `by` from `GET /covariates` (optional, max 4)
  - `filter_base` from `GET /categories` (optional)
  - `poverty_line` when `analysis_var` is `pov_status` or `by` includes `pov_status`
