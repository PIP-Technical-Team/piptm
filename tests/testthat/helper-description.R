# ── Helper for description tests ──────────────────────────────────────────────
# Shared mock table_result builder for description model and renderer tests.
# Also provides fixture helpers for end-to-end tests that need real Arrow data.

library(data.table)

# ── Shared fixture helpers ───────────────────────────────────────────────────

reset_piptm_env_meta <- function() {
  env <- getNamespace("piptm")$.piptm_env
  env$arrow_root <- NULL
  env$manifest_dir <- NULL
  env$manifests <- list()
  env$current_release <- NULL
}

make_tm_fixtures_meta <- function(env = parent.frame()) {
  tmp_arrow <- withr::local_tempdir(.local_envir = env)
  tmp_manifest <- withr::local_tempdir(.local_envir = env)

  write_fixture_parquet_tm(
    arrow_root = tmp_arrow,
    country_code = "COL",
    year = 2010L,
    welfare_type = "INC",
    version = "v01_v01",
    pip_id = "COL_2010_ECH_INC_ALL",
    extra_cols = c("gender", "area")
  )

  write_fixture_parquet_tm(
    arrow_root = tmp_arrow,
    country_code = "BOL",
    year = 2000L,
    welfare_type = "INC",
    version = "v01_v01",
    pip_id = "BOL_2000_ECH_INC_ALL"
  )

  entries <- list(
    list(
      pip_id = "COL_2010_ECH_INC_ALL", survey_id = "S1", country_code = "COL",
      year = 2010L, welfare_type = "INC", version = "v01_v01",
      survey_acronym = "ECH", module = "ALL",
      dimensions = list("gender", "area"),
      welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L
    ),
    list(
      pip_id = "BOL_2000_ECH_INC_ALL", survey_id = "S2", country_code = "BOL",
      year = 2000L, welfare_type = "INC", version = "v01_v01",
      survey_acronym = "ECH", module = "ALL",
      dimensions = list(),
      welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L
    )
  )

  write_fixture_manifest_tm(tmp_manifest, "20260401_TEST", entries)

  list(tmp_arrow = tmp_arrow, tmp_manifest = tmp_manifest)
}

activate_tm_fixtures_meta <- function(fx) {
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
}

make_mock_table_result <- function(
  pip_ids = c("COL_2010_ECH_INC_ALL"),
  analysis_var = "welfare",
  measures = c("mean", "gini"),
  poverty_line = NULL,
  by = c("gender", "area"),
  filter_base = NULL,
  ppp = 2021L,
  pop_share_threshold = 0.01,
  excluded = data.table::data.table(
    pip_id = character(0L), 
    reason = character(0L),
    stage = character(0L)  # NEW: stage column
  ),
  n_suppressed = 0L
) {
  spec <- list(
    pip_id = pip_ids,
    analysis_var = list(name = analysis_var, label = switch(analysis_var,
      welfare = "Welfare", pov_status = "Poverty status", analysis_var)),
    measures = lapply(measures, function(m) list(
      name = m,
      label = switch(m,
        mean = "Mean welfare", gini = "Gini index",
        headcount = "Poverty headcount", poverty_gap = "Poverty gap index",
        pop_share = "Population share", m),
      family = switch(m,
        mean = , median = , sd = , var = , min = , max = , nobs = , p10 = , p25 = , p75 = , p90 = , sum = "summary_stats",
        gini = , mld = "inequality",
        headcount = , poverty_gap = , severity = , watts = , pop_poverty = "poverty",
        pop_share = , target_within_group_share = , target_survey_share = "shares",
        "unknown")
    )),
    poverty_line = poverty_line,
    ppp = ppp,
    by = if (!is.null(by)) lapply(by, function(d) list(
      name = d,
      label = switch(d, gender = "Gender", area = "Area", educat4 = "Education level (4 groups)", age_group = "Age group", d),
      categories = switch(d,
        gender = list(list(code = "0", label = "Female"), list(code = "1", label = "Male")),
        area = list(list(code = "0", label = "Urban"), list(code = "1", label = "Rural")),
        NULL)
    )) else NULL,
    filter_base = if (!is.null(filter_base)) lapply(names(filter_base), function(v) {
      list(varname = v, label = v, kept = as.list(as.character(filter_base[[v]])))
    }) else NULL,
    pop_share_threshold = pop_share_threshold
  )

  # NEW SCHEMA: execution block
  exec <- list(
    requested_pip_id  = pip_ids,                    # NEW
    loaded_surveys    = data.table::data.table(     # RENAMED from included_surveys
      pip_id = pip_ids,
      country_code = rep("COL", length(pip_ids)),
      surveyid_year = rep(2010L, length(pip_ids)),
      welfare_type = rep("INC", length(pip_ids))
    ),
    excluded_surveys  = excluded,                   # now has stage column
    resolved_release  = "20260401_TEST",            # NEW
    resolved_ppp      = ppp,                        # NEW
    ppp_column_used   = "welfare_ppp_2021_01_02",  # NEW
    filters_applied   = filter_base,                # unchanged
    measures_computed = measures,                   # unchanged
    suppression       = list(threshold = pop_share_threshold, n_suppressed_cells = n_suppressed)
  )

  prov <- list(
    release = "20260401_TEST",
    package_version = "0.1.0"
  )

  # Use deterministic values instead of runif()
  dt <- data.table::data.table(
    pip_id = rep(pip_ids, each = length(measures)),
    measure = rep(measures, length(pip_ids)),
    value = seq(10.5, by = 0.5, length.out = length(pip_ids) * length(measures)),  # deterministic
    population = rep(1000L, length(pip_ids) * length(measures))
  )

  list(
    data = dt,
    specification = spec,
    execution = exec,
    provenance = prov,
    warnings = list()
  )
}
