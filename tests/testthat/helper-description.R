# ── Helper for description tests ──────────────────────────────────────────────
# Shared mock table_result builder for description model and renderer tests.

library(data.table)

make_mock_table_result <- function(
  pip_ids = c("COL_2010_ECH_INC_ALL"),
  analysis_var = "welfare",
  measures = c("mean", "gini"),
  poverty_line = NULL,
  by = c("gender", "area"),
  filter_base = NULL,
  ppp = 2021L,
  pop_share_threshold = 0.01,
  excluded = data.table::data.table(pip_id = character(0L), reason = character(0L)),
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

  exec <- list(
    included_surveys = data.table::data.table(
      pip_id = pip_ids,
      country_code = rep("COL", length(pip_ids)),
      surveyid_year = rep(2010L, length(pip_ids)),
      welfare_type = rep("INC", length(pip_ids))
    ),
    excluded_surveys = excluded,
    filters_applied = filter_base,
    measures_computed = measures,
    suppression = list(threshold = pop_share_threshold, n_suppressed_cells = n_suppressed),
    ppp_used = ppp
  )

  prov <- list(
    release = "20260401_TEST",
    package_version = "0.1.0"
  )

  dt <- data.table::data.table(
    pip_id = rep(pip_ids, each = length(measures)),
    measure = rep(measures, length(pip_ids)),
    value = runif(length(pip_ids) * length(measures)),
    population = rep(1000, length(pip_ids) * length(measures))
  )

  list(
    data = dt,
    specification = spec,
    execution = exec,
    provenance = prov,
    warnings = list()
  )
}
