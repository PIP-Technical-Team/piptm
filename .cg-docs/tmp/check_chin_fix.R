devtools::install(pkg = "c:/WBG/Packages/piptm", quiet = TRUE, upgrade = "never")

result <- callr::r(function() {
  pip_ids <- c("COL_2008_GEIH_INC_ALL", "ARG_2018_EPHC-S2_INC_ALL")
  piptm:::table_maker(
    analysis_var = "welfare",
    pip_id = pip_ids,
    measures = "mean",
    by = c("gender", "area"),
    ppp = 2021,
    release = "20260401_TEST"
  )
})

str(result)
