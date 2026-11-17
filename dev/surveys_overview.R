# Dev tool: Interactive PIP-styled HTML overview of the Table Maker surveys catalogue.
# Usage (from package root): Rscript dev/surveys_overview.R [release]
#
# Content comes from piptm_manifest() / piptm_surveys_ui(), matching the
# GET /surveys-ui, GET /countries, and GET /regions endpoints.
suppressMessages(devtools::load_all(".", quiet = TRUE))
args <- commandArgs(trailingOnly = TRUE)
rel <- if (length(args) && nzchar(args[1])) args[1] else NULL

shown_release <- if (is.null(rel)) piptm_current_release() else rel
mf <- piptm_manifest(shown_release)

esc <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- gsub("&", "&amp;", x, fixed = TRUE)
  x <- gsub("<", "&lt;", x, fixed = TRUE)
  x <- gsub(">", "&gt;", x, fixed = TRUE)
  x <- gsub("'", "&#39;", x, fixed = TRUE)
  gsub('"', "&quot;", x, fixed = TRUE)
}

n_surveys   <- nrow(mf)
n_countries <- length(unique(mf$country_code[!is.na(mf$country_code)]))
n_regions   <- length(unique(mf$region_code[!is.na(mf$region_code) & nzchar(mf$region_code)]))
yr_min      <- if (n_surveys > 0L) min(mf$year, na.rm = TRUE) else NA_integer_
yr_max      <- if (n_surveys > 0L) max(mf$year, na.rm = TRUE) else NA_integer_
n_inc       <- sum(toupper(mf$welfare_type) == "INC", na.rm = TRUE)
n_con       <- sum(toupper(mf$welfare_type) == "CON", na.rm = TRUE)

# Order by country_name, year desc, welfare_type
if (n_surveys > 0L) {
  ord <- order(mf$country_name, -mf$year, mf$welfare_type, na.last = TRUE)
  mf <- mf[ord]
}

regions_dt <- unique(mf[!is.na(region_code) & nzchar(region_code), .(region_code, region_name)])
if (nrow(regions_dt) > 0L) data.table::setorder(regions_dt, region_code)

region_options <- paste(
  sprintf("<option value='%s'>%s &mdash; %s</option>",
          esc(regions_dt$region_code), esc(regions_dt$region_code), esc(regions_dt$region_name)),
  collapse = ""
)

rows_html <- paste(vapply(seq_len(n_surveys), function(i) {
  wt <- toupper(as.character(mf$welfare_type[i]))
  wt_badge <- if (identical(wt, "INC")) {
    "<span class='t t-inc'>Income (INC)</span>"
  } else if (identical(wt, "CON")) {
    "<span class='t t-con'>Consumption (CON)</span>"
  } else {
    sprintf("<span class='t'>%s</span>", esc(wt))
  }
  dims <- mf$dimensions[[i]]
  dims_chips <- if (length(dims) > 0L) {
    paste(sprintf("<span class='chip'>%s</span>", esc(dims)), collapse = " ")
  } else {
    "<span class='none'>none</span>"
  }
  n_obs_val <- mf$n_obs[i]
  n_obs_str <- if (is.na(n_obs_val)) "&mdash;" else format(n_obs_val, big.mark = ",")

  sprintf(
    paste0(
      "<tr data-region='%s' data-welfare='%s' data-search='%s'>",
      "<td><strong>%s</strong> <span class='meta'>(%s)</span></td>",
      "<td>%s</td>",
      "<td class='n'><b>%s</b></td>",
      "<td>%s</td>",
      "<td><code>%s</code></td>",
      "<td class='n'>%s</td>",
      "<td><code>%s</code></td>",
      "<td>%s</td>",
      "</tr>"
    ),
    esc(mf$region_code[i]),
    esc(wt),
    esc(tolower(paste(mf$country_name[i], mf$country_code[i], mf$pip_id[i], mf$survey_acronym[i], mf$year[i]))),
    esc(mf$country_name[i]), esc(mf$country_code[i]),
    esc(mf$region_code[i]),
    esc(mf$year[i]),
    wt_badge,
    esc(mf$survey_acronym[i]),
    n_obs_str,
    esc(mf$pip_id[i]),
    dims_chips
  )
}, character(1L)), collapse = "\n")

html <- paste0(
  "<!doctype html><meta charset='utf-8'><title>Table Maker Surveys Catalogue</title><style>
:root {
  --default-font: \"Open Sans\", system-ui, -apple-system, \"Segoe UI\", Roboto, \"Helvetica Neue\", Arial, sans-serif;
  --background-color: #F7F8F8;
  --default-color: #1a1f24;
  --heading-color: #044368;
  --muted-color: #485664;
  --accent-color: #0ea2bd;
  --primary-blue: #00689D;
  --surface-color: #ffffff;
  --tint-color: #e8f2f8;
  --border-color: #dee2e6;
}
body {
  font-family: var(--default-font);
  margin: 0;
  padding: 20px 16px;
  color: var(--default-color);
  background: var(--background-color);
  line-height: 1.5;
}
.pip-catalogue-card {
  max-width: 1200px;
  margin: 0 auto;
  background: var(--surface-color);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  padding: 24px 28px;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.08);
}
.pip-catalogue-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 12px;
  margin: 0 0 14px 0;
  padding: 0 0 12px 0;
  border-bottom: 2px solid var(--primary-blue);
}
h1 {
  margin: 0;
  font-size: 22px;
  font-weight: 700;
  color: var(--heading-color);
  letter-spacing: -0.01em;
}
.kpi-strip {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 10px;
  margin: 14px 0 18px 0;
}
.kpi-box {
  background: var(--background-color);
  border: 1px solid var(--border-color);
  border-left: 4px solid var(--accent-color);
  border-radius: 6px;
  padding: 8px 12px;
}
.kpi-val {
  font-size: 20px;
  font-weight: 700;
  color: var(--heading-color);
  line-height: 1.2;
}
.kpi-lbl {
  font-size: 11px;
  color: var(--muted-color);
  text-transform: uppercase;
  letter-spacing: 0.04em;
  font-weight: 600;
}
.toolbar {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  align-items: center;
  background: var(--tint-color);
  border: 1px solid #c5ddec;
  border-radius: 6px;
  padding: 10px 14px;
  margin-bottom: 14px;
}
.toolbar input, .toolbar select {
  font-family: var(--default-font);
  font-size: 13px;
  padding: 6px 10px;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  background: #fff;
  color: var(--default-color);
}
.toolbar input {
  min-width: 240px;
  flex: 1;
}
.toolbar .count-badge {
  font-size: 12px;
  font-weight: 600;
  color: var(--heading-color);
  margin-left: auto;
}
table {
  border-collapse: collapse;
  background: var(--surface-color);
  width: 100%;
  font-size: 13px;
}
th, td {
  border: 1px solid var(--border-color);
  padding: 7px 10px;
  text-align: left;
  vertical-align: top;
}
th {
  background: var(--tint-color);
  color: var(--heading-color);
  border-bottom: 2px solid var(--primary-blue);
  font-weight: 600;
  position: sticky;
  top: 0;
  z-index: 1;
}
tr:nth-child(even) td {
  background: var(--background-color);
}
tr:hover td {
  background: var(--tint-color);
}
.n {
  text-align: right;
  white-space: nowrap;
}
.meta {
  font-size: 12px;
  color: var(--muted-color);
  font-weight: 400;
}
.none {
  color: #8b95a1;
  font-style: italic;
}
code {
  background: var(--tint-color);
  color: var(--heading-color);
  padding: 1px 5px;
  border-radius: 4px;
  font-size: 11.5px;
}
.chip {
  display: inline-block;
  font-size: 10.5px;
  font-weight: 600;
  padding: 1px 6px;
  margin: 1px 2px 1px 0;
  border-radius: 9px;
  background: var(--tint-color);
  color: var(--heading-color);
  border: 1px solid #b8d8e8;
}
.t {
  display: inline-block;
  font-size: 11px;
  font-weight: 600;
  padding: 2px 8px;
  border-radius: 10px;
  color: #fff;
  white-space: nowrap;
}
.t-inc { background: #00689D; }
.t-con { background: #0ea2bd; }
</style>
<div class='pip-catalogue-card'>
  <div class='pip-catalogue-header'>
    <h1>Table Maker Surveys Catalogue</h1>
    <a href='https://pip.worldbank.org/home' target='_blank' rel='noopener noreferrer' style='display:inline-flex;align-items:center;'>
      <img src='https://pip.worldbank.org/assets/WB-PIP-E-horizontal-RGB-high.png' alt='World Bank PIP' style='width:250px;max-width:100%;height:auto;border:0;' />
    </a>
  </div>
  <div class='meta'>Release: <b>", esc(shown_release), "</b> &mdash; Sourced live from <code>piptm_manifest()</code> / <code>GET /surveys-ui</code>, <code>GET /countries</code>, and <code>GET /regions</code>.</div>
  <div class='kpi-strip'>
    <div class='kpi-box'><div class='kpi-val'>", format(n_surveys, big.mark = ","), "</div><div class='kpi-lbl'>Harmonized Surveys</div></div>
    <div class='kpi-box'><div class='kpi-val'>", n_countries, "</div><div class='kpi-lbl'>Countries / Economies</div></div>
    <div class='kpi-box'><div class='kpi-val'>", yr_min, "&ndash;", yr_max, "</div><div class='kpi-lbl'>Survey Year Span</div></div>
    <div class='kpi-box'><div class='kpi-val'>", format(n_con, big.mark = ","), " / ", format(n_inc, big.mark = ","), "</div><div class='kpi-lbl'>Consumption / Income</div></div>
    <div class='kpi-box'><div class='kpi-val'>", n_regions, "</div><div class='kpi-lbl'>WB Regions</div></div>
  </div>
  <div class='toolbar'>
    <input type='search' id='q' placeholder='Search country, ISO3, year, survey acronym, or pip_id...' oninput='filterRows()' />
    <select id='reg' onchange='filterRows()'>
      <option value=''>All Regions</option>", region_options, "
    </select>
    <select id='wt' onchange='filterRows()'>
      <option value=''>All Welfare Types</option>
      <option value='CON'>Consumption (CON)</option>
      <option value='INC'>Income (INC)</option>
    </select>
    <span class='count-badge' id='cnt'>Showing ", format(n_surveys, big.mark = ","), " of ", format(n_surveys, big.mark = ","), " surveys</span>
  </div>
  <table id='surveys-tbl'>
    <thead>
      <tr>
        <th>Country</th>
        <th>Region</th>
        <th class='n'>Year</th>
        <th>Welfare Type</th>
        <th>Survey</th>
        <th class='n'>Obs (N)</th>
        <th>Canonical <code>pip_id</code></th>
        <th>Available Dimensions</th>
      </tr>
    </thead>
    <tbody>",
    rows_html,
    "</tbody>
  </table>
</div>
<script>
function filterRows() {
  var q = document.getElementById('q').value.toLowerCase().trim();
  var reg = document.getElementById('reg').value;
  var wt = document.getElementById('wt').value;
  var rows = document.querySelectorAll('#surveys-tbl tbody tr');
  var shown = 0;
  for (var i = 0; i < rows.length; i++) {
    var r = rows[i];
    var ok = true;
    if (reg && r.getAttribute('data-region') !== reg) ok = false;
    if (ok && wt && r.getAttribute('data-welfare') !== wt) ok = false;
    if (ok && q && r.getAttribute('data-search').indexOf(q) === -1) ok = false;
    r.style.display = ok ? '' : 'none';
    if (ok) shown++;
  }
  document.getElementById('cnt').textContent = 'Showing ' + shown.toLocaleString() + ' of ' + rows.length.toLocaleString() + ' surveys';
}
</script>"
)

out <- file.path(Sys.getenv("TEMP"), "piptm_surveys_overview.html")
writeLines(html, out, useBytes = TRUE)
cat(out, "\n")
