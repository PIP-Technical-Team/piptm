# Dev tool: renders an HTML overview of the loaded variable registry.
# Usage (from package root): Rscript dev/registry_overview.R [release]
# Rebuild first with build_variable_registry(<release>) to see updated labels.
suppressMessages(devtools::load_all(".", quiet = TRUE))
args <- commandArgs(trailingOnly = TRUE)
rel <- if (length(args)) args[1] else NULL
r <- piptm_variable_registry(rel)
s <- function(x) if (is.null(x) || length(x) == 0L) "" else paste(unlist(x), collapse = ", ")
esc <- function(x) { x <- gsub("&", "&amp;", x, fixed = TRUE); x <- gsub("<", "&lt;", x, fixed = TRUE); gsub(">", "&gt;", x, fixed = TRUE) }

vars <- Filter(function(e) is.list(e) && !is.null(e$varname), r)

# Summary table
sumrows <- vapply(vars, function(e) sprintf(
  "<tr><td><a href='#%s'><code>%s</code></a></td><td>%s</td><td><span class='t t-%s'>%s</span></td><td>%s</td><td>%s</td><td class='n'>%s</td></tr>",
  s(e$varname), s(e$varname), esc(s(e$ui_label)), s(e$tm_type), s(e$tm_type),
  esc(s(e$roles)), esc(s(e$stat_groups)), s(e$n_categories)), "")

# Per-variable category tables
detail <- vapply(vars, function(e) {
  cats <- e$categories
  body <- if (length(cats)) paste(vapply(cats, function(k)
    sprintf("<tr><td><code>%s</code></td><td>%s</td></tr>", esc(s(k$code)), esc(s(k$label))), ""), collapse = "")
  else "<tr><td colspan=2 class='none'>no categories</td></tr>"
  sprintf("<div class='card' id='%s'><h3><code>%s</code> &mdash; %s <span class='t t-%s'>%s</span></h3><div class='meta'>roles: %s | stat groups: %s</div><table class='small'><tr><th>Code</th><th>Label</th></tr>%s</table></div>",
          s(e$varname), s(e$varname), esc(s(e$ui_label)), s(e$tm_type), s(e$tm_type),
          esc(s(e$roles)), esc(s(e$stat_groups)), body)
}, "")

# Measure spec
ms <- r$measure_spec
mrows <- ""
if (length(ms)) {
  mrows <- paste(unlist(lapply(names(ms), function(g) {
    grp <- ms[[g]]
    ms_items <- grp$measures
    if (is.null(ms_items)) ms_items <- list()
    its <- vapply(names(ms_items), function(m) sprintf("<tr><td>%s</td><td><code>%s</code></td><td>%s</td></tr>",
      esc(s(grp$label)), m, esc(s(ms_items[[m]]$label))), "")
    if (!length(its)) its <- sprintf("<tr><td>%s</td><td colspan=2><code>%s</code></td></tr>", esc(s(grp$label)), g)
    its
  })), collapse = "")
}

html <- paste0("<!doctype html><meta charset='utf-8'><title>piptm registry</title><style>
body{font-family:Segoe UI,Arial,sans-serif;margin:24px;color:#222;background:#f6f7f9}
h1{margin-bottom:4px}table{border-collapse:collapse;background:#fff;margin:8px 0 24px}
th,td{border:1px solid #d5d9df;padding:5px 10px;text-align:left;font-size:13px}th{background:#0b4f8a;color:#fff}
tr:nth-child(even) td{background:#f3f6fa}.n{text-align:right}.small{min-width:360px}
.card{background:#fff;border:1px solid #d5d9df;border-radius:6px;padding:8px 14px;margin:10px 0;display:inline-block;vertical-align:top;margin-right:12px;min-width:380px}
.card table{margin:6px 0}.meta{font-size:12px;color:#666}.none{color:#999;font-style:italic}
.t{font-size:11px;padding:2px 7px;border-radius:9px;color:#fff;background:#777}
.t-categorical{background:#2a9d8f}.t-binary{background:#e9a23b}.t-continuous{background:#4c78a8}.t-welfare{background:#8e5ea2}.t-poverty{background:#d1495b}
</style><h1>Variable registry</h1><div class='meta'>Release: ", esc(if (is.null(rel)) piptm_current_release() else rel), " &mdash; ", length(vars), " variables</div>
<h2>Overview</h2><table><tr><th>Variable</th><th>UI label</th><th>Type</th><th>Roles</th><th>Stat groups</th><th>#Cats</th></tr>",
paste(sumrows, collapse = ""), "</table><h2>Categories by variable</h2>", paste(detail, collapse = ""),
"<h2>Measures</h2><table><tr><th>Group</th><th>Measure key</th><th>Label</th></tr>", mrows, "</table>")
out <- file.path(Sys.getenv("TEMP"), "piptm_registry_overview.html")
writeLines(html, out, useBytes = TRUE)
cat(out, "\n")
