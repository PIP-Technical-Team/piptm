# Table Maker ({piptm})

<!-- cg:auto:overview -->
**Table Maker** ({piptm}) is a fast, efficient computation engine that generates
poverty, inequality, and welfare statistics from harmonized PIP survey data,
producing cross-tabulated results across dimensions like gender, education,
and geography. It is built for the PIP platform and is intended for
researchers, policymakers, and analysts who need reliable, disaggregated
socioeconomic indicators.

The package follows a **Manifest-First with Lazy Validation** architecture:
on load, it reads all `manifest_*.json` files from `PIPTM_MANIFEST_DIR` into
memory (keyed by release ID), and no microdata is loaded until a
`table_maker()` call requests it.

Key constraints: reproducibility (identical inputs always produce identical
results), strict adherence to PIP/World Bank methodological standards, and
scalability across large, partitioned Arrow/Parquet datasets.
<!-- cg:auto:end -->

## Contents
- [API Reference](api-reference.md)
- [Vignettes](vignettes.md)
- [Changelog](changelog.md)

<!-- cg:auto:installation -->
## Installation

`{piptm}` is an R package. Install it from source (or your internal package
repository) alongside its companion data-preparation package `{pipdata}`:

```r
# From a local checkout
devtools::install()

# Or, once published internally
# install.packages("piptm", repos = "<internal-repo-url>")
```

`{piptm}` expects harmonized survey data produced by `{pipdata}` (clean
`.qs2` survey datasets, Arrow/Parquet partitions, and a release manifest) to
be available under the directory pointed to by the `PIPTM_MANIFEST_DIR`
environment variable.
<!-- cg:auto:end -->

<!-- cg:auto:quick-start -->
## Quick Start

```r
library(piptm)

# Compute cross-tabulated poverty/inequality/welfare measures
result <- table_maker(
  survey_id = "<survey-identifier>",
  measures  = c("fgt", "gini", "mean_welfare"),
  by        = c("gender", "education")
)

result
```

See [Vignettes](vignettes.md) for worked examples and
[API Reference](api-reference.md) for the full function surface.
<!-- cg:auto:end -->
