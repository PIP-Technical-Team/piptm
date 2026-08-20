# Table Maker

## Overview

<!-- cg:auto:overview -->

{piptm} is a fast, efficient computation engine that generates poverty, inequality, and welfare statistics from harmonized PIP survey data. It produces cross-tabulated results across dimensions like gender, education, and geography. Built for the PIP platform, it targets researchers, policymakers, and analysts who need reliable, disaggregated socioeconomic indicators.

The package follows a **Manifest-First with Lazy Validation** architecture — on load, it reads all `manifest_*.json` files from `PIPTM_MANIFEST_DIR` into memory. No microdata is loaded at startup; all loading is on-demand.

### Key capabilities

- **Multi-survey batch processing** using `collapse::GRP` for grouped computation
- **Arrow/Parquet I/O** with column pruning for efficient network reads
- **19 measures** across poverty, inequality, and welfare families
- **Deterministic, reproducible** results from identical inputs

<!-- cg:auto:end -->

## Contents

- [API Reference](api-reference.md)
- [Vignettes](vignettes.md)
- [Changelog](changelog.md)

## Installation

<!-- cg:auto:installation -->

### Prerequisites

- R >= 4.1
- Arrow C++ library (via `arrow` R package)
- `collapse` >= 2.0

### Install from source

```r
# Install from the internal PIP repository
install.packages("piptm")
```

### Development version

```r
# Install the latest development version
remotes::install_github("worldbank/piptm")
```

### Environment setup

Set the manifest directory environment variable:

```r
Sys.setenv(PIPTM_MANIFEST_DIR = "/path/to/manifests")
```

<!-- cg:auto:end -->

## Quick Start

<!-- cg:auto:quick-start -->

Load the package and run a basic multi-survey computation:

```r
library(piptm)

# Compute headcount and Gini for all surveys with default breakdowns
results <- table_maker(
  measures = c("headcount", "gini"),
  by       = c("gender", "area")
)

# Compute a single measure with a custom poverty line
results <- compute_fgt(
  welfare    = survey_data$welfare,
  weight     = survey_data$weight,
  povertyline = 2.15
)
```

See the [Vignettes](vignettes.md) page for detailed examples and [API Reference](api-reference.md) for full function documentation.

<!-- cg:auto:end -->

---

← [Home](README.md)
