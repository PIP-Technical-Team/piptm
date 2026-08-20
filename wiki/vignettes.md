# Vignettes

## Examples

<!-- cg:auto:examples -->

### Basic single-survey computation

```r
library(piptm)
library(data.table)

# Load survey data
dt <- load_survey_microdata("ZAF_2020")

# Compute headcount ratio
headcount <- compute_fgt(
  welfare = dt$welfare,
  weight  = dt$weight,
  povertyline = 2.15,
  alpha   = 0
)

# Compute Gini coefficient
gini <- compute_gini(
  welfare = dt$welfare,
  weight  = dt$weight
)
```

### Multi-survey batch with breakdowns

```r
# Process all surveys with gender and area breakdowns
results <- table_maker(
  measures = c("headcount", "poverty_gap", "gini", "mean"),
  by       = c("gender", "area")
)
```

### Custom Arrow column selection

```r
results <- table_maker(
  measures = c("headcount", "gini"),
  cols     = c("welfare", "weight", "gender", "area")
)
```

<!-- cg:auto:end -->

## Use Cases

<!-- cg:auto:use-cases -->

### Poverty profiling by demographic group

Compute disaggregated poverty indicators to understand which population segments are most affected:

```r
results <- table_maker(
  measures = c("headcount", "poverty_gap", "severity"),
  by       = c("gender", "area", "educat4")
)
```

### Trend analysis across survey years

```r
# Multiple surveys are processed in batch; results include survey metadata
results <- table_maker(measures = "headcount")

# The keyed output can be merged with survey metadata for trend plots
```

### Pipeline integration

The `table_maker()` function is designed to be called from an API service layer, passing structured JSON requests and returning structured data.tables that can be serialized for downstream consumption.

<!-- cg:auto:end -->

---

← [Home](README.md)
