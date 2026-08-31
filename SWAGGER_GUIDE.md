# How to Test POST /description in Swagger UI

## Start the API

In R:
```r
library(piptm)
pr <- plumber::plumb(system.file("plumber", "plumber.R", package = "piptm"))
pr$run(port = 8080)
```

## Open Swagger UI

Navigate to: http://localhost:8080/__docs__/

## Find POST /description

Scroll down to the `POST /description` endpoint

## What You'll See Now

✅ **A JSON request body editor** (not a query parameter!)

The editor will show a placeholder like:
```json
{
  "body": "object"
}
```

## How to Use It

### Option 1: Fallback Path (Simple Example)

Replace the placeholder with:
```json
{
  "pip_id": ["TEST_2020_SURVEY_CON_ALL"],
  "analysis_var": "welfare",
  "measures": ["mean", "gini"],
  "ppp": 2021
}
```

Click **Execute** → You should get a Markdown description back!

### Option 2: Fast Path (With Metadata)

If you have pre-computed metadata:
```json
{
  "description_metadata": {
    "params": {
      "pip_id": ["TEST_2020_SURVEY_CON_ALL"],
      "analysis_var": "welfare",
      "measures": ["mean", "gini"],
      "ppp": 2021,
      "by": "gender"
    },
    "provenance": {
      "release": "TEST_2024",
      "ppp_year": 2021
    },
    "surveys": {
      "loaded": [...],
      "summary": [...]
    },
    "resolved_labels": {...}
  }
}
```

## What Changed

**Before the fix:**
- Swagger showed: `POST /description?req=<string>` ❌
- You would see a text box for "req" parameter
- It didn't work!

**After the fix:**
- Swagger shows: `POST /description` with JSON body editor ✅
- You paste JSON directly into the body
- It works!

## Key Points

1. **The JSON goes in the "body" field** - this is the request body
2. **Don't worry about the placeholder** - just replace the entire content
3. **The endpoint accepts TWO formats**:
   - Fast path: wrap everything in `{"description_metadata": {...}}`
   - Fallback path: put table parameters directly `{"pip_id": [...], ...}`
