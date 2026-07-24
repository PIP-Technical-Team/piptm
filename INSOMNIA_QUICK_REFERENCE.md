# Insomnia Quick Reference - piptm API

## Import Instructions

### Recommended: Import Collection
```
1. Insomnia → Create → Import From → File
2. Select: Insomnia_collection.json
3. Done!
```

### Alternative: Import OpenAPI
```
1. Insomnia → Create → Import From → File
2. Select: piptm-api-clean.yaml
3. Generate requests
```

## Environment Variables

Edit these in Insomnia (click environment dropdown):

| Variable | Default Value | Purpose |
|----------|---------------|---------|
| `base_url` | `http://127.0.0.1:8080` | API server address |
| `release` | `20260401_TEST` | Data release version |
| `pip_id` | `IDN_2023_SUSENAS_CON_ALL` | Default survey ID |

Use in requests: `{{ _.base_url }}`

## Request Folders

### 1️⃣ Health & Info
- **Start here!** Check if API is running
- `/health` - API status
- `/releases` - Available releases

### 2️⃣ Catalogue (9 endpoints)
- **Get metadata** before querying
- `/surveys` ⭐ - **GET pip_id VALUES HERE**
- `/countries`, `/regions`, `/dimensions`, etc.

### 3️⃣ Query — Happy Paths
- **Basic successful queries**
- `GET /lookup` - Find pip_id by country/year/type
- `GET /table` - Basic welfare query
- `POST /table` - Same with JSON body

### 4️⃣ Query — Parameter Variants
- **Different ways to use /table**
- Poverty status (requires poverty_line)
- By gender (disaggregation)
- With filter (sample filtering)
- No suppression

### 5️⃣ Error Cases
- **Test error handling** (expect 400)
- Missing required params
- Invalid values
- Nonexistent IDs

## Common Query Patterns

### Pattern 1: Basic Welfare Stats
```
GET /table
  ?analysis_var=welfare
  &pip_id=IDN_2023_SUSENAS_CON_ALL
  &measures=mean
  &ppp=2021
  &pop_share_threshold=0.01
```

### Pattern 2: Poverty Analysis
```
GET /table
  ?analysis_var=pov_status
  &pip_id=IDN_2023_SUSENAS_CON_ALL
  &measures=headcount
  &poverty_line=2.15
  &ppp=2021
```

### Pattern 3: Disaggregated
```
GET /table
  ?analysis_var=welfare
  &pip_id=IDN_2023_SUSENAS_CON_ALL
  &measures=mean
  &by=gender
  &ppp=2021
```

### Pattern 4: Filtered Sample
```
GET /table
  ?analysis_var=welfare
  &pip_id=IDN_2023_SUSENAS_CON_ALL
  &measures=mean
  &filter_base={"gender":[1]}
  &ppp=2021
```

## Parameter Reference

### Required (all /table queries)
- `analysis_var`: `welfare` or `pov_status`
- `pip_id`: Survey ID from `/surveys`
- `measures`: `mean`, `gini`, `headcount`, etc.

### Conditional
- `poverty_line`: **Required** if `analysis_var=pov_status`

### Optional
- `ppp`: PPP year (e.g., 2021)
- `pop_share_threshold`: Suppression (0.01 = 1%)
- `by`: Disaggregation dimension
- `filter_base`: JSON filter object
- `release`: Override default release

## Workflow

```
Step 1: GET /health
        ↓
Step 2: GET /surveys (grab pip_id)
        ↓
Step 3: GET /table (use pip_id)
        ↓
Step 4: Experiment with variants
```

## Common Issues

| Problem | Solution |
|---------|----------|
| Connection refused | Check API is running on port 8080 |
| 400 on valid request | Verify pip_id exists in `/surveys` |
| Variables not working | Check environment is selected |
| Original YAML fails | Use `piptm-api-clean.yaml` instead |

## Tips

✅ **Always start with `/health`**  
✅ **Get pip_ids from `/surveys` first**  
✅ **Use environment variables for common values**  
✅ **Check "Error Cases" folder to understand validation**  
✅ **Copy and modify working requests**  

## Request Naming Convention

- `GET /endpoint — description` - Successful cases
- `ERR — description → 400` - Expected errors
- Variants use `—` (em dash) in name

## File Reference

| File | Purpose | When to Use |
|------|---------|-------------|
| `Insomnia_collection.json` | Native collection | **Import this first** |
| `piptm-api-clean.yaml` | OpenAPI spec | For tools/codegen |
| `piptm-api.yaml` | Original spec | **Don't use** (has issues) |
| `API_TESTING_README.md` | Full guide | Detailed instructions |
| `API_ISSUES_AND_FIXES.md` | Technical details | Understanding fixes |

## Keyboard Shortcuts (Insomnia)

- `Ctrl+Enter` / `Cmd+Enter` - Send request
- `Ctrl+E` / `Cmd+E` - Edit environment
- `Ctrl+F` / `Cmd+F` - Search requests
- `Ctrl+K` / `Cmd+K` - Quick switcher

## Support

If requests aren't working as expected:
1. Check API logs
2. Verify endpoint in browser (for GET requests)
3. Compare with working examples
4. Check parameter spelling and values

---

**Quick Start:** Import `Insomnia_collection.json` → Run `GET /health` → Run `GET /surveys` → Start testing!
