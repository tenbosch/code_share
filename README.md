# Databricks Sample Notebooks

Two sample Databricks notebooks demonstrating geospatial and AI-powered data processing capabilities.

---

## Geolocation Provider Search Demo

**File:** `Geolocation Provider Search Demo.ipynb`

Demonstrates Databricks native geospatial SQL functions to find healthcare providers within a configurable search radius of customer locations.

### What it does

1. Creates sample customer and provider tables with realistic Colorado coordinates across 10 specialties (Primary Care, Dental, Vision, Orthopedics, Urgent Care, Physical Therapy, Pediatrics, Cardiology, Dermatology, Mental Health)
2. Validates geocoding coverage across both datasets
3. Performs proximity search using spatial SQL — finds providers within a specified radius of a customer, filtered by specialty
4. Returns results sorted by distance in kilometers

### Key spatial functions

| Function | Purpose |
|----------|---------|
| `ST_Point(lon, lat)` | Creates a geometry point from longitude/latitude coordinates |
| `ST_DistanceSpheroid(geom1, geom2)` | Calculates geodesic distance in meters using the WGS84 ellipsoid |
| `ST_DWithin(geom1, geom2, distance)` | Optimized proximity join using spatial indexing (DBR 18.1+) |

### Requirements

- Databricks Runtime with geospatial SQL support
- `dbutils.widgets` for catalog/schema parameterization

---

## USPS Address Standardization Demo

**File:** `usps_address_standardization_demo.ipynb`

Demonstrates how to use Databricks `ai_query` with Foundation Model APIs to standardize, validate, and correct US mailing addresses according to [USPS Publication 28](https://pe.usps.com/text/pub28/welcome.htm) rules.

### What it does

1. Generates 30 synthetic addresses covering a range of quality issues: clean addresses, abbreviation problems, typos, missing components, wrong ZIP codes, formatting chaos, directional errors, and non-standard unit designators
2. Calls `databricks-meta-llama-3-3-70b-instruct` via `ai_query` with a detailed USPS standardization prompt — returns structured JSON with corrected address fields, confidence rating, list of changes made, and warnings
3. Parses the JSON response into structured columns for side-by-side comparison
4. Produces a quality analysis dashboard: confidence distribution, change rate, and addresses flagged for human review
5. Shows a production pre-filtering pattern to reduce API calls by only sending addresses that appear to need correction
6. Demonstrates `ai_extract` for parsing single-line addresses into components
7. Shows a combined `ai_extract` → `ai_query` pipeline for messy unstructured input

### AI function comparison

| Approach | Best for |
|----------|---------|
| `ai_query` | Full standardization, typo correction, missing component inference |
| `ai_extract` | Parsing single-line addresses into components |
| `ai_classify` | Quality scoring / categorical classification only |

### Requirements

- Databricks Runtime with AI Functions enabled
- Access to the `databricks-meta-llama-3-3-70b-instruct` Foundation Model endpoint
