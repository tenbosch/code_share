# Databricks notebook source
# DBTITLE 1,Pharmacy Provider Search Demo
# MAGIC %md
# MAGIC # Pharmacy Provider Search Demo
# MAGIC This notebook demonstrates Databricks geospatial capabilities for finding healthcare providers within a search radius of a pharmacy location. It uses `ai_query()` to geocode addresses and built-in spatial SQL functions (`ST_Point`, `ST_DistanceSpheroid`) to perform proximity searches.

# COMMAND ----------

# DBTITLE 1,Summary of Functions Used
# MAGIC %md
# MAGIC ## Summary of Functions Used
# MAGIC | Function | Purpose |
# MAGIC | --- | --- |
# MAGIC | `ai_query(endpoint, prompt)` | Geocode addresses to lat/lon using a foundation model |
# MAGIC | `ST_Point(lon, lat)` | Create a geometry point from longitude/latitude |
# MAGIC | `ST_DistanceSpheroid(geom1, geom2)` | Geodesic distance in meters (WGS84 ellipsoid) |
# MAGIC | **Tip**: For DBR 18.1+, use `ST_DWithin(geom1, geom2, distance)` in the JOIN predicate for optimized spatial index-based proximity joins |

# COMMAND ----------

# DBTITLE 1,Step 1 Header
# MAGIC %md
# MAGIC ## Step 1: Configuration
# MAGIC Set catalog, schema, and model endpoint for geocoding.

# COMMAND ----------

# DBTITLE 1,Widget configuration
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "schema", "Schema")
dbutils.widgets.text("model_endpoint", "databricks-meta-llama-3-3-70b-instruct", "Model Endpoint")

# COMMAND ----------

# DBTITLE 1,Apply configuration
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
model_endpoint = dbutils.widgets.get("model_endpoint")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# DBTITLE 1,Step 2 Header
# MAGIC %md
# MAGIC ## Step 2: Create Sample Data
# MAGIC Create pharmacies and providers with addresses only — no coordinates. Geocoding happens in the next step.

# COMMAND ----------

# DBTITLE 1,Create sample pharmacy data
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE pharmacies AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'High Plains Wellness Pharmacy',           '2001 Blake St, Denver, CO 80205'),
# MAGIC   (2, 'Boulder Family Rx',      '1650 30th St, Boulder, CO 80301'),
# MAGIC   (3, 'Cornerstone Care Pharmacy',         '1502 N Academy Blvd, Colorado Springs, CO 80909'),
# MAGIC   (4, 'Welcome Home Pharmacy',           '2160 W Drake Rd, Fort Collins, CO 80526'),
# MAGIC   (5, 'Highline Trail Pharmacy',           '14000 E Exposition Ave, Aurora, CO 80012')
# MAGIC AS pharmacies(pharmacy_id, pharmacy_name, address);
# MAGIC
# MAGIC SELECT * FROM pharmacies;

# COMMAND ----------

# DBTITLE 1,Create sample provider data
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE providers AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (101, 'Summit Health Clinic',         '1601 E 19th Ave, Denver, CO 80218'),
# MAGIC   (102, 'Rocky Mountain Medical',       '6500 W Alameda Ave, Lakewood, CO 80226'),
# MAGIC   (103, 'Peak Health Center',           '1155 Canyon Blvd, Boulder, CO 80302'),
# MAGIC   (104, 'Front Range Medical Group',    '1551 Professional Ln, Longmont, CO 80501'),
# MAGIC   (105, 'Pikes Peak Health Partners',   '3475 Briargate Blvd, Colorado Springs, CO 80920'),
# MAGIC   (106, 'Mile High Medical Center',     '8101 E Belleview Ave, Centennial, CO 80111'),
# MAGIC   (107, 'Alpine Wellness Clinic',       '17497 W Colfax Ave, Golden, CO 80401'),
# MAGIC   (108, 'Prairie Health Associates',    '1900 16th St, Greeley, CO 80631'),
# MAGIC   (109, 'Flatiron Medical Group',       '11990 Grant St, Broomfield, CO 80020'),
# MAGIC   (110, 'Canyon Creek Health',          '4675 Town Center Dr, Castle Rock, CO 80109'),
# MAGIC   (111, 'Mountain View Medical',        '6900 W 117th Ave, Westminster, CO 80020'),
# MAGIC   (112, 'Columbine Health Center',      '5631 S Curtice St, Littleton, CO 80120'),
# MAGIC   (113, 'Highlands Medical Practice',   '9285 S Broadway, Highlands Ranch, CO 80129'),
# MAGIC   (114, 'Cherry Creek Health Group',    '3300 E 1st Ave, Denver, CO 80206'),
# MAGIC   (115, 'Arvada Family Health',         '5761 Olde Wadsworth Blvd, Arvada, CO 80002'),
# MAGIC   (116, 'Parker Medical Associates',    '18941 Mainstreet, Parker, CO 80134'),
# MAGIC   (117, 'Smile Health Denver',          '1660 Wynkoop St, Denver, CO 80202'),
# MAGIC   (118, 'Boulder Health Arts',          '2800 Arapahoe Ave, Boulder, CO 80303'),
# MAGIC   (119, 'Springs Care Center',          '2550 Tenderfoot Hill St, Colorado Springs, CO 80906'),
# MAGIC   (120, 'Broomfield Family Health',     '1 DesCombes Dr, Broomfield, CO 80020'),
# MAGIC   (121, 'Thornton Medical Group',       '9551 Grant St, Thornton, CO 80229'),
# MAGIC   (122, 'Mile High Health Care',        '1400 N Ogden St, Denver, CO 80218'),
# MAGIC   (123, 'Crystal Clear Health',         '15290 E Iliff Ave, Aurora, CO 80014'),
# MAGIC   (124, 'Mountain Sight Clinic',        '807 Miner St, Idaho Springs, CO 80452'),
# MAGIC   (125, 'Springs Health Center',        '6071 E Woodmen Rd, Colorado Springs, CO 80923')
# MAGIC AS providers(provider_id, provider_name, address);
# MAGIC
# MAGIC SELECT * FROM providers;

# COMMAND ----------

# DBTITLE 1,Step 3 Header
# MAGIC %md
# MAGIC ## Step 3: Geocode Addresses Using ai_query()
# MAGIC Use a foundation model to convert addresses to latitude/longitude. Each unique address is geocoded once.

# COMMAND ----------

# DBTITLE 1,Geocode pharmacies
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE pharmacies_geocoded AS
# MAGIC SELECT
# MAGIC   p.*,
# MAGIC   ai_query(
# MAGIC     '${model_endpoint}',
# MAGIC     CONCAT(
# MAGIC       'Return ONLY the latitude and longitude for this address as two comma-separated numbers with 6 decimal places. ',
# MAGIC       'No other text, no explanation, no labels. Just two numbers separated by a comma. ',
# MAGIC       'Example output: 40.758896,-73.985130\n',
# MAGIC       'Address: ', p.address
# MAGIC     ),
# MAGIC     modelParameters => named_struct('max_tokens', 30, 'temperature', 0.0)
# MAGIC   ) AS lat_lon_raw
# MAGIC FROM pharmacies p;
# MAGIC
# MAGIC SELECT * FROM pharmacies_geocoded;

# COMMAND ----------

# DBTITLE 1,Geocode providers
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE providers_geocoded AS
# MAGIC SELECT
# MAGIC   p.*,
# MAGIC   ai_query(
# MAGIC     '${model_endpoint}',
# MAGIC     CONCAT(
# MAGIC       'Return ONLY the latitude and longitude for this address as two comma-separated numbers with 6 decimal places. ',
# MAGIC       'No other text, no explanation, no labels. Just two numbers separated by a comma. ',
# MAGIC       'Example output: 40.758896,-73.985130\n',
# MAGIC       'Address: ', p.address
# MAGIC     ),
# MAGIC     modelParameters => named_struct('max_tokens', 30, 'temperature', 0.0)
# MAGIC   ) AS lat_lon_raw
# MAGIC FROM providers p;
# MAGIC
# MAGIC SELECT * FROM providers_geocoded;

# COMMAND ----------

# DBTITLE 1,Step 4 Header
# MAGIC %md
# MAGIC ## Step 4: Parse Geocoded Coordinates
# MAGIC Extract latitude and longitude from the model response and validate the results.

# COMMAND ----------

# DBTITLE 1,Parse pharmacy coordinates
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE pharmacies_parsed AS
# MAGIC SELECT
# MAGIC   pharmacy_id,
# MAGIC   pharmacy_name,
# MAGIC   address,
# MAGIC   lat_lon_raw,
# MAGIC   CASE
# MAGIC     WHEN lat_lon_raw RLIKE '^-?[0-9]+\\.?[0-9]*,\\s*-?[0-9]+\\.?[0-9]*$'
# MAGIC     THEN CAST(TRIM(SPLIT(lat_lon_raw, ',')[0]) AS DOUBLE)
# MAGIC     ELSE NULL
# MAGIC   END AS lat,
# MAGIC   CASE
# MAGIC     WHEN lat_lon_raw RLIKE '^-?[0-9]+\\.?[0-9]*,\\s*-?[0-9]+\\.?[0-9]*$'
# MAGIC     THEN CAST(TRIM(SPLIT(lat_lon_raw, ',')[1]) AS DOUBLE)
# MAGIC     ELSE NULL
# MAGIC   END AS lon
# MAGIC FROM pharmacies_geocoded;
# MAGIC
# MAGIC SELECT * FROM pharmacies_parsed;

# COMMAND ----------

# DBTITLE 1,Parse provider coordinates
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE providers_parsed AS
# MAGIC SELECT
# MAGIC   provider_id,
# MAGIC   provider_name,
# MAGIC   address,
# MAGIC   lat_lon_raw,
# MAGIC   CASE
# MAGIC     WHEN lat_lon_raw RLIKE '^-?[0-9]+\\.?[0-9]*,\\s*-?[0-9]+\\.?[0-9]*$'
# MAGIC     THEN CAST(TRIM(SPLIT(lat_lon_raw, ',')[0]) AS DOUBLE)
# MAGIC     ELSE NULL
# MAGIC   END AS lat,
# MAGIC   CASE
# MAGIC     WHEN lat_lon_raw RLIKE '^-?[0-9]+\\.?[0-9]*,\\s*-?[0-9]+\\.?[0-9]*$'
# MAGIC     THEN CAST(TRIM(SPLIT(lat_lon_raw, ',')[1]) AS DOUBLE)
# MAGIC     ELSE NULL
# MAGIC   END AS lon
# MAGIC FROM providers_geocoded;
# MAGIC
# MAGIC SELECT * FROM providers_parsed;

# COMMAND ----------

# DBTITLE 1,Validate geocoding results
# MAGIC %sql
# MAGIC SELECT 'Pharmacies' AS source,
# MAGIC        COUNT(*) AS total_records,
# MAGIC        COUNT_IF(lat IS NOT NULL AND lon IS NOT NULL) AS geocoded,
# MAGIC        COUNT_IF(lat IS NULL OR lon IS NULL) AS failed,
# MAGIC        ROUND(COUNT_IF(lat IS NOT NULL AND lon IS NOT NULL) * 100.0 / COUNT(*), 1) AS pct_geocoded
# MAGIC FROM pharmacies_parsed
# MAGIC UNION ALL
# MAGIC SELECT 'Providers',
# MAGIC        COUNT(*),
# MAGIC        COUNT_IF(lat IS NOT NULL AND lon IS NOT NULL),
# MAGIC        COUNT_IF(lat IS NULL OR lon IS NULL),
# MAGIC        ROUND(COUNT_IF(lat IS NOT NULL AND lon IS NOT NULL) * 100.0 / COUNT(*), 1)
# MAGIC FROM providers_parsed;

# COMMAND ----------

# DBTITLE 1,Step 5 Header
# MAGIC %md
# MAGIC ## Step 5: Provider Proximity Search
# MAGIC Find the 5 closest providers within a configurable search radius of a selected pharmacy. Uses `ST_Point` to create geometry objects and `ST_DistanceSpheroid` for accurate geodesic distance calculation on the WGS84 ellipsoid.

# COMMAND ----------

# DBTITLE 1,Proximity search with parameterized radius
# MAGIC %sql
# MAGIC DECLARE OR REPLACE provider_count INT = 5;
# MAGIC DECLARE OR REPLACE radius_km DOUBLE = 50;
# MAGIC DECLARE OR REPLACE pharmacy_name_filter = 'Boulder Family Rx';
# MAGIC
# MAGIC WITH ph AS (
# MAGIC   SELECT pharmacy_id,
# MAGIC          pharmacy_name,
# MAGIC          address AS pharmacy_address,
# MAGIC          lat AS pharm_lat,
# MAGIC          lon AS pharm_lon,
# MAGIC          ST_Point(lon, lat) AS pharm_geom
# MAGIC   FROM pharmacies_parsed
# MAGIC   WHERE lat IS NOT NULL AND lon IS NOT NULL
# MAGIC ),
# MAGIC pr AS (
# MAGIC   SELECT provider_id,
# MAGIC          provider_name,
# MAGIC          address AS provider_address,
# MAGIC          lat AS prov_lat,
# MAGIC          lon AS prov_lon,
# MAGIC          ST_Point(lon, lat) AS prov_geom
# MAGIC   FROM providers_parsed
# MAGIC   WHERE lat IS NOT NULL AND lon IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   ph.pharmacy_id,
# MAGIC   ph.pharmacy_name,
# MAGIC   ph.pharmacy_address,
# MAGIC   pr.provider_id,
# MAGIC   pr.provider_name,
# MAGIC   pr.provider_address,
# MAGIC   ph.pharm_lat,
# MAGIC   ph.pharm_lon,
# MAGIC   pr.prov_lat,
# MAGIC   pr.prov_lon,
# MAGIC   ROUND(ST_DistanceSpheroid(ph.pharm_geom, pr.prov_geom) / 1000.0, 2) AS distance_km
# MAGIC FROM ph
# MAGIC JOIN pr
# MAGIC   ON ST_DistanceSpheroid(ph.pharm_geom, pr.prov_geom) <= radius_km * 1000
# MAGIC WHERE
# MAGIC   ph.pharmacy_name = pharmacy_name_filter
# MAGIC ORDER BY distance_km
# MAGIC LIMIT provider_count