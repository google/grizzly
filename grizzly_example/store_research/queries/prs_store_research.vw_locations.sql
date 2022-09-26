-- Copyright 2022 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

SELECT
  zip_code,
  year,
  supermarket_count,
  num_of_hazards,
  num_of_abandoned,
  riot_per_year,
  households_per_sq_mile,
  commuters_by_public_transportation_rate,
  crime_violent_rate,
  crime_property_rate,
  scout_score,
  NULL AS sys_job_id,
  # BIGQUERY and Superset has different JSON format for work with a geo data.
  # Bellow is the removal extra breckets and begin of string.
  # SUBSTRING 39 for Polygon, 46 for MultiPolygon
  IF(SUBSTR(ST_ASGEOJSON(zip_code_geom), 0, 37) = '{ "type": "Polygon", "coordinates": [',
    REPLACE(REPLACE(REPLACE(SUBSTR(ST_ASGEOJSON(zip_code_geom), 39),' ] ] }',']'),'[ ','['),'] ], [[','], ['),
    REPLACE(REPLACE(REPLACE(REPLACE(SUBSTR(ST_ASGEOJSON(zip_code_geom), 46),' ] ] ] }',']'),'[ ','['),'] ] ], [[[','], ['),'] ], [[','], [')
  ) AS geometry
FROM `prs_store_research.locations`

UNION ALL

SELECT
  zip_code,
  year,
  NULL AS supermarket_count,
  NULL AS num_of_hazards,
  NULL AS num_of_abandoned,
  NULL AS riot_per_year,
  NULL AS households_per_sq_mile,
  NULL AS commuters_by_public_transportation_rate,
  NULL AS crime_violent_rate,
  NULL AS crime_property_rate,
  NULL AS scout_score,
  NULL AS sys_job_id,
  REPLACE(geometry,'] ], [[','], [') AS geometry
FROM
  `biz_store_research.zip_around_chicago`
