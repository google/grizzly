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

WITH CTE_years AS (
  SELECT year
  FROM UNNEST([2013, 2014, 2015, 2016, 2017, 2018]) AS year
)
SELECT
  zc.zip_code,
  y.year,
  m.supermarket_count,
  pd.households_per_sq_mile,
  cd.commuters_by_public_transportation_rate,
  crime.crime_violent_rate,
  crime.crime_property_rate,
  h.num_of_hazards,
  ab.num_of_abandoned,
  r.riot_per_year,
  zc.zip_code_geom
FROM `biz_store_research.chicago_zip_codes` AS zc
CROSS JOIN CTE_years AS y
LEFT OUTER JOIN `biz_store_research.existing_supermarkets` AS m
  ON m.zip_code = zc.zip_code
  AND m.year = y.year
LEFT OUTER JOIN `biz_store_research.population_density` AS pd
  ON pd.zip_code = zc.zip_code
  AND pd.year = y.year
LEFT OUTER JOIN `biz_store_research.commuter_density` AS cd
  ON cd.zip_code = zc.zip_code
  AND cd.year = y.year
LEFT OUTER JOIN `biz_store_research.crime_rate` AS crime
  ON crime.zip_code = zc.zip_code
  AND crime.year = y.year
LEFT OUTER JOIN `biz_store_research.hazards` AS h
  ON h.zip_code = zc.zip_code
  AND h.year = y.year
LEFT OUTER JOIN `biz_store_research.abandoned_buildings` AS ab
  ON ab.zip_code = zc.zip_code
  AND ab.year = y.year
LEFT OUTER JOIN `biz_store_research.riots` AS r
  ON r.zip_code = zc.zip_code
  AND r.year = y.year
