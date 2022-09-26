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

WITH CTE_crime_by_type AS (
  SELECT c.unique_key,
    c.case_number,
    c.date,
    EXTRACT(YEAR FROM c.date) AS year,
    c.geo_point,
    c.zip_code,
    c.zip_code_geom,
    CASE
      WHEN primary_type IN (
        'ASSAULT',
        'CRIM SEXUAL ASSAULT',
        'CRIMINAL SEXUAL ASSAULT',
        'HOMICIDE',
        'HUMAN TRAFFICKING',
        'KIDNAPPING',
        'OFFENSE INVOLVING CHILDREN',
        'PROSTITUTION',
        'RITUALISM',
        'SEX OFFENSE'
      ) THEN 'VIOLENT'
      WHEN primary_type IN (
        'ARSON',
        'BURGLARY',
        'CRIMINAL DAMAGE',
        'CRIMINAL TRESPASS',
        'MOTOR VEHICLE THEFT',
        'ROBBERY',
        'THEFT'
      ) THEN 'PROPERTY'
      ELSE 'OTHER'  END AS crime_type
  FROM `biz_store_research.crime` AS c
)
SELECT acs.geo_id AS zip_code,
  ANY_VALUE(acs.zip_code_geom) AS zip_code_geom,
  c.year,
  COUNT(*) AS crime_total_count,
  COUNT(*) / acs.total_pop AS crime_total_rate,
  COUNTIF(crime_type='VIOLENT') / acs.total_pop AS crime_violent_rate,
  COUNTIF(crime_type='PROPERTY') / acs.total_pop AS crime_property_rate,
  COUNTIF(crime_type='OTHER') / acs.total_pop AS crime_other_rate
FROM `biz_store_research.census` AS acs
LEFT OUTER JOIN CTE_crime_by_type AS c
  ON c.zip_code = acs.geo_id
  AND c.year = acs.year
WHERE c.year BETWEEN 2013 and 2018
GROUP BY
  zip_code,
  c.year,
  acs.total_pop