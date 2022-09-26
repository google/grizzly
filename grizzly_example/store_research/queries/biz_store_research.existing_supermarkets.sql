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
  FROM UNNEST(GENERATE_ARRAY(2000, 2018)) AS year
), CTE_zip_code_by_year AS (
  SELECT zs.zip_code,
    y.year
  FROM `biz_store_research.chicago_zip_codes` AS zs
  CROSS JOIN CTE_years AS y
), CTE_store_by_year AS (
  SELECT t.zip_code,
    EXTRACT(YEAR FROM osm_timestamp) AS year,
    COUNT(1) as cnt
  FROM `biz_store_research.street_history_notes` AS t
  CROSS JOIN UNNEST(t.all_tags) AS tags
  WHERE tags.value = 'supermarket'
    AND tags.key = 'shop'
  GROUP BY zip_code, year
), CTE_store_by_year_comulative AS (
  SELECT
    z.zip_code,
    IFNULL(z.year, s.year) AS year,
    SUM(cnt)OVER(
        PARTITION BY z.zip_code
        ORDER BY IFNULL(z.year, s.year)
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS supermarket_count
  FROM CTE_zip_code_by_year AS z
  FULL OUTER JOIN CTE_store_by_year AS s
    ON s.zip_code = z.zip_code
    AND s.year = z.year
)
SELECT
  s.zip_code,
  s.year,
  IFNULL(supermarket_count, 0) AS supermarket_count
FROM CTE_store_by_year_comulative AS s
WHERE s.year BETWEEN 2013 AND 2018
