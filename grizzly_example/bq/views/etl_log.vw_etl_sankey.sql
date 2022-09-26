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

CREATE OR REPLACE VIEW etl_log.vw_etl_sankey AS
WITH
prjct AS (SELECT catalog_name AS projectid FROM INFORMATION_SCHEMA.SCHEMATA_OPTIONS LIMIT 1),
composer AS (
  SELECT
    concat('[',projectid, ":", LOWER(TRIM(source)) ,']') AS source,
    concat('[',projectid, ":", LOWER(TRIM(target)) ,']') AS target,
    subject_area
  FROM (
    SELECT
      source_table_fl AS source,
      a.target_table AS target,
      p.projectid,
      a.subject_area
    FROM `etl_log.composer_job_details` AS a
    CROSS JOIN UNNEST(a.source_table) AS source_table_fl
    CROSS JOIN prjct p
    WHERE
      #a.subject_area = @domain
      #AND
      UPPER(TRIM(a.job_status)) = 'SUCCESS'
      AND source_table_fl != ''
      AND source_table_fl IS NOT NULL
      #AND CAST(a.job_start_timestamp AS date) >= DATE_ADD(CURRENT_DATE(), INTERVAL -@days_range DAY)
    GROUP BY  1, 2, 3, 4 )
  WHERE concat('[',projectid, ":", LOWER(TRIM(source)) ,']') <>
    concat('[',projectid, ":", LOWER(TRIM(target)) ,']')
),
num_table AS(
  SELECT DISTINCT
      tbl,
      DENSE_RANK() OVER(ORDER BY tbl) -1 AS tabl_num
  FROM (
      SELECT source AS tbl FROM composer
      UNION ALL
      SELECT target AS tbl FROM composer
  )
  ORDER BY tabl_num
)
SELECT DISTINCT
  composer.subject_area AS subject_area,
  composer.source AS source,
  sr.tabl_num AS sr,
  composer.target AS target,
  tr.tabl_num AS tr,
  1 AS value,
  CONCAT('rgba(', FLOOR(RAND()*256), ', ', FLOOR(RAND()*256), ', ', FLOOR(RAND()*256), ', 0.8)') AS color,
  'rgba(200, 200, 200, 0.8)' AS link_color,
  CONCAT('path ',sr.tabl_num ,' -> ',  tr.tabl_num) AS lbl
FROM composer
LEFT JOIN num_table AS sr
  ON sr.tbl = composer.source
LEFT JOIN num_table AS tr
  ON tr.tbl = composer.target
