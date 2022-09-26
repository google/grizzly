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

WITH
riot_per_year AS (
    SELECT
        Year AS year,
        zip_code,
        COUNT(zip_code) AS riot_per_year
    FROM `biz_store_research.events`
    WHERE
        Year < 2019
        AND EventBaseCode IN ('144', '145')
    GROUP BY
        Year,
        zip_code
)
SELECT
riot_per_year.*,
zc.zip_code_geom
FROM
riot_per_year
LEFT JOIN
    `biz_store_research.chicago_zip_codes` AS zc
USING(zip_code)
