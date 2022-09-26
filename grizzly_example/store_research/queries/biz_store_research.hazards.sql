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

WITH hazard_per_year AS (
    SELECT
        zip_code,
        EXTRACT(YEAR FROM osm_timestamp) AS year,
        COUNT(zip_code)AS num_of_hazards
    FROM
        `biz_store_research.street_history_notes`,
        UNNEST(all_tags)
    WHERE
        key = 'hazard'
        AND value NOT IN ('school_zone', 'children')
        AND EXTRACT(YEAR FROM osm_timestamp) > 2012
        AND EXTRACT(YEAR FROM osm_timestamp) < 2019
    GROUP BY
        1,2
)
SELECT
    hazard_per_year.*,
    zc.zip_code_geom
FROM
    hazard_per_year
LEFT JOIN
    `biz_store_research.chicago_zip_codes` AS zc
USING(zip_code)