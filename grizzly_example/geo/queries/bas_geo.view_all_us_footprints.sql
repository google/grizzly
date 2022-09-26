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

WITH CTE_geo AS (
  SELECT
    ST_GEOGFROMTEXT(b.footprint) AS footprint
  FROM `bas_gis.building_footprint_usa` AS b
)
SELECT g.footprint,
    ST_CENTROID(g.footprint) AS center,
    ST_AREA(g.footprint) AS area_in_meters
FROM CTE_geo AS g
