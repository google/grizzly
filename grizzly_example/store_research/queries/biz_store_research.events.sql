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
    event.*,
    ST_GEOGPOINT(event.Actor1Geo_Long, event.Actor1Geo_Lat) AS geo_point,
    zc.zip_code,
    zc.zip_code_geom
FROM `bas_gdelt.events` AS event
INNER JOIN
    `biz_store_research.chicago_zip_codes` AS zc
ON ST_CONTAINS(zc.zip_code_geom, ST_GEOGPOINT(event.Actor1Geo_Long, event.Actor1Geo_Lat))
WHERE
    event.Year > 2012
    AND event.EventRootCode = '14'
