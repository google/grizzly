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
  st.state,
  c.geo_id AS county_geo_id,
  c.county_name,
  c.lsad_name AS county_lsad_name,
  b.footprint,
  b.center,
  b.area_in_meters
FROM `bas_geo.view_all_us_footprints` AS b
INNER JOIN `bas_geo_us_boundaries.states` AS st
  ON ST_CONTAINS(st.state_geom , b.center)
INNER JOIN `bas_geo_us_boundaries.counties` AS c
  ON ST_CONTAINS(c.county_geom, b.center)
