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
  b.state,
  NOT(ST_WITHIN(b.center, k.known_space)) AS is_untagged_structure,
  b.county_geo_id,
  b.county_name,
  b.county_lsad_name,
  b.footprint,
  b.center,
  b.area_in_meters
FROM `bas_geo.building_footprint_bystate` AS b
INNER JOIN `biz_geo.known_area` AS k
  ON k.state = b.state
  AND k.county_geo_id = b.county_geo_id
-- WHERE b.state in ('AK', 'TX', 'WA', 'NV', 'ND', 'VA');
WHERE b.state in ('AK', 'TX', 'NV', 'ND');
