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

DECLARE v_distance FLOAT64 DEFAULT 3218.69;

-- add 2 miles around county for correct proccessing of known and unknown structures
CREATE TEMP TABLE TMP_counties_borders AS
SELECT
    s.state,
    c.geo_id AS county_geo_id,
    c.county_name,
    c.lsad_name AS county_lsad_name,
    ST_BUFFER(c.county_geom, v_distance, 2) AS county_geom_adjusted
FROM `bas_geo_us_boundaries.states` AS s
INNER JOIN `bas_geo_us_boundaries.counties` AS c
  ON c.state_fips_code = s.geo_id
WHERE s.state in ('AK', 'TX', 'NV', 'ND');
-- ('AK', 'TX', 'WA', 'NV', 'ND', 'VA');

-- Get a buffer geometry around all known structures in county
CREATE TEMP TABLE TMP_known_buffers
CLUSTER BY state, county_geo_id AS
SELECT
    cb.state,
    cb.county_geo_id,
    cb.county_name,
    cb.county_lsad_name,
    ST_BUFFER(ko.center, v_distance, 2) AS known_space
FROM `bas_geo.known_objects_bystate` AS ko
INNER JOIN TMP_counties_borders AS cb
  ON ST_CONTAINS(cb.county_geom_adjusted, ko.center);

-- Aggregate all buffers around known objects per county
SELECT
  kb.state,
  kb.county_geo_id,
  kb.county_name,
  kb.county_lsad_name,
  ST_UNION_AGG(kb.known_space) AS known_space
FROM TMP_known_buffers  AS kb
GROUP BY kb.state,  kb.county_geo_id,
  kb.county_name,   kb.county_lsad_name;