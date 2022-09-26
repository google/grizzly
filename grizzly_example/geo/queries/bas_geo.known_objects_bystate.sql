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

SELECT st.state,
  f.geometry,
  ST_CENTROID(f.geometry) AS center,
  f.all_tags
FROM `bas_geo_openstreetmap.planet_features_points` AS f
INNER JOIN `bas_geo_us_boundaries.states` AS st
  ON ST_CONTAINS(st.state_geom , f.geometry)
UNION ALL
SELECT st.state,
  n.geometry,
  ST_CENTROID(n.geometry) AS center,
  n.all_tags
FROM `bas_geo_openstreetmap.planet_nodes` AS n
INNER JOIN `bas_geo_us_boundaries.states` AS st
  ON ST_CONTAINS(st.state_geom , n.geometry)
WHERE n.visible = true
UNION ALL
SELECT st.state,
  fp.geometry,
  ST_CENTROID(fp.geometry) AS center,
  fp.all_tags
FROM `bas_geo_openstreetmap.planet_features_multipolygons` AS fp
INNER JOIN `bas_geo_us_boundaries.states` AS st
  ON ST_CONTAINS(st.state_geom , fp.geometry)
CROSS JOIN UNNEST(fp.all_tags) AS tags
WHERE (
  tags.key LIKE '%building%'
  OR tags.key LIKE '%landuse%'
  OR tags.key LIKE '%industrial%'
  OR tags.key LIKE '%factory%'
  OR tags.key LIKE '%man_made%'
  OR tags.key LIKE '%military%'
)
