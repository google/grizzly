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
  t.zip_code,
  t.city,
  t.county,
  t.state_fips_code,
  t.state_code,
  t.state_name,
  t.fips_class_code,
  t.mtfcc_feature_class_code,
  t.functional_status,
  t.area_land_meters,
  t.area_water_meters,
  t.internal_point_lat,
  t.internal_point_lon,
  t.internal_point_geom,
  t.zip_code_geom
FROM `bigquery-public-data.geo_us_boundaries.zip_codes` AS t
