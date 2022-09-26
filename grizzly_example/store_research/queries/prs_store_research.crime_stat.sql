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
  unique_key,
  case_number,
  date,
  block,
  iucr,
  primary_type,
  description,
  location_description,
  arrest,
  domestic,
  beat,
  district,
  ward,
  community_area,
  fbi_code,
  x_coordinate,
  y_coordinate,
  year,
  updated_on,
  latitude,
  longitude,
  location,
  geo_point,
  zip_code
FROM `biz_store_research.crime`