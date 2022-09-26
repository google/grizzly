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

CREATE OR REPLACE table `bas_austin_crime.crime_dlp`
(
  unique_key STRING,
  address STRING,
  census_tract STRING,
  clearance_date STRING,
  clearance_status STRING,
  council_district_code STRING,
  description STRING,
  district STRING,
  latitude STRING,
  longitude STRING,
  location STRING,
  location_description STRING,
  primary_type STRING,
  timestamp STRING,
  x_coordinate STRING,
  y_coordinate STRING,
  year STRING,
  zipcode STRING
)
