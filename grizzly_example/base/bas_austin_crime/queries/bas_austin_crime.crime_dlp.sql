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

select
unique_key,
address,
census_tract,
clearance_date,
clearance_status,
council_district_code,
description,
district,
latitude,
longitude,
location,
location_description,
primary_type,
timestamp,
x_coordinate,
y_coordinate,
year,
zipcode
from `bigquery-public-data.austin_crime.crime`