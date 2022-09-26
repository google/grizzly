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
    cc.unique_key,
    cc.case_number,
    cc.date,
    cc.block,
    cc.iucr,
    cc.primary_type,
    cc.description,
    cc.location_description,
    cc.arrest,
    cc.domestic,
    cc.beat,
    cc.district,
    cc.ward,
    cc.community_area,
    cc.fbi_code,
    cc.x_coordinate,
    cc.y_coordinate,
    cc.year,
    cc.updated_on,
    cc.latitude,
    cc.longitude,
    cc.location
FROM `bigquery-public-data.chicago_crime.crime`  AS cc
