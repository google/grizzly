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
  zip_code,
  year,
  supermarket_count,
  num_of_hazards,
  num_of_abandoned,
  riot_per_year,
  zip_code_geom,
  households_per_sq_mile,
  commuters_by_public_transportation_rate,
  crime_violent_rate,
  crime_property_rate,
  scout_score
FROM `prs_store_research.locations`
