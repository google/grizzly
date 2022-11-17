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
  IF (LENGTH(ap.Postcode)<4, LPAD(ap.Postcode, 4, '0'), ap.Postcode) AS postcode,
  ap.State__Territory1 AS state_erritory,
  ap.Total AS total_population,
  ap.age_65_over AS over_65_population,
  ap.age_65_over / ap.Total AS over_65_population_rate
FROM `bas_geo_australia.tax_individual_age_by_postcode` AS ap