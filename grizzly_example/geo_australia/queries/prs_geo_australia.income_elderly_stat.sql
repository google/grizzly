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
  pc.POA_CODE21 AS postcode,
  e.state_territory,
  e.total_population,
  e.over_65_population,
  e.over_65_population_rate,
  inc.median_total_income_or_loss,
  inc.median_total_income_or_loss * e.over_65_population_rate AS income_metric,
  pc.AREASQKM21 AS area_sqkm,
  ST_GEOGFROMTEXT(pc.geometry) AS geometry
FROM `bas_geo_australia.post_codes` AS pc
LEFT OUTER JOIN `biz_geo_australia.tax_elderly_by_postcode` AS e
  ON pc.POA_CODE21 = e.postcode
LEFT OUTER JOIN `biz_geo_australia.tax_income_average_median_by_postcode` AS inc
  ON pc.POA_CODE21 = inc.postcode
WHERE pc.geometry IS NOT NULL