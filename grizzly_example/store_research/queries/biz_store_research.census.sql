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
  CAST(_TABLE_SUFFIX AS INT) AS year,
  acs.geo_id,
  acs.households,
  acs.commuters_by_public_transportation,
  acs.total_pop,
  zc.zip_code_geom
FROM `bas_census_bureau_acs.zip_codes_*` AS acs
INNER JOIN `biz_store_research.chicago_zip_codes` AS zc
ON zc.zip_code = acs.geo_id
WHERE _TABLE_SUFFIX BETWEEN '2013' AND '2018'