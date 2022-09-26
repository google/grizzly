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
    c.year AS year,
    c.total_pop,
    c.commuters_by_public_transportation,
    c.commuters_by_public_transportation / c.total_pop AS commuters_by_public_transportation_rate,
    z.zip_code,
    z.zip_code_geom
FROM
    `biz_store_research.census` c
JOIN
    `biz_store_research.chicago_zip_codes` z
  ON c.geo_id = z.zip_code
-- WHERE commuters_by_public_transportation/total_pop > 0.25