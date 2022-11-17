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
  FORMAT('%04d', m.Postcode) AS postcode,
  m.Count_taxable_income_or_loss AS count_taxable_income_or_loss,
  m.Average_total_income_or_loss AS average_total_income_or_loss,
  m.Median_total_income_or_loss AS median_total_income_or_loss
FROM `bas_geo_australia.tax_income_average_median_by_postcode` AS m